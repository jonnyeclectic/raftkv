"""The Raft node. Rule methods are SYNCHRONOUS — under asyncio's single event loop
they run start-to-finish with no interleaving, which is this design's answer to the
lock-ordering bugs the Students' Guide catalogues. Async methods re-validate state
after every await."""

import asyncio
import contextlib
import logging
import random
import time

from raftkv.config import NodeConfig
from raftkv.logging_setup import log_event
from raftkv.models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    Command,
    LogEntry,
    Metrics,
    NodeState,
    RequestVoteRequest,
    RequestVoteResponse,
    Role,
)
from raftkv.storage import Storage
from raftkv.transport import Transport, TransportError

logger = logging.getLogger("raftkv.raft")


class NotLeaderError(Exception):
    def __init__(self, leader_id: str | None) -> None:
        super().__init__(f"not the leader; try {leader_id or 'unknown'}")
        self.leader_id = leader_id


class RaftNode:
    def __init__(self, cfg: NodeConfig, storage: Storage, transport: Transport) -> None:
        self.cfg = cfg
        self.storage = storage
        self.transport = transport
        term, voted_for, last_applied = storage.load()
        self.current_term = term
        self.voted_for = voted_for
        self.role: Role = Role.FOLLOWER
        self.leader_id: str | None = None
        # everything applied was committed, so this is a safe floor after restart
        self.commit_index = last_applied
        self.last_applied = last_applied
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self.crashed = False                    # simulated failure; see crash()
        self.blocked: set[str] = set()          # simulated partition; see set_blocked()
        self.metrics = Metrics()
        self._last_reset = time.monotonic()
        self._election_timeout = random.uniform(*cfg.election_timeout_range)
        self._replicate_now = asyncio.Event()   # submit() nudges the replication loop
        self._apply_ready = asyncio.Event()     # commit-index advances wake the applier
        self._waiters: dict[int, asyncio.Future[LogEntry]] = {}  # submits awaiting apply
        self._tasks: list[asyncio.Task] = []

    @property
    def quorum(self) -> int:
        return (len(self.cfg.peers) + 1) // 2 + 1

    # ---- helpers -----------------------------------------------------------
    def _log(self, event: str, **ctx: object) -> None:
        log_event(logger, event, node=self.cfg.node_id, term=self.current_term,
                  role=self.role, **ctx)

    def _reset_election_timer(self) -> None:
        """Reset ONLY on: AppendEntries from the current leader, granting a vote,
        or starting an election (Students' Guide bug #1)."""
        self._last_reset = time.monotonic()
        self._election_timeout = random.uniform(*self.cfg.election_timeout_range)

    def _become_follower(self) -> None:
        if self.role is Role.LEADER:
            # LogCabin-style stepDown: a leader's election timer has been stale for
            # its whole tenure; without this reset a deposed leader re-elects itself
            # within one tick and churns the cluster.
            self._reset_election_timer()
        if self.role is not Role.FOLLOWER:
            self._log("became_follower")
        self.role = Role.FOLLOWER

    def _observe_term(self, term: int) -> None:
        """Rule 4 (§5.1): a higher term in ANY request or response deposes us.
        Runs BEFORE the handler's own logic (Students' Guide)."""
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.storage.save_term_and_vote(term, None)  # rule 1: persist first
            self.leader_id = None
            self._become_follower()

    def _last_log_position(self) -> tuple[int, int]:
        """(last_log_term, last_log_index) — the §5.4.1 comparison key."""
        idx = self.storage.last_log_index()
        term = self.storage.term_at(idx)
        assert term is not None  # index 0 sentinel guarantees existence
        return (term, idx)

    # ---- RPC receivers (synchronous => atomic under the event loop) --------
    def handle_request_vote(self, req: RequestVoteRequest) -> RequestVoteResponse:
        self._observe_term(req.term)
        if req.term < self.current_term:  # stale candidate
            return RequestVoteResponse(term=self.current_term, vote_granted=False)
        up_to_date = (req.last_log_term, req.last_log_index) >= self._last_log_position()
        may_vote = self.voted_for in (None, req.candidate_id)  # rule 6: one vote/term
        if not (up_to_date and may_vote):  # rule 2: §5.4.1 election restriction
            return RequestVoteResponse(term=self.current_term, vote_granted=False)
        self.voted_for = req.candidate_id
        self.storage.save_term_and_vote(self.current_term, req.candidate_id)  # rule 1
        self._reset_election_timer()  # granting a vote: legal reset
        self.metrics.votes_granted += 1
        self._log("vote_granted", candidate=req.candidate_id)
        return RequestVoteResponse(term=self.current_term, vote_granted=True)

    def handle_append_entries(self, req: AppendEntriesRequest) -> AppendEntriesResponse:
        self._observe_term(req.term)
        self.metrics.append_entries_received += 1
        if req.term < self.current_term:  # stale leader
            return AppendEntriesResponse(term=self.current_term, success=False)
        # Equal term: this IS the current leader (election safety gives uniqueness).
        self.leader_id = req.leader_id
        if self.role is Role.CANDIDATE:
            self._become_follower()
        self._reset_election_timer()  # AppendEntries from current leader: legal reset
        # Rule 5 (§5.3): consistency check — heartbeats run it too.
        if self.storage.term_at(req.prev_log_index) != req.prev_log_term:
            return AppendEntriesResponse(term=self.current_term, success=False)
        # Conflict-only truncation (§5.3): never chop entries that already match.
        for offset, incoming in enumerate(req.entries):
            idx = req.prev_log_index + 1 + offset
            existing_term = self.storage.term_at(idx)
            if existing_term is None:  # clean append from here
                self.storage.append(req.entries[offset:])
                self._log("log_appended", from_index=idx, count=len(req.entries) - offset)
                break
            if existing_term != incoming.term:  # conflict: truncate then append
                self.storage.truncate_from(idx)
                self.storage.append(req.entries[offset:])
                self._log("log_truncated", from_index=idx)
                break
        # Fig. 2 step 5: only trust leaderCommit as far as entries we just verified.
        if req.leader_commit > self.commit_index:
            last_new_entry = req.prev_log_index + len(req.entries)
            self.commit_index = min(req.leader_commit, last_new_entry)
            self._apply_ready.set()
        return AppendEntriesResponse(term=self.current_term, success=True)

    # ---- candidacy (async: every await yields — re-validate state after) ---
    async def _start_election(self) -> None:
        self.role = Role.CANDIDATE
        self.current_term += 1
        self.voted_for = self.cfg.node_id
        self.storage.save_term_and_vote(self.current_term, self.voted_for)  # rule 1
        self.leader_id = None
        self._reset_election_timer()  # starting an election: legal reset
        self.metrics.elections_started += 1
        term_when_started = self.current_term
        last_term, last_index = self._last_log_position()
        req = RequestVoteRequest(
            term=term_when_started, candidate_id=self.cfg.node_id,
            last_log_index=last_index, last_log_term=last_term,
        )
        self._log("election_started")

        async def ask(peer_id: str) -> RequestVoteResponse | None:
            if peer_id in self.blocked:  # partitioned link: same shape as unreachable
                self.metrics.rpc_failures += 1
                return None
            try:
                return await self.transport.request_vote(peer_id, req)
            except TransportError:
                self.metrics.rpc_failures += 1
                return None

        votes = 1  # our own
        if votes >= self.quorum:  # single-node cluster elects itself instantly
            self._become_leader()
            return
        pending = [asyncio.create_task(ask(p)) for p in sorted(self.cfg.peers)]
        try:
            for future in asyncio.as_completed(pending):
                resp = await future
                if resp is None:
                    continue
                self._observe_term(resp.term)
                # Stale-reply guard: if the world moved while we awaited, stop.
                if self.role is not Role.CANDIDATE or self.current_term != term_when_started:
                    return
                if resp.vote_granted:
                    votes += 1
                    if votes >= self.quorum:
                        self._become_leader()
                        return
        finally:
            for task in pending:
                task.cancel()

    def _become_leader(self) -> None:
        self.role = Role.LEADER
        self.leader_id = self.cfg.node_id
        last = self.storage.last_log_index()
        self.next_index = {p: last + 1 for p in self.cfg.peers}
        self.match_index = {p: 0 for p in self.cfg.peers}
        self._log("became_leader", quorum=self.quorum)
        self._replicate_now.set()  # assert leadership with an immediate heartbeat

    # ---- leader replication ------------------------------------------------
    async def _append_to_peer(self, peer_id: str, term_when_sent: int) -> None:
        if peer_id in self.blocked:  # partitioned link: same shape as unreachable
            self.metrics.rpc_failures += 1
            return
        prev_index = self.next_index[peer_id] - 1
        prev_term = self.storage.term_at(prev_index)
        if prev_term is None:  # our log shrank under this nextIndex (deposed+truncated)
            self.next_index[peer_id] = self.storage.last_log_index() + 1
            return
        entries = self.storage.entries_from(self.next_index[peer_id])
        req = AppendEntriesRequest(
            term=term_when_sent, leader_id=self.cfg.node_id,
            prev_log_index=prev_index, prev_log_term=prev_term,
            entries=entries, leader_commit=self.commit_index,
        )
        self.metrics.append_entries_sent += 1  # counts sends, not acks
        try:
            resp = await self.transport.append_entries(peer_id, req)
        except TransportError:
            self.metrics.rpc_failures += 1  # peer down is weather, not an error
            return
        self._observe_term(resp.term)
        # Stale-reply guard (Students' Guide): act only if nothing changed meanwhile.
        if self.role is not Role.LEADER or self.current_term != term_when_sent:
            return
        if resp.success:
            # matchIndex from the ARGUMENTS WE SENT — never from current state and
            # never nextIndex - 1 (Students' Guide).
            self.match_index[peer_id] = prev_index + len(entries)
            self.next_index[peer_id] = self.match_index[peer_id] + 1
            self._advance_commit_index()
        else:
            self.next_index[peer_id] = max(1, self.next_index[peer_id] - 1)
            # naive backoff — one step per rejection; swap in
            # conflictIndex/conflictTerm accelerated backtracking if logs get long.

    def _advance_commit_index(self) -> None:
        """Rule 3 (§5.4.2 / Figure 8): majorities count only for CURRENT-term
        entries; older entries commit transitively when a newer one commits."""
        for candidate_index in range(self.storage.last_log_index(), self.commit_index, -1):
            if self.storage.term_at(candidate_index) != self.current_term:
                break  # terms only shrink going backwards; nothing below qualifies
            acked = 1 + sum(1 for m in self.match_index.values() if m >= candidate_index)
            if acked >= self.quorum:
                self.commit_index = candidate_index
                self._log("commit_advanced", commit_index=candidate_index)
                self._apply_ready.set()
                break

    # ---- client path -------------------------------------------------------
    async def submit(self, cmd: Command) -> None:
        """Append, replicate, and wait until APPLIED on this node.
        Raises NotLeaderError (retry at the leader) or TimeoutError."""
        if self.role is not Role.LEADER:
            raise NotLeaderError(self.leader_id)
        self.storage.append([LogEntry(term=self.current_term, command=cmd)])
        index = self.storage.last_log_index()
        self._log("submitted", key=cmd.key, op=cmd.op, index=index, request_id=cmd.request_id)
        waiter: asyncio.Future[LogEntry] = asyncio.get_running_loop().create_future()
        self._waiters[index] = waiter
        self._advance_commit_index()  # a single-node cluster commits immediately
        self._replicate_now.set()
        try:
            applied = await asyncio.wait_for(waiter, timeout=self.cfg.commit_timeout)
        finally:
            self._waiters.pop(index, None)
        # Students' Guide "re-appearing index": another leader may have filled this
        # index with a different entry. Trust the request_id, not the index.
        if applied.command.request_id != cmd.request_id:
            raise NotLeaderError(self.leader_id)

    # ---- background tasks --------------------------------------------------
    async def _election_timer_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.tick_interval)
            if self.role is Role.LEADER:
                continue
            if time.monotonic() - self._last_reset >= self._election_timeout:
                await self._start_election()  # also re-fires for a stuck candidate

    async def _replication_loop(self) -> None:
        while True:
            if self.role is not Role.LEADER:
                await asyncio.sleep(self.cfg.tick_interval)
                continue
            round_started = time.monotonic()
            term_when_sent = self.current_term
            await asyncio.gather(
                *(self._append_to_peer(p, term_when_sent) for p in sorted(self.cfg.peers))
            )
            # Heartbeat cadence minus time already spent this round — a slow peer must
            # not stretch the gap toward election_timeout.
            remaining = max(
                0.0, self.cfg.heartbeat_interval - (time.monotonic() - round_started)
            )
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._replicate_now.wait(), timeout=remaining)
            self._replicate_now.clear()

    async def _apply_loop(self) -> None:
        """THE single applier (Students' Guide): one task applies, strictly in order."""
        while True:
            await self._apply_ready.wait()
            self._apply_ready.clear()
            while self.last_applied < self.commit_index:
                index = self.last_applied + 1
                applied_entry = self.storage.entry(index)
                assert applied_entry is not None  # committed implies present
                self.storage.apply(index, applied_entry.command)  # atomic w/ last_applied
                self._log("applied", index=index, key=applied_entry.command.key)
                self.last_applied = index
                waiter = self._waiters.get(index)
                if waiter is not None and not waiter.done():
                    waiter.set_result(applied_entry)

    def start(self) -> None:
        self._tasks = [
            asyncio.create_task(coro(), name=f"{self.cfg.node_id}:{coro.__name__}")
            for coro in (self._election_timer_loop, self._replication_loop, self._apply_loop)
        ]
        for task in self._tasks:
            # a silently-dead loop degrades the node with zero signal — make it loud
            task.add_done_callback(self._on_task_death)

    def _on_task_death(self, task: asyncio.Task) -> None:
        if task.cancelled() or task.exception() is None:
            return
        logger.error(
            "background task died",
            exc_info=task.exception(),
            extra={"ctx": {"event": "task_died", "node": self.cfg.node_id,
                           "task": task.get_name()}},
        )

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    # ---- simulated failure (demo affordance, see NodeConfig.admin_enabled) --
    def set_blocked(self, peers: list[str]) -> None:
        """Cut this node's links to the named peers, in BOTH directions: outbound
        RPCs are skipped here, inbound ones are refused by the app layer. Replace
        semantics, so healing is set_blocked([]).

        A partition is not a crash, and the difference is the whole point: a
        partitioned leader stays up, keeps accepting writes, and cannot commit any
        of them, because commit needs a majority it can no longer reach. Blocking
        counts as an rpc failure rather than an error, which is what an unreachable
        peer already looks like from in here."""
        unknown = [p for p in peers if p not in self.cfg.peers]
        if unknown:
            raise ValueError(f"not peers of {self.cfg.node_id}: {', '.join(sorted(unknown))}")
        self.blocked = set(peers)
        self._log("partition_set", blocked=sorted(self.blocked))

    async def crash(self) -> None:
        """Play dead without dying: stop every background task and let the app refuse
        RPCs, so peers observe a node that has gone away. The process survives only so
        that /admin/recover stays reachable — nothing about the Raft state machine is
        special-cased for this.

        Volatile state is dropped exactly as a real crash drops it. Durable state
        (term, vote, log, applied KV) stays on disk untouched, which is the point:
        recover() proves persistence rather than asserting it. Metrics are kept
        deliberately — they are observability, not Raft state, and the election history
        is the interesting part of the story afterwards."""
        if self.crashed:
            return
        self.crashed = True
        await self.stop()
        self.role = Role.FOLLOWER
        self.leader_id = None
        self.next_index = {}
        self.match_index = {}
        # in-flight writes die with the node; TimeoutError is already the 504 path
        for waiter in self._waiters.values():
            if not waiter.done():
                waiter.set_exception(TimeoutError("node crashed before commit"))
        self._waiters = {}
        self._log("crashed")

    def recover(self) -> None:
        """Come back the way a restarted process would: volatile state from zero,
        durable state re-read from SQLite. The node rejoins as a follower and the
        leader repairs its log through the normal nextIndex walk."""
        if not self.crashed:
            return
        term, voted_for, last_applied = self.storage.load()
        self.current_term = term
        self.voted_for = voted_for
        self.role = Role.FOLLOWER
        self.leader_id = None
        self.commit_index = last_applied
        self.last_applied = last_applied
        self._replicate_now = asyncio.Event()
        self._apply_ready = asyncio.Event()
        self._reset_election_timer()
        self.crashed = False
        self.start()
        self._log("recovered")

    # ---- introspection -----------------------------------------------------
    def state(self) -> NodeState:
        last_term, last_index = self._last_log_position()
        return NodeState(
            node_id=self.cfg.node_id, role=self.role, term=self.current_term,
            voted_for=self.voted_for, leader_id=self.leader_id,
            log_length=last_index, last_log_term=last_term,
            commit_index=self.commit_index, last_applied=self.last_applied,
            kv=self.storage.kv_all(), peers=dict(self.cfg.peers),
            blocked=sorted(self.blocked), metrics=self.metrics,
        )
