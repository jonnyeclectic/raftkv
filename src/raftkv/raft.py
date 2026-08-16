"""The Raft node. Rule methods are SYNCHRONOUS — under asyncio's single event loop
they run start-to-finish with no interleaving, which is this design's answer to the
lock-ordering bugs the Students' Guide catalogues. Async methods re-validate state
after every await."""

import asyncio
import logging
import random
import time

from raftkv.config import NodeConfig
from raftkv.logging_setup import log_event
from raftkv.models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    Metrics,
    NodeState,
    RequestVoteRequest,
    RequestVoteResponse,
    Role,
)
from raftkv.storage import Storage
from raftkv.transport import Transport

logger = logging.getLogger("raftkv.raft")


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
        self.metrics = Metrics()
        self._last_reset = time.monotonic()
        self._election_timeout = random.uniform(*cfg.election_timeout_range)
        self._apply_ready = asyncio.Event()     # commit-index advances wake the applier

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

    # ---- introspection -----------------------------------------------------
    def state(self) -> NodeState:
        last_term, last_index = self._last_log_position()
        return NodeState(
            node_id=self.cfg.node_id, role=self.role, term=self.current_term,
            voted_for=self.voted_for, leader_id=self.leader_id,
            log_length=last_index, last_log_term=last_term,
            commit_index=self.commit_index, last_applied=self.last_applied,
            kv=self.storage.kv_all(), peers=dict(self.cfg.peers),
            metrics=self.metrics,
        )
