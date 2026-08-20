"""The five Raft safety properties, checked against a whole live cluster instead of one
scenario's expected outcome.

The names come from the paper — Figure 3, restated in §5.4.3 — and NOT from Ongaro's
`raft.tla`, which is a common thing to assume and wrong: that spec ends at
`Spec == Init /\\ [][Next]_vars` and declares no invariants, no theorems and no `.cfg` at
all. The nearest thing to a canonical machine-checkable list is etcd's own
`etcd-io/raft/tla`, which adds `ElectionSafetyInv`, `LogMatchingInv`,
`LeaderCompletenessInv`, `MoreThanOneLeaderInv` and `CommittedIsDurableInv` for exactly
this purpose.

Every existing simulation test asserts a *specific* consequence: this partitioned leader
does not commit, that restarted node keeps its vote. Each is a sample of one path. The
properties below are what must hold on EVERY path, at every instant, whatever the schedule
was — so they can be pointed at a randomized run whose outcome nobody predicted in
advance, which is precisely the run a hand-written assertion cannot be written for.

Two of the five are not functions of a single snapshot, and that is the interesting part:

  - **ElectionSafety** compares leaders across *time*. Two nodes can each be leader of
    term 7 without ever being leader simultaneously in any one observation — the second
    elected after the first was deposed but before it noticed. Only a monitor that
    remembers term 7's first leader catches that.
  - **LeaderAppendOnly** is a statement about a log's history, not its contents. A log that
    truncated an entry and re-appended an identical one looks untouched afterwards.

So this is a monitor, not an assertion helper: construct it once, `observe()` it
repeatedly through a run, and it accumulates the history the two temporal properties need.
`observe()` raises on the first violation, with the state that broke it.

§5.4.3 of the paper collects all five as the properties the algorithm guarantees, and is
the reference for their statements.
"""

import sqlite3

from raftkv.models import Role


class InvariantViolation(AssertionError):
    """A Raft safety property was violated. Never a flake — safety properties hold on
    every schedule, so a failure here is a bug in the implementation or in a rule this
    module states wrongly. Both are worth stopping for."""


def _readable(node) -> bool:
    """A crashed node's Storage is closed (`SimCluster.crash`), and a closed sqlite
    connection raises rather than returning empty. Skipping it is correct rather than
    lenient: an unreachable node makes no claims, so it can violate nothing."""
    try:
        node.storage.last_log_index()
        return True
    except (sqlite3.ProgrammingError, sqlite3.OperationalError):
        return False


def _log(node) -> list[tuple[int, object]]:
    """[(term, command)] by index, 1-indexed and read as a whole. Small in tests; the
    monitor is for correctness runs, not for the throughput path."""
    last = node.storage.last_log_index()
    return [(e.term, e.command) for e in node.storage.entries_from(1)] if last else []


class RaftInvariantMonitor:
    """Accumulates history across observations and checks all five safety properties."""

    def __init__(self) -> None:
        self.leader_of_term: dict[int, str] = {}
        # node_id -> {index: term} for every index that node has ever held, so a later
        # truncation of a *committed* index is visible even after the log looks tidy again.
        self.committed_entry: dict[int, tuple[int, object]] = {}
        # The log each node held the last time it was observed AS LEADER, keyed by
        # (incarnation, term) — see _leader_append_only for why the term belongs in the key.
        self.leader_log: dict[tuple[int, int], list[tuple[int, object]]] = {}
        # Per-incarnation volatile watermarks, for the monotonicity checks.
        self.watermarks: dict[int, tuple[int, int, int]] = {}
        self.observations = 0

    @staticmethod
    def _incarnation(node) -> int:
        """Identity of one RUN of a node, not of the node.

        `SimCluster.restart()` builds a FRESH RaftNode on the same database file, and a
        restart legitimately moves volatile state BACKWARDS: `commit_index` is rebuilt
        from `last_applied`, so a node that had committed to 10 and applied to 7 restarts
        at 7. Keying the watermarks on `id(node)` scopes them to one incarnation, which is
        the only span over which "never decreases" is a real rule rather than a
        misunderstanding of what a restart is."""
        return id(node)

    def observe(self, cluster) -> None:
        live = [n for n in cluster.nodes.values() if _readable(n)]
        if not live:
            return
        self.observations += 1
        logs = {n.cfg.node_id: _log(n) for n in live}
        self._election_safety(live)
        self._log_matching(logs)
        self._state_machine_safety(live, logs)
        self._leader_completeness(live, logs)
        self._leader_append_only(live, logs)
        self._local_monotonicity(live, logs)

    # --- the five properties -------------------------------------------------------

    def _election_safety(self, live) -> None:
        """At most one leader per term (§5.2). The temporal one: compare against every
        leader ever seen in that term, not just the ones leading right now."""
        for node in live:
            if node.role is not Role.LEADER:
                continue
            term, who = node.current_term, node.cfg.node_id
            seen = self.leader_of_term.setdefault(term, who)
            if seen != who:
                raise InvariantViolation(
                    f"ElectionSafety: term {term} had two leaders, {seen} and {who}. "
                    "Two majorities that do not overlap, or a vote counted twice."
                )

    def _log_matching(self, logs) -> None:
        """If two logs hold an entry with the same index and term, the logs are identical
        in every entry up to that index (§5.3). Checking only the entry itself would miss
        the case the property exists for — matching tails over divergent prefixes."""
        ids = sorted(logs)
        for i, a in enumerate(ids):
            for b in ids[i + 1:]:
                for idx in range(min(len(logs[a]), len(logs[b])), 0, -1):
                    term_a, term_b = logs[a][idx - 1][0], logs[b][idx - 1][0]
                    if term_a != term_b:
                        continue
                    if logs[a][:idx] != logs[b][:idx]:
                        raise InvariantViolation(
                            f"LogMatching: {a} and {b} agree at index {idx} (term "
                            f"{term_a}) but their prefixes differ. The consistency check "
                            "let a conflicting entry through."
                        )
                    break  # the highest agreeing index implies every lower one

    def _state_machine_safety(self, live, logs) -> None:
        """No two nodes apply a DIFFERENT command at the same index (§5.4.3) — the
        property the whole algorithm exists to deliver. Read against last_applied rather
        than the log tail: an uncommitted tail is allowed to differ, an applied prefix
        never is."""
        applied: dict[int, tuple[str, tuple[int, object]]] = {}
        for node in live:
            nid = node.cfg.node_id
            for idx in range(1, min(node.last_applied, len(logs[nid])) + 1):
                entry = logs[nid][idx - 1]
                owner, seen = applied.get(idx, (nid, entry))
                if seen != entry:
                    raise InvariantViolation(
                        f"StateMachineSafety: index {idx} applied as {seen!r} by {owner} "
                        f"and as {entry!r} by {nid}. Two nodes' state machines have "
                        "permanently diverged."
                    )
                applied[idx] = (owner, seen)

    def _leader_completeness(self, live, logs) -> None:
        """An entry committed in term T is present in the log of every leader of every
        term GREATER than T (§5.4.3) — the property that makes an acknowledged write safe
        to acknowledge.

        The "greater than" is the whole content of the rule, and stating it as "every
        leader" instead is wrong in a way that looks like a bug in the implementation. A
        deposed leader does not learn it was deposed until someone tells it: `role` stays
        LEADER and `current_term` stays behind while the cluster elects a successor and
        commits new entries. Its log at those indices is stale by definition and will be
        truncated on contact. Checking it against a later term's committed entry reports a
        lost write on a cluster that has lost nothing — which is exactly what the first
        version of this method did, on all four seeds.

        So the commit TERM is recorded alongside the entry, and a leader is only held to an
        entry committed strictly before its own term.
        """
        for node in live:
            nid = node.cfg.node_id
            log = logs[nid]
            for idx in range(1, min(node.commit_index, len(log)) + 1):
                entry = log[idx - 1]
                previous = self.committed_entry.get(idx)
                if previous is None:
                    self.committed_entry[idx] = (entry, node.current_term)
                elif previous[0] != entry:
                    raise InvariantViolation(
                        f"two nodes report index {idx} as committed with different "
                        f"entries: {previous[0]!r} and {entry!r} (on {nid}). A committed "
                        "index is supposed to be decided forever."
                    )
                elif node.current_term < previous[1]:
                    # Keep the EARLIEST term we saw it committed in; that is the term the
                    # rule quantifies over, and a later sighting must not weaken it.
                    self.committed_entry[idx] = (entry, node.current_term)

        for node in live:
            if node.role is not Role.LEADER:
                continue
            log = logs[node.cfg.node_id]
            for idx, (entry, commit_term) in self.committed_entry.items():
                if node.current_term <= commit_term:
                    continue  # not a later leader: this rule says nothing about it
                if idx > len(log):
                    raise InvariantViolation(
                        f"LeaderCompleteness: index {idx} was committed in term "
                        f"{commit_term} as {entry!r}, but leader "
                        f"{node.cfg.node_id} of term {node.current_term} has only "
                        f"{len(log)} entries. An acknowledged write was lost."
                    )
                if log[idx - 1] != entry:
                    raise InvariantViolation(
                        f"LeaderCompleteness: index {idx} was committed in term "
                        f"{commit_term} as {entry!r}, but leader "
                        f"{node.cfg.node_id} of term {node.current_term} holds "
                        f"{log[idx - 1]!r}. An acknowledged write was overwritten."
                    )

    def _leader_append_only(self, live, logs) -> None:
        """A leader never overwrites or deletes entries in its own log; it only appends
        (§5.3, Figure 3). The fifth property — described in this module's docstring since
        it was written, and until now not actually checked.

        It is temporal, and that is the whole difficulty: a log that truncated an entry and
        re-appended an identical one looks untouched in any single snapshot. So the
        previous log is kept and the current one must EXTEND it — same entries at every
        index they share, and no shorter.

        Keyed by `(incarnation, term)` rather than by node. A node may legally hold a
        different log at index 5 across two different tenures: it is deposed, a new leader
        truncates its divergent tail, and it is elected again later. Only within ONE term
        is a leader's log strictly append-only, because within one term there is only one
        leader (Election Safety) and nobody else can be telling it to truncate. Checking it
        across terms reports the ordinary repair of a deposed leader as a violation.

        The bug this catches is a leader that truncates its OWN log — which the receiver
        rules make possible, because `handle_append_entries` truncates on conflict and a
        leader that failed to step down first would run those rules while still LEADER.
        Rule 4 (§5.1) is what prevents it: a higher term deposes us BEFORE the handler's
        own logic. Weaken `_observe_term`, or move the role change after the truncation,
        and this is what notices.
        """
        for node in live:
            if node.role is not Role.LEADER:
                continue
            key = (self._incarnation(node), node.current_term)
            log = logs[node.cfg.node_id]
            previous = self.leader_log.get(key)
            if previous is not None:
                if len(log) < len(previous):
                    raise InvariantViolation(
                        f"LeaderAppendOnly: leader {node.cfg.node_id} of term "
                        f"{node.current_term} went from {len(previous)} entries to "
                        f"{len(log)}. A leader deleted entries from its own log."
                    )
                if log[: len(previous)] != previous:
                    differ = next(
                        i for i, (a, b) in enumerate(zip(previous, log, strict=False))
                        if a != b
                    )
                    raise InvariantViolation(
                        f"LeaderAppendOnly: leader {node.cfg.node_id} of term "
                        f"{node.current_term} changed index {differ + 1} from "
                        f"{previous[differ]!r} to {log[differ]!r}. A leader overwrote its "
                        "own log."
                    )
            self.leader_log[key] = log

    # --- per-node monotonicity ------------------------------------------------------

    def _local_monotonicity(self, live, logs) -> None:
        """What must never run backwards ON ONE NODE, within one run of it.

        None of the five properties above is local: they all compare nodes to each other,
        so a node that corrupts its own state in a way the others happen to mirror slips
        through every one of them. These are the cheap local checks that close that gap,
        and each names a real bug class:

          * **currentTerm decreasing** — terms are the algorithm's logical clock and Figure
            2 only ever raises them. A node that lowered its term could vote a second time
            in a term it had already voted in, which is two leaders in one term.
          * **commitIndex decreasing** — a committed index is decided forever. Moving it
            back means the node is prepared to commit something ELSE there.
          * **lastApplied decreasing** — the state machine would replay an index, applying
            two different commands at one position.
          * **lastApplied > commitIndex** — applying what was never committed: the entry
            can still be truncated, and the state machine cannot un-apply it.
          * **commitIndex > lastLogIndex** — the node claims to have committed an entry it
            does not hold. This is the one that catches a follower truncating below its own
            commit index, which no cross-node property necessarily sees: if every surviving
            node truncated consistently, the logs still match each other.

        Scoped per INCARNATION (see `_incarnation`): a restart rebuilds `commit_index` from
        `last_applied` and may legitimately move it backwards.
        """
        for node in live:
            key = self._incarnation(node)
            term, commit, applied = node.current_term, node.commit_index, node.last_applied
            nid = node.cfg.node_id

            if applied > commit:
                raise InvariantViolation(
                    f"{nid} has last_applied={applied} above commit_index={commit}. It "
                    "applied an entry that was never committed."
                )
            if commit > len(logs[nid]):
                raise InvariantViolation(
                    f"{nid} has commit_index={commit} but only {len(logs[nid])} entries. "
                    "It committed an index it does not hold — a log was truncated below "
                    "its own commit index."
                )
            seen = self.watermarks.get(key)
            if seen is not None:
                for name, was, now in (
                    ("current_term", seen[0], term),
                    ("commit_index", seen[1], commit),
                    ("last_applied", seen[2], applied),
                ):
                    if now < was:
                        raise InvariantViolation(
                            f"{nid} moved {name} BACKWARDS, {was} -> {now}, without "
                            "restarting. Nothing in Figure 2 lowers it."
                        )
            self.watermarks[key] = (term, commit, applied)


def assert_no_acknowledged_write_is_lost(cluster, acknowledged: dict[str, str]) -> None:
    """The client-visible half, and the one a user would actually notice.

    The properties above are statements about logs. This is a statement about promises: a
    write the cluster answered 200 to must still be readable afterwards. That is the
    Jepsen "lost update" check in its simplest form, and it catches the same class of bug
    from the outside — a committed entry that a later leader truncated is invisible in a
    log comparison if every surviving node truncated it consistently.
    """
    survivors = [n for n in cluster.nodes.values() if _readable(n)]
    assert survivors, "no readable node left to check acknowledged writes against"
    for key, value in acknowledged.items():
        holders = {n.cfg.node_id: n.storage.kv_get(key) for n in survivors}
        if not any(v == value for v in holders.values()):
            raise InvariantViolation(
                f"acknowledged write {key}={value!r} is on no surviving node: {holders}. "
                "The cluster answered a client and then lost the write."
            )
