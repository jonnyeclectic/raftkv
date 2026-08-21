"""The monitor that watches the cluster, watched in turn.

`tests/invariants.py` is the backstop for every randomized run: if it is wrong, every
chaos seed passes while proving nothing. It is also the one file the mutation harness
cannot reach — `scripts/mutate.py` only rewrites `src/raftkv` — so a check that can never
fire would never be noticed there. These tests are that harness's stand-in: each hands the
monitor a deliberately-broken fake and asserts it raises, and two of them assert it does
NOT raise on a shape that merely looks broken (a restart), which is the false positive that
would make the whole monitor unusable on the failure suite.

A `FakeNode` is the smallest thing `observe()` reads: an in-memory log, the four
watermarks, a role, and a real (in-memory) Storage so `_readable`/`entries_from` work. No
RaftNode, no timers — the monitor's own logic is what is under test, not Raft's.
"""

import pytest

from invariants import InvariantViolation, RaftInvariantMonitor
from raftkv.models import Command, LogEntry, Role
from raftkv.storage import Storage


def cmd(rid: str) -> Command:
    return Command(op="set", key="k", value="v", request_id=rid)


class FakeNode:
    """Quacks like RaftNode to the degree `RaftInvariantMonitor.observe()` reads it."""

    def __init__(self, node_id: str, terms_and_rids, *, role=Role.FOLLOWER,
                 current_term=1, commit_index=0, last_applied=0, leader_id=None):
        self.cfg = type("Cfg", (), {"node_id": node_id})()
        self.role = role
        self.current_term = current_term
        self.commit_index = commit_index
        self.last_applied = last_applied
        self.leader_id = leader_id
        self.storage = Storage(":memory:")
        if terms_and_rids:
            self.storage.append([LogEntry(term=t, command=cmd(r)) for t, r in terms_and_rids])

    def set_log(self, terms_and_rids):
        self.storage.wipe()
        if terms_and_rids:
            self.storage.append([LogEntry(term=t, command=cmd(r)) for t, r in terms_and_rids])


class FakeCluster:
    def __init__(self, *nodes):
        self.nodes = {n.cfg.node_id: n for n in nodes}


# ---- the checks fire on a genuine violation ---------------------------------


def test_two_leaders_in_one_term_seen_at_different_instants_is_caught():
    """ElectionSafety is temporal: the two leaders need never be observed together."""
    mon = RaftInvariantMonitor()
    a = FakeNode("node-1", [(1, "x")], role=Role.LEADER, current_term=7)
    mon.observe(FakeCluster(a))
    a.role = Role.FOLLOWER  # deposed, before it noticed
    b = FakeNode("node-2", [(1, "x")], role=Role.LEADER, current_term=7)
    with pytest.raises(InvariantViolation, match="ElectionSafety"):
        mon.observe(FakeCluster(a, b))


def test_a_leader_that_truncated_its_own_log_is_caught():
    """LeaderAppendOnly: within one term a leader's log may only grow."""
    mon = RaftInvariantMonitor()
    leader = FakeNode("node-1", [(1, "a"), (1, "b"), (1, "c")], role=Role.LEADER,
                      current_term=1)
    mon.observe(FakeCluster(leader))
    leader.set_log([(1, "a"), (1, "b")])  # deleted an entry it had already shown
    with pytest.raises(InvariantViolation, match="LeaderAppendOnly"):
        mon.observe(FakeCluster(leader))


def test_a_leader_that_overwrote_its_own_log_is_caught():
    mon = RaftInvariantMonitor()
    leader = FakeNode("node-1", [(1, "a"), (1, "b")], role=Role.LEADER, current_term=1)
    mon.observe(FakeCluster(leader))
    leader.set_log([(1, "a"), (1, "DIFFERENT")])  # same length, changed index 2
    with pytest.raises(InvariantViolation, match="LeaderAppendOnly"):
        mon.observe(FakeCluster(leader))


def test_a_term_that_moved_backwards_is_caught():
    mon = RaftInvariantMonitor()
    n = FakeNode("node-1", [(1, "a")], current_term=9)
    mon.observe(FakeCluster(n))
    n.current_term = 4  # a term never decreases within one run
    with pytest.raises(InvariantViolation, match="current_term BACKWARDS"):
        mon.observe(FakeCluster(n))


def test_a_commit_index_that_moved_backwards_is_caught():
    # last_applied held at 1 throughout, so lowering commit_index to 1 isolates the
    # backwards-commit check without tripping the applied>commit guard first.
    mon = RaftInvariantMonitor()
    n = FakeNode("node-1", [(1, "a"), (1, "b"), (1, "c")], commit_index=3, last_applied=1)
    mon.observe(FakeCluster(n))
    n.commit_index = 1
    with pytest.raises(InvariantViolation, match="commit_index BACKWARDS"):
        mon.observe(FakeCluster(n))


def test_applying_past_the_commit_index_is_caught():
    mon = RaftInvariantMonitor()
    n = FakeNode("node-1", [(1, "a"), (1, "b")], commit_index=1, last_applied=2)
    with pytest.raises(InvariantViolation, match="never committed"):
        mon.observe(FakeCluster(n))


def test_committing_past_the_end_of_the_log_is_caught():
    """The one a cross-node property can miss: a node claiming a commit_index above its
    own log, which is what a truncation below the commit index looks like."""
    mon = RaftInvariantMonitor()
    n = FakeNode("node-1", [(1, "a")], commit_index=5, last_applied=0)
    with pytest.raises(InvariantViolation, match="does not hold"):
        mon.observe(FakeCluster(n))


# ---- and it does NOT fire on a shape that only looks broken ------------------


def test_a_restart_is_not_reported_as_a_regression():
    """The load-bearing false-positive guard. `SimCluster.restart()` builds a FRESH
    RaftNode on the same file, and volatile state legitimately moves backwards there —
    commit_index is rebuilt from last_applied. Because the monitor keys its watermarks on
    the node OBJECT (`_incarnation`), a new object with a lower commit_index is a new
    incarnation, not a regression. If this test fails, the monitor false-positives on
    every crash-restart test in the suite."""
    mon = RaftInvariantMonitor()
    original = FakeNode("node-1", [(1, "a"), (1, "b"), (1, "c")], commit_index=3,
                        last_applied=3)
    mon.observe(FakeCluster(original))
    # A restart: same node_id, same log, but a fresh object whose commit_index rebuilt
    # from a lower last_applied floor.
    restarted = FakeNode("node-1", [(1, "a"), (1, "b"), (1, "c")], commit_index=0,
                         last_applied=0)
    mon.observe(FakeCluster(restarted))  # must not raise


def test_a_deposed_leaders_stale_log_is_not_a_leader_completeness_violation():
    """A node still calling itself LEADER at an OLD term holds a stale log by definition;
    the rule quantifies over leaders of terms strictly greater than the commit term, so
    the monitor must not hold it to a later term's committed entry."""
    mon = RaftInvariantMonitor()
    # node-2 commits index 1 in term 5.
    committer = FakeNode("node-2", [(5, "real")], commit_index=1, last_applied=1,
                         current_term=5)
    # node-1 still thinks it leads term 3 (deposed, hasn't noticed) with a divergent log.
    stale = FakeNode("node-1", [(3, "stale")], role=Role.LEADER, current_term=3)
    mon.observe(FakeCluster(committer, stale))  # must not raise — 3 < 5


def test_a_later_leader_missing_a_committed_entry_is_caught():
    """The other side of that rule: a leader of a LATER term really is held to it."""
    mon = RaftInvariantMonitor()
    committer = FakeNode("node-2", [(5, "real")], commit_index=1, last_applied=1,
                         current_term=5)
    mon.observe(FakeCluster(committer))
    later_leader = FakeNode("node-1", [], role=Role.LEADER, current_term=6)
    with pytest.raises(InvariantViolation, match="LeaderCompleteness"):
        mon.observe(FakeCluster(committer, later_leader))
