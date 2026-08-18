"""CheckQuorum (thesis §6.2): a leader resigns when it stops hearing back.

Raft guarantees one leader per TERM, not one leader. Every mechanism that deposes a
leader in Figure 2 arrives in a message — and a partitioned leader receives none, which
is the entire problem. So the only evidence available to it is its own silence, and
this is the rule that reads it.

The live end of this behaviour is in tests/test_partition.py, over real timers. These
are the unit tests: no loops running, `_check_quorum()` driven by hand against a clock
this test controls, so each decision can be pinned on its own.
"""

import asyncio

from mockito import ANY

from conftest import StubTransport, eventually
from raftkv.models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    ClusterConfig,
    Role,
)
from raftkv.transport import TransportError


def lead(node, *, contacts: dict[str, float] | None = None):
    """Put a node in the leader seat WITHOUT `_become_leader()`.

    These tests set `_last_contact` to specific ages, and `_become_leader` deliberately
    overwrites it with a fresh seed for every peer (see test_becoming_leader_seeds_...
    below, which is the one test that must go through the real thing)."""
    node.role = Role.LEADER
    node.leader_id = node.cfg.node_id
    node.current_term = 4
    node._last_contact = dict(contacts or {})
    return node


def ago(seconds: float) -> float:
    import time
    return time.monotonic() - seconds


def test_a_leader_in_contact_with_a_majority_keeps_leading(make_node):
    node = lead(make_node(), contacts={"node-2": ago(0.0), "node-3": ago(0.0)})
    node._check_quorum()
    assert node.role is Role.LEADER


def test_one_of_two_followers_is_a_majority_of_three(make_node):
    """Quorum is 2 of 3 and the leader is one of them, so a single reachable follower is
    the whole requirement. A rule that demanded all peers would resign on any single
    failure — turning the fault tolerance the cluster is built for into an outage."""
    node = lead(make_node(), contacts={"node-2": ago(0.0), "node-3": ago(99.0)})
    node._check_quorum()
    assert node.role is Role.LEADER


def test_a_leader_that_hears_from_nobody_resigns(make_node):
    node = lead(make_node(), contacts={"node-2": ago(99.0), "node-3": ago(99.0)})
    node._check_quorum()
    assert node.role is Role.FOLLOWER
    assert node.leader_id is None   # it no longer knows who leads, and must not guess
    assert node.current_term == 4   # a resignation, NOT a campaign: the term stays put


def test_resigning_restarts_the_election_timer(make_node):
    """A leader's election timer has been stale for its entire tenure, so a step-down
    that did not restart it would campaign on the very next tick — before PreVote could
    establish anything and before any follower had a chance to win instead."""
    node = lead(make_node(), contacts={"node-2": ago(99.0), "node-3": ago(99.0)})
    node._last_reset = ago(99.0)
    node._check_quorum()
    assert node._last_reset > ago(1.0)


def test_the_window_is_the_full_election_timeout(make_node):
    """The constant is a choice, not an accident. `election_timeout_max` is the longest
    any follower will wait before campaigning, so resigning at that age fires only once
    every voter that could have replaced us has had its chance. Tightening it to
    `election_timeout_min` would sometimes depose a leader the cluster was still
    perfectly happy with, trading a stale read for an election."""
    # Read from the node rather than restated as a literal: the fixture's timers scale
    # with RAFT_TEST_TIMING_SCALE, and a hardcoded 0.2 here silently becomes an assertion
    # that the window is SHORTER than it is, which passes for the wrong reason at 1x and
    # fails outright above it.
    node = make_node()
    window = node.cfg.election_timeout_max
    inside = lead(node, contacts={"node-2": ago(window * 0.5),
                                  "node-3": ago(99.0)})
    inside._check_quorum()
    assert inside.role is Role.LEADER

    outside = lead(make_node("node-9"), contacts={"node-2": ago(window * 1.5),
                                                  "node-3": ago(99.0)})
    outside._check_quorum()
    assert outside.role is Role.FOLLOWER


def test_a_lone_voter_is_its_own_majority(make_node):
    """A single-voter cluster is its own majority, so it is in contact with a quorum by
    definition. Resigning here would produce a node that steps down every election
    timeout and can never be written to at all."""
    node = lead(make_node(peer_ids=()))
    node._check_quorum()
    assert node.role is Role.LEADER


def test_a_joint_leader_needs_a_majority_of_BOTH_halves(make_node):
    """§6, and the sharp edge. Reachability is agreement, so it goes through
    `_has_agreement` rather than counting heads: during a joint transition a set that is
    a majority of C-new alone is not contact with a quorum. Counting the union instead
    would let a leader keep the role on the strength of the new servers only — the
    two-disjoint-majorities shape Figure 10 exists to forbid, arrived at from the
    liveness side."""
    node = lead(make_node())
    node.config = ClusterConfig(
        voters={n: n for n in ["node-1", "node-2", "node-3", "node-4", "node-5"]},
        old_voters={n: n for n in ["node-1", "node-2", "node-3"]},
    )

    joint = node.config
    node._last_contact = {"node-4": ago(0.0), "node-5": ago(0.0)}  # 3 of 5 new, 1 of 3 old
    node._check_quorum()
    assert node.role is Role.FOLLOWER, "a C-new majority alone is not agreement"

    # Same configuration, contact with the overlap instead: 3 of 5 AND 3 of 3.
    both = lead(make_node("node-1b"))
    both.cfg.node_id = "node-1"  # sit in node-1's seat in the rosters above
    both.config = joint
    both._last_contact = {"node-2": ago(0.0), "node-3": ago(0.0)}
    both._check_quorum()
    assert both.role is Role.LEADER


def test_a_follower_running_the_check_is_a_no_op(make_node):
    """The replication loop calls this every round, and a node deposed mid-round is a
    follower by the time it lands. Nothing here may touch a node that is not leading."""
    node = make_node()
    node.role = Role.FOLLOWER
    node.leader_id = "node-2"
    node._check_quorum()
    assert node.role is Role.FOLLOWER
    assert node.leader_id == "node-2"  # untouched


def test_becoming_leader_seeds_contact_so_a_fresh_leader_survives_its_first_check(
    make_node,
):
    """An unseeded map reads as a cluster nobody has ever answered. The first check would
    then depose a leader elected a millisecond ago — and would do so every time, which is
    not a slow cluster but a cluster that can never keep one. The majority that just
    elected it is the evidence the seed stands on."""
    node = make_node()
    node.current_term = 4
    node._become_leader()
    node._check_quorum()
    assert node.role is Role.LEADER
    assert set(node._last_contact) == {"node-2", "node-3"}


async def test_any_answer_counts_as_contact_including_a_rejection(make_node, when):
    """Contact is about the LINK, not about agreement. A follower rejecting a consistency
    check is a follower we can still reach and are actively repairing; counting only
    successes would depose a leader for the crime of having a peer behind — exactly when
    stepping down is most expensive."""
    transport = StubTransport()
    node = make_node(transport=transport)
    node.current_term = 4
    node._become_leader()
    when(transport).append_entries("node-2", ANY(AppendEntriesRequest)).thenReturn(
        AppendEntriesResponse(term=4, success=False, conflict_index=1)
    )
    node._last_contact = {}

    await node._append_to_peer("node-2", term_when_sent=4)

    assert "node-2" in node._last_contact


async def test_an_unanswered_append_is_not_contact(make_node, when):
    transport = StubTransport()
    node = make_node(transport=transport)
    node.current_term = 4
    node._become_leader()
    when(transport).append_entries("node-2", ANY(AppendEntriesRequest)).thenRaise(
        TransportError("down")
    )
    node._last_contact = {}

    await node._append_to_peer("node-2", term_when_sent=4)

    assert node._last_contact == {}


async def test_a_blocked_peer_is_not_contact(make_node):
    """The simulated partition has to be indistinguishable from a real one here, or the
    one failure control we can inject would be the one thing CheckQuorum could not see."""
    node = make_node()
    node.current_term = 4
    node._become_leader()
    node.set_blocked(["node-2"])
    node._last_contact = {}

    await node._append_to_peer("node-2", term_when_sent=4)

    assert node._last_contact == {}


async def test_a_crash_forgets_contact(make_node):
    """Volatile state, and it must die with the node. A recovered process that inherited
    stale timestamps would consider itself in contact with peers it has not spoken to
    since before it stopped."""
    node = make_node()
    node.current_term = 4
    node._become_leader()
    assert node._last_contact
    await node.crash()
    assert node._last_contact == {}


async def test_losing_one_follower_is_survivable_and_losing_both_is_not(cluster):
    """The two sides of the same partition over a live cluster, which is the only way to
    ask this honestly: `_check_quorum` reads answers that real peers have to send. One
    follower cut off leaves the leader in contact with a majority and it keeps leading;
    cut off from both, it resigns. The difference between those two is the fault
    tolerance the cluster exists to provide, so a CheckQuorum that got it wrong would
    resign on every single failure."""
    await eventually(lambda: cluster.leader() is not None)
    leader = cluster.leader()
    followers = [n for n in cluster.ids if n != leader.cfg.node_id]

    leader.set_blocked(followers[:1])
    await asyncio.sleep(0.5)  # several election timeouts at test timing
    assert leader.role is Role.LEADER, "one unreachable peer is not a lost quorum"

    leader.set_blocked(followers)
    await eventually(lambda: leader.role is not Role.LEADER, timeout=2.0)
