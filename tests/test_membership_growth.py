"""Growing a live cluster 3 -> 4 -> 5, over real timers and a simulated network.

The unit tests in test_joint_consensus.py check the pieces. This checks the thing: a
running cluster that keeps electing and keeps committing while its own membership
changes underneath it, and that afterwards tolerates the number of failures the new
size promises. If joint consensus is wrong, this is where it shows.
"""

import asyncio

from conftest import eventually, make_cfg
from raftkv.models import ClusterConfig, Command, Role
from raftkv.raft import RaftNode
from raftkv.storage import Storage


def spawn_learner(cluster, node_id):
    """A brand-new process that nobody knows about yet: no peers, never campaigns."""
    cfg = make_cfg(node_id, peers={}, learner=True)
    storage = Storage(str(cluster.tmp_path / f"{node_id}.db"))
    node = RaftNode(cfg, storage, cluster.transport)
    cluster.transport.register(node_id, node)
    cluster.nodes[node_id] = node
    node.start()
    return node


async def settle(cluster, predicate, timeout=5.0):
    await eventually(predicate, timeout=timeout)


async def promote_when_allowed(leader, node_id, timeout=5.0):
    """Retry through the preconditions rather than sleeping a guessed interval: a 409
    here means "not yet" (no-op not committed, previous change in flight), not "no"."""
    async def attempt():
        try:
            leader.promote_learner(node_id)
            return True
        except ValueError:
            return False
    await eventually(lambda: leader.role is Role.LEADER, timeout=timeout)
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if await attempt():
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"never became able to promote {node_id}")


async def test_cluster_grows_three_to_five_and_keeps_committing(cluster):
    """The headline. Add two learners, promote them one at a time, and write across
    every stage: the cluster must never stop being able to commit."""
    await settle(cluster, lambda: cluster.leader() is not None)
    leader = cluster.leader()
    await leader.submit(Command(op="set", key="k", value="at-three", request_id="r0"))
    assert len(leader.config.voters) == 3 and leader.quorum == 2

    for new_id, value in [("node-4", "at-four"), ("node-5", "at-five")]:
        spawn_learner(cluster, new_id)
        leader = cluster.leader()
        leader.add_learner(new_id, new_id)

        # the learner must actually catch up before it is worth promoting
        await settle(cluster,
                     lambda nid=new_id: cluster.nodes[nid].storage.last_log_index() > 0)

        await promote_when_allowed(leader, new_id)
        assert leader.config.joint, "promotion must open a joint phase"

        # the transition completes on its own once C-old,new commits
        await settle(cluster, lambda: not cluster.leader().config.joint)

        leader = cluster.leader()
        assert new_id in leader.config.voters
        # ...and the cluster is still writable at the new size
        await leader.submit(Command(op="set", key="k", value=value, request_id=value))

    assert len(leader.config.voters) == 5
    assert leader.quorum == 3
    await settle(cluster, lambda: all(
        n.storage.kv_get("k") == "at-five"
        for n in cluster.nodes.values() if n.cfg.node_id in leader.config.voters
    ))


async def test_five_voters_tolerate_two_failures_where_four_tolerated_one(cluster):
    """Why odd sizes. This is the assertion the 3 -> 4 -> 5 growth path exists to make."""
    await settle(cluster, lambda: cluster.leader() is not None)
    leader = cluster.leader()

    sizes = {}
    for new_id in ["node-4", "node-5"]:
        spawn_learner(cluster, new_id)
        leader = cluster.leader()
        leader.add_learner(new_id, new_id)
        await settle(cluster,
                     lambda nid=new_id: cluster.nodes[nid].storage.last_log_index() > 0)
        await promote_when_allowed(leader, new_id)
        await settle(cluster, lambda: not cluster.leader().config.joint)
        leader = cluster.leader()
        n = len(leader.config.voters)
        sizes[n] = n - leader.quorum

    assert sizes[4] == 1, "four voters tolerate one failure -- no better than three"
    assert sizes[5] == 2, "five tolerate two: the whole argument for odd sizes"


async def test_membership_survives_a_leader_change_mid_life(cluster):
    """Configuration is in the log, so a new leader inherits it rather than forgetting
    it. This is precisely what the old leader-local learner set could not do."""
    await settle(cluster, lambda: cluster.leader() is not None)
    leader = cluster.leader()
    spawn_learner(cluster, "node-4")
    leader.add_learner("node-4", "node-4")
    await promote_when_allowed(leader, "node-4")
    await settle(cluster, lambda: not cluster.leader().config.joint)

    deposed = cluster.leader().cfg.node_id
    await cluster.crash(deposed)
    await settle(cluster, lambda: (cluster.leader() is not None
                                   and cluster.leader().cfg.node_id != deposed),
                 timeout=8.0)

    successor = cluster.leader()
    assert "node-4" in successor.config.voters, "new leader lost the membership change"
    assert successor.quorum == 3  # 4 voters


async def test_no_two_leaders_during_a_joint_transition(cluster):
    """The safety property, asserted continuously rather than once: at no sampled
    instant may two nodes both believe they lead in the same term."""
    await settle(cluster, lambda: cluster.leader() is not None)
    leader = cluster.leader()
    spawn_learner(cluster, "node-4")
    leader.add_learner("node-4", "node-4")
    await promote_when_allowed(leader, "node-4")

    violations = []
    for _ in range(120):
        by_term: dict[int, list[str]] = {}
        for node in cluster.nodes.values():
            if node.role is Role.LEADER:
                by_term.setdefault(node.current_term, []).append(node.cfg.node_id)
        for term, leaders in by_term.items():
            if len(leaders) > 1:
                violations.append((term, sorted(leaders)))
        await asyncio.sleep(0.01)

    assert not violations, f"two leaders in one term: {violations[:3]}"


async def test_a_joint_configuration_never_collapses_to_one_majority(cluster):
    """Direct attack on the predicate itself, using the live cluster's own node: a set
    that is a majority of the union but not of C-old must NOT be agreement."""
    await settle(cluster, lambda: cluster.leader() is not None)
    node = cluster.leader()
    node.config = ClusterConfig(
        voters={n: n for n in ["a", "b", "c", "d", "e"]},
        old_voters={n: n for n in ["a", "b", "c"]},
    )
    assert not node._has_agreement({"c", "d", "e"})  # 3 of 5 new, only 1 of 3 old
    assert node._has_agreement({"a", "b", "c"})      # majority of both


async def test_an_undialable_voter_does_not_kill_the_election_loop(tmp_path):
    """Found by adversarial review, and the worst kind of bug: silent and permanent.

    A configuration can name a voter this node cannot dial -- config entries replicate,
    and one carrying an empty advertise address (a bootstrap node never told its own)
    reaches a later-joined member as exactly that. The transport raised KeyError rather
    than TransportError, which escaped the `except TransportError` in _start_election
    and killed _election_timer_loop for the life of the process. The node then sat as a
    follower forever, campaigning never again, with nothing logged to say so.

    Asserted on `pre_votes_started` rather than `elections_started` since PreVote landed:
    the undialable peer is now dialled first by the straw poll, so that is where a
    non-TransportError would escape -- and a node that cannot reach a quorum deliberately
    never starts a real election at all (thesis §9.6). Same regression, same loop, one
    round earlier.
    """
    from raftkv.transport import MemoryTransport

    transport = MemoryTransport()
    # Two voters, one of them unreachable: quorum 2, so no election can ever be won.
    # What matters is that it keeps TRYING.
    cfg = make_cfg("node-1", peers={"ghost": ""})
    node = RaftNode(cfg, Storage(str(tmp_path / "node-1.db")), transport)
    transport.register("node-1", node)
    node.start()
    try:
        await eventually(lambda: node.metrics.pre_votes_started >= 3, timeout=3.0)
        assert all(not task.done() for task in node._tasks), \
            "an unreachable voter killed a background task"
    finally:
        await node.stop()
        node.storage.close()
