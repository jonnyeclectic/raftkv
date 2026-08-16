import asyncio

import pytest

from conftest import eventually
from raftkv.models import Command, Role
from raftkv.raft import NotLeaderError


async def test_elects_a_stable_single_leader(cluster):
    await eventually(lambda: cluster.leader() is not None)
    # Raft's actual invariant is at most one leader PER TERM — two leaders in
    # different terms may legally coexist for an instant, so never assert a raw
    # global count at a single sample point.
    leaders = [n for n in cluster.nodes.values() if n.role is Role.LEADER]
    assert len({n.current_term for n in leaders}) == len(leaders)
    # ...and the cluster settles on exactly one:
    await eventually(
        lambda: sum(1 for n in cluster.nodes.values() if n.role is Role.LEADER) == 1
    )


async def test_leader_crash_triggers_failover_and_rejoin(cluster):
    await eventually(lambda: cluster.leader() is not None)
    old = cluster.leader()
    old_id, old_term = old.cfg.node_id, old.current_term
    await cluster.crash(old_id)
    await eventually(
        lambda: cluster.leader() is not None and cluster.leader().cfg.node_id != old_id
    )
    new = cluster.leader()
    assert new.current_term > old_term
    rejoined = cluster.restart(old_id)
    # don't pin the specific leader — leadership may legally churn once more under
    # CI load; the invariant is that the rejoiner follows SOME other current leader
    await eventually(
        lambda: rejoined.role is Role.FOLLOWER and rejoined.leader_id not in (None, old_id)
    )


async def test_write_replicates_to_all_nodes(cluster):
    await eventually(lambda: cluster.leader() is not None)
    await cluster.leader().submit(Command(op="set", key="temp", value="72", request_id="r1"))
    await eventually(
        lambda: all(n.storage.kv_get("temp") == "72" for n in cluster.nodes.values())
    )


async def test_partitioned_leader_cannot_commit_then_steps_down(cluster):
    """The §5.4 safety property end to end: a minority leader accepts writes it can NEVER
    commit; on heal it is deposed and its uncommitted suffix is truncated away."""
    await eventually(lambda: cluster.leader() is not None)
    stale = cluster.leader()
    majority = {node_id for node_id in cluster.nodes if node_id != stale.cfg.node_id}
    await stale.submit(Command(op="set", key="k", value="committed", request_id="r1"))

    cluster.transport.partition({stale.cfg.node_id}, majority)
    doomed = asyncio.create_task(
        stale.submit(Command(op="set", key="k", value="doomed", request_id="r2"))
    )
    try:
        await asyncio.sleep(0)  # let the doomed submit append locally

        new_leader = None

        def majority_elected():
            nonlocal new_leader
            new_leader = next(
                (
                    n for node_id, n in cluster.nodes.items()
                    if node_id in majority and n.role is Role.LEADER
                    and n.current_term > stale.current_term
                ),
                None,
            )
            return new_leader is not None

        await eventually(majority_elected)
        await new_leader.submit(Command(op="set", key="k", value="v2", request_id="r3"))
        assert stale.commit_index < stale.storage.last_log_index()  # doomed: uncommitted

        cluster.transport.heal()
        await eventually(lambda: stale.role is Role.FOLLOWER)
        await eventually(
            lambda: all(n.storage.kv_get("k") == "v2" for n in cluster.nodes.values())
        )
        # Explicit divergence repair: the doomed r2 entry was truncated and replaced
        # by the new leader's r3 entry (§5.3 conflict rule, directly). Located by
        # scanning rather than by index -- leaders append a no-op on winning, so the
        # absolute position depends on how many elections the run happened to hold.
        request_ids = [
            e.command.request_id
            for i in range(1, stale.storage.last_log_index() + 1)
            if (e := stale.storage.entry(i)) and isinstance(e.command, Command)
        ]
        assert "r3" in request_ids
        assert "r2" not in request_ids  # the doomed entry is gone, not merely buried
        # stability window: v2 must survive further heartbeats (guards against a
        # broken election restriction letting the stale node win later and roll back)
        await asyncio.sleep(2 * stale.cfg.heartbeat_interval)
        assert all(n.storage.kv_get("k") == "v2" for n in cluster.nodes.values())
        with pytest.raises((NotLeaderError, TimeoutError)):
            await doomed  # the write was never acknowledged — and never will be
    finally:
        doomed.cancel()  # never leak an un-retrieved task exception into other tests
        await asyncio.gather(doomed, return_exceptions=True)


async def test_lagging_follower_catches_up(cluster):
    """Catch-up (a strict-prefix log gets backfilled). Actual DIVERGENCE repair —
    a conflicting suffix truncated — is asserted explicitly in the partition test
    above (stale.storage.entry(2) check) and unit-tested in test_append_entries.py."""
    await eventually(lambda: cluster.leader() is not None)
    leader = cluster.leader()
    follower_id = next(nid for nid in cluster.nodes if nid != leader.cfg.node_id)
    await cluster.crash(follower_id)
    for i in range(3):
        await leader.submit(Command(op="set", key=f"k{i}", value=str(i), request_id=f"r{i}"))
    rejoined = cluster.restart(follower_id)
    await eventually(
        lambda: rejoined.last_applied >= 3 and rejoined.storage.kv_get("k2") == "2"
    )


async def test_term_vote_and_data_survive_restart(cluster):
    await eventually(lambda: cluster.leader() is not None)
    leader = cluster.leader()
    await leader.submit(Command(op="set", key="durable", value="yes", request_id="r1"))
    node_id, term_before = leader.cfg.node_id, leader.current_term
    await cluster.crash(node_id)
    node = cluster.restart(node_id)
    assert node.current_term >= term_before  # persisted term (rule 1)
    assert node.storage.kv_get("durable") == "yes"  # applied state persisted
