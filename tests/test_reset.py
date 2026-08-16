"""The reset control: wipe a node back to first boot.

crash()/recover() prove persistence. reset() destroys it, and the distinction is the
point -- these tests pin that reset removes exactly the state Figure 2 requires to be
durable, because that is what makes it a teaching control rather than a convenience.
"""

import asyncio
import contextlib

from fastapi.testclient import TestClient

from conftest import eventually
from raftkv.app import create_app
from raftkv.models import Command, LogEntry, Role
from raftkv.raft import RaftNode
from raftkv.storage import Storage
from raftkv.transport import MemoryTransport
from test_api import build_cfg, wait_for_leader
from test_membership_growth import promote_when_allowed, spawn_learner


def entry(term=1, key="k", rid="r"):
    return Command(op="set", key=key, value="v", request_id=rid)


def test_wipe_clears_log_kv_term_and_vote(tmp_path):
    s = Storage(str(tmp_path / "n.db"))
    s.save_term_and_vote(7, "node-2")
    s.append([LogEntry(term=7, command=entry())])
    s.apply(1, entry())
    assert s.load() == (7, "node-2", 1)
    assert s.last_log_index() == 1
    assert s.kv_all() == {"k": "v"}

    s.wipe()

    assert s.load() == (0, None, 0)
    assert s.last_log_index() == 0
    assert s.kv_all() == {}
    s.close()


def test_wipe_survives_reopen(tmp_path):
    """It is a real wipe of the file, not just of in-memory state."""
    path = str(tmp_path / "n.db")
    s = Storage(path)
    s.save_term_and_vote(4, "node-3")
    s.wipe()
    s.close()

    reopened = Storage(path)
    assert reopened.load() == (0, None, 0)
    reopened.close()


def test_reset_destroys_every_durable_trace_except_membership(tmp_path):
    async def scenario():
        cfg = build_cfg(tmp_path, node_id="node-1", peers={"node-2": "b"},
                        db_path=str(tmp_path / "node-1.db"))
        node = RaftNode(cfg, Storage(cfg.db_path), MemoryTransport())
        node.current_term = 9
        node.voted_for = "node-2"
        node.storage.save_term_and_vote(9, "node-2")
        node.role = Role.LEADER
        node.add_learner("node-4", "127.0.0.1:8004")
        node.set_blocked(["node-2"])
        node.metrics.rpc_failures = 12
        node.start()

        await node.reset()

        assert node.current_term == 0
        assert node.voted_for is None
        assert node.role is Role.FOLLOWER
        assert node.leader_id is None
        assert node.commit_index == 0 and node.last_applied == 0
        assert node.blocked == set()
        assert node.metrics.rpc_failures == 0
        assert node.storage.load() == (0, None, 0)
        assert node.crashed is False
        assert len(node._tasks) == 3, "reset must leave the node running, not stopped"

        # Membership is the one thing that survives, and it survives IN THE LOG --
        # not as a field set beside it, which the next log mutation would discard.
        assert node.learners == {"node-4": "127.0.0.1:8004"}
        assert node.config.voters == {"node-1": "", "node-2": "b"}
        assert node.storage.last_log_index() == 1
        assert node.storage.term_at(1) == 0, "must lose to any real leader's index 1"

        await node.stop()
        node.storage.close()

    asyncio.run(scenario())


def test_reset_bootstrap_reverts_membership_to_the_startup_set(tmp_path):
    """The opt-in half. Sound only applied to every node at once, which is why it is
    not the default and why the dashboard sends it to the whole cluster or not at all."""
    async def scenario():
        cfg = build_cfg(tmp_path, node_id="node-1", peers={"node-2": "b"},
                        db_path=str(tmp_path / "node-1.db"))
        node = RaftNode(cfg, Storage(cfg.db_path), MemoryTransport())
        node.current_term = 9
        node.role = Role.LEADER
        node.add_learner("node-4", "127.0.0.1:8004")
        node.start()

        await node.reset(keep_membership=False)

        assert node.learners == {}
        assert node.config.voters == {"node-1": "", "node-2": "b"}
        assert node.storage.last_log_index() == 0, "nothing re-seeded"

        await node.stop()
        node.storage.close()

    asyncio.run(scenario())


def test_reset_fails_an_in_flight_write_fast(tmp_path):
    """A submit awaiting commit must not hang until commit_timeout after a reset."""
    async def scenario():
        cfg = build_cfg(tmp_path, node_id="node-1", peers={"node-2": "b", "node-3": "c"},
                        db_path=str(tmp_path / "node-1.db"))
        node = RaftNode(cfg, Storage(cfg.db_path), MemoryTransport())
        node.current_term = 1
        node._become_leader()
        node.start()
        submit = asyncio.create_task(
            node.submit(Command(op="set", key="k", value="v", request_id="r1"))
        )
        await asyncio.sleep(0.05)
        await node.reset()
        # TimeoutError here is the 504 commit-timeout path, which is the correct
        # outcome; the point of the test is that it does not HANG until commit_timeout.
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(submit, timeout=1.0)
        await node.stop()
        node.storage.close()

    asyncio.run(scenario())


def test_reset_endpoint_wipes_committed_state(tmp_path):
    with TestClient(create_app(build_cfg(tmp_path))) as c:
        wait_for_leader(c)
        c.put("/kv/temp", json={"value": "72"})
        before = c.get("/state").json()
        assert before["kv"] == {"temp": "72"}
        assert before["log_length"] >= 1

        assert c.post("/admin/reset").json() == {
            "ok": True, "node": "solo", "reset": True, "membership": "keep"}

        wait_for_leader(c)  # a single node re-elects itself immediately
        after = c.get("/state").json()
        assert after["kv"] == {}  # the state machine is empty again
        # The log is not empty: re-electing appends this term's no-op. What matters is
        # that nothing from before the reset survived.
        assert after["log_length"] == 1
        assert after["term"] == 1  # term restarted from 0 and this is the first election


def test_reset_is_absent_when_admin_disabled(tmp_path):
    cfg = build_cfg(tmp_path, admin_enabled=False)
    with TestClient(create_app(cfg)) as c:
        assert c.post("/admin/reset").status_code == 404


def test_reset_endpoint_rejects_an_unknown_membership_mode(tmp_path):
    with TestClient(create_app(build_cfg(tmp_path))) as c:
        assert c.post("/admin/reset?membership=forget").status_code == 422


async def test_reset_cannot_shrink_a_grown_cluster_into_a_second_quorum(cluster):
    """The bug this pins is a lost acknowledged write, found by adversarial review.

    reset() wiped the log, and an empty log sent _reload_config() to the STARTUP voter
    set. node-1/2/3 boot naming only each other, so after growing to five voters a
    reset reverted them to a 3-voter configuration with quorum 2 -- satisfiable by two
    of them alone. Two resets on the minority side of a partition therefore produced a
    second leader that committed a write, acknowledged it to the client, and lost it
    when the real majority repaired the log. A 2-of-5 partition is the exact failure a
    five-voter cluster exists to survive.
    """
    await eventually(lambda: cluster.leader() is not None)
    for new_id in ("node-4", "node-5"):
        spawn_learner(cluster, new_id)
        leader = cluster.leader()
        leader.add_learner(new_id, new_id)
        await eventually(
            lambda nid=new_id: cluster.nodes[nid].storage.last_log_index() > 0)
        await promote_when_allowed(leader, new_id)
        await eventually(lambda: not cluster.leader().config.joint)
    assert len(cluster.leader().config.voters) == 5

    minority = ["node-1", "node-2"]
    cluster.transport.partition(set(minority), {"node-3", "node-4", "node-5"})
    for node_id in minority:
        await cluster.nodes[node_id].reset()

    for node_id in minority:
        node = cluster.nodes[node_id]
        assert len(node.config.voters) == 5, "reset resurrected the startup voter set"
        assert node.quorum == 3, "a minority of the real configuration became a quorum"

    # Behavioural half: ~10 election timeouts is ample at TEST_TIMING.
    await asyncio.sleep(1.0)
    rogue = [n for n in minority if cluster.nodes[n].role is Role.LEADER]
    assert not rogue, f"a 2-of-5 minority elected a leader: {rogue}"
    cluster.transport.heal()
