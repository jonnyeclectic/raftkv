"""Simulated network partitions, driven from the dashboard.

A partition is not a crash, and the gap between them is the most interesting thing
this cluster does: a partitioned leader stays up, keeps accepting writes, and cannot
commit a single one of them. Crash-testing never shows you that, because a crashed
leader simply stops.

Since CheckQuorum landed that gap is TIME-BOUNDED rather than permanent (thesis §6.2).
The leader still accepts and still cannot commit -- nothing about the quorum arithmetic
moved -- but it now notices its own silence and resigns after one election timeout
instead of claiming the role for the length of the partition. Both halves are asserted
below, and the ordering between them is the whole behaviour: the window is real, and it
closes.
"""

import asyncio

import pytest
from fastapi.testclient import TestClient

from conftest import eventually
from raftkv.app import create_app
from raftkv.models import Command, Role
from raftkv.raft import RaftNode
from raftkv.storage import Storage
from raftkv.transport import MemoryTransport
from test_api import build_cfg, wait_for_leader


def make_node(tmp_path, node_id="node-1", peers=("node-2", "node-3")):
    cfg = build_cfg(tmp_path, node_id=node_id, peers={p: p for p in peers},
                    db_path=str(tmp_path / f"{node_id}.db"))
    return RaftNode(cfg, Storage(cfg.db_path), MemoryTransport())


def test_blocked_peer_is_skipped_outbound_and_counted_as_an_rpc_failure(tmp_path):
    node = make_node(tmp_path)
    node.set_blocked(["node-2"])
    node.current_term = 1
    node._become_leader()

    asyncio.run(node._append_to_peer("node-2", term_when_sent=1))
    assert node.metrics.rpc_failures == 1
    assert node.match_index["node-2"] == 0  # nothing was delivered
    node.storage.close()


def test_partitioned_leader_accepts_the_write_and_cannot_commit_it(tmp_path):
    """The headline behaviour. Quorum is 2 of 3; with both followers unreachable the
    leader holds the entry in its log and the client times out.

    The write is submitted before the node starts, so the assertions land inside the
    CheckQuorum window rather than racing it: the point here is that a partitioned leader
    is not a crashed one. It is up, it is still the leader, and it accepts work it will
    never be able to finish. The step-down that ends that window is the test below."""
    async def scenario():
        node = make_node(tmp_path)
        node.current_term = 1
        node._become_leader()
        node.set_blocked(["node-2", "node-3"])
        node.start()

        with pytest.raises(TimeoutError):
            await asyncio.wait_for(
                node.submit(Command(op="set", key="k", value="v", request_id="r1")),
                timeout=0.15,  # inside election_timeout_max (0.2), so still leading
            )
        # index 1 is the no-op appended on winning; index 2 is the doomed write
        assert node.storage.last_log_index() == 2  # appended...
        assert node.commit_index == 0              # ...neither can ever commit
        assert node.role is Role.LEADER            # and still leader: this is not a crash
        await node.stop()
        node.storage.close()

    asyncio.run(scenario())


def test_a_partitioned_leader_resigns_once_it_has_heard_from_nobody(tmp_path):
    """CheckQuorum (thesis §6.2). Raft guarantees one leader per TERM, not one leader --
    so nothing in Figure 2 ever tells a leader it has been replaced when the telling is
    what the partition prevents. Its own silence has to be the signal.

    Without this the node above stays `role: leader` for the length of the partition, and
    every read it serves is stale for exactly that long: unbounded in time, not merely in
    value. The dashboard hides it by polling all three nodes and believing the highest
    term, which a client behind one address cannot do.

    Resigning must NOT bump the term -- this is a resignation, not a campaign -- and with
    PreVote in front of the election timer the deposed node then stays quiet instead of
    burning a term per timeout. That pairing is what makes the resignation free: see
    tests/test_pre_vote.py."""
    async def scenario():
        node = make_node(tmp_path)
        node.current_term = 1
        node._become_leader()
        node.set_blocked(["node-2", "node-3"])
        node.start()

        await eventually(lambda: node.role is not Role.LEADER, timeout=2.0)
        assert node.leader_id is None       # it does not know who leads now, and says so
        assert node.current_term == 1       # a resignation, not a campaign
        await node.stop()
        node.storage.close()

    asyncio.run(scenario())


def test_a_lone_voter_never_resigns(tmp_path):
    """The boundary CheckQuorum must not cross. A single-voter cluster IS its own
    majority, so it is in contact with a quorum by definition and no amount of silence
    from nobody can depose it. Getting this wrong turns a one-node cluster into a node
    that resigns every election timeout and can never be written to."""
    async def scenario():
        node = make_node(tmp_path, peers=())
        node.current_term = 1
        node._become_leader()
        node.start()
        await asyncio.sleep(0.5)  # several election timeouts at test timing
        assert node.role is Role.LEADER
        await node.stop()
        node.storage.close()

    asyncio.run(scenario())


def test_set_blocked_replaces_and_heals(tmp_path):
    node = make_node(tmp_path)
    node.set_blocked(["node-2", "node-3"])
    assert node.blocked == {"node-2", "node-3"}
    node.set_blocked(["node-3"])  # replace, not accumulate
    assert node.blocked == {"node-3"}
    node.set_blocked([])
    assert node.blocked == set()
    node.storage.close()


def test_set_blocked_rejects_a_node_that_is_not_a_peer(tmp_path):
    node = make_node(tmp_path)
    with pytest.raises(ValueError):
        node.set_blocked(["node-9"])
    assert node.blocked == set()  # nothing applied on the failing call
    node.storage.close()


def test_a_learner_link_can_be_partitioned_too(tmp_path):
    node = make_node(tmp_path)
    node.current_term = 1
    node._become_leader()
    node.add_learner("node-4", "127.0.0.1:8004")
    node.set_blocked(["node-4"])
    assert node.blocked == {"node-4"}
    node.storage.close()


def test_inbound_rpcs_are_refused_from_a_blocked_peer(tmp_path):
    """A one-way cut is not a partition. Both halves have to close."""
    with TestClient(create_app(build_cfg(tmp_path, peers={"node-2": "x"}))) as c:
        c.post("/admin/partition", json={"peers": ["node-2"]})
        vote = {"term": 9, "candidate_id": "node-2",
                "last_log_index": 0, "last_log_term": 0}
        beat = {"term": 9, "leader_id": "node-2", "prev_log_index": 0,
                "prev_log_term": 0, "entries": [], "leader_commit": 0}
        assert c.post("/raft/request-vote", json=vote).status_code == 503
        assert c.post("/raft/append-entries", json=beat).status_code == 503

        # an unblocked peer still gets through
        vote["candidate_id"] = "node-3"
        beat["leader_id"] = "node-3"
        assert c.post("/raft/request-vote", json=vote).status_code == 200
        assert c.post("/raft/append-entries", json=beat).status_code == 200


def test_partition_endpoint_round_trip_and_state_exposure(tmp_path):
    # No wait_for_leader: with real peers this node can never reach quorum alone,
    # and /admin/partition is deliberately not leader-only -- you cut links on the
    # node you are cutting them from.
    with TestClient(create_app(build_cfg(tmp_path, peers={"node-2": "x", "node-3": "y"}))) as c:
        r = c.post("/admin/partition", json={"peers": ["node-2"]})
        assert r.json() == {"ok": True, "node": "solo", "blocked": ["node-2"]}
        assert c.get("/state").json()["blocked"] == ["node-2"]

        assert c.post("/admin/partition", json={"peers": []}).json()["blocked"] == []
        assert c.get("/state").json()["blocked"] == []


def test_partition_endpoint_rejects_unknown_peers(tmp_path):
    with TestClient(create_app(build_cfg(tmp_path, peers={"node-2": "x"}))) as c:
        assert c.post("/admin/partition", json={"peers": ["nope"]}).status_code == 422
        assert c.get("/state").json()["blocked"] == []


def test_partition_is_refused_while_crashed(tmp_path):
    """A crashed node answers nothing; partitioning one is a contradiction."""
    with TestClient(create_app(build_cfg(tmp_path))) as c:
        wait_for_leader(c)
        c.post("/admin/crash")
        assert c.post("/admin/partition", json={"peers": []}).status_code == 503
