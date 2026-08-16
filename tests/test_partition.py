"""Simulated network partitions.

A partition is not a crash, and the gap between them is the most interesting thing
this cluster does: a partitioned leader stays up, keeps accepting writes, and cannot
commit a single one of them. Crash-testing never shows you that, because a crashed
leader simply stops.
"""

import asyncio

import pytest
from fastapi.testclient import TestClient

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
    leader holds the entry in its log forever and the client times out."""
    async def scenario():
        node = make_node(tmp_path)
        node.current_term = 1
        node._become_leader()
        node.set_blocked(["node-2", "node-3"])
        node.start()

        with pytest.raises(TimeoutError):
            await asyncio.wait_for(
                node.submit(Command(op="set", key="k", value="v", request_id="r1")),
                timeout=1.5,
            )
        assert node.storage.last_log_index() == 1  # the doomed write, appended...
        assert node.commit_index == 0              # ...and it can never commit
        assert node.role is Role.LEADER            # still leader: this is not a crash
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
