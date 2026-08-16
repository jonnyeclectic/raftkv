"""Non-voting members (§6 staging, without §6's configuration changes).

A learner is safe to attach to a live cluster *only* because it cannot affect any
quorum. Every test here defends that one property from a different direction, because
the failure mode is not a crash — it is a silently smaller majority, which is how you
lose committed data.
"""

import asyncio

import pytest
from fastapi.testclient import TestClient

from raftkv.app import create_app
from raftkv.config import NodeConfig
from raftkv.models import Command, LogEntry, RequestVoteRequest, Role
from raftkv.raft import NotLeaderError, RaftNode
from raftkv.storage import Storage
from raftkv.transport import MemoryTransport
from test_api import build_cfg, wait_for_leader


def make_leader(tmp_path, peers):
    cfg = build_cfg(tmp_path, node_id="leader", peers=peers,
                    db_path=str(tmp_path / "leader.db"))
    node = RaftNode(cfg, Storage(cfg.db_path), MemoryTransport())
    node.current_term = 1
    node._become_leader()
    return node


def test_adding_a_learner_does_not_change_quorum(tmp_path):
    """THE property. Quorum is computed from cfg.peers, and a learner must never
    reach that dict — 2-of-3 must not silently become 3-of-4."""
    node = make_leader(tmp_path, {"node-2": "a", "node-3": "b"})
    assert node.quorum == 2
    node.add_learner("node-4", "127.0.0.1:8004")
    assert node.quorum == 2, "learner moved the quorum"
    assert "node-4" not in node.cfg.peers
    node.storage.close()


def test_learner_is_replicated_to_but_never_counted_for_commit(tmp_path):
    """A learner acking an entry must not commit it. With one voting peer silent, the
    entry stays uncommitted no matter how enthusiastically the learner replies."""
    node = make_leader(tmp_path, {"node-2": "a", "node-3": "b"})
    node.add_learner("node-4", "127.0.0.1:8004")
    node.storage.append(
        [LogEntry(term=1, command=Command(op="set", key="k", value="v", request_id="r"))]
    )
    assert node._replication_targets() == ["node-2", "node-3", "node-4"]

    node.match_index["node-4"] = 1  # the learner has it...
    node._advance_commit_index()
    assert node.commit_index == 0, "a learner's ack committed an entry"

    node.match_index["node-2"] = 1  # ...now a real voter does
    node._advance_commit_index()
    assert node.commit_index == 1
    node.storage.close()


def test_learner_never_campaigns(tmp_path):
    """An idle learner with no peers would have quorum == 1 and elect itself within
    one timeout. The learner flag is the only thing stopping it."""
    async def scenario():
        cfg = build_cfg(tmp_path, node_id="node-4", peers={}, learner=True,
                        db_path=str(tmp_path / "n4.db"))
        node = RaftNode(cfg, Storage(cfg.db_path), MemoryTransport())
        assert node.quorum == 1  # it WOULD win instantly if it ever ran
        node.start()
        await asyncio.sleep(cfg.election_timeout_max * 3)
        assert node.role is Role.FOLLOWER
        assert node.metrics.elections_started == 0
        await node.stop()
        node.storage.close()

    asyncio.run(scenario())


def test_learner_refuses_to_vote(tmp_path):
    """Even if someone asks. A learner's vote would corrupt a majority count."""
    cfg = build_cfg(tmp_path, node_id="node-4", peers={}, learner=True,
                    db_path=str(tmp_path / "n4.db"))
    node = RaftNode(cfg, Storage(cfg.db_path), MemoryTransport())
    resp = node.handle_request_vote(
        RequestVoteRequest(term=5, candidate_id="node-1", last_log_index=0, last_log_term=0)
    )
    assert not resp.vote_granted
    assert node.voted_for is None
    node.storage.close()


def test_add_learner_is_idempotent_and_rejects_existing_voters(tmp_path):
    node = make_leader(tmp_path, {"node-2": "a"})
    node.add_learner("node-4", "127.0.0.1:8004")
    node.add_learner("node-4", "127.0.0.1:8004")  # button pressed twice
    assert node.learners == {"node-4": "127.0.0.1:8004"}
    with pytest.raises(ValueError):
        node.add_learner("node-2", "127.0.0.1:8002")  # already a voter
    with pytest.raises(ValueError):
        node.add_learner("leader", "127.0.0.1:8001")  # itself
    node.storage.close()


def test_only_the_leader_accepts_a_learner(tmp_path):
    cfg = build_cfg(tmp_path, node_id="follower", peers={"node-2": "a"},
                    db_path=str(tmp_path / "f.db"))
    node = RaftNode(cfg, Storage(cfg.db_path), MemoryTransport())
    with pytest.raises(NotLeaderError):
        node.add_learner("node-4", "127.0.0.1:8004")
    node.storage.close()


def test_add_learner_endpoint_round_trip(tmp_path):
    with TestClient(create_app(build_cfg(tmp_path))) as c:
        wait_for_leader(c)
        r = c.post("/admin/add-learner",
                   json={"node_id": "node-4", "addr": "127.0.0.1:8004"})
        assert r.status_code == 200
        state = c.get("/state").json()
        assert state["learners"] == {"node-4": "127.0.0.1:8004"}
        assert state["peers"] == {}  # still not a voter
        # a second add is fine; adding a voter is a conflict
        assert c.post("/admin/add-learner",
                      json={"node_id": "node-4", "addr": "127.0.0.1:8004"}).status_code == 200
        assert c.post("/admin/add-learner",
                      json={"node_id": "solo", "addr": "127.0.0.1:8001"}).status_code == 409


def test_add_learner_rejects_a_malformed_address(tmp_path):
    """The address is dialled by the leader, so it is a trust boundary."""
    with TestClient(create_app(build_cfg(tmp_path))) as c:
        wait_for_leader(c)
        for bad in ["http://evil.example/x", "no-port", "host:99999999", ""]:
            r = c.post("/admin/add-learner", json={"node_id": "n", "addr": bad})
            assert r.status_code == 422, f"{bad!r} was accepted"


def test_transport_add_peer_cannot_write_through_to_the_voting_set():
    """Regression: HttpTransport used to alias NodeConfig.peers, so registering a
    learner's dial address added it to the voting set — and _advance_commit_index()
    reads that dict live, so the learner would have counted toward commit."""
    from raftkv.transport import HttpTransport

    voting = {"node-2": "a", "node-3": "b"}
    transport = HttpTransport(voting, rpc_timeout=0.1)
    transport.add_peer("node-4", "127.0.0.1:8004")
    assert voting == {"node-2": "a", "node-3": "b"}


def test_learner_flags_parse_from_env(monkeypatch, tmp_path):
    monkeypatch.setenv("RAFT_NODE_ID", "node-4")
    monkeypatch.setenv("RAFT_DB_PATH", str(tmp_path / "n4.db"))
    monkeypatch.setenv("RAFT_LEARNER", "1")
    monkeypatch.setenv("RAFT_ADVERTISE", "raft-node-4:8000")
    cfg = NodeConfig.from_env()
    assert cfg.learner is True
    assert cfg.advertise_addr == "raft-node-4:8000"
