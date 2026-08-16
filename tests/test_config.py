import pytest
from pydantic import ValidationError

from raftkv.config import NodeConfig


def test_from_env_parses_peers(monkeypatch):
    monkeypatch.setenv("RAFT_NODE_ID", "node-1")
    monkeypatch.setenv("RAFT_PEERS", "node-2=raft-node-2:8000,node-3=raft-node-3:8000")
    monkeypatch.setenv("RAFT_DB_PATH", "/data/raft.db")
    cfg = NodeConfig.from_env()
    assert cfg.node_id == "node-1"
    assert cfg.peers == {"node-2": "raft-node-2:8000", "node-3": "raft-node-3:8000"}
    assert cfg.db_path == "/data/raft.db"


def test_from_env_single_node(monkeypatch):
    monkeypatch.setenv("RAFT_NODE_ID", "solo")
    monkeypatch.delenv("RAFT_PEERS", raising=False)
    cfg = NodeConfig.from_env()
    assert cfg.peers == {}


def test_timeout_range_and_tick():
    cfg = NodeConfig(
        node_id="n1", election_timeout_min=1.0, election_timeout_max=2.0,
        heartbeat_interval=0.2, rpc_timeout=0.2,
    )
    assert cfg.election_timeout_range == (1.0, 2.0)
    assert cfg.tick_interval == pytest.approx(0.1)


def test_rejects_inverted_timeouts():
    with pytest.raises(ValidationError):
        NodeConfig(node_id="n1", election_timeout_min=3.0, election_timeout_max=1.0)


def test_rejects_heartbeat_slower_than_election():
    with pytest.raises(ValidationError):
        NodeConfig(node_id="n1", heartbeat_interval=2.0, election_timeout_min=1.5)


def test_rejects_rpc_timeout_eating_the_liveness_margin():
    """One blackholed peer stalls a replication round for rpc_timeout; the next
    heartbeat must still land inside election_timeout_min or followers churn."""
    with pytest.raises(ValidationError):
        NodeConfig(
            node_id="n1", rpc_timeout=1.2, heartbeat_interval=0.5, election_timeout_min=1.5
        )
