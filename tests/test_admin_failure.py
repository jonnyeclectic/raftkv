"""The dashboard's kill/revive buttons, from the API side.

These exist so failure can be injected with no shell, no docker CLI and no
kubectl. The contract that matters: a crashed node must be indistinguishable from a
dead one to its peers, and recovering must reload durable state from disk rather than
from anything the crash left behind in memory.
"""

import pytest
from fastapi.testclient import TestClient

from raftkv.app import create_app
from raftkv.config import NodeConfig
from raftkv.models import Role
from test_api import build_cfg, wait_for_leader


@pytest.fixture
def client(tmp_path):
    with TestClient(create_app(build_cfg(tmp_path))) as c:
        yield c


def test_crash_makes_every_raft_and_client_surface_refuse(client):
    wait_for_leader(client)
    assert client.post("/admin/crash").json() == {
        "ok": True, "node": "solo", "crashed": True
    }
    # Bodies are the real thing a peer sends: FastAPI validates before the handler
    # runs, so a malformed RPC would 422 on the way in and never reach the crash check.
    vote = {"term": 9, "candidate_id": "node-2", "last_log_index": 0, "last_log_term": 0}
    beat = {"term": 9, "leader_id": "node-2", "prev_log_index": 0, "prev_log_term": 0,
            "entries": [], "leader_commit": 0}
    responses = {
        "/state": client.get("/state"),
        "/healthz": client.get("/healthz"),
        "/kv/anything": client.get("/kv/anything"),
        "/raft/request-vote": client.post("/raft/request-vote", json=vote),
        "/raft/append-entries": client.post("/raft/append-entries", json=beat),
    }
    for path, r in responses.items():
        assert r.status_code == 503, f"{path} answered {r.status_code} while crashed"


def test_dashboard_and_recover_stay_reachable_while_crashed(client):
    """Everything else is dark, but the page that owns the revive button is not —
    otherwise the only way back is a terminal, which is exactly what this avoids."""
    wait_for_leader(client)
    client.post("/admin/crash")
    assert client.get("/").status_code == 200
    assert client.post("/admin/recover").status_code == 200


def test_crash_is_idempotent_and_so_is_recover(client):
    wait_for_leader(client)
    assert client.post("/admin/crash").status_code == 200
    assert client.post("/admin/crash").status_code == 200  # not an error, still down
    assert client.post("/admin/recover").status_code == 200
    assert client.post("/admin/recover").status_code == 200
    assert client.get("/state").status_code == 200


def test_recover_reloads_durable_state_and_drops_volatile_state(client):
    """The whole point of the button: prove persistence, not restart cosmetics."""
    wait_for_leader(client)
    client.put("/kv/temp", json={"value": "72"})
    before = client.get("/state").json()
    assert before["kv"] == {"temp": "72"}

    client.post("/admin/crash")
    client.post("/admin/recover")
    after = client.get("/state").json()

    assert after["kv"] == {"temp": "72"}  # applied state survived
    assert after["term"] == before["term"]  # term is durable
    assert after["log_length"] == before["log_length"]  # so is the log
    assert after["role"] == Role.FOLLOWER  # volatile: a restart never resumes as leader
    assert after["leader_id"] is None


def test_in_flight_write_fails_rather_than_hanging_when_the_node_crashes(tmp_path):
    """A submit awaiting commit must not sit there until commit_timeout. It is
    resolved as a TimeoutError, which is already the 504 path."""
    import asyncio

    from raftkv.models import Command
    from raftkv.raft import RaftNode
    from raftkv.storage import Storage
    from raftkv.transport import MemoryTransport

    async def scenario():
        cfg = build_cfg(tmp_path, peers={"node-2": "x", "node-3": "y"})
        node = RaftNode(cfg, Storage(cfg.db_path), MemoryTransport())
        node.role = Role.LEADER  # no quorum reachable, so the write cannot commit
        node.current_term = 1
        node.start()
        submit = asyncio.create_task(
            node.submit(Command(op="set", key="k", value="v", request_id="r1"))
        )
        await asyncio.sleep(0.05)
        await node.crash()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(submit, timeout=1.0)  # NOT cfg.commit_timeout
        node.storage.close()

    asyncio.run(scenario())


def test_admin_endpoints_absent_when_disabled(tmp_path):
    """RAFT_ADMIN_ENABLED=0 is how a real deployment removes an unauthenticated
    remote stop-serving control."""
    cfg = build_cfg(tmp_path, admin_enabled=False)
    with TestClient(create_app(cfg)) as c:
        assert c.post("/admin/crash").status_code == 404
        assert c.post("/admin/recover").status_code == 404
        assert c.get("/state").status_code == 200


def test_admin_enabled_parses_from_env(monkeypatch, tmp_path):
    monkeypatch.setenv("RAFT_NODE_ID", "n1")
    monkeypatch.setenv("RAFT_DB_PATH", str(tmp_path / "n1.db"))
    monkeypatch.setenv("RAFT_ADMIN_ENABLED", "0")
    assert NodeConfig.from_env().admin_enabled is False
    monkeypatch.setenv("RAFT_ADMIN_ENABLED", "1")
    assert NodeConfig.from_env().admin_enabled is True
