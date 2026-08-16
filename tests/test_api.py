import time

import pytest
from fastapi.testclient import TestClient

from raftkv.app import create_app
from raftkv.config import NodeConfig


def build_cfg(tmp_path, **overrides):
    params = dict(
        node_id="solo", peers={}, db_path=str(tmp_path / "solo.db"),
        log_dir=str(tmp_path / "logs"), heartbeat_interval=0.03,
        election_timeout_min=0.1, election_timeout_max=0.2,
        rpc_timeout=0.05, commit_timeout=2.0,
    )
    params.update(overrides)
    return NodeConfig(**params)


@pytest.fixture
def client(tmp_path):
    with TestClient(create_app(build_cfg(tmp_path))) as c:  # `with` runs lifespan
        yield c


def wait_for_leader(client, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if client.get("/state").json()["role"] == "leader":
            return
        time.sleep(0.02)
    raise AssertionError("single node never became leader")


def test_put_get_round_trip(client):
    wait_for_leader(client)
    assert client.put("/kv/temp", json={"value": "72"}).status_code == 200
    body = client.get("/kv/temp").json()
    assert body["value"] == "72"
    assert body["read_from"] == "solo"


def test_get_missing_key_404(client):
    assert client.get("/kv/nope").status_code == 404


def test_delete_key(client):
    wait_for_leader(client)
    client.put("/kv/gone", json={"value": "x"})
    assert client.delete("/kv/gone").status_code == 200
    assert client.get("/kv/gone").status_code == 404


def test_put_on_follower_returns_leader_hint(tmp_path):
    cfg = build_cfg(
        tmp_path, node_id="f1", peers={"node-2": "nowhere:1"},
        heartbeat_interval=0.05, election_timeout_min=30.0, election_timeout_max=60.0,
    )
    with TestClient(create_app(cfg)) as c:  # huge election timeout: stays follower
        r = c.put("/kv/k", json={"value": "v"})
        assert r.status_code == 503
        assert r.json()["error"] == "not_leader"


def test_validation_rejects_empty_value(client):
    wait_for_leader(client)
    assert client.put("/kv/k", json={"value": ""}).status_code == 422


def test_state_endpoint_shape(client):
    body = client.get("/state").json()
    assert {"node_id", "role", "term", "commit_index", "kv", "metrics"} <= set(body)


def test_healthz(client):
    assert client.get("/healthz").json() == {"ok": True, "node": "solo"}


def test_state_exposes_peer_addresses_not_just_ids(client, tmp_path):
    """/state carries the peer map so a client has somewhere to dial. Nodes can
    legitimately disagree about it, which is why it is reported per node."""
    cfg = build_cfg(tmp_path, peers={"node-2": "10.0.0.2:8000"})
    with TestClient(create_app(cfg)) as c:
        assert c.get("/state").json()["peers"] == {"node-2": "10.0.0.2:8000"}


def test_logs_endpoint_and_pii_redaction(client):
    wait_for_leader(client)
    client.put("/kv/heart_rate", json={"value": "61bpm-secret"})
    events = client.get("/logs").json()
    assert any(e.get("event") == "submitted" for e in events)
    assert "61bpm-secret" not in str(events)  # PII policy: values never logged


def test_unhandled_exception_returns_structured_500(tmp_path):
    app = create_app(build_cfg(tmp_path))

    @app.get("/boom")
    async def boom():
        raise RuntimeError("kaput")

    with TestClient(app, raise_server_exceptions=False) as c:
        r = c.get("/boom")
        assert r.status_code == 500
        assert r.json() == {"error": "internal"}
