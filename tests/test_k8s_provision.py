"""The k8s provisioning backend: `POST /admin/spawn-node` as a StatefulSet scale-up.

Weighted the same way test_provision.py is — toward the properties that make an
unauthenticated button admissible, not toward the happy path:

  1. it runs nowhere but a demonstrable Kubernetes pod (env var AND mounted
     ServiceAccount, never /.dockerenv), so no configuration mistake can point it at
     docker, compose, or a bare host
  2. it grows by exactly one, under the cap, optimistically locked — no input reaches
     the replica count except `current + 1`
  3. a scale it cannot vouch for is rolled back, mirroring the subprocess backend
     terminating an unhealthy child

No cluster anywhere: the API server and the new pod are both played by an
httpx.MockTransport, which k8s_provision.scale_up accepts for exactly this purpose. The
requests the fake receives ARE the assertions — which path, which verb, which body.
"""

import json
import pathlib

import httpx
import pytest
from fastapi.testclient import TestClient

from conftest import scaled
from raftkv import k8s_provision
from raftkv.app import create_app
from raftkv.config import NodeConfig
from raftkv.provision import ProvisionError

# The real refusal ladder starts at /.dockerenv, so on a host that is itself a container
# the environment tests below would refuse for the HOST's reason. Same guard as
# test_provision.py's NOT_A_CONTAINER.
NOT_A_CONTAINER = pytest.mark.skipif(
    pathlib.Path("/.dockerenv").exists(),
    reason="the host is itself a container; the refusal fires for its reason",
)

SCALE_PATH = "/apis/apps/v1/namespaces/default/statefulsets/node/scale"


@pytest.fixture
def pod(monkeypatch, tmp_path):
    """The two halves of 'demonstrably a pod': the kubelet's env var and a mounted
    ServiceAccount. Pointed at tmp_path so no test ever reads the real mount."""
    sa = tmp_path / "serviceaccount"
    sa.mkdir()
    (sa / "token").write_text("test-token\n")
    (sa / "ca.crt").write_text("not a real cert; MockTransport skips verification\n")
    (sa / "namespace").write_text("default")
    monkeypatch.setattr(k8s_provision, "_SA_DIR", sa)
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.96.0.1")
    monkeypatch.setenv("KUBERNETES_SERVICE_PORT", "443")
    return sa


class FakeCluster:
    """Answers as the API server (by host) and as the new pod's /healthz (by DNS name),
    recording every request so tests can assert on the wire rather than the wrapper."""

    def __init__(self, replicas=3, healthy=True, patch_status=200, get_status=200):
        self.replicas = replicas
        self.healthy = healthy
        self.patch_status = patch_status
        self.get_status = get_status
        self.patches: list[dict] = []
        self.requests: list[httpx.Request] = []
        self.transport = httpx.MockTransport(self._handle)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if "raftkv-hl" in (request.url.host or ""):
            return httpx.Response(200 if self.healthy else 503)
        assert request.url.path == SCALE_PATH, f"unexpected API path {request.url.path}"
        assert request.headers["authorization"] == "Bearer test-token"
        if request.method == "GET":
            return httpx.Response(
                self.get_status,
                json={
                    "metadata": {"resourceVersion": "41"},
                    "spec": {"replicas": self.replicas},
                },
            )
        assert request.method == "PATCH"
        assert request.headers["content-type"] == "application/merge-patch+json"
        self.patches.append(json.loads(request.content))
        return httpx.Response(self.patch_status, json={})


# ---- the environment wall --------------------------------------------------------------


@NOT_A_CONTAINER
async def test_it_refuses_anywhere_the_kubelet_env_is_absent(monkeypatch):
    """A bare host, a compose container, a dev shell: none of them carry the var the
    kubelet injects unconditionally, so the flag alone must buy nothing there."""
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    with pytest.raises(ProvisionError, match="only runs inside a Kubernetes pod"):
        await k8s_provision.scale_up(max_nodes=12)


@NOT_A_CONTAINER
async def test_the_env_var_alone_is_not_a_pod(monkeypatch, tmp_path):
    """The var can leak into places that are not pods (direnv, a k8s toolbox shell).
    Without mounted ServiceAccount credentials no API call could be authenticated
    anyway, so their absence fails the check closed rather than failing the call."""
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.96.0.1")
    monkeypatch.setattr(k8s_provision, "_SA_DIR", tmp_path / "nothing-mounted")
    with pytest.raises(ProvisionError, match="ServiceAccount"):
        await k8s_provision.scale_up(max_nodes=12)


# ---- growth, bounded and locked --------------------------------------------------------


async def test_it_scales_by_exactly_one_under_the_read_lock(pod):
    """No caller input reaches the replica count: the PATCH says `current + 1` and
    carries the resourceVersion from the read, so a concurrent change conflicts at the
    API server instead of racing this one to the same answer."""
    cluster = FakeCluster(replicas=3)
    result = await k8s_provision.scale_up(max_nodes=12, transport=cluster.transport)
    assert cluster.patches == [
        {"metadata": {"resourceVersion": "41"}, "spec": {"replicas": 4}}
    ]
    assert result["node_id"] == "node-4"
    assert result["addr"] == "127.0.0.1:8004", "the port-forward convention: node-N ↔ 800N"
    assert result["staged"] is True, "a replica is a process, not a member"


async def test_a_lost_race_is_a_refusal_not_a_double_count(pod):
    cluster = FakeCluster(replicas=3, patch_status=409)
    with pytest.raises(ProvisionError, match="another scale change landed first"):
        await k8s_provision.scale_up(max_nodes=12, transport=cluster.transport)


async def test_the_cap_holds_before_any_write(pod):
    cluster = FakeCluster(replicas=5)
    with pytest.raises(ProvisionError, match="RAFT_PROVISION_MAX"):
        await k8s_provision.scale_up(max_nodes=5, transport=cluster.transport)
    assert cluster.patches == [], "refused, but patched anyway"


async def test_an_explicit_ordinal_cannot_jump_the_sequence(pod):
    """StatefulSet ordinals are sequential by construction; an explicit ordinal is only
    honoured when it names the replica the scale-up would create anyway."""
    cluster = FakeCluster(replicas=3)
    with pytest.raises(ProvisionError, match="node-4"):
        await k8s_provision.scale_up(7, max_nodes=12, transport=cluster.transport)
    assert cluster.patches == []
    result = await k8s_provision.scale_up(4, max_nodes=12, transport=cluster.transport)
    assert result["node_id"] == "node-4"


async def test_a_missing_role_reads_as_the_fix_not_a_500(pod):
    """403 from the API server means the RBAC triple in k8s/raftkv.yaml is missing or
    unbound — a deployment fact the operator can act on, so it must arrive as the 409
    refusal text, not as raise_for_status noise."""
    cluster = FakeCluster(get_status=403)
    with pytest.raises(ProvisionError, match="raftkv-provisioner"):
        await k8s_provision.scale_up(max_nodes=12, transport=cluster.transport)


async def test_a_pod_that_never_answers_rolls_the_scale_back(pod, monkeypatch):
    """The mirror of the subprocess backend reaping a child that never got healthy:
    never leave a spawn the endpoint cannot vouch for. The rollback is the one
    scale-DOWN this module ever writes, and it only ever restores the count it read."""
    monkeypatch.setattr(k8s_provision, "HEALTH_TIMEOUT", 0.3)
    monkeypatch.setattr(k8s_provision, "HEALTH_INTERVAL", 0.05)
    cluster = FakeCluster(replicas=3, healthy=False)
    with pytest.raises(ProvisionError, match="rolled back"):
        await k8s_provision.scale_up(max_nodes=12, transport=cluster.transport)
    assert [p["spec"]["replicas"] for p in cluster.patches] == [4, 3]
    assert "metadata" not in cluster.patches[1], (
        "the rollback is best-effort by design: locking it to the stale "
        "resourceVersion would guarantee it loses to the change it is undoing"
    )


# ---- the HTTP surface -------------------------------------------------------------------


@NOT_A_CONTAINER
def test_the_flag_routes_the_endpoint_to_the_k8s_backend(tmp_path, monkeypatch):
    """RAFT_PROVISION_K8S=1 must swap the backend BEFORE any subprocess code runs: on a
    non-pod the k8s backend's own refusal comes back as the 409 detail, naming the flag
    — proof the dispatch went right, with no cluster anywhere. (The subprocess path's
    refusal would talk about containers instead.)"""
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    cfg = NodeConfig(
        node_id="solo", peers={}, db_path=str(tmp_path / "solo.db"),
        log_dir=str(tmp_path / "logs"), provision_k8s=True,
        **scaled(heartbeat_interval=0.03, election_timeout_min=0.1, election_timeout_max=0.2),
        rpc_timeout=0.05, commit_timeout=2.0,
    )
    with TestClient(create_app(cfg)) as c:
        r = c.post("/admin/spawn-node", json={})
    assert r.status_code == 409
    assert "RAFT_PROVISION_K8S" in r.json()["detail"]
