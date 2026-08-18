"""`POST /admin/spawn-node`, and the module behind it.

Provisioning is the sharpest endpoint in the build: every other admin route makes *this*
process misbehave, while this one starts new ones, unauthenticated, on request. So the
tests here are weighted toward the three properties that keep it from being a remote
code execution endpoint rather than toward the happy path:

  1. the argv is a constant shaped by one integer — no shell, no interpolation
  2. it is bounded — a cap, and a floor at node-4
  3. it refuses inside a container, where the child would be unreachable by design

Nothing here spawns a real process. A test that binds a port and forks a uvicorn worker
is slow, is flaky on a loaded CI runner, and would leak a listener into every later test
in the session if it failed halfway. The subprocess call and the health wait are both
stubbed, and what is asserted is the decision — which ordinal, which argv, which
environment — because that is the part with the bugs in it.
"""

import asyncio

import pytest
from fastapi.testclient import TestClient

from conftest import scaled
from raftkv import provision
from raftkv.app import create_app
from raftkv.config import NodeConfig
from raftkv.provision import FIRST_ORDINAL, PORT_BASE, ProvisionError


@pytest.fixture(autouse=True)
def clean_registry():
    """The child registry is module state, so one test's fake children are the next
    test's occupied slots unless it is cleared between them."""
    provision._reset_for_tests()
    yield
    provision._reset_for_tests()


class FakeChild:
    """Enough of asyncio.subprocess.Process for the registry and the failure path."""

    def __init__(self, pid=4242):
        self.pid = pid
        self.returncode = None
        self.terminated = False

    def terminate(self):
        self.terminated = True
        self.returncode = -15


@pytest.fixture
def spawned(monkeypatch):
    """Record what would have been executed, without executing it."""
    calls = []

    async def fake_exec(*argv, **kwargs):
        calls.append({"argv": list(argv), **kwargs})
        return FakeChild()

    async def instantly_healthy(addr, timeout=None):
        return None

    async def nothing_is_listening(port):
        return False

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr(provision, "_wait_healthy", instantly_healthy)
    monkeypatch.setattr(provision, "_port_is_bound", nothing_is_listening)
    monkeypatch.setattr(provision, "_containerised", lambda: False)
    return calls


# ---- the security argument, asserted rather than commented --------------------------


async def test_the_command_is_a_constant_shaped_by_one_integer(spawned, tmp_path):
    """The only caller-supplied value is an ordinal, and it reaches the command line as
    a port number and nothing else. If a future edit ever interpolates a string into
    this list, this test is what notices."""
    await provision.spawn(log_dir=str(tmp_path))
    argv = spawned[0]["argv"]
    assert argv == provision._argv(PORT_BASE + FIRST_ORDINAL)
    assert argv[0].endswith("python") or "python" in argv[0], argv[0]
    assert "raftkv.app:create_app" in argv
    # No shell anywhere: create_subprocess_exec takes argv, create_subprocess_shell takes
    # a string. Using the latter is the single edit that would turn this into RCE.
    assert all(isinstance(a, str) for a in argv)
    assert not any(c in "".join(argv) for c in ";|&$`\n"), argv


async def test_the_child_is_staged_not_a_voter(spawned, monkeypatch, tmp_path):
    """A spawned node must arrive with an EMPTY voter set. Inheriting this process's
    RAFT_PEERS would make it a voter in a configuration no one else holds — a second
    cluster wearing this one's node ids, which is the failure run_local.sh's port guard
    exists to prevent from the other direction."""
    monkeypatch.setenv("RAFT_PEERS", "node-1=127.0.0.1:8001")
    monkeypatch.setenv("RAFT_NODE_ID", "node-1")
    await provision.spawn(log_dir=str(tmp_path))
    env = spawned[0]["env"]
    assert env["RAFT_LEARNER"] == "1"
    assert env["RAFT_PEERS"] == ""
    assert env["RAFT_NODE_ID"] == f"node-{FIRST_ORDINAL}"
    assert env["RAFT_ADVERTISE"] == f"127.0.0.1:{PORT_BASE + FIRST_ORDINAL}"


async def test_it_refuses_inside_a_container(spawned, monkeypatch, tmp_path):
    """Not hardening — correctness. A child in node-1's network namespace binds that
    container's loopback, so the address it advertises resolves for every peer to the
    peer's own loopback: the attach succeeds and replication never connects."""
    monkeypatch.setattr(provision, "_containerised", lambda: True)
    with pytest.raises(ProvisionError, match="container"):
        await provision.spawn(log_dir=str(tmp_path))
    assert spawned == [], "refused, but spawned anyway"


async def test_the_bootstrap_cluster_is_off_limits(spawned, tmp_path):
    """Nodes 1-3 boot as voters with a peer list. Restarting one as a staged learner on
    top of the running original is two processes answering to one node id."""
    with pytest.raises(ProvisionError, match="bootstrap"):
        await provision.spawn(FIRST_ORDINAL - 1, log_dir=str(tmp_path))
    assert spawned == []


# ---- ordinal allocation --------------------------------------------------------------


async def test_the_first_press_yields_node_4(spawned, tmp_path):
    """The whole point of the change: nothing above node-3 exists until asked for, so
    the first provision is node-4 rather than node-6."""
    result = await provision.spawn(log_dir=str(tmp_path))
    assert result["node_id"] == "node-4"
    assert result["addr"] == "127.0.0.1:8004"
    assert result["staged"] is True


async def test_repeated_presses_walk_upward(spawned, tmp_path):
    ids = [(await provision.spawn(log_dir=str(tmp_path)))["node_id"] for _ in range(4)]
    assert ids == ["node-4", "node-5", "node-6", "node-7"]


async def test_a_stopped_slot_is_reused_rather_than_skipped(spawned, tmp_path):
    """Lowest free ordinal, not highest-plus-one. Drifting upward would strand nodes on
    ports the dashboard's short probe list never knocks on."""
    await provision.spawn(log_dir=str(tmp_path))
    await provision.spawn(log_dir=str(tmp_path))
    provision._children[FIRST_ORDINAL].returncode = 0  # node-4 was stopped
    assert (await provision.spawn(log_dir=str(tmp_path)))["node_id"] == "node-4"


async def test_the_cap_is_enforced(spawned, tmp_path):
    """An unauthenticated endpoint that spawns unbounded processes is a fork bomb with a
    REST API — the same reason flood.MAX_TOTAL exists."""
    for _ in range(3):
        await provision.spawn(max_nodes=3, log_dir=str(tmp_path))
    with pytest.raises(ProvisionError, match="slots are in use"):
        await provision.spawn(max_nodes=3, log_dir=str(tmp_path))
    assert provision.running() == [4, 5, 6]


async def test_a_busy_port_is_never_taken(spawned, monkeypatch, tmp_path):
    """Something already listening is a previous spawn, a compose publish, or a node the
    operator started by hand. All three mean the slot is spoken for."""

    async def only_8004_is_busy(port):
        return port == PORT_BASE + FIRST_ORDINAL

    monkeypatch.setattr(provision, "_port_is_bound", only_8004_is_busy)
    assert (await provision.spawn(log_dir=str(tmp_path)))["node_id"] == "node-5"


async def test_an_explicit_ordinal_on_a_busy_port_is_refused(spawned, monkeypatch, tmp_path):
    async def everything_is_busy(port):
        return True

    monkeypatch.setattr(provision, "_port_is_bound", everything_is_busy)
    with pytest.raises(ProvisionError, match="already in use"):
        await provision.spawn(FIRST_ORDINAL, log_dir=str(tmp_path))


async def test_an_explicit_ordinal_cannot_skip_the_cap(spawned, tmp_path):
    """The cap must hold on BOTH allocation paths. The automatic path only ever searches
    inside the max_nodes window, but an explicit ordinal used to be checked against the
    floor and the port and nothing else — so pydantic's 4..99 bound meant one anonymous
    caller could walk the ordinals and start 96 processes, which is exactly the fork
    bomb the cap exists to prevent. The last slot inside the window must still work:
    refusing the window's own maximum would break the operator who asked for it by name."""
    with pytest.raises(ProvisionError, match="window"):
        await provision.spawn(FIRST_ORDINAL + 3, max_nodes=3, log_dir=str(tmp_path))
    assert spawned == [], "refused, but spawned anyway"
    top = await provision.spawn(FIRST_ORDINAL + 2, max_nodes=3, log_dir=str(tmp_path))
    assert top["node_id"] == f"node-{FIRST_ORDINAL + 2}"


async def test_a_child_that_never_answers_is_reaped(monkeypatch, tmp_path):
    """Leaving a half-started process behind would hold its port, so the next press
    would silently land on a different ordinal — and the operator would be told about a
    node-5 they never asked for instead of about the node-4 that failed."""
    children = []

    async def fake_exec(*argv, **kwargs):
        child = FakeChild()
        children.append(child)
        return child

    async def never_healthy(addr, timeout=None):
        raise ProvisionError(f"{addr} did not answer")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr(provision, "_wait_healthy", never_healthy)
    monkeypatch.setattr(provision, "_port_is_bound", lambda port: _false())
    monkeypatch.setattr(provision, "_containerised", lambda: False)

    with pytest.raises(ProvisionError, match="did not answer"):
        await provision.spawn(log_dir=str(tmp_path))
    assert children[0].terminated, "the failed child was left running"
    assert provision.running() == []


async def _false():
    return False


# ---- the HTTP surface -----------------------------------------------------------------


def build_cfg(tmp_path, **overrides):
    params = dict(
        node_id="solo", peers={}, db_path=str(tmp_path / "solo.db"),
        log_dir=str(tmp_path / "logs"), **scaled(
            heartbeat_interval=0.03, election_timeout_min=0.1, election_timeout_max=0.2
        ),
        rpc_timeout=0.05, commit_timeout=2.0,
    )
    params.update(overrides)
    return NodeConfig(**params)


def test_the_endpoint_reports_refusal_as_409(tmp_path, monkeypatch):
    """409, not 500. Every reason this declines is a fact about the environment that the
    operator can act on — the cap, a taken port, a container — not a bug."""
    monkeypatch.setattr(provision, "_containerised", lambda: True)
    with TestClient(create_app(build_cfg(tmp_path))) as c:
        r = c.post("/admin/spawn-node", json={})
        assert r.status_code == 409
        assert "container" in r.json()["detail"]


def test_an_out_of_range_ordinal_never_reaches_the_module(tmp_path):
    """Bounded on the pydantic model, so FastAPI rejects it before any code that could
    act on it runs. Same placement as flood's bounds, for the same reason."""
    with TestClient(create_app(build_cfg(tmp_path))) as c:
        assert c.post("/admin/spawn-node", json={"ordinal": 3}).status_code == 422
        assert c.post("/admin/spawn-node", json={"ordinal": 100}).status_code == 422
        assert c.post("/admin/spawn-node", json={"ordinal": -1}).status_code == 422


def test_the_route_is_absent_when_provisioning_is_off(tmp_path):
    """Its own flag, not admin_enabled's: a deployment that wants the failure controls
    should not have to accept process spawning to get them."""
    cfg = build_cfg(tmp_path, provision_enabled=False)
    with TestClient(create_app(cfg)) as c:
        assert c.post("/admin/spawn-node", json={}).status_code == 404
        assert c.post("/admin/crash").status_code == 200  # the rest still there


def test_admin_disabled_removes_it_too(tmp_path):
    cfg = build_cfg(tmp_path, admin_enabled=False)
    with TestClient(create_app(cfg)) as c:
        assert c.post("/admin/spawn-node", json={}).status_code == 404
