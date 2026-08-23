"""The provisioning fixes, watched from a real browser.

Everything else in the suite runs one scope smaller on purpose: test_provision.py stubs
the subprocess call, test_dashboard_provision.py slices the page's JS out and runs it
under node. The bug these tests pin was invisible at every one of those scopes — the
server refused correctly, the refusal just never survived the trip: an unmapped OSError
became a 500, the generic 500 is sent from OUTSIDE the CORS middleware, and a
cross-origin tab therefore rendered the whole exchange as `provision failed: TypeError:
Failed to fetch`. Only a browser pointed at real uvicorn processes sees what the
operator saw (docs/FAILURE_MODES.md tells the incident).

So this file is the end-to-end lane: real nodes on dynamic ports, real chromium, the
shipped dashboard, and assertions on the one string the operator actually reads — the
status line. It is opt-in (`make e2e` installs playwright ephemerally via `uv run
--with` and runs exactly this file) and skips rather than fails wherever the
environment cannot carry it: no playwright, no chromium, or a sandbox that forbids
binding sockets.
"""

import contextlib
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

pw = pytest.importorskip(
    "playwright.sync_api",
    reason="playwright is not installed — this lane runs via `make e2e`",
)


def _can_bind() -> bool:
    try:
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        probe.close()
        return True
    except OSError:
        return False


pytestmark = [
    pytest.mark.skipif(
        not _can_bind(), reason="this environment forbids binding sockets (agent sandbox)"
    ),
    # On a host that is itself a container, `_containerised()` fires for the HOST's
    # reason, so every spawn here would be refused before reaching the path under test.
    pytest.mark.skipif(
        Path("/.dockerenv").exists(),
        reason="the host is a container; every spawn would be refused for its reason",
    ),
]


@pytest.fixture(scope="module")
def browser():
    try:
        manager = pw.sync_playwright().start()
    except Exception as exc:  # missing driver, hostile event loop — not a product failure
        pytest.skip(f"playwright could not start ({exc}); `make e2e` sets it up")
    try:
        try:
            b = manager.chromium.launch()
        except Exception as exc:  # missing browser binary, not a product failure
            pytest.skip(f"chromium is not installed ({exc}); `make e2e` fetches it")
        yield b
        b.close()
    finally:
        manager.stop()


@pytest.fixture
def page(browser):
    page = browser.new_page()
    yield page
    page.close()


# Children spawned during a test, reaped after it — CENTRALLY, not per test, because
# the tests that leak are the ones that fail: a refusal test whose fix has regressed
# gets a *successful* spawn it never asked for, and that child is detached
# (start_new_session) so no parent teardown reaps it. press_provision registers every
# 200 it intercepts; the fixture sweeps them whatever the test's verdict was.
_spawned_pids: list[int] = []


def _reap(pid: int) -> None:
    with contextlib.suppress(ProcessLookupError):
        os.kill(pid, signal.SIGTERM)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            time.sleep(0.1)
            os.kill(pid, 0)  # raises ProcessLookupError once it is gone
        os.kill(pid, signal.SIGKILL)


@pytest.fixture(autouse=True)
def reap_spawned_children():
    yield
    while _spawned_pids:
        _reap(_spawned_pids.pop())


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@contextlib.contextmanager
def raftkv_node(run_dir: Path, *, env_extra: dict | None = None, cwd: Path | None = None):
    """One real node on a dynamic port: healthy before the body runs, dead after.

    Dynamic ports because 8001-8005 belong to whatever cluster the developer has up
    (compose, kind port-forwards); a solo node with RAFT_PEERS="" is a one-voter
    cluster and elects itself within a second, which is all the dashboard needs to
    show a leader card and enable the provision button.
    """
    port = _free_port()
    env = dict(os.environ)
    # A developer shell can carry either containment tell (a k8s toolbox, direnv);
    # each test sets them deliberately or not at all.
    env.pop("KUBERNETES_SERVICE_HOST", None)
    env.pop("container", None)
    env.update(
        {
            "RAFT_NODE_ID": "node-1",
            "RAFT_PEERS": "",
            "RAFT_DB_PATH": str(run_dir / f"node-{port}.db"),
            "RAFT_LOG_DIR": str(run_dir / "logs"),
        }
    )
    env.update(env_extra or {})
    sink = (run_dir / f"uvicorn-{port}.log").open("ab")
    try:
        proc = subprocess.Popen(
            [
                sys.executable, "-m", "uvicorn", "--factory", "raftkv.app:create_app",
                "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning",
            ],
            cwd=cwd or run_dir,
            env=env,
            stdout=sink,
            stderr=subprocess.STDOUT,
        )
    except BaseException:
        sink.close()
        raise
    addr = f"127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 30
        while True:
            if proc.poll() is not None:
                raise RuntimeError(
                    f"node on {addr} exited {proc.returncode} before answering; "
                    f"see {sink.name}"
                )
            with contextlib.suppress(httpx.HTTPError):
                if httpx.get(f"http://{addr}/healthz", timeout=1.0).status_code == 200:
                    break
            if time.monotonic() > deadline:
                raise RuntimeError(f"node on {addr} never answered /healthz; see {sink.name}")
            time.sleep(0.1)
        yield addr
    finally:
        proc.terminate()
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            proc.kill()
        sink.close()


SETTLED = (
    "() => {"
    "  const t = document.getElementById('status').textContent;"
    "  return t.includes('provision failed') || t.includes('staged; attach');"
    "}"
)


def press_provision(page, serve_addr: str, target_addr: str | None = None):
    """Load the dashboard from one node, aim it at another (default: itself), press the
    button, and return (status line, intercepted /admin/spawn-node response) — the
    string the operator reads and the wire truth behind it."""
    target = target_addr or serve_addr
    page.goto(f"http://{serve_addr}/?nodes={target}")
    # provisionTarget needs a classified leader card; the page polls twice a second.
    page.wait_for_function(
        "() => typeof provisionTarget === 'function'"
        "      && provisionTarget(states, NODES) !== null",
        timeout=15_000,
    )
    with page.expect_response(
        # Method-filtered because the cross-origin press is preflighted: without it,
        # an OPTIONS response could satisfy the wait and stand in for the POST.
        lambda r: "/admin/spawn-node" in r.url and r.request.method == "POST",
        timeout=30_000,
    ) as caught:
        page.click("#provbtn")
    page.wait_for_function(SETTLED, timeout=30_000)
    status = page.evaluate("document.getElementById('status').textContent")
    resp = caught.value
    if resp.status == 200:
        # Register the child for the autouse sweep even when the caller expected a
        # refusal — an unexpected success is exactly the regression case, and its
        # detached child must not outlive the test that caught it.
        with contextlib.suppress(Exception):
            _spawned_pids.append(resp.json()["pid"])
    return status, resp


def test_a_kubernetes_pod_refusal_reaches_the_operator(page, tmp_path):
    """Fix 1, end to end: with only the env var the kubelet injects, the press must end
    in the refusal that names the real growth verb — not the opaque exchange the kind
    demo produced ('provision failed: TypeError: Failed to fetch')."""
    with raftkv_node(tmp_path, env_extra={"KUBERNETES_SERVICE_HOST": "10.96.0.1"}) as addr:
        status, resp = press_provision(page, addr)
    assert resp.status == 409
    assert "kubectl scale statefulset/node" in status
    assert "Failed to fetch" not in status
    assert "internal" not in status


def test_a_filesystem_refusal_is_readable_not_a_500(page, tmp_path):
    """Fix 3, end to end: an unwritable working directory (the shape of a read-only
    root filesystem) used to surface as `{"error": "internal"}`. The operator must get
    the ProvisionError text instead."""
    ro = tmp_path / "ro"
    ro.mkdir()
    writable = tmp_path / "writable"
    writable.mkdir()
    ro.chmod(0o500)
    try:
        with raftkv_node(writable, cwd=ro) as addr:
            status, resp = press_provision(page, addr)
    finally:
        ro.chmod(0o700)  # or pytest cannot clean its own tmp tree
    assert resp.status == 409
    assert "filesystem refused" in status
    assert "internal" not in status
    assert "Failed to fetch" not in status


def test_the_failure_survives_a_cross_origin_tab(page, tmp_path):
    """The incident's geometry, replayed on the fixed pipeline: dashboard served by one
    node, provision aimed at another — every k8s/compose tab is cross-origin like this,
    and cross-origin is where the pre-fix exchange collapsed to `TypeError: Failed to
    fetch` (the unmapped OSError's 500 left from outside the CORS middleware, so the
    browser refused to show it). With fix 3 the failure is a 409, which travels the
    routed path CORS headers and all; this asserts that refusal text genuinely crosses
    origins. The 500 handler's own header is pinned at unit scope in
    tests/test_api.py::test_unhandled_exception_returns_structured_500."""
    ro = tmp_path / "ro"
    ro.mkdir()
    writable = tmp_path / "writable"
    writable.mkdir()
    ro.chmod(0o500)
    try:
        with (
            raftkv_node(writable, cwd=ro) as target,
            raftkv_node(writable, env_extra={"RAFT_NODE_ID": "node-2"}) as server,
        ):
            status, resp = press_provision(page, server, target_addr=target)
    finally:
        ro.chmod(0o700)
    assert resp.status == 409
    assert "filesystem refused" in status
    assert "Failed to fetch" not in status


def test_disabled_provisioning_reads_as_policy_not_not_found(page, tmp_path):
    """Fix 2's browser face: a deployment that turns provisioning off entirely
    (RAFT_PROVISION_ENABLED=0) never mounts the route, so the press comes back 404 —
    which the page must present as the deployment decision it is, naming the flag,
    not as FastAPI's stock 'Not Found'."""
    with raftkv_node(tmp_path, env_extra={"RAFT_PROVISION_ENABLED": "0"}) as addr:
        status, resp = press_provision(page, addr)
    assert resp.status == 404
    assert "RAFT_PROVISION_ENABLED" in status
    assert "Not Found" not in status


def test_provision_still_spawns_on_a_plain_host(page, tmp_path):
    """The happy path, unbroken by any of the above: on a bare host the press must
    still end in a real staged process. The child outlives its parent by design
    (start_new_session); press_provision registered its pid, and the autouse
    reap_spawned_children fixture kills it after this test either way."""
    with raftkv_node(tmp_path) as addr:
        status, resp = press_provision(page, addr)
        assert resp.status == 200, resp.text()
        body = resp.json()
        assert body["ok"] is True
        assert body["staged"] is True
        assert "staged; attach it to join" in status
        # The child answers as what the response claims it is: a node in nobody's
        # configuration.
        state = httpx.get(f"http://{body['addr']}/state", timeout=5.0).json()
        assert state["node_id"] == body["node_id"]
        assert state["config"]["voters"] == {}
