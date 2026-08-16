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


def test_dashboard_served(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "raftkv cluster" in r.text
    # literal 127.0.0.1, never "localhost": localhost resolves to 127.0.0.1 OR ::1, and
    # a second cluster bound to the other one is invisible until the cards disagree
    assert "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003" in r.text
    assert "localhost:800" not in r.text


def test_dashboard_write_control_takes_click_and_enter_without_navigating(client):
    """The writer is the one interactive control on the page and has no build step to
    test through, so assert on the served markup. Three things it must keep doing:
    respond to Enter (the reflex nobody suppresses), never submit (a navigation
    loses the page), and reject an empty value client-side -- the server answers 422
    from KVWrite(min_length=1), which is true but unhelpful as UI feedback."""
    page = client.get("/").text
    assert "<form" not in page  # a submit event is a navigation waiting to happen
    assert 'type="button"' in page  # ...so the button is never an implicit submit
    assert 'e.key === "Enter"' in page  # Enter still writes
    assert '"value required"' in page  # client-side guard mirrors KVWrite.value


def test_dashboard_shows_newest_log_lines_first(client):
    """A live feed that appends at the bottom makes you scroll to see the thing that
    just happened."""
    page = client.get("/").text
    assert "lines.sort((a, b) => b.ts - a.ts)" in page  # descending
    assert "shown.slice(0, 60)" in page  # ...newest are the head, after filtering


def test_dashboard_feed_can_isolate_the_commit_path(client):
    """`submitted -> log_appended -> commit_advanced -> applied` is the whole story of
    an entry becoming real, and heartbeat noise buries it. The filter has to group
    exactly those four, or the commit path has to be reconstructed by eye from the noise."""
    page = client.get("/").text
    assert "const GROUPS = {" in page
    for event in ["submitted", "log_appended", "commit_advanced", "applied"]:
        assert f'"{event}"' in page, event
    assert "function renderFilters(" in page and "function renderFeed(" in page
    assert 'aria-pressed' in page  # the chips are real toggle buttons
    # switching filter must repaint immediately rather than wait for the next poll
    assert "renderFeed(lastLines)" in page


def test_dashboard_javascript_parses(client):
    """The dashboard has no build step, so a typo ships silently and only surfaces as
    a blank page in the browser. If node is unavailable, skip rather than
    pretend -- a skipped check is honest, a missing one is not."""
    import re
    import shutil
    import subprocess
    import tempfile

    node_bin = shutil.which("node")
    if node_bin is None:
        pytest.skip("node not available to parse the inline script")
    script = re.search(r"<script>(.*)</script>", client.get("/").text, re.S)
    assert script, "dashboard has no inline script"
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as fh:
        fh.write(script.group(1))
        path = fh.name
    result = subprocess.run([node_bin, "--check", path], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr




def test_dashboard_caps_the_state_machine_list_instead_of_growing_the_card(client):
    """A flood puts hundreds of keys in the state machine. Unbounded, the card grows
    past the viewport and pushes the topology graph and the other cards off screen, so
    the one panel whose length is set by the data scrolls inside a fixed box."""
    page = client.get("/").text
    assert ".kvscroll" in page and "max-height" in page
    assert "overflow-y: auto" in page
    # the position has to outlive the 500ms rebuild or the list cannot be read at all
    assert "SCROLL_POS" in page and "function restoreScroll(" in page
    assert "data-scrollkey" in page or "scrollkey" in page


def test_dashboard_details_block_cannot_re_render_from_its_own_toggle(client):
    """A regression guard with a specific near-miss behind it.

    The state machine's rows are built lazily, so an obvious way to make opening one feel
    instant is to call renderNodes() from the toggle handler. That is an unbounded loop:
    assigning `d.open = true` while rebuilding a card is itself a change, so it fires
    `toggle` too — which re-renders, which sets open, which fires again, as fast as the
    event queue allows, for every block left open, on every one of the two ticks a second.
    The handler therefore fills its own table in place, which cannot recurse."""
    page = client.get("/").text
    body = page.split("function detailsBlock")[1].split("function restoreScroll")[0]
    assert "renderNodes()" not in body, "detailsBlock re-renders from its own toggle"
    assert "hasChildNodes()" in body, "nothing stops a second toggle doubling the table"


def test_dashboard_orders_state_machine_keys_numerically(client):
    """`kv_all()` is `ORDER BY key`, i.e. byte order: k1, k10, k100, k11, k2. The panel
    re-sorts so a flood's keys read in sequence."""
    page = client.get("/").text
    assert "function sortedKv(" in page
    assert "numeric: true" in page  # Intl.Collator, not a hand-rolled parser



def test_dashboard_renders_topology_without_external_dependencies(client):
    """The graph is Canvas 2D on purpose. The page is served from inside the container
    and must work offline with no build step, so any CDN import would be a regression
    even if it rendered nicely on the machine that wrote it."""
    page = client.get("/").text
    assert 'id="topo"' in page and 'getContext("2d")' in page
    assert "function drawTopology(" in page
    assert "http://cdn" not in page and "https://" not in page  # nothing fetched offsite
    assert "<script src=" not in page and "<link rel=\"stylesheet\"" not in page
    # pulses come from real replication events, not an idle timer
    assert 'e.event === "log_appended"' in page


def test_dashboard_summarises_the_cluster_before_the_detail(client):
    """Summary before detail: quorum and write availability are the two questions an
    operator asks first, and neither is derivable at a glance from three node cards."""
    page = client.get("/").text
    assert "function renderSystem(" in page
    assert '"nodes up"' in page and '"quorum"' in page and '"writes"' in page
    assert "unavailable" in page  # writes are called out when quorum is lost


def test_state_exposes_peer_addresses_not_just_ids(client, tmp_path):
    """The collapsible cluster view needs somewhere to dial, so /state carries the
    peer map. Nodes can legitimately disagree about it, which is why it is per-node."""
    cfg = build_cfg(tmp_path, peers={"node-2": "10.0.0.2:8000"})
    with TestClient(create_app(cfg)) as c:
        assert c.get("/state").json()["peers"] == {"node-2": "10.0.0.2:8000"}


def test_dashboard_survives_a_hostile_dom(client):
    """The page polls every 500 ms forever and does not own its own DOM -- a browser
    extension removing a mount point once made tick() throw twice a second for as long as
    the page stayed open. Every mount and status write must tolerate a missing element."""
    page = client.get("/").text
    assert "function mount(" in page
    for target in ['mount("nodes")', 'mount("log")', 'mount("system")', 'mount("health")']:
        assert target in page, target
    assert "if (!document.body)" in page  # document.body has gone null in the wild
    # the raw lookups those replaced must not creep back in
    for raw in ['getElementById("nodes").', 'getElementById("log").',
                'getElementById("system").', 'getElementById("health").']:
        assert raw not in page, raw


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
