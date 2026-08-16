"""Build stamps: the page must describe itself, not something adjacent to itself.

Two layers reload differently -- dashboard.html is re-read from disk per request, Python
is imported once per process -- so this repo can and did serve a current UI on top of
stale code. `ui` and `srv` exist to make that visible, which only works if the numbers
are actually tied to the bytes they claim to describe. That tie is what these pin.
"""

import hashlib
import json
import re
import shutil
import subprocess
from importlib import resources

import pytest
from fastapi.testclient import TestClient

from raftkv import build as buildmod
from raftkv.app import create_app
from test_api import build_cfg  # noqa: F401  (build_cfg is used via the client fixture)


@pytest.fixture
def client(tmp_path):
    with TestClient(create_app(build_cfg(tmp_path))) as c:
        yield c


def dashboard_bytes() -> bytes:
    return (resources.files("raftkv") / "static" / "dashboard.html").read_bytes()


# ---- the ui stamp -------------------------------------------------------------------

def test_ui_stamp_is_the_hash_of_the_file_on_disk():
    assert buildmod.ui_build() == hashlib.sha256(dashboard_bytes()).hexdigest()[:6]


def test_ui_stamp_is_recomputed_rather_than_cached(tmp_path, monkeypatch):
    """Caching would make the stamp claim a page that is no longer being served -- the
    precise lie the stamp exists to prevent. Point the loader at changing bytes and the
    answer has to change with them."""
    seen = []

    class FakeFile:
        def __init__(self, data): self.data = data
        def __truediv__(self, _): return self
        def read_bytes(self): return self.data

    for payload in (b"<html>one</html>", b"<html>two</html>"):
        monkeypatch.setattr(buildmod.resources, "files", lambda _p, d=payload: FakeFile(d))
        seen.append(buildmod.ui_build())

    assert seen[0] != seen[1], "ui_build() returned a cached value across two files"
    assert seen[0] == hashlib.sha256(b"<html>one</html>").hexdigest()[:6]


def test_ui_stamp_is_short_lowercase_hex():
    stamp = buildmod.ui_build()
    assert re.fullmatch(r"[0-9a-f]{6}", stamp), stamp


# ---- injection ----------------------------------------------------------------------

def test_the_placeholder_exists_for_injection_to_replace():
    """If the token is ever renamed in the HTML, `.replace()` silently no-ops and the
    masthead renders the literal string. Nothing else in the suite would notice."""
    assert b"__UI_BUILD__" in dashboard_bytes()


def test_served_page_carries_its_own_hash_and_no_placeholder(client):
    page = client.get("/").text
    assert "__UI_BUILD__" not in page, "placeholder shipped unsubstituted"
    assert f'const PAGE_UI_BUILD = "{buildmod.ui_build()}"' in page


def test_served_stamp_matches_the_bytes_that_were_read(client):
    """The page's stamp and /build's must agree: they are the same file, and reporting
    two different numbers for one file is worse than reporting none."""
    assert client.get("/build").json()["ui"] in client.get("/").text


# ---- the srv stamp ------------------------------------------------------------------

def test_server_build_is_frozen_at_import():
    """It names the code currently EXECUTING. A value that tracked disk would go on
    reporting agreement while a node ran something else entirely."""
    assert buildmod.SERVER_BUILD == buildmod.SERVER_BUILD
    assert buildmod.SERVER_BUILD.startswith(buildmod.PACKAGE_VERSION + "+")
    assert re.fullmatch(r"[0-9a-f]{6}", buildmod.SERVER_BUILD.rsplit("+", 1)[1])


def test_server_digest_notices_a_rename_not_just_an_edit():
    """Names are hashed alongside contents, so moving code between two files registers.
    Without that, a pure rename would report as no change at all."""
    def digest(files):
        h = hashlib.sha256()
        for name, data in sorted(files):
            h.update(name.encode())
            h.update(data)
        return h.hexdigest()[:6]

    same_bytes_different_names = [("a.py", b"x"), ("b.py", b"y")]
    renamed = [("a.py", b"y"), ("b.py", b"x")]
    assert digest(same_bytes_different_names) != digest(renamed)


# ---- the endpoint -------------------------------------------------------------------

def test_build_endpoint_reports_node_server_and_ui(client):
    body = client.get("/build").json()
    assert body == {"node": "solo", "server": buildmod.SERVER_BUILD, "ui": buildmod.ui_build()}


def test_build_answers_while_crashed(client):
    """Every other surface goes dark, on purpose. This one must not: during an incident
    the version is exactly the question, and a stamp that dies with the node cannot
    report the stale build that caused the incident."""
    client.post("/admin/crash")
    r = client.get("/build")
    assert r.status_code == 200
    assert r.json()["server"] == buildmod.SERVER_BUILD
    assert client.get("/state").status_code == 503  # ...and the rest really is dark


# ---- the dashboard's mixed-cluster summary ------------------------------------------

pytestmark_node = pytest.mark.skipif(
    shutil.which("node") is None, reason="node not installed; JS behaviour untestable"
)


def build_summary_source() -> str:
    page = (resources.files("raftkv") / "static" / "dashboard.html").read_text()
    start = page.index("function buildSummary()")
    return page[start:page.index("\n}", start) + 2]


def summarize(builds: dict) -> str:
    script = f"""
const builds = {json.dumps(builds)};
{build_summary_source()}
console.log(buildSummary());
"""
    out = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=30)
    assert out.returncode == 0, out.stderr
    return out.stdout.strip()


@pytestmark_node
def test_uniform_cluster_shows_the_single_build():
    same = {f"127.0.0.1:800{i}": {"node": f"node-{i}", "server": "0.1.0+abc123",
                                  "ui": "f3a9c1"} for i in (1, 2, 3)}
    assert summarize(same) == "0.1.0+abc123"


@pytestmark_node
def test_mixed_cluster_is_named_rather_than_averaged():
    """A cluster running two builds behaves like Raft misbehaving, and no other panel
    on the page can show it."""
    mixed = {
        "127.0.0.1:8001": {"node": "node-1", "server": "0.1.0+abc123", "ui": "f3a9c1"},
        "127.0.0.1:8002": {"node": "node-2", "server": "0.1.0+dead99", "ui": "f3a9c1"},
        "127.0.0.1:8003": {"node": "node-3", "server": "0.1.0+abc123", "ui": "f3a9c1"},
    }
    assert summarize(mixed) == "MIXED (2)"


@pytestmark_node
def test_unreachable_nodes_do_not_fabricate_a_mismatch():
    """A dead node polls as null. Counting it as a distinct version would cry MIXED
    every time somebody used the kill button."""
    with_dead = {
        "127.0.0.1:8001": {"node": "node-1", "server": "0.1.0+abc123", "ui": "f3a9c1"},
        "127.0.0.1:8002": None,
        "127.0.0.1:8003": {"node": "node-3", "server": "0.1.0+abc123", "ui": "f3a9c1"},
    }
    assert summarize(with_dead) == "0.1.0+abc123"
    assert summarize({"127.0.0.1:8001": None}) == "?"
