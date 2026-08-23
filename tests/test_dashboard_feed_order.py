"""The event feed's merge order, executed rather than grepped.

Same technique as the other dashboard tests: slice the shipped code out of the HTML, run
it under node, assert on what renders. The block here is `refreshLogs` itself — it has no
marker comments because it is not a pure helper, so the slice is anchored on the
function's own boundaries and everything it touches (fetch, the two renderers, the module
state it reads) is stubbed in the harness. Stubbing fetch is the point: it makes network
completion order an input the test controls.

What is pinned is determinism. A flood produces hundreds of events in the same
millisecond, so `ts` is full of ties, and the feed used to push each node's batch into a
shared array as its response arrived — ordering ties by whichever response completed
first, which flips between polls. The symptom was a feed that visibly reshuffled twice a
second while nothing was happening. The fix builds the array with Promise.all (which
preserves NODES order regardless of completion order) and a stable newest-first sort, so
the same cluster state renders the same way on every poll. These tests run the real
function against both completion orders and require identical output.
"""

import json
import shutil
import subprocess
from importlib import resources

import pytest

pytestmark = pytest.mark.skipif(
    shutil.which("node") is None, reason="node not installed; JS behaviour untestable"
)

# The declaration and the next top-level declaration after it. index() raises if either
# ever disappears, which is this file's cue that the anchor needs re-pointing.
START = "async function refreshLogs"
END = "\nfunction renderFeed"

NODES = ["127.0.0.1:8001", "127.0.0.1:8002"]

# Two batches with `ts` tied ACROSS nodes — the same millisecond seen by both — which is
# exactly the input whose order used to depend on the network.
BATCHES = {
    "127.0.0.1:8001": [
        {"node": "node-1", "event": "log_appended", "ts": 100, "term": 1},
        {"node": "node-1", "event": "commit_advanced", "ts": 200, "term": 1},
    ],
    "127.0.0.1:8002": [
        {"node": "node-2", "event": "log_appended", "ts": 100, "term": 1},
        {"node": "node-2", "event": "applied", "ts": 200, "term": 1},
    ],
}


def feed_source() -> str:
    """The shipped function, not a copy of it."""
    page = (resources.files("raftkv") / "static" / "dashboard.html").read_text()
    start = page.index(START)
    return page[start:page.index(END, start)]


def run(completion_order: list[str]) -> list[str]:
    """One poll, with the nodes' responses landing in the given order.

    The module-scope `fetch` shadows the global inside the slice and parks every request
    unresolved; the loop below then answers them one at a time, with a macrotask tick
    between answers so each response's `.json()` await settles before the next lands —
    completion order is therefore exactly `completion_order`, not a race.
    """
    script = f"""
let lastEventTs = 0;
let lastLines = [];
const pulses = [];
let captured = null;
function renderFilters(lines) {{}}
function renderFeed(lines) {{ captured = lines; }}
const NODES = {json.dumps(NODES)};
const BATCHES = {json.dumps(BATCHES)};
const pending = new Map();
function fetch(url, opts) {{
  const addr = url.slice("http://".length, url.length - "/logs".length);
  return new Promise(resolve => pending.set(addr, resolve));
}}
{feed_source()}
const poll = refreshLogs();
for (const addr of {json.dumps(completion_order)}) {{
  pending.get(addr)({{ok: true, json: async () => BATCHES[addr]}});
  await new Promise(r => setTimeout(r, 0));
}}
await poll;
console.log(JSON.stringify(captured.map(e => `${{e.node}} ${{e.event}} ${{e.ts}}`)));
"""
    out = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        capture_output=True, text=True, timeout=30,
    )
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def test_ties_render_in_nodes_order_whatever_the_network_did():
    """THE regression: per-response pushes put the faster node's events ahead of the
    other's for every tied `ts`, so the same state drew two different feeds on two
    polls. Ties must follow the fixed NODES list, under both completion orders."""
    expected = [
        "node-1 commit_advanced 200",
        "node-2 applied 200",
        "node-1 log_appended 100",
        "node-2 log_appended 100",
    ]
    assert run(NODES) == expected
    assert run(list(reversed(NODES))) == expected


def test_the_feed_stays_newest_first():
    """The regression guard on the fix itself: restoring determinism by sorting on some
    other key would be a different feed, not this one. The interesting line is the one
    that just happened, and nobody should scroll a live feed to find it."""
    stamps = [int(line.rsplit(" ", 1)[1]) for line in run(NODES)]
    assert stamps == sorted(stamps, reverse=True)
