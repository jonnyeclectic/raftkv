"""The dashboard's discovery tick, executed against a stubbed network.

Same slice-and-run-under-node technique as the other dashboard tests, with one addition:
`discover()` is the only sliced block that does I/O, so the harness installs its own
`fetch` — one that answers on a chosen set of addresses, after a chosen delay. That delay
is the whole point. The bug this pins is a race, and a race that only shows up when the
replies arrive out of order cannot be tested by a stub that answers instantly.

The bug: the probe results were pushed into NODES from inside the per-address callback,
so the staged row was ordered by who answered FIRST. Observed live as `node-5, node-4,
node-6` on one page load and `node-4, node-5, node-6` on the next, with nothing changed
in between. NODES order is card order, so the cards reshuffled between reloads for no
reason visible from the page — on the one view whose job is to say which processes are
and are not in the cluster.
"""

import json
import shutil
import subprocess
from importlib import resources

import pytest

pytestmark = pytest.mark.skipif(
    shutil.which("node") is None, reason="node not installed; JS behaviour untestable"
)

START = "/* --- discovery"
END = "/* --- end discovery"

CLUSTER = ["127.0.0.1:8001", "127.0.0.1:8002", "127.0.0.1:8003"]
PROBE = ["127.0.0.1:8004", "127.0.0.1:8005", "127.0.0.1:8006"]


def discovery_source() -> str:
    """The shipped block, not a copy of it."""
    page = (resources.files("raftkv") / "static" / "dashboard.html").read_text()
    start, end = page.index(START), page.index(END)
    return page[start:end]


def run(*, nodes, probe, answers, states=None, delays=None) -> list[str]:
    """Run one discovery tick and return NODES afterwards.

    `answers` are the addresses a /state request succeeds on; `delays` maps an address to
    how many milliseconds it takes to answer, which is what lets a test force a specific
    completion order.
    """
    script = f"""
const NODES = {json.dumps(nodes)};
const PROBE = {json.dumps(probe)};
const states = {json.dumps(states or {})};
const ANSWERS = new Set({json.dumps(answers)});
const DELAYS = {json.dumps(delays or {})};

// Stand-in for the browser's fetch: resolves ok for addresses that "have a node", and
// takes DELAYS[addr] ms to do it so completion order is controlled, not incidental.
globalThis.fetch = (url) => {{
  const addr = new URL(url).host;
  return new Promise((resolve, reject) => setTimeout(
    () => ANSWERS.has(addr) ? resolve({{ok: true}}) : reject(new Error("refused")),
    DELAYS[addr] ?? 0));
}};

// adoptableAddrs is a sibling block; discovery only needs it to be callable here.
function adoptableAddrs(states, nodes) {{
  const hosts = new Set(nodes.map(a => a.split(":")[0]));
  const found = [];
  for (const s of Object.values(states)) {{
    if (!s) continue;
    const cfg = s.config ?? {{}};
    for (const addr of Object.values({{...cfg.voters, ...cfg.old_voters, ...cfg.learners}})) {{
      if (!addr || nodes.includes(addr) || found.includes(addr)) continue;
      if (!/^[A-Za-z0-9.-]+:\\d+$/.test(addr)) continue;
      if (!hosts.has(addr.split(":")[0])) continue;
      found.push(addr);
    }}
  }}
  return found;
}}

{discovery_source()}
await discover();
console.log(JSON.stringify(NODES));
"""
    out = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        capture_output=True, text=True, timeout=30,
    )
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def test_probe_order_survives_replies_arriving_backwards():
    """THE regression. 8006 answers first, 8004 last — the order that made the cards
    reshuffle. NODES must still come back ascending, because PROBE is ascending."""
    got = run(nodes=list(CLUSTER), probe=list(PROBE), answers=PROBE,
              delays={"127.0.0.1:8004": 60, "127.0.0.1:8005": 30, "127.0.0.1:8006": 0})
    assert got == CLUSTER + PROBE, "staged cards ordered by who answered first"


def test_order_is_the_same_when_replies_arrive_forwards():
    """The same call with the delays reversed must produce the identical list. A result
    that depends on network timing is the definition of the bug."""
    backwards = run(nodes=list(CLUSTER), probe=list(PROBE), answers=PROBE,
                    delays={"127.0.0.1:8004": 60, "127.0.0.1:8006": 0})
    forwards = run(nodes=list(CLUSTER), probe=list(PROBE), answers=PROBE,
                   delays={"127.0.0.1:8004": 0, "127.0.0.1:8006": 60})
    assert backwards == forwards == CLUSTER + PROBE


def test_an_address_with_nothing_there_is_not_adopted():
    """A probe slot for a node that was never started must leave no card behind — it is
    an address that might be nothing, not a node that has gone quiet."""
    got = run(nodes=list(CLUSTER), probe=list(PROBE), answers=["127.0.0.1:8004"])
    assert got == [*CLUSTER, "127.0.0.1:8004"]


def test_an_address_is_never_adopted_twice():
    """discover() runs twice a second forever. Re-adding an address already in NODES
    would poll the same node N times per tick and grow the list without bound."""
    already = [*CLUSTER, "127.0.0.1:8004"]
    got = run(nodes=already, probe=list(PROBE), answers=PROBE)
    assert got == [*CLUSTER, "127.0.0.1:8004", "127.0.0.1:8005", "127.0.0.1:8006"]
    assert len(got) == len(set(got))


def test_members_are_adopted_from_the_configuration_before_probing():
    """A configured member needs no probe, so it must already be in NODES by the time
    the probe loop checks — otherwise the same address is fetched anyway and appended
    twice on the same tick."""
    states = {CLUSTER[0]: {"config": {"voters": {"node-4": "127.0.0.1:8004"}}}}
    got = run(nodes=list(CLUSTER), probe=list(PROBE), answers=PROBE, states=states)
    assert got == [*CLUSTER, "127.0.0.1:8004", "127.0.0.1:8005", "127.0.0.1:8006"]
    assert len(got) == len(set(got)), "a configured member was also probed in"
