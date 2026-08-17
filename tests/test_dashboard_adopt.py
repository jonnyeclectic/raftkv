"""The dashboard's membership adoption, executed rather than grepped.

Same technique as the other dashboard tests: slice the shipped block out of the HTML, run
it under node, assert on return values.

What the block does: read every address out of the configurations the polled nodes report,
and start polling the ones this page is not already watching. A member is self-announcing
-- the configuration is a replicated log entry carrying id -> advertise address -- so a
node that has joined the cluster should never need to be probed for or typed in.

The bug it exists for is a page RELOAD. `NODES` is rebuilt from the query string on load,
so a node that attach() added at runtime is gone the moment anyone refreshes: node-6 after
`make node-up`, and node-4 and node-5 the moment they are promoted. On the page that reads
as the cluster having shrunk.

The risk it has to avoid is the opposite one. Those addresses are `advertise_addr`, which
is what PEERS dial -- on compose, a container name that resolves nowhere near the browser.
Adopting one would put a permanently unreachable card on the page that looks exactly like
a crashed node, which is worse than the problem being fixed.
"""

import json
import shutil
import subprocess
from importlib import resources

import pytest

pytestmark = pytest.mark.skipif(
    shutil.which("node") is None, reason="node not installed; JS behaviour untestable"
)

START = "/* --- membership adoption"
END = "/* --- end membership adoption"

LOCAL = ["127.0.0.1:8001", "127.0.0.1:8002", "127.0.0.1:8003"]


def adoption_source() -> str:
    """The shipped block, not a copy of it."""
    page = (resources.files("raftkv") / "static" / "dashboard.html").read_text()
    start, end = page.index(START), page.index(END)
    return page[start:end]


def run(states: dict, nodes: list[str]) -> list[str]:
    script = f"""
{adoption_source()}
console.log(JSON.stringify(
  adoptableAddrs({json.dumps(states)}, {json.dumps(nodes)})));
"""
    out = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        capture_output=True, text=True, timeout=30,
    )
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def local(**config):
    """A node reporting a run-local configuration (browser-reachable addresses)."""
    return {"node_id": "node-1", "role": "leader", "term": 3, "config": config}


def test_nothing_to_adopt_when_the_page_already_polls_every_member():
    cfg = {v: f"127.0.0.1:800{v[-1]}" for v in ("node-1", "node-2", "node-3")}
    assert run({LOCAL[0]: local(voters=cfg)}, LOCAL) == []


def test_a_promoted_node_survives_a_page_reload():
    """The reload case. node-4 was attached and promoted at runtime, so it is in
    the configuration but not in the query string the refreshed page started from."""
    cfg = {"node-1": "127.0.0.1:8001", "node-2": "127.0.0.1:8002",
           "node-3": "127.0.0.1:8003", "node-4": "127.0.0.1:8004"}
    assert run({LOCAL[0]: local(voters=cfg)}, LOCAL) == ["127.0.0.1:8004"]


def test_a_learner_is_adopted_too():
    """A learner is a member — replicated to, just never counted. It belongs on the
    page for the whole catch-up walk, which is the part worth watching."""
    got = run({LOCAL[0]: local(voters={"node-1": "127.0.0.1:8001"},
                               learners={"node-6": "127.0.0.1:8006"})}, LOCAL)
    assert got == ["127.0.0.1:8006"]


def test_the_old_half_of_a_joint_configuration_is_adopted():
    """During C-old,new a node can be leaving the new configuration but still voting in
    the old one. Dropping it mid-transition hides half of what §6 is doing."""
    got = run({LOCAL[0]: local(voters={"node-4": "127.0.0.1:8004"},
                               old_voters={"node-5": "127.0.0.1:8005"})}, LOCAL)
    assert sorted(got) == ["127.0.0.1:8004", "127.0.0.1:8005"]


def test_a_peer_only_address_is_never_adopted():
    """THE regression guard. On compose, advertise addresses are container names that
    resolve inside the pod network and nowhere else. Adopting one adds a card that can
    never load and is indistinguishable from a crashed node."""
    compose_cfg = {"node-1": "raft-node-1:8000", "node-2": "raft-node-2:8000",
                   "node-4": "raft-node-4:8000"}
    browser_view = ["127.0.0.1:8001", "127.0.0.1:8002", "127.0.0.1:8003"]
    assert run({browser_view[0]: local(voters=compose_cfg)}, browser_view) == []


def test_a_second_hostname_is_adopted_once_the_page_already_reaches_that_host():
    """The guard is 'this browser has proven it can reach that host', not 'loopback'.
    A page opened against a remote cluster still grows with it."""
    nodes = ["10.0.0.7:8001", "10.0.0.7:8002"]
    got = run({nodes[0]: local(voters={"a": "10.0.0.7:8004", "b": "10.0.0.9:8004"})}, nodes)
    assert got == ["10.0.0.7:8004"], "adopted a host this page has never reached"


def test_an_address_is_adopted_only_once_however_many_nodes_report_it():
    """Every member reports the same configuration. Pushing per-report would grow NODES
    without bound and poll the same node N times per tick, forever."""
    cfg = {"node-4": "127.0.0.1:8004"}
    states = {a: local(voters=cfg) for a in LOCAL}
    assert run(states, LOCAL) == ["127.0.0.1:8004"]


def test_dead_nodes_and_empty_configurations_are_skipped():
    """A crashed node polls as null and a staged one has no configuration at all;
    reading through either would throw and take the render loop down with it."""
    states = {LOCAL[0]: None, LOCAL[1]: local(), LOCAL[2]: {"node_id": "node-3"}}
    assert run(states, LOCAL) == []


def test_a_malformed_address_is_dropped_rather_than_polled():
    """An empty advertise address is what a node started without RAFT_ADVERTISE puts in
    the configuration — a mistake easy to make and slow to surface (see
    scripts/debug_node.py, which spells out why every node needs one)."""
    got = run({LOCAL[0]: local(voters={"node-4": "", "node-5": "127.0.0.1",
                                       "node-6": "127.0.0.1:8006"})}, LOCAL)
    assert got == ["127.0.0.1:8006"]
