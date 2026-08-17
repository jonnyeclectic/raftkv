"""The dashboard's provisioning decisions, executed rather than grepped.

Same technique as the other dashboard tests: slice the shipped block out of the HTML,
run it under node, assert on return values. The page has no build step, so this is the
only way to test its logic by running it — a test that re-implemented `provisionTarget`
would pass forever regardless of what the browser actually loads.

Two decisions live in this block, and both fail quietly rather than loudly:

  provisionTarget    picks WHICH node is asked to start a process. Pick a staged one and
                     it still works — every node shares a host — so nothing goes red, and
                     the page just reads as though a node in no cluster grew the cluster.
  registerProvisioned decides what happens to the address that comes back. Push it into
                     NODES instead of PROBE and a card renders before discovery has
                     classified the node, showing a member with an empty log for a tick.
"""

import json
import shutil
import subprocess
from importlib import resources

import pytest

pytestmark = pytest.mark.skipif(
    shutil.which("node") is None, reason="node not installed; JS behaviour untestable"
)

START = "/* --- provisioning"
END = "/* --- end provisioning"

VOTERS = {"node-1": "127.0.0.1:8001", "node-2": "127.0.0.1:8002"}


def block() -> str:
    """The shipped block, not a copy of it."""
    page = (resources.files("raftkv") / "static" / "dashboard.html").read_text()
    start, end = page.index(START), page.index(END)
    return page[start:end]


def run(expr: str, states: dict, nodes: list, probe: list | None = None) -> object:
    script = f"""
{block()}
const states = {json.dumps(states)};
const NODES = {json.dumps(nodes)};
const PROBE = {json.dumps(probe or [])};
console.log(JSON.stringify({{value: {expr}, PROBE}}));
"""
    out = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        capture_output=True, text=True, timeout=30,
    )
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def member(role="follower", term=1, learner=False):
    return {"role": role, "term": term, "learner": learner, "config": {"voters": VOTERS}}


def staged():
    """Running, in nobody's configuration: an empty voter set is what that means."""
    return {"role": "follower", "term": 0, "learner": True, "config": {"voters": {}}}


# ---- which node gets asked ------------------------------------------------------------


def test_the_leader_is_asked_when_there_is_one():
    """Provisioning is not a Raft operation, but the attach that follows is leader-only.
    Sending both to one node keeps the event feed readable: `node_provisioned` and
    `learner_added` arrive from the same card."""
    states = {
        "127.0.0.1:8001": member(),
        "127.0.0.1:8002": member(role="leader", term=3),
    }
    r = run("provisionTarget(states, NODES)", states, list(states))
    assert r["value"] == "127.0.0.1:8002"


def test_the_highest_term_leader_wins():
    """Raft guarantees one leader PER TERM, not one leader. A partitioned stale leader
    keeps reporting role=leader and is telling the truth — asking it to provision
    produces a node that cannot be attached, because it has no majority to append with.
    Same rule as leaderEntry(); getting it wrong here undoes it there."""
    states = {
        "127.0.0.1:8001": member(role="leader", term=2),
        "127.0.0.1:8002": member(role="leader", term=9),
    }
    r = run("provisionTarget(states, NODES)", states, list(states))
    assert r["value"] == "127.0.0.1:8002"


def test_a_learner_is_never_treated_as_the_leader():
    """A promoted-but-still-catching-up node can report role=leader with learner=true in
    the window before its configuration entry applies."""
    states = {
        "127.0.0.1:8004": member(role="leader", term=9, learner=True),
        "127.0.0.1:8001": member(role="leader", term=2),
    }
    r = run("provisionTarget(states, NODES)", states, list(states))
    assert r["value"] == "127.0.0.1:8001"


def test_it_falls_back_to_a_member_when_no_leader_is_visible():
    """Mid-election, or a partition with no majority. The process still needs starting —
    refusing to provision because an election is in flight would make the button feel
    broken in exactly the state an operator is most likely to be reacting to."""
    states = {"127.0.0.1:8001": member(), "127.0.0.1:8002": member()}
    r = run("provisionTarget(states, NODES)", states, list(states))
    assert r["value"] == "127.0.0.1:8001"


def test_a_staged_node_is_never_asked():
    """It would work — every node shares a host — which is exactly the problem. A node
    in no configuration appearing to grow the cluster reads as an accident."""
    states = {"127.0.0.1:8004": staged(), "127.0.0.1:8005": staged()}
    r = run("provisionTarget(states, NODES)", states, list(states))
    assert r["value"] is None


def test_a_crashed_node_is_never_asked():
    """/state 503s on a crashed node, so the dashboard holds null for it. Reading
    `.config` off that null is what takes the whole render loop down."""
    states = {"127.0.0.1:8001": None, "127.0.0.1:8002": member(role="leader", term=4)}
    r = run("provisionTarget(states, NODES)", states, list(states))
    assert r["value"] == "127.0.0.1:8002"


def test_nothing_answering_at_all_returns_null():
    """The caller shows an error instead of POSTing to `http://null/`."""
    r = run("provisionTarget(states, NODES)", {}, [])
    assert r["value"] is None


# ---- what happens to the address that comes back --------------------------------------


def test_a_new_address_joins_the_probe_list():
    """PROBE, not NODES: discover() adopts it on the next tick in probe order. Pushed
    straight into NODES it would render as a member with an empty log for one tick."""
    r = run('registerProvisioned("127.0.0.1:8006", PROBE)', {}, [], ["127.0.0.1:8004"])
    assert r["value"] is True
    assert r["PROBE"] == ["127.0.0.1:8004", "127.0.0.1:8006"]


def test_an_address_already_probed_is_not_added_twice():
    """node-4 lands on 8004, which the default probe list already covers. Adding it
    again would double the fetch rate on that address for the rest of the session."""
    r = run('registerProvisioned("127.0.0.1:8004", PROBE)', {}, [], ["127.0.0.1:8004"])
    assert r["value"] is False
    assert r["PROBE"] == ["127.0.0.1:8004"]


@pytest.mark.parametrize("addr", ["", "not-an-address", "127.0.0.1", ":8006", None])
def test_a_malformed_address_is_dropped_rather_than_probed(addr):
    """An address that never answers is a console line of ERR_CONNECTION_REFUSED twice a
    second for as long as the page stays open."""
    r = run(f"registerProvisioned({json.dumps(addr)}, PROBE)", {}, [], [])
    assert r["value"] is False
    assert r["PROBE"] == []
