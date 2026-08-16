"""The dashboard's leader pick, executed rather than grepped.

Every other dashboard test asserts on the served markup, because the page has no build
step to test through. That works for "is the control present", but it cannot tell a
correct comparator from a backwards one -- and this is the one piece of dashboard logic
where being subtly wrong is indistinguishable from Raft being broken.

So: slice the leader-selection block out of the shipped HTML, run it under node with
hand-built /state payloads, and assert on return values. Skips when node is absent, so
`make test` still passes on a machine that has never installed it.

The bug this pins: `states` is keyed by address and was scanned in address order, taking
the FIRST node self-reporting role=leader. Raft guarantees one leader per TERM, not one
leader, so during a partition a stale leader and the real one both report truthfully --
and the stale one wins whenever it sorts first. It then owns the header, the topology
edges, and (via leaderAddr) every write the control panel sends — which lands every one
of those writes on the one node in the cluster that cannot commit them.
"""

import json
import shutil
import subprocess
from importlib import resources

import pytest

pytestmark = pytest.mark.skipif(
    shutil.which("node") is None, reason="node not installed; JS behaviour untestable"
)

START = "/* --- leader selection"
END = "/* --- end leader selection"


def selection_source() -> str:
    """The shipped block, not a copy of it. A test that re-implements the function it is
    testing passes forever regardless of what the browser loads."""
    page = (resources.files("raftkv") / "static" / "dashboard.html").read_text()
    start, end = page.index(START), page.index(END)
    return page[start:end]


def run(states: dict) -> dict:
    """Evaluate the block against `states` and report what each entry point returned."""
    script = f"""
const states = {json.dumps(states)};
{selection_source()}
const e = leaderEntry();
console.log(JSON.stringify({{
  addr: leaderAddr(),
  node: e ? e.s.node_id : null,
  term: e ? e.s.term : null,
  reach: quorumReach(e ? e.s : null),
}}));
"""
    out = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        capture_output=True, text=True, timeout=30,
    )
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def node_state(node_id, role, term, blocked=(), learner=False):
    return {"node_id": node_id, "role": role, "term": term,
            "blocked": list(blocked), "learner": learner}


# The screenshot case: node-2 partitioned at t17 still calls itself leader, node-3 won
# t18 with node-1's vote. node-2 sorts first by address, so first-match picked the loser.
SPLIT_BRAIN = {
    "127.0.0.1:8001": node_state("node-1", "follower", 18),
    "127.0.0.1:8002": node_state("node-2", "leader", 17, blocked=["node-1", "node-3"]),
    "127.0.0.1:8003": node_state("node-3", "leader", 18),
}


def test_split_brain_picks_the_current_term_leader():
    got = run(SPLIT_BRAIN)
    assert got["node"] == "node-3", "stale t17 leader hijacked the pick"
    assert got["term"] == 18
    assert got["addr"] == "127.0.0.1:8003", "writes would be routed to the stale leader"


def test_pick_does_not_depend_on_address_order():
    """The original bug was invisible whenever the real leader happened to sort first.
    Reversing insertion order must not change the answer."""
    reversed_states = dict(reversed(list(SPLIT_BRAIN.items())))
    assert run(reversed_states)["node"] == "node-3"
    assert run(SPLIT_BRAIN)["node"] == run(reversed_states)["node"]


def test_real_leader_still_reaches_a_majority_through_the_partition():
    """node-3 + node-1 = 2 of 3. The cluster is writable and the strip must say so."""
    assert run(SPLIT_BRAIN)["reach"] == 2


def test_isolated_leader_reports_no_quorum():
    """`nodes up 3/3` is true and irrelevant here: node-2 is cut from both peers, so
    every submit() it accepts times out uncommitted. Counting live nodes said
    'accepting'; counting reachable ones is what makes the strip honest."""
    isolated = {
        "127.0.0.1:8001": node_state("node-1", "follower", 17, blocked=["node-2"]),
        "127.0.0.1:8002": node_state("node-2", "leader", 17, blocked=["node-1", "node-3"]),
        "127.0.0.1:8003": node_state("node-3", "follower", 17, blocked=["node-2"]),
    }
    got = run(isolated)
    assert got["node"] == "node-2"  # it is the only leader; the pick is not the problem
    assert got["reach"] == 1, "a leader cut from every peer cannot commit"


def test_a_cut_counts_from_either_side():
    """set_blocked severs both directions (raft.py outbound, app.py inbound), so a peer
    naming the leader is as much a partition as the leader naming the peer."""
    one_sided = {
        "127.0.0.1:8001": node_state("node-1", "follower", 9, blocked=["node-2"]),
        "127.0.0.1:8002": node_state("node-2", "leader", 9),  # names nobody
        "127.0.0.1:8003": node_state("node-3", "follower", 9, blocked=["node-2"]),
    }
    assert run(one_sided)["reach"] == 1


def test_healthy_cluster_is_unchanged():
    healthy = {
        "127.0.0.1:8001": node_state("node-1", "leader", 7),
        "127.0.0.1:8002": node_state("node-2", "follower", 7),
        "127.0.0.1:8003": node_state("node-3", "follower", 7),
    }
    got = run(healthy)
    assert (got["node"], got["addr"], got["reach"]) == ("node-1", "127.0.0.1:8001", 3)


def test_learners_never_win_the_pick_or_count_toward_reach():
    """A learner is outside cfg.peers and outside the quorum; if one ever self-reported
    leader it would still be wrong to route a write to it."""
    with_learner = {
        "127.0.0.1:8001": node_state("node-1", "leader", 4),
        "127.0.0.1:8002": node_state("node-2", "follower", 4),
        "127.0.0.1:8004": node_state("node-4", "leader", 99, learner=True),
    }
    got = run(with_learner)
    assert got["node"] == "node-1", "a learner outranked a voter on term"
    assert got["reach"] == 2, "learner counted toward quorum"


def test_no_leader_and_dead_nodes_are_handled():
    """A dead node polls as null. Reading .role off it would blank the whole panel."""
    none_up = {
        "127.0.0.1:8001": None,
        "127.0.0.1:8002": node_state("node-2", "candidate", 21),
        "127.0.0.1:8003": node_state("node-3", "follower", 21),
    }
    got = run(none_up)
    assert got["node"] is None and got["addr"] is None
    assert got["reach"] == 0


def test_dead_peers_do_not_count_as_reachable():
    """Two nodes killed leaves the survivor a leader that cannot commit."""
    mostly_dead = {
        "127.0.0.1:8001": node_state("node-1", "leader", 12),
        "127.0.0.1:8002": None,
        "127.0.0.1:8003": None,
    }
    assert run(mostly_dead)["reach"] == 1
