"""The per-node role automaton, executed rather than grepped.

The panel draws follower / candidate / leader and lights the edge the node last took. The
drawing is not the interesting part — the reconstruction is. Roles are sampled at 2 Hz and
the transitions worth seeing are shorter than that, so the panel derives them from the
node's own event log instead. That derivation is what this pins.

Three claims it has to get right, each of which reads as a plausible diagram when wrong:

  * **A lost straw poll is a self-loop.** PreVote's entire content is that a node which
    would lose does not run: no term, no candidacy. Drawn as candidate → follower it would
    state the mechanism backwards on the one panel built to show it.
  * **"became follower" has three different causes** and the role field carries none of
    them. A leader that lost contact with a majority, a leader that met a newer term, and
    a candidate that met a live leader are three different failures wanting three
    different responses.
  * **`_become_follower` logs before it assigns**, so the event carries the role being
    LEFT. That is the only reason an edge has a tail at all.

Sliced out of the shipped HTML and run under node, like the other dashboard-logic tests;
skipped when node is absent so `make test` still passes without it.
"""

import json
import shutil
import subprocess
from importlib import resources

import pytest

pytestmark = pytest.mark.skipif(
    shutil.which("node") is None, reason="node not installed; JS behaviour untestable"
)

START = "/* --- role automaton"
END = "/* --- end role automaton"


def automaton_source() -> str:
    """The shipped block, not a copy. A test that re-implements its subject passes
    forever regardless of what the browser actually loads."""
    page = (resources.files("raftkv") / "static" / "dashboard.html").read_text()
    start, end = page.index(START), page.index(END)
    return page[start:end]


def ev(event: str, ts: float, node: str = "node-1", term: int = 1, **extra) -> dict:
    """One structured log line, in the shape raftkv.logging_setup emits."""
    return {"event": event, "ts": ts, "node": node, "term": term, **extra}


def run(events: list[dict], node_id: str = "node-1", limit: int = 8) -> dict:
    script = f"""
{automaton_source()}
const out = roleTransitions({json.dumps(events)}, {json.dumps(node_id)}, {limit});
console.log(JSON.stringify({{transitions: out, lastEdge: fsmEdge(out[0])}}));
"""
    proc = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        capture_output=True, text=True, timeout=30,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


def edges(result: dict) -> list[str]:
    return [f"{t['from']}->{t['to']}" for t in result["transitions"]]


# ---- the ordinary path ---------------------------------------------------------


def test_an_uncontested_election_is_two_edges_newest_first():
    result = run([
        ev("pre_vote_started", 100.0, term=4, would_be_term=5),
        ev("election_started", 100.1, term=5, role="candidate"),
        ev("became_leader", 100.2, term=5, role="leader", quorum=2),
    ])
    assert edges(result) == ["candidate->leader", "follower->candidate"]
    assert result["lastEdge"] == "candidate->leader"
    assert "quorum 2" in result["transitions"][0]["cause"]


def test_a_quiet_node_reports_nothing_rather_than_inventing_an_edge():
    result = run([ev("applied", 100.0, index=3, key="k")])
    assert result["transitions"] == []
    assert result["lastEdge"] is None


# ---- the claim PreVote exists to make ------------------------------------------


def test_a_lost_straw_poll_is_a_self_loop_not_a_candidacy():
    """The one that would misteach the mechanism. A node that loses its poll never left
    follower, never incremented its term, and disturbed nobody — which is the whole
    reason PreVote is here (thesis §9.6)."""
    result = run([ev("pre_vote_failed", 100.0, term=4, role="follower",
                     granted=["node-1"])])

    assert edges(result) == ["follower->follower"]
    assert "candidate" not in json.dumps(result["transitions"]), \
        "a lost straw poll was drawn as a trip through candidate"
    assert "1 granted" in result["transitions"][0]["cause"]


# ---- the three different meanings of "became follower" -------------------------


def test_a_checkquorum_resignation_is_named_as_such():
    """quorum_lost and the step-down are logged inside one synchronous block, so the
    pair is a single event and must read as one."""
    result = run([
        ev("became_leader", 100.0, term=5, role="leader", quorum=2),
        ev("quorum_lost", 130.0, term=5, role="leader", reachable=["node-1"], quorum=2),
        ev("became_follower", 130.001, term=5, role="leader"),
    ])
    assert edges(result)[0] == "leader->follower"
    assert "CheckQuorum" in result["transitions"][0]["cause"]


def test_a_leader_deposed_by_a_higher_term_is_not_blamed_on_checkquorum():
    """No quorum_lost anywhere: this leader was fine and simply met a newer term. Calling
    it a resignation would send someone hunting a partition that never happened."""
    result = run([
        ev("became_leader", 100.0, term=5, role="leader", quorum=2),
        ev("became_follower", 130.0, term=9, role="leader"),
    ])
    assert result["transitions"][0]["cause"] == "higher term seen"


def test_a_stale_quorum_lost_does_not_explain_a_much_later_stepdown():
    """The window is why this is a timestamp comparison and not a flag: a resignation
    from an earlier tenure must not annotate an unrelated step-down minutes later."""
    result = run([
        ev("quorum_lost", 100.0, term=5, role="leader", reachable=["node-1"], quorum=2),
        ev("became_follower", 100.001, term=5, role="leader"),
        ev("became_leader", 200.0, term=6, role="leader", quorum=2),
        ev("became_follower", 400.0, term=11, role="leader"),
    ])
    causes = [t["cause"] for t in result["transitions"]]
    assert causes[0] == "higher term seen", "a stale quorum_lost explained a later step-down"
    assert "CheckQuorum" in causes[-1], "the real resignation lost its cause"


def test_a_candidate_that_meets_a_live_leader_differs_from_one_that_meets_a_new_term():
    """Same event, same role, different term movement — and different advice. One says
    the election was simply lost; the other says this node is behind."""
    same_term = run([
        ev("election_started", 100.0, term=4, role="candidate"),
        ev("became_follower", 100.5, term=4, role="candidate"),
    ])
    assert same_term["transitions"][0]["cause"] == "a live leader appeared"

    higher = run([
        ev("election_started", 100.0, term=4, role="candidate"),
        ev("became_follower", 100.5, term=7, role="candidate"),
    ])
    assert higher["transitions"][0]["cause"] == "higher term seen"


# ---- scoping and ordering ------------------------------------------------------


def test_only_this_nodes_events_are_counted():
    """`/logs` is merged across every node for the feed, so the panel is handed the whole
    cluster's history and has to take its own. Without the filter, node-1's card would
    draw node-2's election."""
    result = run([
        ev("election_started", 100.0, term=5, role="candidate", node="node-2"),
        ev("became_leader", 100.1, term=5, role="leader", quorum=2, node="node-2"),
        ev("pre_vote_failed", 100.2, term=4, role="follower", granted=["node-1"]),
    ], node_id="node-1")
    assert edges(result) == ["follower->follower"]


def test_history_is_newest_first_and_bounded():
    events = []
    for i in range(12):
        events.append(ev("election_started", 100.0 + i, term=i + 1, role="candidate"))
    result = run(events, limit=3)
    assert len(result["transitions"]) == 3
    terms = [t["term"] for t in result["transitions"]]
    assert terms == sorted(terms, reverse=True), "the feed reads newest first; so does this"
    assert terms[0] == 12


def test_events_arriving_out_of_order_are_still_read_in_time_order():
    """`/logs` is sorted newest-first for the feed, and the merge across nodes is not
    stable in any useful sense — so the input order here is not the causal order."""
    result = run([
        ev("became_leader", 100.2, term=5, role="leader", quorum=2),
        ev("election_started", 100.1, term=5, role="candidate"),
    ])
    assert edges(result) == ["candidate->leader", "follower->candidate"]


# ---- the operator events, which change the role without saying so --------------
#
# crash(), reset() and recover() assign `role = FOLLOWER` directly instead of going
# through _become_follower, so they emit no `became_follower` and log role="follower"
# themselves. Found live: a killed leader kept `candidate -> leader` as its newest edge,
# so the card drew FOLLOWER lit beside a last transition claiming it had just been
# elected. The lit state and the lit edge disagreed, which is the one thing this panel
# must never do.


def test_a_killed_leader_does_not_keep_claiming_it_was_just_elected():
    """The live bug, in one assertion."""
    result = run([
        ev("election_started", 100.0, term=4, role="candidate"),
        ev("became_leader", 100.1, term=4, role="leader", quorum=2),
        ev("crashed", 130.0, term=4, role="follower"),
    ])
    assert edges(result)[0] == "leader->follower", \
        "a crashed leader still reported candidate -> leader as its latest edge"
    assert result["transitions"][0]["cause"] == "crashed (simulated)"


def test_a_reset_leader_reports_the_wipe():
    result = run([
        ev("election_started", 100.0, term=4, role="candidate"),
        ev("became_leader", 100.1, term=4, role="leader", quorum=2),
        ev("reset", 200.0, term=0, role="follower"),
    ])
    assert edges(result)[0] == "leader->follower"
    assert result["transitions"][0]["cause"] == "state wiped by reset"


def test_crashing_a_follower_invents_no_transition():
    """Every row has to be an edge that was really traversed, or the `last taken`
    highlight stops meaning anything. A follower that crashes was already a follower."""
    result = run([
        ev("crashed", 100.0, term=4, role="follower"),
        ev("recovered", 130.0, term=4, role="follower"),
    ])
    assert result["transitions"] == []


def test_the_full_live_sequence_ends_where_the_node_actually_is():
    """node-2's real log, from the cluster this was built against: elected, killed,
    recovered, elected again, killed, recovered, wiped. It ends a follower, and the
    newest edge has to agree with that."""
    result = run([
        ev("election_started", 100.0, term=2, role="candidate"),
        ev("became_leader", 100.1, term=2, role="leader", quorum=2),
        ev("crashed", 130.0, term=2, role="follower"),
        ev("recovered", 180.0, term=2, role="follower"),
        ev("pre_vote_failed", 240.0, term=3, role="follower", granted=["node-2"]),
        ev("election_started", 300.0, term=4, role="candidate"),
        ev("became_leader", 300.1, term=4, role="leader", quorum=2),
        ev("crashed", 330.0, term=4, role="follower"),
        ev("recovered", 380.0, term=4, role="follower"),
        ev("reset", 5000.0, term=0, role="follower"),
    ], limit=20)

    assert result["transitions"][0]["to"] == "follower", "panel disagreed with the node"
    assert result["transitions"][0]["cause"] == "crashed (simulated)"
    # the wipe of an already-crashed node moves no role, so it adds no edge
    assert [t["cause"] for t in result["transitions"]].count("state wiped by reset") == 0


def test_a_stuck_candidate_recampaigning_is_candidate_to_candidate():
    """`_election_timer_loop` re-fires for a stuck candidate, so a second
    election_started leaves candidate rather than follower. Reconstructing `from` from a
    running fold is what makes that come out right."""
    result = run([
        ev("election_started", 100.0, term=4, role="candidate"),
        ev("election_started", 200.0, term=5, role="candidate"),
    ])
    assert edges(result) == ["candidate->candidate", "follower->candidate"]
