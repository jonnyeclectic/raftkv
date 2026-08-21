"""The ballot and log automata, executed rather than grepped.

A node runs three state machines at three different scopes. `test_dashboard_role_automaton`
covers the first — follower/candidate/leader, one per NODE. These are the other two:

  * **ballot**, one per TERM. `voted_for` is durable and write-once: rule 6 grants only
    when `voted_for in (None, candidate_id)`, and `_observe_term` clears it on a higher
    term. The automaton's missing edge — spent straight to differently-spent — IS Figure 2
    rule 6, so a panel that drew one would teach the opposite of the guarantee.
  * **log**, one per INDEX. `appended -> committed -> applied`, monotone, with exactly one
    way back out: truncation, and only from `appended`. Truncating a *committed* entry is
    the single failure the whole implementation exists to rule out, and the panel is
    already holding the evidence, so it checks.

Both light their current box from `/state` and take only their edges from `/logs`, which
is why `ballotState`, `logStage` and `logInvariants` are pinned here alongside the two
reconstructions: those three are what the card actually displays.

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

START = "/* --- ballot and log automata"
END = "/* --- end ballot and log automata"


def automaton_source() -> str:
    """The shipped block, not a copy. A test that re-implements its subject passes
    forever regardless of what the browser actually loads."""
    page = (resources.files("raftkv") / "static" / "dashboard.html").read_text()
    start, end = page.index(START), page.index(END)
    return page[start:end]


def ev(event: str, ts: float, node: str = "node-1", term: int = 1, **extra) -> dict:
    """One structured log line, in the shape raftkv.logging_setup emits."""
    return {"event": event, "ts": ts, "node": node, "term": term, **extra}


def run_js(expr: str) -> object:
    script = f"{automaton_source()}\nconsole.log(JSON.stringify({expr}));"
    proc = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        capture_output=True, text=True, timeout=30,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


def ballot(events: list[dict], node_id: str = "node-1", limit: int = 8) -> dict:
    got = run_js(
        f"ballotTransitions({json.dumps(events)}, {json.dumps(node_id)}, {limit})"
    )
    got["edges"] = [f"{r['from']}->{r['to']}" for r in got["rows"]]
    return got


def pipeline(events: list[dict], node_id: str = "node-1", limit: int = 8) -> dict:
    got = run_js(
        f"logTransitions({json.dumps(events)}, {json.dumps(node_id)}, {limit})"
    )
    got["edges"] = [f"{r['from']}->{r['to']}" for r in got["rows"]]
    return got


# ---- the ballot: where this term's one vote went -------------------------------


def test_a_granted_vote_is_one_edge_that_names_the_candidate():
    got = ballot([ev("vote_granted", 100.0, term=5, candidate="node-3")])
    assert got["edges"] == ["unvoted->granted"]
    assert got["rows"][0]["cause"] == "granted to node-3"
    assert got["conflicts"] == []


def test_starting_an_election_is_a_vote_for_itself():
    """`_start_election` writes `voted_for = self.cfg.node_id` immediately after the term
    bump, so a candidacy is a ballot movement and not only a role movement."""
    got = ballot([ev("election_started", 100.0, term=5, role="candidate")])
    assert got["edges"] == ["unvoted->self"]


def test_ballot_state_is_read_from_voted_for_not_from_the_log():
    """The lit box comes off `/state`, which cannot be stale relative to the node the way
    a reconstruction can — the role panel's one live bug was exactly that disagreement."""
    assert run_js('ballotState(null, "node-1")') == "unvoted"
    assert run_js('ballotState("", "node-1")') == "unvoted"
    assert run_js('ballotState("node-1", "node-1")') == "self"
    assert run_js('ballotState("node-3", "node-1")') == "granted"


# ---- the claim PreVote exists to make, again -----------------------------------


def test_a_straw_poll_leaves_the_ballot_exactly_where_it_was():
    """A pre-vote persists nothing and touches no term. If it moved this diagram it would
    be indistinguishable from a real vote, which is the one thing it is defined not to be."""
    got = ballot([
        ev("pre_vote_started", 100.0, term=4, would_be_term=5),
        ev("pre_vote_failed", 100.2, term=4, granted=["node-1"]),
    ])
    assert got["edges"] == ["unvoted->unvoted", "unvoted->unvoted"]
    assert "ballot untouched" in got["rows"][0]["cause"]
    assert got["conflicts"] == []


def test_a_straw_poll_from_a_spent_ballot_loops_where_it_stands():
    """The case that decides whether the drawn loop can be pinned under NO VOTE: a
    follower whose leader has gone quiet polls from `granted`, having already voted for
    the leader that died. It is one loop serving all three states."""
    got = ballot([
        ev("vote_granted", 100.0, term=5, candidate="node-3"),
        ev("pre_vote_started", 160.0, term=5, would_be_term=6),
    ])
    assert got["edges"] == ["granted->granted", "unvoted->granted"]
    assert run_js('ballotEdge({from: "granted", to: "granted"})') == "poll"
    assert run_js('ballotEdge({from: "unvoted", to: "granted"})') == "unvoted->granted"


# ---- one vote per term, which is the whole point -------------------------------


def test_a_new_term_clears_a_spent_ballot():
    """No event announces the clearing — `_observe_term` just does it — so it is inferred
    from the first line carrying the newer term, which is also when the node learned."""
    got = ballot([
        ev("vote_granted", 100.0, term=5, candidate="node-3"),
        ev("vote_granted", 200.0, term=6, candidate="node-2"),
    ])
    assert got["edges"] == ["unvoted->granted", "granted->unvoted", "unvoted->granted"]
    assert got["rows"][1]["cause"] == "term 5 ended, ballot cleared"
    assert got["conflicts"] == [], "two terms, two votes — entirely legal"


def test_a_new_term_on_an_unspent_ballot_draws_nothing():
    """Every row has to be an edge really traversed or the `last taken` highlight stops
    meaning anything. A term this node never voted in moved nothing."""
    got = ballot([
        ev("log_appended", 100.0, term=5, from_index=2, count=1),
        ev("log_appended", 200.0, term=9, from_index=3, count=1),
    ])
    assert got["rows"] == []


def test_re_granting_to_the_same_candidate_in_one_term_is_not_a_conflict():
    """A retried RequestVote takes the `voted_for === candidate_id` branch and is granted
    again. Flagging it would fire the safety alarm on an ordinary lost packet."""
    got = ballot([
        ev("vote_granted", 100.0, term=5, candidate="node-3"),
        ev("vote_granted", 100.4, term=5, candidate="node-3"),
    ])
    assert got["conflicts"] == []


def test_two_different_candidates_in_one_term_is_flagged():
    """The violation this diagram exists to make visible. It is upstream of the Election
    Safety counter, which can only ever see the two leaders after the fact."""
    got = ballot([
        ev("vote_granted", 100.0, term=5, candidate="node-3"),
        ev("vote_granted", 100.4, term=5, candidate="node-2"),
    ])
    assert len(got["conflicts"]) == 1
    assert got["conflicts"][0] == {
        "term": 5, "ts": 100.4, "first": "node-3", "second": "node-2",
    }


def test_campaigning_in_a_term_this_node_already_voted_in_is_flagged():
    """The same violation from the other direction: a node that granted to a peer must
    not then vote for itself without the term moving."""
    got = ballot([
        ev("vote_granted", 100.0, term=5, candidate="node-3"),
        ev("election_started", 100.4, term=5, role="candidate"),
    ])
    assert len(got["conflicts"]) == 1
    assert got["conflicts"][0]["second"] == "node-1"


def test_a_reset_destroys_the_ballot_and_says_so():
    """reset()'s own docstring calls this out as the double-vote Fig. 2's durability
    requirement exists to prevent, so the panel names it rather than folding it into an
    ordinary term change."""
    got = ballot([
        ev("vote_granted", 100.0, term=5, candidate="node-3"),
        ev("reset", 200.0, term=0, role="follower"),
    ])
    assert got["edges"][0] == "granted->unvoted"
    assert got["rows"][0]["cause"] == "state wiped by reset"


def test_only_this_nodes_ballot_is_drawn():
    """`/logs` is merged across every node for the feed, so the panel is handed the whole
    cluster's history and has to take its own."""
    got = ballot([
        ev("vote_granted", 100.0, term=5, candidate="node-9", node="node-2"),
        ev("vote_granted", 100.1, term=5, candidate="node-3"),
    ], node_id="node-1")
    assert got["edges"] == ["unvoted->granted"]
    assert got["rows"][0]["cause"] == "granted to node-3"


# ---- the log pipeline: appended -> committed -> applied -------------------------


def test_a_leader_write_walks_the_whole_pipeline():
    got = pipeline([
        ev("submitted", 100.0, term=5, key="k", op="set", index=7),
        ev("commit_advanced", 100.2, term=5, commit_index=7),
        ev("applied", 100.3, term=5, index=7, key="k"),
    ])
    assert got["edges"] == [
        "committed->applied", "appended->committed", "absent->appended",
    ]
    assert got["rows"][-1]["cause"] == 'set "k" accepted at index 7'
    assert got["rows"][1]["cause"] == "a majority holds index 7"
    assert got["violations"] == []


def test_replicated_entries_enter_at_appended():
    got = pipeline([ev("log_appended", 100.0, term=5, from_index=4, count=3)])
    assert got["edges"] == ["absent->appended"]
    assert got["rows"][0]["cause"] == "3 entries replicated from index 4"


def test_a_single_replicated_entry_is_not_pluralised():
    got = pipeline([ev("log_appended", 100.0, term=5, from_index=4, count=1)])
    assert got["rows"][0]["cause"] == "1 entry replicated from index 4"


def test_truncating_an_uncommitted_entry_is_ordinary_repair():
    """§5.3 conflict repair. A follower that took entries from a leader who then lost is
    supposed to have them cut away; alarming on it would alarm on Raft working."""
    got = pipeline([
        ev("log_appended", 100.0, term=5, from_index=4, count=3),
        ev("log_truncated", 200.0, term=6, from_index=5),
    ])
    assert got["edges"][0] == "appended->truncated"
    assert got["violations"] == [], "ordinary repair was reported as a safety violation"


def test_truncating_a_committed_entry_is_flagged():
    """State Machine Safety, broken. A majority acknowledged index 7 and a client may
    have been told so; cutting at 6 erases it."""
    got = pipeline([
        ev("commit_advanced", 100.0, term=5, commit_index=7),
        ev("log_truncated", 200.0, term=6, from_index=6),
    ])
    assert len(got["violations"]) == 1
    assert got["violations"][0]["kind"] == "a committed entry was truncated"
    assert "index 7 was already committed" in got["violations"][0]["detail"]


def test_truncating_exactly_at_the_committed_index_is_flagged():
    """The boundary, and the direction it has to fall: commit_index NAMES a committed
    entry, so cutting from that index erases it. `<` instead of `<=` here would let the
    single worst case through while every other case still reported correctly."""
    got = pipeline([
        ev("commit_advanced", 100.0, term=5, commit_index=7),
        ev("log_truncated", 200.0, term=6, from_index=7),
    ])
    assert len(got["violations"]) == 1


def test_an_applied_index_proves_commitment_on_a_follower():
    """A follower never logs `commit_advanced` — handle_append_entries moves commit_index
    from leader_commit silently, because it moves on nearly every append round and logging
    it would bury the feed. So `applied` is the only evidence a follower leaves, and
    without treating it as proof the check above is dead on every node but the leader."""
    got = pipeline([
        ev("log_appended", 100.0, term=5, from_index=4, count=4),
        ev("applied", 100.5, term=5, index=7, key="k"),
        ev("log_truncated", 200.0, term=6, from_index=6),
    ])
    assert len(got["violations"]) == 1, "a follower's committed entry was cut unnoticed"
    assert got["violations"][0]["kind"] == "a committed entry was truncated"


def test_a_reset_clears_the_committed_watermark():
    """An operator destroying the log is not Raft violating itself. Without dropping the
    watermark the refill afterwards trips the truncation check on its first conflict and
    the panel cries safety violation at a documented operation."""
    got = pipeline([
        ev("commit_advanced", 100.0, term=5, commit_index=7),
        ev("reset", 200.0, term=0, role="follower"),
        ev("log_appended", 300.0, term=6, from_index=1, count=2),
        ev("log_truncated", 400.0, term=6, from_index=1),
    ])
    assert got["violations"] == []
    assert got["rows"][-1]["cause"] == "a majority holds index 7"
    assert "log destroyed by reset" in [r["cause"] for r in got["rows"]]


def test_commit_index_moving_backwards_is_flagged():
    got = pipeline([
        ev("commit_advanced", 100.0, term=5, commit_index=9),
        ev("commit_advanced", 200.0, term=5, commit_index=4),
    ])
    assert len(got["violations"]) == 1
    assert got["violations"][0]["kind"] == "commit index moved backwards"
    assert got["violations"][0]["detail"] == "9 -> 4"


def test_log_history_is_newest_first_and_bounded():
    events = [ev("applied", 100.0 + i, term=5, index=i + 1, key="k") for i in range(12)]
    got = pipeline(events, limit=3)
    assert len(got["rows"]) == 3
    indices = [r["index"] for r in got["rows"]]
    assert indices == [12, 11, 10], "the feed reads newest first; so does this"


def test_log_events_arriving_out_of_order_are_read_in_time_order():
    """The merge across nodes is not stable in any useful sense, so input order is not
    causal order — and here it decides whether the truncation check fires at all."""
    got = pipeline([
        ev("log_truncated", 200.0, term=6, from_index=6),
        ev("commit_advanced", 100.0, term=5, commit_index=7),
    ])
    assert len(got["violations"]) == 1


def test_only_this_nodes_log_is_drawn():
    got = pipeline([
        ev("commit_advanced", 100.0, term=5, commit_index=7, node="node-2"),
        ev("log_truncated", 200.0, term=6, from_index=6),
    ], node_id="node-1")
    assert got["violations"] == [], "node-2's commit was used to indict node-1"


# ---- where the frontier is, read from /state -----------------------------------


@pytest.mark.parametrize("length,commit,applied,stage", [
    (0, 0, 0, "empty"),        # nothing exists, so nothing is in any stage
    (14, 12, 12, "appended"),  # two entries still awaiting a majority
    (12, 12, 9, "committed"),  # majority reached, applier behind
    (12, 12, 12, "applied"),   # caught up
    (1, 0, 0, "appended"),
])
def test_the_lit_box_is_where_the_newest_entry_has_reached(length, commit, applied, stage):
    got = run_js(
        f"logStage({{log_length: {length}, commit_index: {commit}, last_applied: {applied}}})"
    )
    assert got == stage


def test_an_empty_log_lights_nothing():
    """`applied` would be technically true of a log with no entries and would read as
    "caught up" on a node that has never received anything."""
    assert run_js("logStage({log_length: 0, commit_index: 0, last_applied: 0})") == "empty"


def test_the_containment_invariants_are_checked_from_state_alone():
    """These hold whether or not the ring buffer still has the events, which is the point
    of checking them here rather than in the reconstruction."""
    assert run_js(
        "logInvariants({log_length: 14, commit_index: 12, last_applied: 12})") == []
    # All three equal is a fully caught-up node, not a broken containment: the bounds are
    # <=, and a strict comparison would flag every idle cluster on the page.
    assert run_js(
        "logInvariants({log_length: 12, commit_index: 12, last_applied: 12})") == []
    past_end = run_js(
        "logInvariants({log_length: 3, commit_index: 9, last_applied: 1})")
    assert len(past_end) == 1 and "past the end of the log" in past_end[0]
    ahead = run_js(
        "logInvariants({log_length: 9, commit_index: 4, last_applied: 7})")
    assert len(ahead) == 1 and "ahead of commit_index" in ahead[0]


# ---- the panels as actually drawn ----------------------------------------------
#
# Everything above pins the reconstructions. These pin the WIRING, which they cannot: the
# lit box, the lit edge and the red line are three separate arguments threaded from three
# separate functions into one grid, and handing `logEdge` a ballot row -- or dropping the
# violations into a variable nobody appends -- produces a panel that is wrong in exactly
# the way a reader would never suspect, because every number on it is right.
#
# Run under node against a minimal DOM, so the assertions are on what the browser would
# build rather than on the source that builds it.

RENDER_START = "/* --- automaton rendering"
RENDER_END = "/* --- end automaton rendering"

DOM_SHIM = """
const document = {
  createElement: (tag) => ({
    tagName: tag, className: "", textContent: "", kids: [],
    append(...xs) { this.kids.push(...xs); },
  }),
};
const flat = (n) => n.textContent + n.kids.map(flat).join("");
const pick = (n, cls) =>
  (n.className.split(" ").includes(cls) ? [n] : []).concat(n.kids.flatMap(k => pick(k, cls)));
const shape = (root) => ({
  lit: pick(root, "here").map(n => flat(n)),
  took: pick(root, "took").map(n => flat(n)),
  caption: pick(root, "fsm-last").map(n => flat(n)),
  bad: pick(root, "fsm-bad").map(n => flat(n)),
  boxes: pick(root, "st").map(n => flat(n)),
  edges: pick(root, "ed").map(n => flat(n)),
});
"""


def render_source() -> str:
    page = (resources.files("raftkv") / "static" / "dashboard.html").read_text()
    render = page[page.index(RENDER_START): page.index(RENDER_END)]
    # `el` is the page's own element constructor and is shared with every other panel, so
    # it is taken from the page rather than reimplemented here.
    head = "function el(tag, className, text) {"
    start = page.index(head)
    end = page.index("\n}\n", start) + 3
    return page[start:end] + "\n" + render


def draw(which: str, state: dict, events: list[dict]) -> dict:
    script = (
        DOM_SHIM
        + f"\nlet lastLines = {json.dumps(events)};\n"
        + automaton_source() + "\n"
        + render_source() + "\n"
        + f'console.log(JSON.stringify(shape({which}("127.0.0.1:8001", '
        + f"{json.dumps(state)}))));"
    )
    proc = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        capture_output=True, text=True, timeout=30,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


def node_state(**over) -> dict:
    base = {
        "node_id": "node-1", "role": "follower", "term": 12, "voted_for": None,
        "log_length": 14, "commit_index": 12, "last_applied": 12,
    }
    return {**base, **over}


def test_the_ballot_panel_lights_the_state_voted_for_says():
    got = draw("ballotAutomaton", node_state(voted_for="node-3"),
               [ev("vote_granted", 100.0, term=12, candidate="node-3")])
    assert got["lit"] == ["GRANTED"]
    assert got["took"] == ["grants →"]
    assert "granted to node-3" in got["caption"][0]
    assert got["bad"] == []


def test_the_ballot_panel_lights_its_own_candidacy():
    got = draw("ballotAutomaton", node_state(voted_for="node-1", role="candidate"),
               [ev("election_started", 100.0, term=12, role="candidate")])
    assert got["lit"] == ["VOTED FOR ITSELF"]
    assert got["took"] == ["campaigns ↓"]


def test_a_straw_poll_lights_the_one_drawn_loop_wherever_the_ballot_stands():
    """The loop cell is shared by all three states, so this is what proves `poll` is
    routed to it rather than quietly matching nothing and lighting no edge at all."""
    got = draw("ballotAutomaton", node_state(voted_for="node-3"), [
        ev("vote_granted", 100.0, term=12, candidate="node-3"),
        ev("pre_vote_started", 160.0, term=12, would_be_term=13),
    ])
    assert got["lit"] == ["GRANTED"]
    assert got["took"] == ["↺ straw poll"]


def test_a_double_vote_reaches_the_panel_in_red():
    """The logic flagging it is worth nothing if the row never gets appended."""
    got = draw("ballotAutomaton", node_state(voted_for="node-2"), [
        ev("vote_granted", 100.0, term=12, candidate="node-3"),
        ev("vote_granted", 100.4, term=12, candidate="node-2"),
    ])
    assert len(got["bad"]) == 1
    assert "two votes in term 12: node-3, then node-2" in got["bad"][0]
    assert "§5.2" in got["bad"][0]


def test_the_log_panel_carries_the_three_watermarks_and_both_gaps():
    got = draw("logAutomaton", node_state(log_length=14, commit_index=12, last_applied=9),
               [ev("commit_advanced", 100.0, term=12, commit_index=12)])
    assert got["boxes"] == ["APPENDED · 14", "COMMITTED · 12", "APPLIED · 9"]
    assert got["lit"] == ["APPENDED · 14"], "the newest entry has only been appended"
    assert got["took"] == ["majority →"]
    gaps = got["caption"][0]
    assert "2 awaiting a majority" in gaps
    assert "3 committed, not yet applied" in gaps


def test_a_caught_up_log_lights_applied_and_reports_no_backlog():
    got = draw("logAutomaton", node_state(log_length=12, commit_index=12, last_applied=12),
               [ev("applied", 100.0, term=12, index=12, key="a")])
    assert got["lit"] == ["APPLIED · 12"]
    assert got["caption"][0].startswith("0 awaiting a majority · 0 committed")


def test_an_empty_log_lights_no_box_at_all():
    got = draw("logAutomaton", node_state(log_length=0, commit_index=0, last_applied=0), [])
    assert got["lit"] == []
    assert "no log activity in the retained window" in got["caption"][1]


def test_ordinary_repair_draws_the_cut_edge_without_the_red_line():
    """A follower losing an uncommitted suffix is Raft working. The panel has to show the
    edge and stay quiet, or the alarm means nothing the day it fires."""
    got = draw("logAutomaton", node_state(log_length=13, commit_index=12, last_applied=12), [
        ev("commit_advanced", 100.0, term=12, commit_index=12),
        ev("log_truncated", 200.0, term=12, from_index=13),
    ])
    assert got["took"] == ["✂ cut back"]
    assert got["bad"] == []


def test_a_committed_entry_being_cut_reaches_the_panel_in_red():
    got = draw("logAutomaton", node_state(log_length=13, commit_index=12, last_applied=12), [
        ev("commit_advanced", 100.0, term=12, commit_index=12),
        ev("log_truncated", 200.0, term=12, from_index=6),
    ])
    assert len(got["bad"]) == 1
    assert "a committed entry was truncated" in got["bad"][0]


def test_broken_containment_in_state_is_reported_even_with_an_empty_log_window():
    """The /state half of the check, which is the half that survives the ring buffer
    rolling past every event."""
    got = draw("logAutomaton",
               node_state(log_length=12, commit_index=7, last_applied=9), [])
    assert len(got["bad"]) == 1
    assert "last_applied 9 is ahead of commit_index 7" in got["bad"][0]


def test_a_healthy_panel_shows_no_alarm_row_at_all():
    """Not a green "0 violations" badge: a permanent all-clear is something the eye stops
    reading long before the day it changes."""
    got = draw("logAutomaton", node_state(),
               [ev("applied", 100.0, term=12, index=12, key="a")])
    assert got["bad"] == []
