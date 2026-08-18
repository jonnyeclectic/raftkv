"""The commit scan: same answer as the exhaustive walk, without the O(N²) cost.

`_advance_commit_index()` used to walk every index from `last_log_index` down to
`commit_index`, one SQLite `term_at()` per index, once per `submit()`. While a burst of N
writes was in flight that walk was O(N) per call, so the burst cost O(N²) queries —
instrumented, 200 concurrent writes issued **20,315** `term_at()` calls against 203
appends. The queries are synchronous on the event loop that also owes followers a
heartbeat, which is why a big enough burst ends in an election instead of in commits.

`acked` is a step function of the candidate index: it changes only where a peer's
matchIndex sits, so only those values (plus `last_log_index`, for the lone-voter case)
can be the answer. This file pins two things:

  1. EQUIVALENCE — the sparse scan commits exactly the index the exhaustive walk would.
     `exhaustive()` below is the old implementation, kept as the oracle rather than as a
     comment, and every case is checked against it.
  2. COST — the number of `term_at()` calls no longer grows with the log.

Equivalence is the one that matters. A commit index that is one too high is an
acknowledged write that a later leader can lose.
"""

import itertools

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from raftkv.models import Command, LogEntry


def entry(term: int, rid: str = "r") -> LogEntry:
    return LogEntry(term=term, command=Command(op="set", key="k", value="v", request_id=rid))


def seed(node, terms: list[int]) -> None:
    node.storage.append([entry(t, f"r{i}") for i, t in enumerate(terms)])


def exhaustive(node) -> int:
    """The old implementation, as an oracle: walk every index, return where it would
    commit. Deliberately a copy — an oracle that shares code with the thing it checks
    proves nothing."""
    for candidate in range(node.storage.last_log_index(), node.commit_index, -1):
        if node.storage.term_at(candidate) != node.current_term:
            break
        acked = {node.cfg.node_id} | {
            p for p, m in node.match_index.items() if m >= candidate
        }
        if node._has_agreement(acked):
            return candidate
    return node.commit_index


def leader(make_node, *, voters=("node-1", "node-2", "node-3"), term=1):
    """A node set up as leader of `voters`, with no timers running."""
    node = make_node(node_id="node-1", peer_ids=tuple(v for v in voters if v != "node-1"))
    node.current_term = term
    return node


def commit_via_scan(node) -> int:
    node._advance_commit_index()
    return node.commit_index


# ---- equivalence, case by case ----------------------------------------------


@pytest.mark.parametrize(
    "match, expected_note",
    [
        ({"node-2": 0, "node-3": 0}, "nobody has anything"),
        ({"node-2": 1, "node-3": 0}, "one peer: with us that is a majority of three"),
        ({"node-2": 5, "node-3": 0}, "one peer far ahead"),
        ({"node-2": 5, "node-3": 3}, "two peers at different points"),
        ({"node-2": 10, "node-3": 10}, "both peers at the end"),
        ({"node-2": 10, "node-3": 7}, "the lower of the two decides nothing extra"),
        ({"node-2": 4, "node-3": 4}, "both at the same middling index"),
    ],
)
def test_the_sparse_scan_commits_where_the_exhaustive_walk_would(
    make_node, match, expected_note
):
    node = leader(make_node)
    seed(node, [1] * 10)
    node.match_index = dict(match)
    want = exhaustive(node)  # read-only, so it can run first on the same node
    assert commit_via_scan(node) == want, expected_note


def test_entries_from_an_older_term_never_commit_on_their_own(make_node):
    """§5.4.2 / Figure 8. A majority holding an old-term entry is NOT enough; it commits
    only transitively, when something from the current term commits above it."""
    node = leader(make_node, term=5)
    seed(node, [1, 1, 1])  # everything is from term 1; we are in term 5
    node.match_index = {"node-2": 3, "node-3": 3}
    assert commit_via_scan(node) == 0, "an old-term entry committed on a majority alone"


def test_a_current_term_entry_carries_the_older_ones_with_it(make_node):
    node = leader(make_node, term=5)
    seed(node, [1, 1, 5])  # index 3 is ours
    node.match_index = {"node-2": 3, "node-3": 0}
    assert commit_via_scan(node) == 3, "committing index 3 must carry 1 and 2"


def test_commit_index_never_moves_backwards(make_node):
    node = leader(make_node)
    seed(node, [1] * 10)
    node.commit_index = 8
    node.match_index = {"node-2": 2, "node-3": 2}  # peers fell behind
    assert commit_via_scan(node) == 8


def test_a_single_voter_commits_without_any_peer(make_node):
    """No peer has a matchIndex at all, so the step set is empty and only
    `last_log_index` is left to test. Drop it from the candidates and a one-node cluster
    can never commit — which also makes every membership change impossible."""
    node = make_node(node_id="node-1", peer_ids=())
    node.current_term = 1
    seed(node, [1, 1, 1])
    node.match_index = {}
    assert commit_via_scan(node) == 3


# ---- joint consensus still needs BOTH majorities ----------------------------


def test_a_joint_configuration_still_needs_separate_majorities(make_node):
    """The candidate set is an optimisation of WHICH indices to test, never of the test.
    Here C-new alone would be satisfied and C-old would not, so nothing may commit."""
    from raftkv.models import ClusterConfig

    node = make_node(node_id="node-1", peer_ids=("node-2", "node-3", "node-4", "node-5"))
    node.current_term = 1
    seed(node, [1] * 6)
    addr = "127.0.0.1:8000"
    node.config = ClusterConfig(
        voters={n: addr for n in ["node-1", "node-4", "node-5"]},          # C-new
        old_voters={n: addr for n in ["node-1", "node-2", "node-3"]},      # C-old
    )
    node.match_index = {"node-2": 0, "node-3": 0, "node-4": 6, "node-5": 6}
    assert commit_via_scan(node) == 0, "a C-new-only majority committed during a joint phase"

    node.match_index["node-2"] = 6  # now C-old has a majority too
    assert commit_via_scan(node) == 6


# ---- the cost ---------------------------------------------------------------


def count_term_at(node) -> tuple[int, int]:
    """Run one scan, returning (where it committed, how many term_at() calls it cost)."""
    calls = []
    real = node.storage.term_at
    node.storage.term_at = lambda idx: (calls.append(idx), real(idx))[1]
    try:
        node._advance_commit_index()
    finally:
        node.storage.term_at = real
    return node.commit_index, len(calls)


def test_the_scan_does_not_get_more_expensive_as_the_log_grows(make_node):
    """The regression guard, in the shape the burst actually takes.

    The leader has appended 3000 entries; the followers have acknowledged the first 1000.
    The exhaustive walk starts at the END of the log and has to step down through all
    2000 unacknowledged indices before it reaches one a majority holds — 2001 `term_at()`
    calls, and it pays them again on the next `submit()`, and the one after that. That
    repetition is the O(N²).

    Only two indices can be the answer here: 3000 (the end) and 1000 (where the peers
    are). Everything between them asks an identical question.
    """
    node = leader(make_node)
    seed(node, [1] * 3000)
    node.match_index = {"node-2": 1000, "node-3": 1000}

    committed, calls = count_term_at(node)

    assert committed == 1000, "must still commit exactly where the majority reaches"
    assert calls <= 4, (
        f"{calls} term_at() calls to cross a 2000-entry gap; the exhaustive walk made 2001"
    )


def test_cost_is_flat_as_the_unacknowledged_gap_widens(make_node):
    """Stronger than a fixed bound: the same peers, three log sizes. The old walk's cost
    tracks the gap; this one must not move at all."""
    costs = {}
    for i, size in enumerate((500, 2000, 5000)):
        # A distinct node_id per size: make_node keys the sqlite file on it, so reusing
        # one would append each log on top of the last and measure the wrong thing.
        node = make_node(node_id=f"scan-{i}", peer_ids=("node-2", "node-3"))
        node.current_term = 1
        seed(node, [1] * size)
        node.match_index = {"node-2": 100, "node-3": 100}
        committed, calls = count_term_at(node)
        assert committed == 100, f"log of {size} committed at {committed}, wanted 100"
        costs[size] = calls
    assert len(set(costs.values())) == 1, f"cost grew with the log: {costs}"


# ---- the same equivalence, generated rather than chosen -----------------------
#
# Everything above compares the sparse scan against `exhaustive()` for term sequences a
# person picked: all-ones, [1,1,5], a 3000-entry run. That proves equivalence for the
# cases someone thought of, which is exactly the shape of gap this file exists to close —
# a commit index one too high is an acknowledged write a later leader can lose, and it
# only has to happen on one term sequence.
#
# The oracle was already written; only the generator was missing.

_ids = itertools.count()

# Function-scoped fixture + @given would normally reuse one node across examples, which is
# what the health check is for. Each example draws a fresh node_id below and make_node keys
# its sqlite file on that, so every example gets its own empty log. Suppressed for that
# specific reason, not to quiet a warning.
PROPERTY = settings(
    max_examples=250,
    deadline=None,  # sqlite timing varies; this asserts equivalence, never latency
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)


@st.composite
def cluster_state(draw):
    """A leader state the implementation can actually reach.

    `match_index` is bounded by the log length, and that bound is load-bearing rather than
    tidiness. Hypothesis's first counterexample was `terms=[1]` with a peer at
    `match_index=2`: the exhaustive walk starts at `last_log_index` so it never considers
    index 2 and commits 1, while the sparse scan puts 2 in its candidate set, finds
    `term_at(2)` is None, and breaks before reaching 1. A real disagreement — on a state
    the leader cannot produce.

    `match_index[peer] = prev_index + len(entries)` (raft.py), and those entries came out
    of the leader's own log, so the value can never exceed the leader's `last_log_index`.
    A leader also never truncates its own log, so it cannot fall behind a matchIndex it
    already recorded. Generating past that bound tests a cluster that does not exist.

    Terms come from a tiny alphabet on purpose: the interesting cases are where the same
    term repeats and a run boundary lands near a matchIndex, not where every entry differs.
    """
    # SORTED, because a Raft log's terms are non-decreasing: a leader appends only its
    # own term, and terms only ever increase. This is the second bound Hypothesis forced
    # into the open. Given `terms=[1,2,1,2]` with current_term 2 and a peer at index 2, the
    # exhaustive walk stops at index 3 (term 1, not current) and commits nothing, while the
    # sparse scan skips straight from 4 to 2 and commits 2 — which Figure 2 says is
    # correct, since index 2 holds a current-term entry a majority has. The two disagree
    # only because the walk's early `break` assumes non-decreasing terms, which raft.py
    # states outright: "terms only shrink going backwards; nothing below qualifies".
    # An unsorted log is not a log this algorithm can produce.
    terms = draw(
        st.lists(st.integers(min_value=1, max_value=3), min_size=1, max_size=40).map(sorted)
    )
    last_index = len(terms)
    matches = draw(st.tuples(st.integers(0, last_index), st.integers(0, last_index)))
    return {
        "terms": terms,
        "matches": matches,
        "current_term": draw(st.integers(1, 3)),
        "commit_index": draw(st.integers(0, last_index)),
    }


def _generated_leader(make_node, state):
    node = make_node(node_id=f"prop-{next(_ids)}", peer_ids=("node-2", "node-3"))
    node.current_term = state["current_term"]
    seed(node, state["terms"])
    node.match_index = {"node-2": state["matches"][0], "node-3": state["matches"][1]}
    node.commit_index = state["commit_index"]
    return node


@PROPERTY
@given(state=cluster_state())
def test_the_sparse_scan_always_agrees_with_the_exhaustive_walk(make_node, state):
    """The property the whole optimisation rests on, over generated inputs."""
    node = _generated_leader(make_node, state)
    expected = exhaustive(node)  # pure: reads commit_index, never moves it
    actual = commit_via_scan(node)  # mutates, so it must run second
    assert actual == expected, (
        f"sparse scan committed {actual}, exhaustive walk says {expected}. "
        f"terms={state['terms']} match_index={node.match_index}"
    )


@PROPERTY
@given(state=cluster_state())
def test_the_commit_index_never_moves_backwards(make_node, state):
    """Figure 2's monotonicity, which no hand-written case in this file states directly.
    A commit index that retreats un-commits an entry the leader already acknowledged."""
    node = _generated_leader(make_node, state)
    before = node.commit_index
    assert commit_via_scan(node) >= before


@PROPERTY
@given(state=cluster_state())
def test_nothing_commits_past_the_end_of_the_log(make_node, state):
    """The leader must never commit an index it does not itself hold, whatever the peers
    report — the generated half of "matchIndex comes from the arguments sent"."""
    node = _generated_leader(make_node, state)
    assert commit_via_scan(node) <= node.storage.last_log_index()


def test_a_match_index_beyond_the_log_is_assumed_unreachable(make_node):
    """Pins the assumption `cluster_state()` encodes, so it is written down rather than
    only implied by a strategy bound.

    If `match_index` could ever exceed the leader's `last_log_index`, the candidate set
    would lead with an index the log does not hold, `term_at()` would return None, and the
    scan would break before testing any real candidate. The cluster would stop committing.

    That is fail-safe rather than a lost write, which is why it has never been observed —
    but it is also the reason `match_index` must keep coming from the arguments sent. Set
    it from anything peer-reported and this becomes reachable.
    """
    node = leader(make_node)
    seed(node, [1])
    node.match_index = {"node-2": 0, "node-3": 2}  # impossible: 2 > last_log_index of 1

    assert commit_via_scan(node) == 0, (
        "if this now commits, the scan gained a clamp and cluster_state()'s bound can be "
        "relaxed; if it still stalls, match_index must stay bounded by the leader's log"
    )


def test_the_scan_relies_on_log_terms_being_non_decreasing(make_node):
    """The second precondition `cluster_state()` encodes, pinned in the open.

    `_advance_commit_index` breaks out of its candidate loop on the first index whose term
    is not the current one, on the reasoning raft.py states directly: "terms only shrink
    going backwards". That holds because a leader appends only its own term and terms only
    increase, so a valid log is non-decreasing.

    Feed it a log that is not — [1, 2, 1, 2], which Raft cannot produce — and the sparse
    scan and the exhaustive walk part company: the walk halts at index 3 and commits
    nothing, while the sparse scan skips the intervening indices entirely and commits 2.
    Figure 2 agrees with the SPARSE one here (index 2 holds a current-term entry a majority
    has); it is the walk's early `break` that is wrong on an impossible input.

    Recorded because it is the assumption a future change is most likely to break without
    noticing — anything that appends an entry with a term below the log's tail.
    """
    node = leader(make_node)
    node.current_term = 2
    seed(node, [1, 2, 1, 2])  # not a reachable Raft log
    node.match_index = {"node-2": 0, "node-3": 2}

    assert commit_via_scan(node) == 2, (
        "the sparse scan is expected to commit 2 on this impossible log; if that changed, "
        "the candidate set or the break condition moved and cluster_state()'s sorted "
        "bound needs revisiting"
    )
