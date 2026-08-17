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

import pytest

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
