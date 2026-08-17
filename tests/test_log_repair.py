"""Accelerated log backtracking (§5.3): the conflict hint, and the walk it replaces.

Why this file exists, in numbers. `_append_to_peer` used to move nextIndex by exactly one
index per rejection. Measured on 2026-08-16 against the running 5-node cluster, with the
leader replicating to four peers: ~2 rejections per second reach any one follower. A node
wiped by `/admin/reset` while the log held 6224 entries therefore needed **6224 round
trips — about 52 minutes** to rejoin, and it spends every one of them showing an empty
state machine on the dashboard. Safe, and indistinguishable from broken.

The fix is the standard §5.3 optimisation: on a consistency-check rejection the follower
says where to resume, and the leader jumps there. Two properties are being pinned here,
and only the second is about speed:

  1. SAFETY — the hint is advice, never authority. Every jump still lands on an ordinary
     consistency check, and a nonsense hint must cost a round trip rather than corrupt a
     log or hang the repair. `test_a_hint_that_would_not_make_progress_*` is that.
  2. SPEED — the walk terminates in a couple of round trips instead of one per entry.

The leader half is exercised through `_next_index_after_rejection` directly: the walk is
a pure function of (current nextIndex, response, our log), and driving it through the
transport would only add a mock to assert against.
"""

from raftkv.models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    Command,
    LogEntry,
)


def entry(term: int, rid: str = "r") -> LogEntry:
    return LogEntry(term=term, command=Command(op="set", key="k", value="v", request_id=rid))


def ae(term=1, prev_idx=0, prev_term=0, entries=(), commit=0, leader="node-9"):
    return AppendEntriesRequest(
        term=term, leader_id=leader, prev_log_index=prev_idx,
        prev_log_term=prev_term, entries=list(entries), leader_commit=commit,
    )


def seed(node, terms: list[int]) -> None:
    """Give a node a log whose entry at index i has terms[i-1]."""
    node.storage.append([entry(t, f"r{i}") for i, t in enumerate(terms)])


# ---- the follower half: what the hint says ----------------------------------


def test_a_short_log_reports_the_first_index_it_is_missing(make_node):
    node = make_node()
    seed(node, [1, 1, 1])  # we hold 1..3
    resp = node.handle_append_entries(ae(term=1, prev_idx=9, prev_term=1))
    assert not resp.success
    assert resp.conflict_index == 4, "first index we do NOT have"
    assert resp.conflict_term is None, "a gap is not a term disagreement"


def test_an_empty_log_reports_index_one(make_node):
    """The 1-indexed log's boundary case: index 0 is the sentinel, so the first index a
    wiped follower is missing is 1. Off by one here and a reset node never repairs."""
    node = make_node()
    resp = node.handle_append_entries(ae(term=1, prev_idx=5, prev_term=1))
    assert not resp.success
    assert resp.conflict_index == 1
    assert resp.conflict_term is None


def test_a_term_mismatch_names_the_term_and_where_that_term_starts(make_node):
    """We hold the index, under the wrong term. The leader can then skip our entire run
    of that term in one step, which is the whole point of sending the term as well."""
    node = make_node()
    seed(node, [1, 2, 2, 2])  # indices 1..4; our run of term 2 starts at index 2
    resp = node.handle_append_entries(ae(term=5, prev_idx=4, prev_term=5))
    assert not resp.success
    assert resp.conflict_term == 2
    assert resp.conflict_index == 2, "first index of OUR conflicting term, not the last"


def test_a_stale_term_rejection_carries_no_hint(make_node):
    """A stale leader is about to step down; there is nothing useful to tell it, and a
    hint computed against a log it has no right to append to would be noise."""
    node = make_node()
    seed(node, [1, 1])
    node.current_term = 9
    resp = node.handle_append_entries(ae(term=3, prev_idx=1, prev_term=1))
    assert not resp.success
    assert resp.conflict_index is None
    assert resp.conflict_term is None


def test_a_successful_append_carries_no_hint(make_node):
    node = make_node()
    resp = node.handle_append_entries(ae(term=1, entries=[entry(1, "a")]))
    assert resp.success
    assert resp.conflict_index is None and resp.conflict_term is None


# ---- the leader half: what the leader does with it --------------------------


def reject(index=None, term=None) -> AppendEntriesResponse:
    return AppendEntriesResponse(
        term=1, success=False, conflict_index=index, conflict_term=term
    )


def test_no_hint_falls_back_to_the_one_step_walk(make_node):
    """A peer running older code sends no hint. That must still repair, just slowly —
    the optimisation is not allowed to become a requirement."""
    leader = make_node()
    seed(leader, [1, 1, 1])
    leader.next_index["node-2"] = 3
    assert leader._next_index_after_rejection("node-2", reject()) == 2


def test_a_gap_hint_jumps_straight_to_the_followers_end(make_node):
    """The 52-minute case, collapsed. Follower holds 3 entries, leader is at 6224."""
    leader = make_node()
    seed(leader, [1] * 6224)
    leader.next_index["node-2"] = 6225
    assert leader._next_index_after_rejection("node-2", reject(index=4)) == 4


def test_a_term_we_also_hold_resumes_past_our_last_entry_in_it(make_node):
    """We share the term, so everything up to our last entry in it is common ground.
    Resuming at the follower's FIRST index of that term would re-send entries that
    already match — correct, but slower, and it is the difference the term buys."""
    leader = make_node()
    seed(leader, [1, 2, 2, 2, 3])  # our run of term 2 is indices 2..4
    leader.next_index["node-2"] = 6
    assert leader._next_index_after_rejection("node-2", reject(index=2, term=2)) == 5


def test_a_term_we_never_had_resumes_at_the_followers_first_index_for_it(make_node):
    """The follower's whole run of that term came from a leader we never heard from, so
    every index in it is suspect and none of it can match."""
    leader = make_node()
    seed(leader, [1, 1, 1, 1, 1])
    leader.next_index["node-2"] = 6
    assert leader._next_index_after_rejection("node-2", reject(index=2, term=7)) == 2


# ---- the hint is advice, not authority --------------------------------------


def test_a_hint_that_would_not_make_progress_degrades_to_one_step(make_node):
    """Termination is the property. A rejection that left nextIndex where it was would
    retry the identical AppendEntries forever, so a hint at or above the current index
    is ignored in favour of the naive step."""
    leader = make_node()
    seed(leader, [1, 1, 1, 1, 1])
    leader.next_index["node-2"] = 4
    assert leader._next_index_after_rejection("node-2", reject(index=9)) == 3
    assert leader._next_index_after_rejection("node-2", reject(index=4)) == 3


def test_a_term_hint_cannot_push_next_index_up_either(make_node):
    """Same guard on the other path: our last index of the conflicting term may sit
    ABOVE the follower's nextIndex if the walk has already gone past it."""
    leader = make_node()
    seed(leader, [1, 1, 1, 9, 9, 9])  # our run of term 9 ends at index 6
    leader.next_index["node-2"] = 3
    assert leader._next_index_after_rejection("node-2", reject(index=1, term=9)) == 2


def test_next_index_never_goes_below_one(make_node):
    """Index 0 is the sentinel and is not a real entry; a nextIndex of 0 would make
    prev_log_index -1 and the consistency check meaningless."""
    leader = make_node()
    seed(leader, [1])
    leader.next_index["node-2"] = 1
    assert leader._next_index_after_rejection("node-2", reject()) == 1
    assert leader._next_index_after_rejection("node-2", reject(index=0)) == 1


# ---- end to end: the two halves actually agree ------------------------------


def test_a_wiped_follower_repairs_in_two_round_trips(make_node):
    """The property the whole file is for, driven through both halves.

    A follower is wiped while the leader holds 200 entries. Each iteration is one round
    trip: the leader builds an AppendEntries from its nextIndex, the follower answers,
    the leader moves nextIndex. Under the old one-step walk this took 200 iterations; the
    bound below is deliberately far under that and still comfortably over the real cost.
    """
    leader, follower = make_node(node_id="node-1"), make_node(node_id="node-2")
    seed(leader, [1] * 200)
    leader.current_term = 1
    leader.next_index["node-2"] = 201

    trips = 0
    while trips < 10:
        trips += 1
        nxt = leader.next_index["node-2"]
        prev_idx = nxt - 1
        resp = follower.handle_append_entries(
            ae(
                term=1,
                prev_idx=prev_idx,
                prev_term=leader.storage.term_at(prev_idx),
                entries=leader.storage.entries_from(nxt),
            )
        )
        if resp.success:
            break
        leader.next_index["node-2"] = leader._next_index_after_rejection("node-2", resp)

    assert resp.success, "repair never converged"
    assert follower.storage.last_log_index() == 200, "follower did not receive the log"
    assert trips <= 3, f"took {trips} round trips; the point of the hint is that it does not"
