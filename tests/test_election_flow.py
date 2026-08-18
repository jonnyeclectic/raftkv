import asyncio
import time

from mockito import ANY, verify

from conftest import StubTransport, eventually
from raftkv.models import (
    Command,
    LogEntry,
    NoOp,
    RequestVoteRequest,
    RequestVoteResponse,
    Role,
)
from raftkv.transport import TransportError

# Several tests below call `_start_election(pre_vote=False)`. They are about the REAL
# election -- the tally, the persisted term and self-vote, what the candidate advertises --
# and a stubbed peer that answers every RequestVote identically cannot distinguish the
# straw poll from the election it precedes. The pre-vote round has its own file
# (tests/test_pre_vote.py); routing past it here keeps each test testing what it is named
# after. The timer-loop tests below deliberately do NOT skip it: walking a follower from
# quiet to CANDIDATE through the poll is the production path.


def entry(term: int, rid: str = "r") -> LogEntry:
    return LogEntry(term=term, command=Command(op="set", key="k", value="v", request_id=rid))


async def test_candidate_wins_with_quorum(make_node, when):
    transport = StubTransport()
    node = make_node(transport=transport)
    when(transport).request_vote("node-2", ANY(RequestVoteRequest)).thenReturn(
        RequestVoteResponse(term=1, vote_granted=True)
    )
    when(transport).request_vote("node-3", ANY(RequestVoteRequest)).thenReturn(
        RequestVoteResponse(term=1, vote_granted=False)
    )
    await node._start_election(pre_vote=False)
    assert node.role is Role.LEADER  # self + node-2 = 2 of 3
    assert node.current_term == 1
    # index 1 is the no-op this leader appended on winning, so followers start at 2
    assert node.next_index == {"node-2": 2, "node-3": 2}
    assert node.match_index == {"node-2": 0, "node-3": 0}
    verify(transport).request_vote("node-2", ANY(RequestVoteRequest))


async def test_candidate_without_quorum_stays_candidate(make_node, when):
    transport = StubTransport()
    node = make_node(transport=transport)
    when(transport).request_vote(ANY(str), ANY(RequestVoteRequest)).thenReturn(
        RequestVoteResponse(term=1, vote_granted=False)
    )
    await node._start_election(pre_vote=False)
    assert node.role is Role.CANDIDATE  # the election timer loop will retry


async def test_candidate_steps_down_on_higher_term_reply(make_node, when):
    transport = StubTransport()
    node = make_node(transport=transport)
    when(transport).request_vote(ANY(str), ANY(RequestVoteRequest)).thenReturn(
        RequestVoteResponse(term=9, vote_granted=False)
    )
    await node._start_election(pre_vote=False)
    assert node.role is Role.FOLLOWER
    assert node.current_term == 9


async def test_unreachable_peers_do_not_crash_election(make_node, when):
    transport = StubTransport()
    node = make_node(transport=transport)
    when(transport).request_vote("node-2", ANY(RequestVoteRequest)).thenRaise(
        TransportError("down")
    )
    when(transport).request_vote("node-3", ANY(RequestVoteRequest)).thenReturn(
        RequestVoteResponse(term=1, vote_granted=True)
    )
    await node._start_election(pre_vote=False)
    assert node.role is Role.LEADER  # self + node-3
    assert node.metrics.rpc_failures == 1


async def test_single_node_cluster_elects_itself(make_node):
    node = make_node(peer_ids=())
    await node._start_election()
    assert node.role is Role.LEADER


async def test_election_persists_term_and_self_vote(make_node, when):
    transport = StubTransport()
    node = make_node(transport=transport)
    when(transport).request_vote(ANY(str), ANY(RequestVoteRequest)).thenReturn(
        RequestVoteResponse(term=1, vote_granted=False)
    )
    await node._start_election(pre_vote=False)
    assert node.storage.load()[:2] == (1, "node-1")  # rule 1: before counting replies


async def test_candidate_advertises_its_last_entry_not_its_commit_index(make_node, when):
    """§5.4.1 compares the candidate's LAST entry. Advertising commit_index instead hides
    the uncommitted suffix the candidate holds, so a node with strictly less data looks
    equally up to date and the election restriction stops restricting anything."""
    transport = StubTransport()
    node = make_node(transport=transport)
    node.storage.append([entry(2, "a"), entry(2, "b"), entry(3, "c")])
    node.current_term = 3
    node.commit_index = 1  # deliberately behind the log
    when(transport).request_vote(ANY(str), ANY(RequestVoteRequest)).thenReturn(
        RequestVoteResponse(term=4, vote_granted=False)
    )

    await node._start_election(pre_vote=False)

    asked = RequestVoteRequest(
        term=4, candidate_id="node-1", last_log_index=3, last_log_term=3
    )
    verify(transport).request_vote("node-2", asked)
    verify(transport).request_vote("node-3", asked)


def test_becoming_leader_appends_a_noop_and_asks_to_replicate_now(make_node):
    """Two things a new leader owes the cluster. The no-op is an entry from its OWN term,
    so committing it flushes through whatever a previous leader committed but never
    applied (thesis §6.4), and it gates every membership change. The immediate heartbeat
    closes the window in which a second node times out and campaigns in the same term."""
    node = make_node()
    node.storage.append([entry(3, "a")])
    node.current_term = 4
    assert not node._replicate_now.is_set()

    node._become_leader()

    assert node._replicate_now.is_set()
    tail = node.storage.entry(node.storage.last_log_index())
    assert isinstance(tail.command, NoOp)
    assert tail.term == 4  # OUR term, not the term of the entry it follows
    assert node._noop_index == 2


class PollingTransport(StubTransport):
    """Grants every straw poll and then goes silent on the real vote.

    That sequence is the production path in miniature: a follower can only reach
    CANDIDATE by first winning a pre-vote, and winning that poll says nothing about
    whether the election it authorises resolves. `req.term - 1` is the poller's own term
    by construction (a poll asks about the term it WOULD run in), so replying with it
    leaves the candidate's term untouched -- a stubbed peer that answered with a higher
    one would depose the node it just encouraged."""

    async def request_vote(self, peer_id, req):
        if req.pre_vote:
            return RequestVoteResponse(term=req.term - 1, vote_granted=True)
        raise TransportError("down")  # peers unreachable, so the election cannot resolve


async def test_follower_campaigns_once_the_timer_expires(make_node):
    node = make_node(transport=PollingTransport())
    node._last_reset = 0.0  # the last heartbeat was arbitrarily long ago
    task = asyncio.create_task(node._election_timer_loop())
    try:
        await eventually(lambda: node.role is Role.CANDIDATE)
    finally:
        task.cancel()
    assert node.current_term >= 1
    # rule 1: the term bump and self-vote are on disk before any reply is counted
    assert node.storage.load()[:2] == (node.current_term, "node-1")


async def test_a_follower_nobody_answers_never_becomes_a_candidate(make_node, when):
    """The behaviour PreVote adds, and the reason the test above needed a transport that
    answers: unreachable peers used to produce a CANDIDATE at a term nobody agreed to.

    An isolated node now stays a quiet follower at its original term no matter how long it
    waits, so it disturbs nothing when the partition heals. `pre_votes_started` is what
    keeps that from being indistinguishable from a wedged loop -- it is the only remaining
    evidence the node is still trying."""
    transport = StubTransport()
    node = make_node(transport=transport)
    when(transport).request_vote(ANY(str), ANY(RequestVoteRequest)).thenRaise(
        TransportError("down")
    )
    node._last_reset = 0.0
    task = asyncio.create_task(node._election_timer_loop())
    try:
        await eventually(lambda: node.metrics.pre_votes_started >= 3, timeout=3.0)
    finally:
        task.cancel()
    assert node.role is Role.FOLLOWER
    assert node.current_term == 0            # no term burnt, so nothing to disrupt later
    assert node.metrics.elections_started == 0
    assert node.storage.load()[:2] == (0, None)  # and nothing persisted on the way


async def test_follower_does_not_campaign_before_the_timer_expires(make_node):
    node = make_node()
    node._last_reset = time.monotonic()
    node._election_timeout = 10.0  # far beyond this test's lifetime
    task = asyncio.create_task(node._election_timer_loop())
    try:
        await asyncio.sleep(node.cfg.tick_interval * 5)
        assert node.role is Role.FOLLOWER
        assert node.current_term == 0
    finally:
        task.cancel()
