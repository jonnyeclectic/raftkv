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
    await node._start_election()
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
    await node._start_election()
    assert node.role is Role.CANDIDATE  # the election timer loop will retry


async def test_candidate_steps_down_on_higher_term_reply(make_node, when):
    transport = StubTransport()
    node = make_node(transport=transport)
    when(transport).request_vote(ANY(str), ANY(RequestVoteRequest)).thenReturn(
        RequestVoteResponse(term=9, vote_granted=False)
    )
    await node._start_election()
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
    await node._start_election()
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
    await node._start_election()
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

    await node._start_election()

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


async def test_follower_campaigns_once_the_timer_expires(make_node, when):
    transport = StubTransport()
    node = make_node(transport=transport)
    when(transport).request_vote(ANY(str), ANY(RequestVoteRequest)).thenRaise(
        TransportError("down")  # peers unreachable, so the election cannot resolve
    )
    node._last_reset = 0.0  # the last heartbeat was arbitrarily long ago
    task = asyncio.create_task(node._election_timer_loop())
    try:
        await eventually(lambda: node.role is Role.CANDIDATE)
    finally:
        task.cancel()
    assert node.current_term >= 1
    # rule 1: the term bump and self-vote are on disk before any reply is counted
    assert node.storage.load()[:2] == (node.current_term, "node-1")


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
