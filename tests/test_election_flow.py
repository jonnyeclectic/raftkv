from mockito import ANY, verify

from conftest import StubTransport
from raftkv.models import RequestVoteRequest, RequestVoteResponse, Role
from raftkv.transport import TransportError


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
    assert node.next_index == {"node-2": 1, "node-3": 1}  # empty log, so start at 1
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
