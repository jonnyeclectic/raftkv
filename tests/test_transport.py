import httpx
import pytest
from mockito import ANY

from raftkv.models import (
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
)
from raftkv.transport import HttpTransport, MemoryTransport, TransportError

VOTE_REQ = RequestVoteRequest(term=1, candidate_id="node-1", last_log_index=0, last_log_term=0)


class FakeNode:
    """Functional mock of the RaftNode surface MemoryTransport calls."""

    def __init__(self):
        self.seen = []

    def handle_request_vote(self, req):
        self.seen.append(req)
        return RequestVoteResponse(term=req.term, vote_granted=True)

    def handle_append_entries(self, req):
        return AppendEntriesResponse(term=req.term, success=True)


async def test_http_transport_round_trip(when):
    transport = HttpTransport({"node-2": "peer:8000"}, rpc_timeout=0.1)
    reply = httpx.Response(
        200,
        json={"term": 1, "vote_granted": True},
        request=httpx.Request("POST", "http://peer:8000/raft/request-vote"),
    )
    # mockito >= 2.0: thenReturn on an async method stays awaitable
    when(transport._client).post(
        "http://peer:8000/raft/request-vote", json=ANY(dict)
    ).thenReturn(reply)
    resp = await transport.request_vote("node-2", VOTE_REQ)
    assert resp == RequestVoteResponse(term=1, vote_granted=True)


async def test_http_transport_maps_network_errors(when):
    transport = HttpTransport({"node-2": "peer:8000"}, rpc_timeout=0.1)
    when(transport._client).post(ANY(str), json=ANY(dict)).thenRaise(
        httpx.ConnectError("connection refused")
    )
    with pytest.raises(TransportError):
        await transport.request_vote("node-2", VOTE_REQ)


async def test_http_transport_maps_bad_status(when):
    transport = HttpTransport({"node-2": "peer:8000"}, rpc_timeout=0.1)
    reply = httpx.Response(
        500, request=httpx.Request("POST", "http://peer:8000/raft/request-vote")
    )
    when(transport._client).post(ANY(str), json=ANY(dict)).thenReturn(reply)
    with pytest.raises(TransportError):
        await transport.request_vote("node-2", VOTE_REQ)


async def test_memory_transport_delivers():
    transport = MemoryTransport()
    node = FakeNode()
    transport.register("node-2", node)
    resp = await transport.request_vote("node-2", VOTE_REQ)
    assert resp.vote_granted
    assert node.seen[0].candidate_id == "node-1"


async def test_memory_transport_crash_blocks_both_directions():
    transport = MemoryTransport()
    transport.register("node-2", FakeNode())
    transport.crash("node-2")
    with pytest.raises(TransportError):
        await transport.request_vote("node-2", VOTE_REQ)
    transport.restore("node-2")
    assert (await transport.request_vote("node-2", VOTE_REQ)).vote_granted


async def test_memory_transport_partition_and_heal():
    transport = MemoryTransport()
    transport.register("node-2", FakeNode())
    transport.partition({"node-1"}, {"node-2", "node-3"})
    with pytest.raises(TransportError):
        await transport.request_vote("node-2", VOTE_REQ)
    transport.heal()
    assert (await transport.request_vote("node-2", VOTE_REQ)).vote_granted
