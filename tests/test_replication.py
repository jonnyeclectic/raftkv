import pytest
from mockito import ANY

from conftest import StubTransport
from raftkv.models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    Command,
    LogEntry,
    Role,
)
from raftkv.raft import NotLeaderError


def entry(term: int, rid: str = "r") -> LogEntry:
    return LogEntry(term=term, command=Command(op="set", key="k", value="v", request_id=rid))


def as_leader(node, term=1):
    node.current_term = term
    node._become_leader()
    return node


async def test_success_updates_match_index_from_sent_args(make_node, when):
    transport = StubTransport()
    node = as_leader(make_node(transport=transport))
    node.storage.append([entry(1, "a"), entry(1, "b")])
    node.next_index["node-2"] = 1  # follower far behind; leader sends both entries
    when(transport).append_entries("node-2", ANY(AppendEntriesRequest)).thenReturn(
        AppendEntriesResponse(term=1, success=True)
    )
    await node._append_to_peer("node-2", term_when_sent=1)
    # prev(0) + 3 sent, FROM THE SENT ARGS: the term's no-op plus both entries
    assert node.match_index["node-2"] == 3
    assert node.next_index["node-2"] == 4  # match + 1


async def test_failure_backs_off_next_index(make_node, when):
    transport = StubTransport()
    node = as_leader(make_node(transport=transport))
    node.storage.append([entry(1, "a")])
    node.next_index["node-2"] = 2
    when(transport).append_entries("node-2", ANY(AppendEntriesRequest)).thenReturn(
        AppendEntriesResponse(term=1, success=False)
    )
    await node._append_to_peer("node-2", term_when_sent=1)
    assert node.next_index["node-2"] == 1
    assert node.match_index["node-2"] == 0  # match NEVER derived from nextIndex


async def test_higher_term_reply_deposes_leader(make_node, when):
    transport = StubTransport()
    node = as_leader(make_node(transport=transport))
    when(transport).append_entries("node-2", ANY(AppendEntriesRequest)).thenReturn(
        AppendEntriesResponse(term=9, success=False)
    )
    await node._append_to_peer("node-2", term_when_sent=1)
    assert node.role is Role.FOLLOWER
    assert node.current_term == 9


async def test_transport_error_is_counted_not_raised(make_node, when):
    from raftkv.transport import TransportError

    transport = StubTransport()
    node = as_leader(make_node(transport=transport))
    when(transport).append_entries("node-2", ANY(AppendEntriesRequest)).thenRaise(
        TransportError("down")
    )
    await node._append_to_peer("node-2", term_when_sent=1)  # must not raise
    assert node.metrics.rpc_failures == 1


async def test_never_commits_prior_term_entries_by_counting(make_node):
    """Figure 8 (§5.4.2): an old-term entry on a majority must NOT commit directly."""
    node = make_node()
    node.storage.append([entry(1, "old")])
    node.current_term = 2
    node._become_leader()
    node.match_index = {"node-2": 1, "node-3": 0}  # majority holds index 1
    node._advance_commit_index()
    assert node.commit_index == 0  # rule 3 held
    # a current-term entry replicated to the same majority commits BOTH
    node.storage.append([entry(2, "new")])
    node.match_index = {"node-2": 2, "node-3": 0}
    node._advance_commit_index()
    assert node.commit_index == 2


@pytest.mark.parametrize("reply_term", [2, 5])
async def test_older_or_equal_term_reply_does_not_depose(make_node, when, reply_term):
    """Only a STRICTLY higher term deposes (§5.1). A leader that stood down for its own
    term would lose leadership to any follower that simply rejected an append."""
    transport = StubTransport()
    node = as_leader(make_node(transport=transport), term=5)
    when(transport).append_entries("node-2", ANY(AppendEntriesRequest)).thenReturn(
        AppendEntriesResponse(term=reply_term, success=False)
    )
    await node._append_to_peer("node-2", term_when_sent=5)
    assert node.role is Role.LEADER, f"deposed by a term-{reply_term} reply"
    assert node.current_term == 5


async def test_a_success_is_dropped_if_we_were_deposed_while_awaiting_it(make_node, when):
    """The stale-reply guard. matchIndex is a claim about the CURRENT term's log; writing
    it from a reply to an RPC sent in a term we no longer hold is how a deposed leader
    corrupts the commit calculation of the term that replaced it."""
    transport = StubTransport()
    node = as_leader(make_node(transport=transport), term=1)
    node.storage.append([entry(1, "a")])
    node.next_index["node-2"] = 1  # without the guard this success writes matchIndex

    def depose_then_succeed(peer_id, req):
        node._observe_term(9)  # a higher term arrives from elsewhere, mid-flight
        return AppendEntriesResponse(term=1, success=True)

    when(transport).append_entries("node-2", ANY(AppendEntriesRequest)).thenAnswer(
        depose_then_succeed
    )

    await node._append_to_peer("node-2", term_when_sent=1)

    assert node.role is Role.FOLLOWER
    assert node.current_term == 9
    assert node.match_index.get("node-2", 0) == 0


async def test_each_peer_gets_the_slice_its_own_next_index_asks_for(make_node, when):
    """prev_log_index/prev_log_term are per-peer, and matchIndex is derived from the
    arguments that were SENT. Index 4 is the no-op _become_leader appends, which is why
    every peer is behind here: a fresh leader holds an entry none of its followers has."""
    transport = StubTransport()
    node = make_node(peer_ids=("node-2", "node-3", "node-4"), transport=transport)
    node.current_term = 1
    node.storage.append([entry(1, "a"), entry(1, "b"), entry(1, "c")])
    node._become_leader()  # appends the term-1 no-op at index 4
    node.next_index = {"node-2": 1, "node-3": 2, "node-4": 5}
    sent: dict[str, AppendEntriesRequest] = {}

    def record(peer_id, req):
        sent[peer_id] = req
        return AppendEntriesResponse(term=1, success=True)

    when(transport).append_entries(ANY(str), ANY(AppendEntriesRequest)).thenAnswer(record)

    for peer in ("node-2", "node-3", "node-4"):
        await node._append_to_peer(peer, term_when_sent=1)

    assert (sent["node-2"].prev_log_index, sent["node-2"].prev_log_term) == (0, 0)
    assert len(sent["node-2"].entries) == 4  # a, b, c, no-op
    assert (sent["node-3"].prev_log_index, sent["node-3"].prev_log_term) == (1, 1)
    assert len(sent["node-3"].entries) == 3  # b, c, no-op
    assert (sent["node-4"].prev_log_index, sent["node-4"].prev_log_term) == (4, 1)
    assert sent["node-4"].entries == []  # already caught up: a pure heartbeat
    assert node.match_index == {"node-2": 4, "node-3": 4, "node-4": 4}


async def test_commit_advances_only_as_far_as_a_majority_holds(make_node):
    node = make_node(peer_ids=("node-2", "node-3", "node-4"))
    node.current_term = 1
    node.storage.append([entry(1, "a"), entry(1, "b"), entry(1, "c")])
    node._become_leader()
    assert node.quorum == 3  # four voters including ourselves

    node.match_index = {"node-2": 3, "node-3": 1, "node-4": 0}
    node._advance_commit_index()
    assert node.commit_index == 1  # self + node-2 + node-3 hold 1; only two hold 3

    node.match_index["node-3"] = 3
    node._advance_commit_index()
    assert node.commit_index == 3


async def test_submit_rejects_non_leader(make_node):
    node = make_node()
    with pytest.raises(NotLeaderError):
        await node.submit(Command(op="set", key="k", value="v", request_id="r"))


async def test_submit_commits_and_applies_on_single_node(make_node):
    node = make_node(peer_ids=())
    await node._start_election()
    node.start()
    try:
        await node.submit(Command(op="set", key="temp", value="72", request_id="r1"))
        assert node.storage.kv_get("temp") == "72"
        assert node.last_applied == 2  # index 1 is the leader's no-op, 2 is the write
        assert node.commit_index == 2  # the term's no-op and the write both commit
    finally:
        await node.stop()


async def test_background_task_death_is_logged(make_node):
    """A loop that dies silently would degrade the node in production with no signal.
    (Attach a handler directly to the raftkv.raft logger rather than using caplog:
    setup_logging sets propagate=False on 'raftkv', so records never reach the
    root logger caplog listens on once any earlier test has initialized logging.)"""
    import asyncio
    import logging

    records: list[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = records.append
    raft_logger = logging.getLogger("raftkv.raft")
    raft_logger.addHandler(handler)
    try:
        node = make_node(peer_ids=())

        async def boom():
            raise RuntimeError("kaput")

        task = asyncio.create_task(boom(), name="boom")
        task.add_done_callback(node._on_task_death)
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0)  # let the done-callback run
        assert any("background task died" in r.getMessage() for r in records)
    finally:
        raft_logger.removeHandler(handler)
