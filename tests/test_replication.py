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
