"""The single applier: what reaches the state machine, in what order, and what it
refuses to skip. Unit-level, so only the apply loop runs — the election and replication
loops stay stopped and cannot move the node underneath an assertion."""

import asyncio
import contextlib

from conftest import eventually
from raftkv.models import ClusterConfig, Command, LogEntry, NoOp


def entry(term: int, rid: str = "r") -> LogEntry:
    """Keyed by request id rather than a fixed "k", so the applied KV map reads back as
    exactly the set of entries that made it through."""
    return LogEntry(term=term, command=Command(op="set", key=rid, value="v", request_id=rid))


@contextlib.asynccontextmanager
async def applier(node):
    task = asyncio.create_task(node._apply_loop())
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_applies_the_committed_prefix_and_stops_there(make_node):
    node = make_node()
    node.storage.append([entry(1, "a"), entry(1, "b"), entry(1, "c")])
    node.commit_index = 2
    async with applier(node):
        node._apply_ready.set()
        await eventually(lambda: node.last_applied == 2)
    # index 3 is in the log but uncommitted, so it must not reach the state machine
    assert node.storage.kv_all() == {"a": "v", "b": "v"}


async def test_resumes_at_last_applied_and_never_reapplies(make_node):
    """last_applied is a resume point, not a hint: re-running index 1 here would apply an
    already-applied command a second time, which is the guarantee storage.apply() exists
    to hold across restarts."""
    node = make_node()
    node.storage.append([entry(1, "a"), entry(1, "b"), entry(1, "c")])
    node.last_applied = 1  # applied before this process woke up
    node.commit_index = 3
    async with applier(node):
        node._apply_ready.set()
        await eventually(lambda: node.last_applied == 3)
    assert node.storage.kv_all() == {"b": "v", "c": "v"}  # "a" was NOT re-applied


async def test_advances_past_entries_with_no_state_machine_effect(make_node):
    """A no-op or a configuration entry has nothing to apply, but last_applied must still
    step over it. The applier is strictly sequential, so an index it will not pass stalls
    every committed entry behind it, permanently."""
    node = make_node()
    node.current_term = 1
    node.storage.append([
        entry(1, "a"),
        LogEntry(term=1, command=NoOp()),
        LogEntry(term=1, command=ClusterConfig(voters={"node-1": "node-1"})),
        entry(1, "b"),
    ])
    node.commit_index = 4
    async with applier(node):
        node._apply_ready.set()
        await eventually(lambda: node.last_applied == 4)
    assert node.storage.kv_all() == {"a": "v", "b": "v"}


async def test_applies_nothing_while_nothing_is_committed(make_node):
    node = make_node()
    node.storage.append([entry(1, "a")])
    node.commit_index = 0  # the index-0 sentinel: nothing committed
    async with applier(node):
        node._apply_ready.set()
        # asserting a non-event, so give the loop several ticks to do the wrong thing
        await asyncio.sleep(node.cfg.tick_interval * 3)
    assert node.last_applied == 0
    assert node.storage.kv_all() == {}
