"""The AppendEntries batch cap, and the re-fire that keeps it from costing anything.

The batch used to be every outstanding entry, to every follower, every round. Measured at
2000 pending entries: ~195 KiB per peer and ~33 ms of event-loop time per round across
four followers — re-read out of SQLite and re-serialised each round until the burst
drained.

Capping it alone would be a regression dressed as a fix: a follower `behind` entries back
would need `ceil(behind / MAX_ENTRIES_PER_APPEND)` rounds, and if each round waits for the
next heartbeat that is the minutes-long repair §5.3 was added to remove. So the cap comes
with a rule: a successful append that leaves the follower still behind re-fires
replication immediately. Both halves are pinned here, because either alone is worse than
neither.

The last test is the one that matters if someone tunes the constant later: the two must
stay coupled.
"""

import asyncio

import pytest

from conftest import StubTransport
from raftkv import raft as raft_module
from raftkv.models import AppendEntriesRequest, Command, LogEntry, Role


def entry(term: int, rid: str = "r") -> LogEntry:
    return LogEntry(term=term, command=Command(op="set", key="k", value="v", request_id=rid))


def seed(node, count: int, term: int = 1) -> None:
    node.storage.append([entry(term, f"r{i}") for i in range(count)])


@pytest.fixture
def small_cap(monkeypatch):
    """A cap small enough to make the batching visible in a short test."""
    monkeypatch.setattr(raft_module, "MAX_ENTRIES_PER_APPEND", 10)
    return 10


class RecordingTransport(StubTransport):
    """Accepts every AppendEntries and records the request, so the test can assert on
    what actually went over the wire rather than on what the leader intended."""

    def __init__(self):
        self.sent: list[AppendEntriesRequest] = []

    async def append_entries(self, peer_id, req):
        from raftkv.models import AppendEntriesResponse

        self.sent.append(req)
        return AppendEntriesResponse(term=req.term, success=True)


def leader_with(make_node, entries: int, transport):
    node = make_node(node_id="node-1", peer_ids=("node-2",), transport=transport)
    node.role = Role.LEADER
    node.current_term = 1
    seed(node, entries)
    node.next_index = {"node-2": 1}
    node.match_index = {"node-2": 0}
    node._replicate_now = asyncio.Event()
    return node


# ---- the cap ----------------------------------------------------------------


async def test_one_append_never_exceeds_the_cap(make_node, small_cap):
    t = RecordingTransport()
    node = leader_with(make_node, 100, t)
    await node._append_to_peer("node-2", 1)
    assert len(t.sent) == 1
    assert len(t.sent[0].entries) == small_cap, "the whole 100-entry log went out at once"


async def test_a_short_tail_is_not_padded(make_node, small_cap):
    """The cap is a maximum, not a batch size — three outstanding entries send three."""
    t = RecordingTransport()
    node = leader_with(make_node, 3, t)
    await node._append_to_peer("node-2", 1)
    assert len(t.sent[0].entries) == 3


async def test_a_caught_up_follower_gets_an_empty_heartbeat(make_node, small_cap):
    t = RecordingTransport()
    node = leader_with(make_node, 5, t)
    node.next_index = {"node-2": 6}
    node.match_index = {"node-2": 5}
    await node._append_to_peer("node-2", 1)
    assert t.sent[0].entries == []


async def test_match_index_counts_what_was_sent_not_the_whole_log(make_node, small_cap):
    """The Students' Guide rule, and the cap is exactly where getting it wrong bites:
    matchIndex must come from the arguments sent, so a capped batch advances it by the
    cap. Deriving it from last_log_index would mark a follower as holding entries it was
    never sent — and that is a commit on a false majority."""
    t = RecordingTransport()
    node = leader_with(make_node, 100, t)
    await node._append_to_peer("node-2", 1)
    assert node.match_index["node-2"] == small_cap
    assert node.next_index["node-2"] == small_cap + 1


# ---- the re-fire ------------------------------------------------------------


async def test_a_still_behind_follower_triggers_another_round_immediately(
    make_node, small_cap
):
    """Without this the cap turns catch-up into one batch per heartbeat."""
    t = RecordingTransport()
    node = leader_with(make_node, 100, t)
    node._replicate_now.clear()
    await node._append_to_peer("node-2", 1)
    assert node._replicate_now.is_set(), "capped append did not ask for another round"


async def test_a_caught_up_follower_does_not_trigger_another_round(make_node, small_cap):
    """The stop condition. Re-firing unconditionally would spin the replication loop at
    full speed forever, heartbeating a healthy cluster as fast as the CPU allows."""
    t = RecordingTransport()
    node = leader_with(make_node, 5, t)
    node.next_index = {"node-2": 6}
    node.match_index = {"node-2": 5}
    node._replicate_now.clear()
    await node._append_to_peer("node-2", 1)
    assert not node._replicate_now.is_set()


async def test_an_unreachable_peer_does_not_trigger_another_round(make_node, small_cap):
    """Only a SUCCESSFUL append re-fires. A peer that is down must not turn the
    replication loop into a spin."""
    from raftkv.transport import TransportError

    class Dead(StubTransport):
        async def append_entries(self, peer_id, req):
            raise TransportError("down")

    node = leader_with(make_node, 100, Dead())
    node._replicate_now.clear()
    await node._append_to_peer("node-2", 1)
    assert not node._replicate_now.is_set()


async def test_a_rejecting_peer_does_not_trigger_another_round(make_node, small_cap):
    """A rejection walks nextIndex back and waits for the ordinary cadence; re-firing
    here would spin a full-speed loop against a follower that is refusing every append."""
    from raftkv.models import AppendEntriesResponse

    class Rejects(StubTransport):
        async def append_entries(self, peer_id, req):
            return AppendEntriesResponse(term=req.term, success=False, conflict_index=1)

    node = leader_with(make_node, 100, Rejects())
    node.next_index = {"node-2": 50}
    node._replicate_now.clear()
    await node._append_to_peer("node-2", 1)
    assert not node._replicate_now.is_set()


# ---- the two halves have to stay coupled ------------------------------------


async def test_catch_up_completes_in_ceil_behind_over_cap_rounds(make_node, small_cap):
    """The property the constant is allowed to be tuned against: a follower `behind`
    entries back is caught up in `ceil(behind / cap)` rounds, each of which the re-fire
    makes immediate. 100 entries at a cap of 10 is ten rounds — not ten heartbeats."""
    t = RecordingTransport()
    node = leader_with(make_node, 100, t)
    rounds = 0
    while node.match_index["node-2"] < node.storage.last_log_index() and rounds < 50:
        rounds += 1
        await node._append_to_peer("node-2", 1)
    assert node.match_index["node-2"] == 100
    assert rounds == 10, f"took {rounds} rounds to ship 100 entries at a cap of 10"
    assert all(len(r.entries) <= small_cap for r in t.sent)
    assert sum(len(r.entries) for r in t.sent) == 100, "every entry sent exactly once"


async def test_the_default_cap_bounds_the_payload(make_node):
    """A guard on the constant itself. The comment above it calls it a payload budget;
    this keeps that true if someone raises it 'just for one experiment'."""
    assert 1 <= raft_module.MAX_ENTRIES_PER_APPEND <= 2048

    t = RecordingTransport()
    node = leader_with(make_node, raft_module.MAX_ENTRIES_PER_APPEND + 500, t)
    await node._append_to_peer("node-2", 1)
    payload = t.sent[0].model_dump_json()
    assert len(payload) < 512 * 1024, f"one AppendEntries serialised to {len(payload)} bytes"
