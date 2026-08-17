"""Repairing a follower that is thousands of entries behind — and the distinction that
decides whether such a test means anything.

There are two ways to fall behind, they cost wildly different amounts, and only one of
them exercises the repair walk:

  * **Strict prefix** — the node crashed and came back with its log intact. The leader's
    `nextIndex` for it is still correct, so the very first AppendEntries succeeds and
    ships the backlog. Measured here: **1 round trip**, with or without any optimisation.
    `test_simulation.py::test_lagging_follower_catches_up` is this case over a three-entry
    gap, and it stayed green through the entire period a real node needed **52 minutes**
    to rejoin. Correct test, wrong regime.

  * **Wiped log** — what `/admin/reset` does, and what a restored-from-nothing replica
    looks like. The leader's `nextIndex` is now far too high, every AppendEntries is
    rejected, and the leader has to find where the follower actually is. THIS is the walk.

Measured on this harness with a 1500-entry gap against a wiped follower:

    conflict hint + batch cap ....  4 AppendEntries, repairs
    naive one-index walk .........  does not finish inside 25 s

So these tests wipe. A crash-and-restart version of them would pass against every
implementation and prove nothing, which is exactly the trap the original coverage fell
into — and the last test here keeps that contrast on the record.

Round trips are the assertion, never wall clock: the same repair takes milliseconds idle
and seconds on a contended CPU, and this repo's rule is that nothing under load asserts
latency.

**What this file does and does not cover.** Reverting the §5.3 hint fails five of these
six. Reverting the batch cap's immediate re-fire fails none of them, and that is correct
rather than a gap: the re-fire changes how long the batches take to go out, not how many
there are, so a round-trip assertion cannot see it and a wall-clock one would be exactly
the flaky test this repo refuses to write. It is pinned directly instead, as the event it
sets — `test_append_batching.py::test_a_still_behind_follower_triggers_another_round_immediately`.
"""

import pytest

from conftest import SimCluster, eventually
from raftkv.models import Command
from raftkv.raft import MAX_ENTRIES_PER_APPEND

# Deliberately larger than MAX_ENTRIES_PER_APPEND, so the backlog needs several batches
# and the re-fire that ships them without waiting a heartbeat is actually exercised.
GAP = 1500
assert GAP > MAX_ENTRIES_PER_APPEND, "gap must exceed the batch cap or batching is untested"


def cmd(i: int) -> Command:
    return Command(op="set", key=f"gap{i:05d}", value=f"v{i}", request_id=f"gap-r{i}")


class CountingTransport:
    """Wraps the cluster transport and counts AppendEntries per peer. Counting sends is
    what makes the assertion about the algorithm rather than about this machine."""

    def __init__(self, inner):
        self._inner = inner
        self.sends: dict[str, int] = {}

    def __getattr__(self, name):  # register / crash / restore / partition / add_peer
        return getattr(self._inner, name)

    async def append_entries(self, peer_id, req):
        self.sends[peer_id] = self.sends.get(peer_id, 0) + 1
        return await self._inner.append_entries(peer_id, req)

    async def request_vote(self, peer_id, req):
        return await self._inner.request_vote(peer_id, req)


# Leadership is held still for every test here, the same way and for the same reason as
# test_flood.py's STABLE_LEADER. `build_backlog` issues GAP (1500) SEQUENTIAL `submit()`
# calls, each waiting for commit and apply, and none of these tests is about elections —
# they are about how many round trips the repair costs. At the default 0.1-0.2 s timeouts
# that build takes long enough that a loaded box can depose the leader partway through,
# and `submit()` then raises `NotLeaderError` SYNCHRONOUSLY, so the backlog stops being
# built and every downstream assertion fails describing the wrong thing.
#
# Observed exactly that way: green in isolation six runs out of six, intermittently red in
# the full suite once tests/test_chaos_soak.py started competing for the same event loop.
# The test was always this fragile; the extra load only made it show.
#
# `heartbeat_interval` is widened for a second, independent reason. Every assertion in
# this file counts AppendEntries **sends**, and a heartbeat is a send — so the counts are
# really "repair round trips PLUS however many heartbeats fitted in the catch-up window".
# At 0.03 s that window holds a heartbeat every 30 ms, which is how a `<= 3` bound fails on
# a loaded box without anything being wrong with the repair. Widening it does not slow the
# repair being measured: a successful append that leaves the peer behind re-fires
# replication immediately rather than waiting for the next heartbeat, which is the coupling
# MAX_ENTRIES_PER_APPEND ships with.
STABLE_LEADER = {
    "election_timeout_min": 0.5,
    "election_timeout_max": 1.0,
    "heartbeat_interval": 0.1,
}


@pytest.fixture
async def counted(tmp_path):
    sim = SimCluster(tmp_path, **STABLE_LEADER)
    sim.start()
    counter = CountingTransport(sim.transport)
    for node in sim.nodes.values():
        node.transport = counter
    sim.counter = counter
    yield sim
    await sim.stop()


async def build_backlog(cluster):
    """Elect, commit GAP entries, and return (leader, victim_id, target_index)."""
    await eventually(lambda: cluster.leader() is not None)
    leader = cluster.leader()
    victim = next(n for n in cluster.nodes if n != leader.cfg.node_id)
    for i in range(GAP):
        await leader.submit(cmd(i))
    return leader, victim, leader.storage.last_log_index()


# ---- the regime that actually exercises the walk ----------------------------


async def test_a_wiped_follower_is_repaired(counted):
    leader, victim, target = await build_backlog(counted)
    await counted.nodes[victim].reset()

    await eventually(lambda: counted.nodes[victim].last_applied >= target, timeout=20)
    node = counted.nodes[victim]
    assert node.storage.kv_get("gap00000") == "v0"
    assert node.storage.kv_get(f"gap{GAP - 1:05d}") == f"v{GAP - 1}"
    assert node.storage.last_log_index() == target


async def test_repairing_a_wiped_follower_takes_a_handful_of_round_trips(counted):
    """The regression guard. Naive backoff needs one rejected AppendEntries per missing
    entry — ~1500 here — and on a real cluster those are heartbeat-paced, which is how a
    reset node came to need most of an hour to rejoin."""
    leader, victim, target = await build_backlog(counted)
    counted.counter.sends[victim] = 0  # count only the repair
    await counted.nodes[victim].reset()

    await eventually(lambda: counted.nodes[victim].last_applied >= target, timeout=20)

    trips = counted.counter.sends.get(victim, 0)
    batches = -(-GAP // MAX_ENTRIES_PER_APPEND)  # ceil
    assert trips < GAP / 10, (
        f"{trips} AppendEntries to close a {GAP}-entry gap; a naive one-index walk needs "
        f"~{GAP}, while a hint plus {batches} batches needs a handful"
    )


async def test_the_wiped_follower_keeps_its_membership(counted):
    """The repair must not be the thing that reverts a configuration. A wiped node
    re-seeds its last known configuration as a term-0 entry precisely so the ordinary
    repair walk can truncate it — see FAILURE_MODES.md table 1c."""
    leader, victim, target = await build_backlog(counted)
    voters_before = set(counted.nodes[victim].config.voters)
    await counted.nodes[victim].reset()

    assert set(counted.nodes[victim].config.voters) == voters_before, "membership reverted"
    await eventually(lambda: counted.nodes[victim].last_applied >= target, timeout=20)
    assert set(counted.nodes[victim].config.voters) == voters_before


async def test_every_node_agrees_after_the_repair(counted):
    """Convergence, not just completeness: the repaired node must agree entry-for-entry
    with the ones that never left, and the ordering invariant must hold on all of them."""
    leader, victim, target = await build_backlog(counted)
    await counted.nodes[victim].reset()
    await eventually(lambda: counted.nodes[victim].last_applied >= target, timeout=20)
    for node in counted.nodes.values():
        await eventually(lambda n=node: n.last_applied >= target, timeout=20)

    kvs = [n.storage.kv_all() for n in counted.nodes.values()]
    assert all(kv == kvs[0] for kv in kvs), "nodes disagree after repair"
    for node in counted.nodes.values():
        assert node.last_applied <= node.commit_index <= node.storage.last_log_index()


async def test_the_repair_survives_the_leader_dying_midway_through_it(counted):
    """Catch-up spans many round trips, and on a real cluster an election can land inside
    one. Order matters and is worth stating: the wipe has to happen while three nodes are
    up, because killing the leader of a three-node cluster with another node already down
    leaves one — no quorum, no election, nothing to repair against."""
    leader, victim, target = await build_backlog(counted)
    await counted.nodes[victim].reset()
    await counted.crash(leader.cfg.node_id)

    await eventually(lambda: counted.leader() is not None, timeout=10)
    await eventually(lambda: counted.nodes[victim].last_applied >= target, timeout=20)
    assert counted.nodes[victim].storage.kv_get(f"gap{GAP - 1:05d}") == f"v{GAP - 1}"


# ---- the cheap regime, kept to document why it proves nothing ----------------


async def test_a_crashed_and_restarted_follower_needs_no_walk_at_all(counted):
    """The contrast that explains the coverage gap. A node whose log survived is a strict
    PREFIX of the leader's, so `nextIndex` still points at the right place and the first
    AppendEntries succeeds. One round trip, no repair walk, no optimisation involved —
    which is why a crash-based catch-up test cannot detect a broken one."""
    leader, victim, target = await build_backlog(counted)
    await counted.crash(victim)
    counted.counter.sends[victim] = 0
    rejoined = counted.restart(victim)
    rejoined.transport = counted.counter

    await eventually(lambda: rejoined.last_applied >= target, timeout=20)
    assert counted.counter.sends.get(victim, 0) <= 3, (
        "a strict-prefix follower should need essentially no repair; if this grew, the "
        "leader is discarding nextIndex it was entitled to keep"
    )
