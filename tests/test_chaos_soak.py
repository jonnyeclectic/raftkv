"""Randomized schedules against the Raft safety properties, with a seed that makes any
failure reproducible.

Every other simulation test in this repo scripts the failure: partition *here*, crash
*that* node, then assert the outcome someone predicted while writing it. That is a sample
of one path, and it can only ever confirm a bug someone already imagined. These tests
invert it — the schedule is drawn from a seeded RNG, nobody predicts the outcome, and what
is asserted instead is `tests/invariants.py`: the five properties that must hold on EVERY
path (§5.4.3), plus the client-visible promise that an acknowledged write is never lost.

Why this catches things the scripted tests structurally cannot:

  - The stale-reply guards in `_start_election`, `_append_to_peer` and `submit` are called
    out by this repo's own rules as load-bearing, but they only fire when a reply lands
    after the sender's role or term has moved. `MemoryTransport` answers within the same event-loop
    turn, so that window is almost never open. `ChaosTransport`'s response delay holds it
    open on purpose.
  - Duplicate delivery tests idempotence of the receiver rules. Nothing else here delivers
    a message twice.
  - Message reordering (a consequence of per-message delay) is how a stale AppendEntries
    gets the chance to truncate entries that already match — rule 5 of the seven.

**A failure here is never a flake.** Safety properties hold on every schedule, so a
violation is either a real bug or a rule stated wrongly in `invariants.py`. The seed is in
the test id: reproduce with `-k "chaos and 13"`, and do not regenerate it.

The inline seed list is deliberately small — this runs on every `make test`, and a soak
that doubles the suite's runtime gets deleted. `RAFT_SOAK_SEEDS=500 uv run pytest -k
chaos` widens it for a nightly job, which is where real systems put their long chaos runs.
"""

import asyncio
import contextlib
import os
import random
from uuid import uuid4

import pytest

from chaos import ChaosTransport
from conftest import SimCluster, eventually, scaled
from invariants import RaftInvariantMonitor, assert_no_acknowledged_write_is_lost
from raftkv.models import Command
from raftkv.raft import NotLeaderError
from raftkv.transport import TransportError

# Timers widened from TEST_TIMING the way test_flood.py widens them: with messages being
# delayed and dropped, the stock 0.1-0.2 s election window turns every run into an
# election storm, which is a test of the harness rather than of Raft. Must still satisfy
# NodeConfig's startup validation (heartbeat < election_min, rpc_timeout + heartbeat <
# election_min), or the node refuses to boot.
CHAOS_TIMING = scaled(
    heartbeat_interval=0.05,
    election_timeout_min=0.3,
    election_timeout_max=0.6,
    rpc_timeout=0.1,
    # Short on purpose: a write that cannot commit should fail fast and let the run
    # continue, not park the test for two seconds.
    commit_timeout=0.5,
)

STEPS = 20

# Long enough for heartbeats to flow between client writes. Without it a run finished in
# ~0.5 s having delivered 17 RPCs total, which is too few messages for a 6% drop rate to
# reliably drop anything — the network was hostile in principle and quiet in practice.
SETTLE = 0.03


def _seeds() -> list[int]:
    """Four inline, or as many as `RAFT_SOAK_SEEDS` asks for. The inline four are fixed
    rather than random so a green CI run means the same thing twice."""
    count = int(os.environ.get("RAFT_SOAK_SEEDS", "4"))
    return list(range(1, count + 1))


async def _attempt_write(cluster, key: str, value: str) -> bool:
    """One client write. Returns True only if the cluster ACKNOWLEDGED it.

    `submit()` returns only once the entry is applied on the leader, which means committed,
    which means on a majority. So a True here is a promise the cluster made, and it is what
    `assert_no_acknowledged_write_is_lost` later holds it to. Every failure mode below is a
    legitimate answer under chaos — a deposed leader, a write that could not reach quorum in
    time — and none of them is an acknowledgement.

    It waits for a leader first, the way a real client retries rather than giving up. That
    is not politeness, it is what makes the run measure anything: an election takes up to
    `election_timeout_max` (0.6 s), which is the same order as the entire step budget, so a
    single leader crash otherwise leaves every remaining step finding no leader and
    skipping. Seed 39 of a 40-seed soak did exactly that and acknowledged nothing in 20
    attempts — a flaw in the sampling, not a liveness bug in Raft.
    """
    with contextlib.suppress(AssertionError):
        await eventually(lambda: cluster.leader() is not None, timeout=2.0)
    leader = cluster.leader()
    if leader is None:
        return False
    try:
        await leader.submit(Command(op="set", key=key, value=value, request_id=uuid4().hex))
        return True
    except (NotLeaderError, TransportError, TimeoutError):
        return False


async def _inject_fault(cluster, rng, impaired: set[str]) -> None:
    """At most ONE node impaired at a time, so a majority always exists.

    That is not timidity: with two of three nodes down, every write correctly fails, the
    run stops exercising the commit path, and the test degenerates into a slow way of
    proving that a cluster without quorum does nothing. Keeping quorum available is what
    keeps the interesting code running while the network misbehaves.
    """
    if impaired:
        node_id = impaired.pop()
        cluster.transport.heal()
        if node_id in cluster.transport.down:
            cluster.restart(node_id)
        return

    node_id = rng.choice(cluster.ids)
    if rng.random() < 0.5:
        await cluster.crash(node_id)
    else:
        cluster.transport.partition({node_id}, set(cluster.ids) - {node_id})
    impaired.add(node_id)


@pytest.mark.parametrize("seed", _seeds())
async def test_chaos_safety_holds_under_a_hostile_network(tmp_path, seed):
    transport = ChaosTransport(seed, drop_rate=0.06, duplicate_rate=0.06, max_delay=0.004)
    cluster = SimCluster(tmp_path, n=3, transport=transport, **CHAOS_TIMING)
    # A separate stream from the transport's, so changing the fault schedule does not
    # silently reshuffle which packets get dropped and vice versa.
    rng = random.Random(seed ^ 0x5EED)
    monitor = RaftInvariantMonitor()
    acknowledged: dict[str, str] = {}
    impaired: set[str] = set()

    cluster.start()
    try:
        await eventually(lambda: cluster.leader() is not None, timeout=6.0)

        for step in range(STEPS):
            monitor.observe(cluster)
            if rng.random() < 0.35:
                await _inject_fault(cluster, rng, impaired)
            key, value = f"k{step}", f"v{step}"
            if await _attempt_write(cluster, key, value):
                acknowledged[key] = value
            monitor.observe(cluster)
            await asyncio.sleep(SETTLE)

        # Heal everything and let the cluster settle before judging what survived. A write
        # is only lost if it is still missing once the network is whole again — asserting
        # mid-partition would fail on a node that is merely behind.
        cluster.transport.heal()
        for node_id in list(impaired):
            if node_id in cluster.transport.down:
                cluster.restart(node_id)
        impaired.clear()
        await eventually(lambda: cluster.leader() is not None, timeout=6.0)
        with contextlib.suppress(AssertionError):
            # Convergence is nice to have, not the property under test; the assertions
            # below stand on their own if the cluster is still catching up.
            await eventually(
                lambda: len({n.commit_index for n in cluster.nodes.values()}) == 1,
                timeout=3.0,
            )

        monitor.observe(cluster)
        assert_no_acknowledged_write_is_lost(cluster, acknowledged)

        # Anti-vacuity, the same guard the docs-citation checks use: a run that injected
        # no chaos and committed nothing would satisfy every assertion above while proving
        # nothing at all.
        #
        # Deterministic claims ONLY. The obvious assertion here is "at least one message
        # was dropped", and it is a trap: a run delivers ~30 RPCs, so at a 6% drop rate
        # roughly one run in fifty legitimately drops nothing and fails a test that found
        # no bug. A chaos suite that cries wolf gets muted, and then it is worth less than
        # no chaos suite at all. Raising the rates only moves the probability around; it
        # never reaches zero.
        #
        # So the knobs are pinned WITHOUT probability, by the three tests below driving
        # them at their extremes (drop_rate=1.0, duplicate_rate=1.0, seed replay). Those
        # prove the mechanism. This one proves the cluster actually ran through it.
        # Not an RPC-count floor: a crashed peer's sends raise in `_check_link` before the
        # chaos layer is reached, so they reach neither counter, and the floor ends up
        # measuring how long a node happened to be crashed rather than whether the run was
        # meaningful. This says the meaningful thing directly — the cluster kept committing
        # while the network misbehaved. With at most one node impaired a majority always
        # exists, so zero acknowledged writes across 20 attempts is a liveness bug worth
        # failing on rather than a bad draw.
        assert transport.delivered > 0, "no RPC was delivered; the cluster never ran"
        assert acknowledged, (
            f"seed {seed} committed nothing in {STEPS} attempts despite a majority always "
            "being available — the cluster lost liveness under chaos"
        )
        assert monitor.observations >= STEPS, "the invariant monitor barely ran"
    finally:
        await cluster.stop()


class _Recorder:
    """Counts handler invocations. Standing in for a RaftNode is enough here — the point
    is how many times the transport calls it, not what it decides."""

    def __init__(self) -> None:
        self.calls = 0

    def handle_append_entries(self, req):
        self.calls += 1
        return "ack"

    def handle_request_vote(self, req):
        self.calls += 1
        return "ack"


def _req():
    """Only `.leader_id` is read before delivery (`_check_link`), so a stub carries it."""
    return type("Req", (), {"leader_id": "a", "candidate_id": "a"})()


async def test_a_drop_rate_of_one_delivers_nothing():
    """The knobs pinned at their extremes, with no probability left in the assertion. The
    soak test above can only say "some hazard fired"; these say exactly which, so a
    ChaosTransport that silently ignored a knob could not hide behind a lucky seed."""
    transport = ChaosTransport(seed=1, drop_rate=1.0)
    peer = _Recorder()
    transport.register("a", _Recorder())
    transport.register("b", peer)

    for _ in range(5):
        with pytest.raises(TransportError):
            await transport.append_entries("b", _req())

    assert peer.calls == 0, "a fully-dropping network still reached the peer"
    assert transport.dropped == 5
    assert transport.delivered == 0


async def test_a_duplicate_rate_of_one_delivers_every_message_twice():
    transport = ChaosTransport(seed=1, duplicate_rate=1.0)
    peer = _Recorder()
    transport.register("a", _Recorder())
    transport.register("b", peer)

    assert await transport.append_entries("b", _req()) == "ack"

    assert peer.calls == 2, "the duplicate never reached the peer"
    assert transport.duplicated == 1
    # The sender sees ONE reply. A duplicate the caller can observe is not a duplicate,
    # it is a protocol change.
    assert transport.delivered == 1


async def test_the_same_seed_replays_the_same_schedule():
    """The property the whole approach rests on: a failing seed is a complete bug report.
    If two transports with one seed disagreed, a reported seed would be worthless."""

    async def run(seed):
        transport = ChaosTransport(seed, drop_rate=0.5, duplicate_rate=0.5)
        transport.register("a", _Recorder())
        transport.register("b", _Recorder())
        outcomes = []
        for _ in range(30):
            try:
                await transport.append_entries("b", _req())
                outcomes.append("ok")
            except TransportError:
                outcomes.append("drop")
        return outcomes, transport.duplicated

    first, dups_first = await run(7)
    second, dups_second = await run(7)
    other, _ = await run(8)

    assert first == second and dups_first == dups_second, "seed 7 did not replay"
    assert first != other, "different seeds produced identical schedules; the seed is ignored"


async def test_chaos_transport_with_no_knobs_set_is_an_ordinary_memory_transport(tmp_path):
    """The default path has to stay boring, or every existing test that might later adopt
    this transport inherits nondeterminism it never asked for."""
    transport = ChaosTransport(seed=1)
    cluster = SimCluster(tmp_path, n=3, transport=transport)
    cluster.start()
    try:
        await eventually(lambda: cluster.leader() is not None)
        assert await _attempt_write(cluster, "k", "v")
        assert transport.dropped == 0
        assert transport.duplicated == 0
        assert transport.delivered > 0
    finally:
        await cluster.stop()
