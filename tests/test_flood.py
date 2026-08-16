"""Concurrency floods: many client writes in flight at once, against a live cluster.

Three workloads, because they fail differently:
  * PARALLEL   — N writes to N distinct keys. Exercises batching and the commit walk.
  * OVERWRITE  — N writes to ONE key. Every entry contends for the same KV row, so the
                 question stops being "did it commit" and becomes "which one won, and
                 does every node agree on the answer".
  * MIXED      — sets, deletes and reads interleaved, which is the only one of the three
                 that puts a non-`set` command through the applier under contention.

WHAT THESE TESTS DO NOT ASSERT: latency, throughput, or a success count under stress.
Measured on this repo, a 200-write burst finishes in ~0.09s on an idle machine and
collapses to 100% commit-timeout when the CPU is contended — same code, same cluster.
Timing assertions here would encode the state of the machine that ran them. So the
strict tests widen `commit_timeout` and assert commitment; the stress tests accept any
mix of outcomes and assert only what must hold regardless of how the race resolved.
See docs/FAILURE_MODES.md for the throughput ceiling this exposes.
"""

import asyncio

from conftest import SimCluster, eventually
from raftkv.models import Command
from raftkv.raft import NotLeaderError

# Every client write ends in exactly one of these. A fourth outcome is a bug.
OUTCOMES = {"ok", "not_leader", "timeout"}

# Leadership is held still for every test in this file. None of them is about elections;
# they are about the applier and the commit walk under concurrent load, and an election
# mid-flood does not stress that -- it ERASES it. `submit()` raises NotLeaderError
# synchronously (raft.py, before its first await), so the instant the captured leader is
# deposed all N writes fail in a single event-loop turn: the flood evaporates, a
# concurrent reader never gets a lap, and the test fails claiming the reader is broken.
# Observed exactly that way, only under full-suite load, at the default 0.1-0.2s timeouts.
#
# commit_timeout is deliberately NOT set here: widening it is what separates the strict
# tests from the stress one, and that distinction is the point of the file.
STABLE_LEADER = {"election_timeout_min": 0.5, "election_timeout_max": 1.0}


def assert_leadership_held(sim: SimCluster, leader, results) -> None:
    """Distinguish "the cluster did the work" from "the flood evaporated".

    Without this, a mid-flood election makes every downstream assertion fail for a
    reason that has nothing to do with what it is testing."""
    rejected = [label for label, _ in results].count("not_leader")
    assert rejected == 0, (
        f"{rejected}/{len(results)} writes hit a deposed leader: leadership churned "
        f"mid-flood, so this run stressed nothing. Current leader is "
        f"{sim.leader() and sim.leader().cfg.node_id}, flood targeted {leader.cfg.node_id}"
    )


async def fire(leader, cmd: Command) -> tuple[str, Command]:
    """One client write, with the three documented outcomes flattened to a label.

    NotLeaderError and TimeoutError are the two failures the HTTP layer maps to 503 and
    504 (app.py exception handlers). Anything else propagates and fails the test, which
    is the point: a flood must not invent new failure modes."""
    try:
        await leader.submit(cmd)
        return ("ok", cmd)
    except NotLeaderError:
        return ("not_leader", cmd)
    except TimeoutError:
        return ("timeout", cmd)


async def settle(sim: SimCluster, timeout: float = 20.0) -> None:
    """Wait until every node has applied everything the leader has committed.

    Deliberately does NOT require last_applied == last_log_index: a leader that churned
    mid-flood carries an uncommitted tail from the losing term, and waiting for that to
    apply would wait forever. Convergence of applied state is the real property."""

    def converged() -> bool:
        leader = sim.leader()
        if leader is None:
            return False
        target = leader.commit_index
        return all(
            n.last_applied == target and n.commit_index >= target
            for n in sim.nodes.values()
        )

    await eventually(converged, timeout=timeout)


def kv_of_every_node(sim: SimCluster) -> list[dict[str, str]]:
    return [n.storage.kv_all() for n in sim.nodes.values()]


def assert_state_machine_invariants(sim: SimCluster) -> None:
    """The three orderings that must hold on every node, flood or no flood."""
    for node in sim.nodes.values():
        nid = node.cfg.node_id
        assert node.last_applied <= node.commit_index, (
            f"{nid} applied {node.last_applied} > committed {node.commit_index}"
        )
        assert node.commit_index <= node.storage.last_log_index(), (
            f"{nid} committed {node.commit_index} beyond its log "
            f"{node.storage.last_log_index()}"
        )
        assert node.last_applied == node.storage.load()[2], (
            f"{nid} in-memory last_applied diverged from the durable one"
        )


def final_log_value(node, key: str) -> str | None:
    """What the LOG says the key should be, read back independently of the KV table.

    Concurrent writes to one key are ordered by log order and nothing else, so the last
    entry touching the key decides the winner. Recomputing that from the log is what
    makes the KV assertion meaningful rather than circular."""
    value: str | None = None
    for idx in range(1, node.storage.last_log_index() + 1):
        if idx > node.last_applied:
            break  # unapplied entries have not reached the KV table yet
        entry = node.storage.entry(idx)
        cmd = entry.command
        if isinstance(cmd, Command) and cmd.key == key:
            value = cmd.value if cmd.op == "set" else None
    return value


# ---- parallel writes, distinct keys ----------------------------------------


async def test_fifty_parallel_writes_to_distinct_keys_all_commit(tmp_path):
    """The baseline: a burst well inside the cluster's capacity loses nothing.

    commit_timeout is widened to 10s so this asserts "these writes commit" rather than
    "these writes commit fast enough on today's machine" — the strictness is about
    durability, not speed."""
    sim = SimCluster(tmp_path, commit_timeout=10.0, **STABLE_LEADER)
    sim.start()
    try:
        await eventually(lambda: sim.leader() is not None, timeout=10.0)
        leader = sim.leader()

        writes = [
            Command(op="set", key=f"key-{i}", value=f"value-{i}", request_id=f"req-{i}")
            for i in range(50)
        ]
        results = await asyncio.gather(*(fire(leader, c) for c in writes))

        labels = [label for label, _ in results]
        assert set(labels) <= OUTCOMES, f"undocumented outcome: {set(labels) - OUTCOMES}"
        assert_leadership_held(sim, leader, results)
        assert labels.count("ok") == 50, f"lost writes: { 50 - labels.count('ok')}"

        await settle(sim)
        assert_state_machine_invariants(sim)

        # Every acknowledged write is present, with its own value, on EVERY node.
        expected = {c.key: c.value for c in writes}
        for node, kv in zip(sim.nodes.values(), kv_of_every_node(sim), strict=True):
            missing = {k: v for k, v in expected.items() if kv.get(k) != v}
            assert not missing, f"{node.cfg.node_id} lost or corrupted {len(missing)} keys"
    finally:
        await sim.stop()


async def test_parallel_flood_never_diverges_however_it_resolves(tmp_path):
    """150 concurrent writes at the DEFAULT commit_timeout — the stress case.

    Any mix of committed, timed-out and rejected is acceptable here; a loaded machine
    legitimately produces all three. What is never acceptable is two nodes disagreeing,
    or an acknowledged write going missing."""
    sim = SimCluster(tmp_path, **STABLE_LEADER)
    sim.start()
    try:
        await eventually(lambda: sim.leader() is not None, timeout=10.0)
        leader = sim.leader()

        writes = [
            Command(op="set", key=f"k{i}", value=f"v{i}", request_id=f"r{i}")
            for i in range(150)
        ]
        results = await asyncio.gather(*(fire(leader, c) for c in writes))
        assert {label for label, _ in results} <= OUTCOMES
        assert_leadership_held(sim, leader, results)

        await settle(sim)
        assert_state_machine_invariants(sim)

        kvs = kv_of_every_node(sim)
        assert all(kv == kvs[0] for kv in kvs), "nodes disagree after the flood"

        # An acknowledged write must survive. A timed-out one MAY be present — the
        # outcome is genuinely ambiguous (FAILURE_MODES.md, at-least-once) — so it is
        # deliberately not asserted either way.
        acked = {c.key: c.value for label, c in results if label == "ok"}
        assert all(kvs[0].get(k) == v for k, v in acked.items()), "an acked write vanished"

        # Guard against a vacuous pass: if the cluster committed nothing at all, the
        # convergence check above is satisfied by three identically empty maps.
        assert kvs[0], "no write survived the flood — convergence proved nothing"
    finally:
        await sim.stop()


# ---- concurrent overwrites of one key --------------------------------------


async def test_one_hundred_overwrites_of_one_key_agree_on_one_winner(tmp_path):
    """Maximum contention: every write targets the same key.

    The winner is decided by LOG ORDER and nothing else — not client submission order,
    which is why this asserts membership in the submitted set rather than a specific
    value. What it does assert exactly is that the KV table matches what the log says,
    on every node: a torn or divergent winner is the failure this rules out."""
    sim = SimCluster(tmp_path, commit_timeout=10.0, **STABLE_LEADER)
    sim.start()
    try:
        await eventually(lambda: sim.leader() is not None, timeout=10.0)
        leader = sim.leader()

        submitted = {f"v{i}" for i in range(100)}
        writes = [
            Command(op="set", key="hot", value=f"v{i}", request_id=f"r{i}")
            for i in range(100)
        ]
        results = await asyncio.gather(*(fire(leader, c) for c in writes))
        assert {label for label, _ in results} <= OUTCOMES
        assert_leadership_held(sim, leader, results)

        await settle(sim)
        assert_state_machine_invariants(sim)

        kvs = kv_of_every_node(sim)
        winners = {kv.get("hot") for kv in kvs}
        assert len(winners) == 1, f"nodes disagree on the winner: {winners}"
        winner = winners.pop()
        assert winner is not None, "the hammered key is missing entirely"
        assert winner in submitted, f"{winner!r} was never submitted — torn value"

        # The KV table agrees with an independent replay of the log, on every node.
        for node in sim.nodes.values():
            assert final_log_value(node, "hot") == winner, (
                f"{node.cfg.node_id}: KV says {winner!r}, its own log says "
                f"{final_log_value(node, 'hot')!r}"
            )
    finally:
        await sim.stop()


# ---- mixed workload ---------------------------------------------------------


async def test_mixed_sets_deletes_and_reads_converge(tmp_path):
    """Sets, deletes and reads interleaved over a small key space.

    Deletes matter here specifically because they take a different branch in
    storage.apply(), and reads matter because GET is served from local applied state —
    a read during a flood is allowed to be stale, and this test pins that it is stale
    rather than wrong (every value it ever returns was really written)."""
    sim = SimCluster(tmp_path, commit_timeout=10.0, **STABLE_LEADER)
    sim.start()
    try:
        await eventually(lambda: sim.leader() is not None, timeout=10.0)
        leader = sim.leader()

        keys = [f"k{i}" for i in range(8)]
        values_written: dict[str, set[str]] = {k: set() for k in keys}
        ops: list[Command] = []
        for i in range(120):
            key = keys[i % len(keys)]
            if i % 5 == 4:  # ~20% deletes
                ops.append(Command(op="delete", key=key, request_id=f"d{i}"))
            else:
                value = f"v{i}"
                values_written[key].add(value)
                ops.append(
                    Command(op="set", key=key, value=value, request_id=f"s{i}")
                )

        observed_reads: list[tuple[str, str]] = []

        follower = next(
            n for n in sim.nodes.values() if n.cfg.node_id != leader.cfg.node_id
        )
        follower_applied_during_flood: list[int] = []

        async def reader(until: asyncio.Task) -> None:
            """Concurrent local reads while the flood is in flight.

            Reads the LEADER, deliberately. A follower applies an entry only once a LATER
            AppendEntries carries the higher leaderCommit, so for the whole duration of a
            burst the followers are still empty — polling one observes nothing and proves
            nothing. That lag is itself recorded below.

            Bounded by the flood's own completion rather than by an iteration count: a
            fixed number of laps finishes long before the first entry is applied."""
            while not until.done():
                for key in keys:
                    got = leader.storage.kv_get(key)
                    if got is not None:
                        observed_reads.append((key, got))
                follower_applied_during_flood.append(follower.last_applied)
                await asyncio.sleep(0.001)

        async def run_flood():
            return await asyncio.gather(*(fire(leader, c) for c in ops))

        flood = asyncio.create_task(run_flood())
        await reader(flood)
        results = await flood
        assert {label for label, _ in results} <= OUTCOMES
        assert_leadership_held(sim, leader, results)

        await settle(sim)
        assert_state_machine_invariants(sim)

        kvs = kv_of_every_node(sim)
        assert all(kv == kvs[0] for kv in kvs), "nodes disagree after the mixed flood"

        # Every value the KV table ended up holding was genuinely submitted for that key.
        for key, value in kvs[0].items():
            assert value in values_written[key], f"{key}={value!r} was never written"

        # A concurrent read may be STALE, but never fabricated: everything it returned
        # was a real value written to that key at some point.
        assert observed_reads, "the reader observed nothing — it proved nothing"
        for key, value in observed_reads:
            assert value in values_written[key], (
                f"read {key}={value!r}, which was never written to {key}"
            )

        # The follower lag that made leader-side reading necessary, pinned as a fact:
        # a follower trails the leader for the whole burst and only catches up after it.
        # This is the honest reason GET /kv is documented as a possibly-stale local read.
        assert follower_applied_during_flood, "the lag sampler never ran"
        assert min(follower_applied_during_flood) < max(
            n.last_applied for n in sim.nodes.values()
        ), "the follower was never behind — the flood did not overlap the reader"
    finally:
        await sim.stop()
