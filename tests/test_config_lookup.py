"""`last_config()` is on the write path, so its cost is a correctness concern.

Membership is a view over the log, never a cache beside it — `_reload_config()` therefore
runs after *every* log mutation, and `last_config()` runs with it. That is the right
design and this file does not argue with it; it pins the consequence.

Unindexed, the lookup is a full table scan, so it grew with the log:

    log  1,000 entries -> 0.175 ms
    log  5,000 entries -> 0.823 ms
    log 15,000 entries -> 2.475 ms

Multiply by concurrency and it becomes the write path's dominant cost. Measured on the
live cluster: 1000 concurrent writes against a 15,000-entry log blocked the leader's event
loop for **1733 ms** — past the 1.5 s floor of the election timeout — so the followers
elected around a leader that was merely busy and every in-flight write failed at once.
That is a liveness failure produced entirely by a missing index.

The fix is a partial index over just the configuration entries. It changes no semantics:
the configuration is still derived from the log, so a truncated config entry still reverts
to the previous one. These tests check the behaviour that must survive, and that the query
plan still uses the index — because the index only applies while `last_config()`'s WHERE
clause matches it exactly, and a plan that silently reverts to a scan is invisible until
the log is big.
"""

import pytest

from raftkv.models import ClusterConfig, Command, LogEntry
from raftkv.storage import Storage


@pytest.fixture
def store(tmp_path):
    s = Storage(str(tmp_path / "s.db"))
    yield s
    s.close()


def cfg(*voters: str) -> ClusterConfig:
    return ClusterConfig(voters={v: f"{v}:8000" for v in voters})


def cmd(i: int) -> LogEntry:
    return LogEntry(term=1, command=Command(op="set", key=f"k{i}", value="v", request_id=f"r{i}"))


# ---- semantics: unchanged by the index --------------------------------------


def test_no_configuration_yet_returns_none(store):
    store.append([cmd(1), cmd(2)])
    assert store.last_config() is None


def test_the_highest_indexed_configuration_wins(store):
    store.append([LogEntry(term=1, command=cfg("a", "b", "c"))])
    store.append([cmd(1), cmd(2)])
    store.append([LogEntry(term=2, command=cfg("a", "b", "c", "d"))])
    store.append([cmd(3)])
    idx, found = store.last_config()
    assert idx == 4
    assert sorted(found.voters) == ["a", "b", "c", "d"]


def test_truncating_a_configuration_reverts_to_the_previous_one(store):
    """The property that makes membership-as-a-view worth having. If this ever needs a
    cache invalidation to pass, the design has been broken."""
    store.append([LogEntry(term=1, command=cfg("a", "b", "c"))])
    store.append([cmd(1)])
    store.append([LogEntry(term=2, command=cfg("a", "b", "c", "d"))])
    assert sorted(store.last_config()[1].voters) == ["a", "b", "c", "d"]

    store.truncate_from(3)
    idx, found = store.last_config()
    assert idx == 1
    assert sorted(found.voters) == ["a", "b", "c"]


def test_a_configuration_is_found_under_a_large_log(store):
    """The index is partial, so the config rows are a tiny minority. Make sure the one
    that matters is still found when it is buried."""
    store.append([LogEntry(term=1, command=cfg("a", "b", "c"))])
    store.append([cmd(i) for i in range(2000)])
    idx, found = store.last_config()
    assert idx == 1
    assert sorted(found.voters) == ["a", "b", "c"]


def test_a_configuration_appended_after_a_large_log_wins(store):
    store.append([LogEntry(term=1, command=cfg("a", "b", "c"))])
    store.append([cmd(i) for i in range(2000)])
    store.append([LogEntry(term=2, command=cfg("a", "b", "c", "d", "e"))])
    idx, found = store.last_config()
    assert idx == 2002
    assert sorted(found.voters) == ["a", "b", "c", "d", "e"]


def test_a_noop_is_not_mistaken_for_a_configuration(store):
    """The index keys on `$.op = 'config'`; every other entry type must stay out of it."""
    from raftkv.models import NoOp

    store.append([LogEntry(term=1, command=cfg("a", "b", "c"))])
    store.append([LogEntry(term=2, command=NoOp()), cmd(1)])
    assert store.last_config()[0] == 1


# ---- the cost, which is the reason the index exists -------------------------


def plan(store) -> str:
    rows = store._conn.execute(
        "EXPLAIN QUERY PLAN "
        "SELECT idx, command FROM log WHERE json_extract(command, '$.op') = 'config' "
        "ORDER BY idx DESC LIMIT 1"
    ).fetchall()
    return " | ".join(str(r[-1]) for r in rows)


def test_the_lookup_uses_the_index_rather_than_scanning(store):
    """A partial index applies only while the query's WHERE clause matches the index's.
    Edit one without the other and this silently becomes a full scan again — which costs
    nothing on a small log and takes the cluster down on a large one."""
    store.append([LogEntry(term=1, command=cfg("a", "b", "c"))])
    store.append([cmd(i) for i in range(200)])
    assert "log_config" in plan(store), f"query plan reverted to a scan: {plan(store)}"


def test_lookup_cost_does_not_grow_with_the_log(store):
    """The regression guard, stated as a ratio so it does not encode this machine's speed.

    Unindexed, a 10x longer log cost ~10x more. The index makes the cost of finding the
    newest configuration independent of how many client commands sit on top of it.
    """
    import time

    store.append([LogEntry(term=1, command=cfg("a", "b", "c"))])

    def timed(reps: int = 300) -> float:
        store.last_config()  # warm
        start = time.perf_counter()
        for _ in range(reps):
            store.last_config()
        return (time.perf_counter() - start) / reps

    store.append([cmd(i) for i in range(500)])
    small = timed()
    store.append([cmd(i) for i in range(500, 10_000)])
    large = timed()

    assert store.last_config()[0] == 1
    assert large < small * 5, (
        f"lookup cost scaled with the log: {small * 1e6:.1f} us at 500 entries, "
        f"{large * 1e6:.1f} us at 10,000 — the index is not being used"
    )
