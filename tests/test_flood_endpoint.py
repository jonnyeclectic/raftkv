"""The server-side load generator and its /admin/flood endpoints.

test_flood.py drives concurrency through SimCluster and asserts on the CLUSTER. This
file tests the generator itself: its accounting, its refusals, and the endpoints that
expose it. They are separate concerns and they fail separately -- a generator that
miscounts, or that lets two floods overlap, breaks nothing about Raft and everything
about the numbers reported back from a run.

The bug this file exists to have caught: `FloodRunner` was imported into app.py but
never constructed, so every one of the three endpoints raised NameError -> 500. It had
no test, so nothing said so.
"""

import asyncio

import pytest
from fastapi.testclient import TestClient

from conftest import eventually
from raftkv.app import create_app
from raftkv.flood import MAX_CONCURRENCY, MAX_TOTAL, FloodBusyError, FloodRunner
from raftkv.models import Command, Role
from raftkv.raft import NotLeaderError
from test_api import build_cfg, wait_for_leader


@pytest.fixture
def client(tmp_path):
    # A wide commit_timeout: this file asserts on ACCOUNTING, and a burst that times out
    # on a loaded CI box would still be counted correctly but would stop exercising the
    # `ok` path that most of these tests are about.
    with TestClient(create_app(build_cfg(tmp_path, commit_timeout=10.0))) as c:
        yield c


# ---- the endpoints, end to end ----------------------------------------------


def test_flood_runs_and_every_write_is_accounted_for(client):
    """The headline: done == total, and the three outcomes partition it exactly.

    A generator whose counters do not add up makes every other number on the dashboard
    unfalsifiable, so this is asserted as an equality rather than a bound."""
    wait_for_leader(client)
    started = client.post("/admin/flood",
                          json={"workload": "distinct", "total": 25, "concurrency": 5})
    assert started.status_code == 200, started.text
    assert started.json()["running"] is True

    final = _await_completion(client)
    assert final["done"] == 25
    assert final["ok"] + final["not_leader"] + final["timeout"] == 25
    assert final["ok"] == 25, "a solo leader should commit every write"
    assert final["elapsed_ms"] is not None and final["elapsed_ms"] >= 0

    # ...and the writes really landed in the state machine, not just in a counter.
    assert len(client.get("/state").json()["kv"]) == 25


def test_status_is_pollable_while_the_flood_is_still_running(client):
    """The endpoint returns immediately and progress is visible mid-flight. If it
    blocked until the burst finished, the dashboard could only ever draw 0% or 100%."""
    wait_for_leader(client)
    body = client.post("/admin/flood",
                       json={"workload": "distinct", "total": 300, "concurrency": 300}).json()
    assert body["running"] is True
    assert body["done"] == 0, "start() must not block until the burst completes"
    assert body["total"] == 300 and body["concurrency"] == 300

    final = _await_completion(client, timeout=30.0)
    assert final["running"] is False
    assert final["done"] == 300


def test_a_second_flood_is_refused_rather_than_queued(client):
    """Two overlapping generators would make the counters impossible to attribute, so
    the second one is a 409 -- the same "not now" the promote button answers."""
    wait_for_leader(client)
    first = client.post("/admin/flood", json={"total": MAX_TOTAL, "concurrency": 400})
    assert first.status_code == 200

    # The refusal only means anything while the first burst is genuinely in flight, so
    # that precondition is asserted rather than assumed. It used to be neither: a 400-write
    # burst finished between the two calls on a slow shared runner, the second was accepted
    # on its merits, and the test reported "no 409" as though the guard had broken.
    assert client.get("/admin/flood").json()["running"] is True, (
        "the first flood finished before the second request; it is too small to race against"
    )

    second = client.post("/admin/flood", json={"total": 5, "concurrency": 1})
    assert second.status_code == 409
    assert "already running" in second.text
    client.delete("/admin/flood")


def test_cancel_stops_the_flood_and_says_so(client):
    wait_for_leader(client)
    client.post("/admin/flood", json={"total": 2000, "concurrency": 2000})
    cancelled = client.delete("/admin/flood").json()
    assert cancelled["running"] is False
    assert cancelled["cancelled"] is True
    # ...and the node is still healthy afterwards, which is the whole point of
    # cancelling rather than killing.
    assert client.get("/state").json()["role"] == "leader"
    assert client.put("/kv/after", json={"value": "ok"}).status_code == 200


def test_cancelling_when_nothing_runs_is_harmless(client):
    """The dashboard's cancel button is always present, so it is always clickable."""
    wait_for_leader(client)
    body = client.delete("/admin/flood").json()
    assert body["running"] is False
    assert body["cancelled"] is False, "nothing was running, so nothing was cancelled"


def test_status_before_any_flood_is_a_clean_idle_snapshot(client):
    body = client.get("/admin/flood").json()
    assert body == {
        "running": False, "workload": None, "total": 0, "concurrency": 0,
        "done": 0, "ok": 0, "not_leader": 0, "timeout": 0,
        "failed": 0, "last_error": None,
        "started_at": None, "finished_at": None, "elapsed_ms": None,
        "cancelled": False,
    }


@pytest.mark.parametrize("workload", ["distinct", "overwrite", "mixed"])
def test_each_workload_runs_and_leaves_the_state_machine_consistent(client, workload):
    """All three shapes go through the real submit() path and the real applier."""
    wait_for_leader(client)
    client.post("/admin/flood",
                json={"workload": workload, "total": 40, "concurrency": 8})
    final = _await_completion(client)
    assert final["done"] == 40 and final["workload"] == workload

    state = client.get("/state").json()
    assert state["last_applied"] == state["commit_index"]
    if workload == "overwrite":
        # Every write hit one key, so exactly one survives -- decided by log order.
        assert list(state["kv"]) == ["hot"]


def test_mixed_workload_actually_applies_its_deletes(client):
    """A delete that silently no-ops would leave every assertion above still passing.

    Pinned deterministically rather than statistically: at concurrency 1 the ops apply
    in submission order, index 4 deletes its key, and in a 5-op run nothing rewrites it.
    So that key's absence is a fact about the applier, not about a race.

    The names come from the generator rather than being written out here. They are
    zero-padded (`k0004`, not `k4`) so that byte order and numeric order agree, and a
    literal spelled out in the test silently pins the padding width as well as the
    behaviour under test — which is exactly how this test broke when the width changed."""
    wait_for_leader(client)
    runner = FloodRunner.__new__(FloodRunner)
    expected = {runner._command("mixed", i).key for i in range(4)}
    deleted = runner._command("mixed", 4).key
    client.post("/admin/flood",
                json={"workload": "mixed", "total": 5, "concurrency": 1})
    _await_completion(client)
    kv = client.get("/state").json()["kv"]
    assert deleted not in kv, f"the delete at index 4 never reached storage.apply(): {kv}"
    assert set(kv) == expected, "the surrounding sets went missing too"


# ---- refusals ----------------------------------------------------------------


@pytest.mark.parametrize("payload", [
    {"total": 0},                       # below the floor
    {"total": MAX_TOTAL + 1},           # above the ceiling
    {"concurrency": 0},
    {"concurrency": MAX_CONCURRENCY + 1},
    {"workload": "nonsense"},
])
def test_out_of_range_requests_are_refused_by_the_schema(client, payload):
    """Bounds live on the pydantic model, so a bad request never reaches the node. A
    public endpoint that makes a cluster do unbounded work on request is a DoS switch."""
    wait_for_leader(client)
    assert client.post("/admin/flood", json=payload).status_code == 422


def test_flood_is_refused_while_crashed(client):
    wait_for_leader(client)
    client.post("/admin/crash")
    assert client.post("/admin/flood", json={"total": 5}).status_code == 503


def test_flood_endpoints_are_absent_when_admin_is_disabled(tmp_path):
    """Same footing as /admin/crash: RAFT_ADMIN_ENABLED=0 removes it entirely."""
    with TestClient(create_app(build_cfg(tmp_path, admin_enabled=False))) as c:
        assert c.post("/admin/flood", json={"total": 5}).status_code == 404
        assert c.get("/admin/flood").status_code == 404
        assert c.delete("/admin/flood").status_code == 404


# ---- the runner in isolation --------------------------------------------------


async def test_a_follower_refuses_to_generate_load(make_node):
    """Checked before the loop, not inside it: a flood aimed at a follower would
    otherwise produce N identical NotLeaderErrors and teach nothing."""
    node = make_node()
    assert node.role is Role.FOLLOWER
    with pytest.raises(NotLeaderError):
        FloodRunner(node).start("distinct", total=5, concurrency=1)


async def test_the_runner_refuses_to_overlap_itself(make_node):
    """The same guard as the 409, asserted without the HTTP layer in the way."""
    node = make_node(peer_ids=())
    await node._start_election()
    node.start()
    runner = FloodRunner(node)
    try:
        runner.start("distinct", total=500, concurrency=500)
        with pytest.raises(FloodBusyError):
            runner.start("distinct", total=1, concurrency=1)
    finally:
        await runner.cancel()
        await node.stop()


async def test_bounds_are_enforced_in_the_runner_not_only_in_the_schema(make_node):
    """Defence in depth, and it matters: the module is importable, so the schema is not
    the only caller. A ValueError here is what app.py maps to 422."""
    node = make_node(peer_ids=())
    await node._start_election()
    runner = FloodRunner(node)
    for total, concurrency in [(0, 1), (MAX_TOTAL + 1, 1), (1, 0), (1, MAX_CONCURRENCY + 1)]:
        with pytest.raises(ValueError):
            runner.start("distinct", total=total, concurrency=concurrency)


async def test_mixed_workload_really_emits_deletes(make_node):
    """The reason `mixed` exists: delete takes a different branch in storage.apply(),
    and a generator that quietly emitted only sets would never exercise it."""
    node = make_node(peer_ids=())
    runner = FloodRunner(node)
    ops = [runner._command("mixed", i) for i in range(20)]
    assert {c.op for c in ops} == {"set", "delete"}
    assert all(isinstance(c, Command) for c in ops)
    # ...and every request_id is unique, or the submit() waiter map would collide.
    assert len({c.request_id for c in ops}) == 20


async def test_a_cancelled_flood_leaves_the_node_usable(make_node):
    """Writes already in flight are left to settle: cancelling a submit mid-commit
    cannot un-append the entry, so the node must stay coherent either way."""
    node = make_node(peer_ids=())
    await node._start_election()
    node.start()
    runner = FloodRunner(node)
    try:
        runner.start("distinct", total=2000, concurrency=2000)
        await asyncio.sleep(0.02)
        status = await runner.cancel()
        assert status["running"] is False and status["cancelled"] is True
        # The node still commits, and its own invariants still hold.
        await node.submit(Command(op="set", key="after", value="ok", request_id="x"))
        assert node.storage.kv_get("after") == "ok"
        assert node.last_applied <= node.commit_index <= node.storage.last_log_index()
    finally:
        await node.stop()


def _await_completion(client, timeout: float = 20.0) -> dict:
    """Poll GET /admin/flood until it reports finished. Deadline-based rather than a
    fixed sleep, for the same reason conftest.eventually() is."""
    import time as _time

    deadline = _time.monotonic() + timeout
    while _time.monotonic() < deadline:
        body = client.get("/admin/flood").json()
        if not body["running"]:
            return body
        _time.sleep(0.02)
    raise AssertionError(f"flood never finished within {timeout}s")


# ---- one write's failure must not end the burst -----------------------------
#
# `gather` propagates the first exception and abandons every task still queued behind
# it. So a single unexpected error from submit() used to strand `done` below `total`
# permanently: the progress bar froze mid-run, and the panel -- seeing no timeouts and
# no rejections -- tinted the stalled result green. Both halves are the bug. The
# generator now settles every write into a counter and raises nothing but CancelledError.


class Boom(RuntimeError):
    """Stands in for anything submit() can raise that the generator does not expect --
    a SQLite `database is locked` under contention being the realistic one."""


async def test_an_unexpected_error_does_not_abandon_the_rest_of_the_burst(make_node):
    node = make_node(peer_ids=())
    node.role = Role.LEADER

    async def always_boom(_cmd):
        raise Boom("sqlite: database is locked")

    node.submit = always_boom
    runner = FloodRunner(node)
    runner.start("distinct", total=20, concurrency=4)
    await asyncio.sleep(0.3)

    status = runner.status()
    assert not status["running"]
    # The whole point: every write is accounted for, not just the ones before the first
    # failure. Before the fix this read done=0.
    assert status["done"] == 20, "the burst was abandoned after the first failure"
    assert status["failed"] == 20
    assert status["ok"] == 0


async def test_one_bad_write_among_many_good_ones_is_isolated(make_node):
    """The realistic shape: an intermittent error, not a total one. The other 19 writes
    must still commit and still be counted."""
    node = make_node(peer_ids=())
    await node._start_election()   # a real leader, so the healthy writes really commit
    node.start()                   # ...and the apply loop is running to settle them
    calls = {"n": 0}
    real_submit = node.submit

    async def flaky(cmd):
        calls["n"] += 1
        if calls["n"] == 5:
            raise Boom("transient")
        await real_submit(cmd)

    node.submit = flaky
    runner = FloodRunner(node)
    try:
        runner.start("distinct", total=20, concurrency=1)  # serial: the 5th is the 5th
        await eventually(lambda: not runner.status()["running"], timeout=5.0)

        status = runner.status()
        assert status["done"] == 20
        assert status["failed"] == 1
        assert status["ok"] == 19, "a single failure took healthy writes down with it"
    finally:
        await node.stop()


async def test_the_failing_error_is_reported_rather_than_only_counted(make_node):
    """A count alone says "something broke" and stops there. The repr is what turns a
    surprise into a diagnosis."""
    node = make_node(peer_ids=())
    node.role = Role.LEADER

    async def always_boom(_cmd):
        raise Boom("sqlite: database is locked")

    node.submit = always_boom
    runner = FloodRunner(node)
    runner.start("distinct", total=3, concurrency=1)
    await asyncio.sleep(0.3)

    assert "database is locked" in runner.status()["last_error"]


async def test_cancel_still_works_when_writes_are_failing(make_node):
    """CancelledError must stay uncaught by the new catch-all. Swallowing it would count
    a cancellation as a failed write AND leave the task uncancellable, so `running`
    would never clear and the panel would poll a flood that no longer exists."""
    node = make_node(peer_ids=())
    node.role = Role.LEADER

    async def slow_boom(_cmd):
        await asyncio.sleep(0.05)
        raise Boom("still broken")

    node.submit = slow_boom
    runner = FloodRunner(node)
    runner.start("distinct", total=500, concurrency=5)
    await asyncio.sleep(0.1)
    status = await runner.cancel()

    assert status["cancelled"] is True
    assert not status["running"], "cancel left the flood marked running"
    assert status["done"] < 500, "nothing was actually cancelled"


async def test_a_clean_flood_reports_no_failures(make_node):
    """The regression guard: `failed` must stay zero on the ordinary path, or the
    dashboard's new red tint fires on every healthy run."""
    node = make_node(peer_ids=())
    await node._start_election()
    node.start()
    runner = FloodRunner(node)
    try:
        runner.start("distinct", total=10, concurrency=5)
        await eventually(lambda: not runner.status()["running"], timeout=5.0)

        status = runner.status()
        assert status["failed"] == 0
        assert status["last_error"] is None
        assert status["done"] == 10
    finally:
        await node.stop()


def test_idle_snapshot_carries_the_new_fields(client):
    """The dashboard reads these keys unconditionally; a snapshot missing them renders
    `undefined` into the summary line."""
    body = client.get("/admin/flood").json()
    assert body["failed"] == 0
    assert body["last_error"] is None
