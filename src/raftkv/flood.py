"""Server-side load generator: a mixed client workload, driven from the dashboard.

Why this lives in the server rather than in a shell script: the interesting failure is a
*concurrency* failure, and a browser cannot hold 200 requests genuinely in flight against
three published ports without the browser's own connection pool becoming the bottleneck
you are measuring. Running the generator inside the leader removes that variable — every
write goes through `RaftNode.submit()`, the same call the HTTP handler makes, so the path
under test is the real one.

It is a DEMO AFFORDANCE, on the same footing as `/admin/crash`: unauthenticated, gated by
`RAFT_ADMIN_ENABLED`, and absent from any deployment that turns that flag off. A public
endpoint that makes a cluster do unbounded work on request is a denial-of-service switch.

Three workloads, because they fail differently:

  distinct  — N writes to N distinct keys. The baseline: no key contention at all, so
              this measures the commit path and nothing else.
  overwrite — N writes to ONE key. Every entry contends for the same KV row. The
              question stops being "did it commit" and becomes "which one won, and does
              every node agree" — the answer is decided by LOG ORDER, never by the order
              a client sent them.
  mixed     — sets and deletes interleaved over a small key space. The only one of the
              three that puts a non-`set` command through the applier under contention.

`concurrency` is the knob that makes the throughput ceiling visible rather than
theoretical. `_advance_commit_index()` scans the log downward from `last_log_index` to
`commit_index`, one SQLite `term_at()` query per index, and it runs once per `submit()`.
While a burst is uncommitted that scan is O(pending), so a burst of N costs O(N^2)
queries — synchronously, on the event loop that also owes its followers a heartbeat every
`heartbeat_interval`. Small bursts are invisible; a large one starves the heartbeat, the
followers elect around a leader that is merely busy, and every in-flight write fails.
Set concurrency low and the same total sails through. That contrast is the point.
"""

import asyncio
import contextlib
import logging
import time
from typing import Literal
from uuid import uuid4

from raftkv.logging_setup import log_event
from raftkv.models import Command
from raftkv.raft import NotLeaderError

logger = logging.getLogger("raftkv.flood")

Workload = Literal["distinct", "overwrite", "mixed"]

# A flood is meant to stress the cluster, not to wedge the box it runs on. These are the
# outer edges of what the endpoint will accept; the dashboard offers a much smaller menu.
MAX_TOTAL = 5000
MAX_CONCURRENCY = 2000
HOT_KEY = "hot"  # the single contended key used by `overwrite`
# Every generated key is zero-padded to this width, so byte order and numeric order are
# the same thing: `kv_all()` is `ORDER BY key`, and so is `curl /state | jq`, so an
# unpadded flood reads k1, k10, k100, k11, k2 everywhere the dashboard is not doing the
# sorting for you. (The dashboard sorts numerically regardless — it has to, since keys
# typed into the write control are arbitrary — but data that arrives already ordered
# beats data that depends on one client to order it.)
#
# Sized from MAX_TOTAL rather than from the run, so every flood on a node writes into ONE
# key namespace. Per-run widths were tried and are wrong: a 50-write run (k00..k49) then a
# 200-write run (k000..k199) leaves 250 keys in two disjoint families that byte order
# interleaves — k00, k000, k001 … k049, k01, k010 — which is less readable than doing
# nothing at all, and re-running the flood with a different number is the ordinary gesture.
KEY_WIDTH = len(str(MAX_TOTAL - 1))
MIXED_KEYSPACE = 8
DELETE_EVERY = 5  # in `mixed`, every 5th op is a delete


class FloodBusyError(Exception):
    """A flood is already running. Refused rather than queued: two overlapping
    generators would make the resulting counters impossible to attribute."""


class FloodRunner:
    """Owns at most one in-flight flood per node, plus the record of the last one.

    Counters are updated as each write settles, so `status()` is a live progress feed —
    that is what lets the dashboard draw a bar rather than freeze until the end."""

    def __init__(self, node) -> None:
        self._node = node
        self._task: asyncio.Task | None = None
        self._state: dict = self._idle_state()

    @staticmethod
    def _idle_state() -> dict:
        return {
            "running": False,
            "workload": None,
            "total": 0,
            "concurrency": 0,
            "done": 0,
            "ok": 0,
            "not_leader": 0,
            "timeout": 0,
            # Anything submit() raised that is NOT one of the two expected refusals.
            # Counted rather than raised: see _run(). A non-zero value here is a real
            # finding -- unlike `timeout` and `not_leader`, which a heavy burst is
            # SUPPOSED to produce.
            "failed": 0,
            "last_error": None,
            "started_at": None,
            "finished_at": None,
            "elapsed_ms": None,
            "cancelled": False,
        }

    # ---- introspection -----------------------------------------------------
    def _busy(self) -> bool:
        """The one answer to "is a flood in flight", for the guard and the status alike.

        These used to disagree, and the disagreement was reachable. `start()` asked the
        task; `status()` read the flag; and `_run`'s `finally` clears the flag before the
        coroutine returns and the task is marked done. Inside that window the dashboard
        drew an idle generator while POST /admin/flood answered 409 -- the same object
        telling an operator "nothing is running" and "already running" in one breath.

        Neither source is sufficient alone. The flag misses a task that has cleared it but
        not finished unwinding; the task is a weaker statement than the flag, because
        cancellation lands asynchronously. Treating either as busy is the conservative
        reading, and the only one under which the two endpoints cannot contradict."""
        if self._state["running"]:
            return True
        return self._task is not None and not self._task.done()

    def status(self) -> dict:
        """A snapshot the dashboard can poll. Includes a live elapsed_ms while running,
        so a stalled flood is visibly stalled instead of merely quiet."""
        state = dict(self._state)
        state["running"] = self._busy()
        if state["running"] and state["started_at"] is not None:
            state["elapsed_ms"] = round((time.monotonic() - state["started_at"]) * 1000)
        return state

    # ---- lifecycle ---------------------------------------------------------
    def start(self, workload: Workload, total: int, concurrency: int) -> dict:
        """Launch a flood in the background and return immediately.

        Leader-only, and checked here rather than inside the loop: a flood aimed at a
        follower would produce N identical NotLeaderErrors and teach nothing."""
        if self._busy():
            raise FloodBusyError("a flood is already running on this node")
        if self._node.role.value != "leader":
            raise NotLeaderError(self._node.leader_id)
        if not 1 <= total <= MAX_TOTAL:
            raise ValueError(f"total must be between 1 and {MAX_TOTAL}")
        if not 1 <= concurrency <= MAX_CONCURRENCY:
            raise ValueError(f"concurrency must be between 1 and {MAX_CONCURRENCY}")

        self._state = self._idle_state() | {
            "running": True,
            "workload": workload,
            "total": total,
            "concurrency": concurrency,
            "started_at": time.monotonic(),
        }
        log_event(logger, "flood_started", node=self._node.cfg.node_id,
                  workload=workload, total=total, concurrency=concurrency)
        self._task = asyncio.create_task(
            self._run(workload, total, concurrency), name="flood"
        )
        return self.status()

    async def cancel(self) -> dict:
        """Stop a flood early. Writes already in flight are left to settle on their own —
        cancelling a submit mid-commit would not un-append the entry, so pretending
        otherwise would be a lie about what the cluster now holds."""
        if self._task is not None and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._state["cancelled"] = True
        return self.status()

    # ---- the workload ------------------------------------------------------
    def _command(self, workload: Workload, i: int) -> Command:
        request_id = uuid4().hex
        if workload == "overwrite":
            return Command(op="set", key=HOT_KEY, value=f"v{i}", request_id=request_id)
        if workload == "mixed":
            key = f"k{i % MIXED_KEYSPACE:0{KEY_WIDTH}d}"
            if i % DELETE_EVERY == DELETE_EVERY - 1:
                return Command(op="delete", key=key, request_id=request_id)
            return Command(op="set", key=key, value=f"v{i}", request_id=request_id)
        return Command(op="set", key=f"k{i:0{KEY_WIDTH}d}", value=f"v{i}",
                       request_id=request_id)

    async def _run(self, workload: Workload, total: int, concurrency: int) -> None:
        gate = asyncio.Semaphore(concurrency)

        async def one(i: int) -> None:
            """Settles EVERY write into a counter and raises nothing but CancelledError.

            One write's failure must not end the burst. `gather` propagates the first
            exception and abandons the rest, so an unexpected error from a single
            submit() used to strand `done` below `total` forever -- the progress bar
            froze mid-run and the panel, seeing no timeouts, tinted the result green.
            The catch-all is therefore load-bearing, not defensive habit."""
            async with gate:
                try:
                    await self._node.submit(self._command(workload, i))
                    outcome = "ok"
                except NotLeaderError:
                    # Mid-flood leadership loss. Expected under a heavy burst, and the
                    # single most interesting number on the dashboard when it appears.
                    outcome = "not_leader"
                except TimeoutError:
                    outcome = "timeout"
                except asyncio.CancelledError:
                    # NOT an outcome. cancel() is a control action, and swallowing it
                    # here would leave the task uncancellable and `running` stuck true.
                    raise
                except Exception as exc:
                    outcome = "failed"
                    # Last one wins: 5000 identical repr()s would be a log flood of its
                    # own, and the first is rarely the informative one.
                    self._state["last_error"] = repr(exc)
                    log_event(logger, "flood_write_failed",
                              node=self._node.cfg.node_id, error=repr(exc))
                self._state[outcome] += 1
                self._state["done"] += 1

        try:
            # return_exceptions=True is belt-and-braces: one() already settles everything
            # into a counter, so nothing should arrive here. If a future edit reopens the
            # hole, this bounds the damage to one write instead of the whole burst.
            results = await asyncio.gather(
                *(one(i) for i in range(total)), return_exceptions=True
            )
            for result in results:
                if isinstance(result, BaseException) and not isinstance(
                    result, asyncio.CancelledError
                ):
                    self._state["failed"] += 1
                    self._state["done"] += 1
                    self._state["last_error"] = repr(result)
                    log_event(logger, "flood_write_escaped",
                              node=self._node.cfg.node_id, error=repr(result))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # a generator bug must not take the node down with it
            self._state["last_error"] = repr(exc)
            log_event(logger, "flood_failed", node=self._node.cfg.node_id,
                      error=repr(exc))
        finally:
            self._state["running"] = False
            self._state["finished_at"] = time.monotonic()
            self._state["elapsed_ms"] = round(
                (self._state["finished_at"] - self._state["started_at"]) * 1000
            )
            log_event(
                logger, "flood_finished", node=self._node.cfg.node_id,
                workload=workload, done=self._state["done"], ok=self._state["ok"],
                not_leader=self._state["not_leader"], timeout=self._state["timeout"],
                failed=self._state["failed"], elapsed_ms=self._state["elapsed_ms"],
            )
