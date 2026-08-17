# RaftKV

RaftKV is a replicated key-value store: three FastAPI nodes running Raft consensus over
HTTP/JSON, persisting term/vote/log and applied state in SQLite, observable through a
zero-dependency dashboard. It exists to demonstrate a distributed algorithm end to end —
how it works (live dashboard, sequence diagrams), where it fails (deterministic
partition/failover/divergence tests), and how those failures are mitigated (quorum
commit, persistence-before-reply, conflict-only log repair) — with every design decision
recorded with evidence in [docs/OVERVIEW.md](docs/OVERVIEW.md).

## Quickstart

```bash
make demo          # docker compose up -d --wait --build
# open http://localhost:8001/   <- the dashboard (any node serves it)
make smoke         # end-to-end: election -> write -> replication -> leader kill -> failover
```

Prove the clean start before relying on the cluster anywhere else — a build that only works
on the dev machine that produced it is the classic failure mode:

```bash
make clean-start-check
```

### Which build am I looking at?

The masthead carries two stamps, because the two layers reload differently and have
already been observed disagreeing:

```
raftkv  REPLICATED KEY-VALUE STORE   ui f3a9c1 · srv 0.1.0+8e21d0
```

- **`ui`** — sha256 of `dashboard.html`, stamped in at serve time. `GET /` re-reads the
  file per request, so an HTML edit is live on browser reload. If the serving node's copy
  no longer matches the page you have open, the stamp turns amber and says `RELOAD`.
- **`srv`** — package version plus a hash of `raftkv/*.py`, frozen at import. Python is
  imported once at process start, so a `.py` edit does nothing until the node restarts.
  This names the code *executing*, not the code on disk.

Both are content hashes, so there is no counter to remember to bump. `GET /build` returns
them per node, and the SYSTEM panel reads `MIXED (n)` when nodes disagree — a cluster
running two builds behaves like Raft misbehaving, and nothing else on the page shows it.
Per-node builds are under each card's **infrastructure** section.

## Repo map

| Path | Responsibility |
|---|---|
| `pyproject.toml` | Project metadata, exact dependency pins, pytest + ruff config |
| `Makefile` | `install` / `test` / `lint` / `demo` / `smoke` / `clean-start-check` / `run-local` / `k8s-*` targets |
| `.gitignore` | Keeps DBs, logs, caches, and pre-existing scratch files out of the repo |
| `.github/workflows/ci.yml` | CI: lint + unit tests, and the compose smoke test |
| `src/raftkv/__init__.py` | Package marker |
| `src/raftkv/models.py` | ALL shared Pydantic shapes: commands, **configuration entries**, no-ops, log entries, RPCs, metrics, `NodeState` |
| `src/raftkv/config.py` | `NodeConfig` + env parsing; validates timing ratios (heartbeat ≪ election) |
| `src/raftkv/logging_setup.py` | JSON logs, rotating files (1 MB × 5), in-memory ring buffer, PII redaction |
| `src/raftkv/storage.py` | SQLite persistence (term/vote/log) + applied KV state machine, one file per node |
| `src/raftkv/transport.py` | `Transport` protocol; `HttpTransport` (httpx) and `MemoryTransport` (partition/crash controls) |
| `src/raftkv/raft.py` | `RaftNode`: synchronous rule methods + election timer, replication loop, apply loop |
| `src/raftkv/build.py` | `ui` / `srv` build stamps: content hashes, one recomputed per request and one frozen at import |
| `src/raftkv/flood.py` | Server-side load generator (`distinct` / `overwrite` / `mixed`), gated by `RAFT_ADMIN_ENABLED` |
| `src/raftkv/app.py` | FastAPI wiring: Raft RPCs, KV API, `/state`, `/logs`, `/healthz`, `/build`, `/admin/*`, exception handlers |
| `src/raftkv/static/dashboard.html` | Operational dashboard: vanilla JS + Canvas 2D, zero dependencies. Health strip, live topology graph, per-node detail, failure injection, event feed |
| `tests/conftest.py` | Shared fixtures: nodes over `MemoryTransport`, fast timers, simulated clusters |
| `tests/test_sanity.py` | Toolchain sanity |
| `tests/test_models.py` | Model round-trips and validation errors |
| `tests/test_config.py` | Env parsing + timing-ratio validation |
| `tests/test_logging_setup.py` | JSON format, rolling files, ring buffer, the PII-redaction guarantee |
| `tests/test_storage.py` | Durability across reopen; atomic apply with `last_applied` |
| `tests/test_transport.py` | HTTP error mapping; memory-transport crash/partition semantics |
| `tests/test_apply_loop.py` | The single applier: committed prefix only, in order, resuming at `last_applied`, stepping over no-ops and config entries |
| `tests/test_election_rules.py` | RequestVote receiver rules and term observation (unit, no timers) |
| `tests/test_append_entries.py` | AppendEntries receiver rules: consistency check, conflict-only truncation, commit-index monotonicity, timer/leader-id ordering |
| `tests/test_election_flow.py` | Candidate side with mockito-stubbed peers, the election timer loop, and the leader's opening no-op |
| `tests/test_replication.py` | Leader side: matchIndex discipline, commit rule, stale-reply guards |
| `tests/test_simulation.py` | Real timers over a simulated network: partition, failover, divergence, restart |
| `tests/test_api.py` | HTTP contract: KV round-trip, 503 + leader hint, structured errors, `/logs` redaction |
| `tests/test_admin_failure.py` | Dashboard kill/revive: a crashed node refuses everything, recovery reloads from disk |
| `tests/test_partition.py` | Simulated partitions: links cut both ways, a partitioned leader commits nothing |
| `tests/test_learner.py` | Non-voting members: a learner is replicated to but never counted for quorum or commit |
| `tests/test_joint_consensus.py` | §6 membership change: separate majorities of both configurations, append-time adoption, truncation revert |
| `tests/test_reset.py` | The reset control: destroys term, vote, log and applied state, while membership survives in the log &mdash; reverting it would hand a wiped node a subset of the real voter set, and therefore a second quorum |
| `tests/test_membership_growth.py` | Growing a live cluster 3 → 4 → 5 over real timers: keeps committing throughout, and five voters really do tolerate two failures |
| `tests/test_flood.py` | Concurrency floods: many writes in flight at once, three workloads that fail differently |
| `tests/test_flood_endpoint.py` | The `/admin/flood` contract and the generator's own accounting: start returns immediately, progress is pollable, every write is accounted for, and two floods never overlap |
| `tests/test_debug_entrypoint.py` | The IDE debug entrypoint, which nobody runs in CI and everybody runs before stepping through node-1 |
| `tests/test_dashboard_leader.py` | The dashboard's leader pick, run under node: highest term wins over first-seen, and `writes` counts reachable voters rather than live ones |
| `tests/test_dashboard_staged.py` | The member-vs-staged split, run under node: a process in nobody's configuration is drawn as unattached, an attached learner is not, and a crashed member never reclassifies itself out of the cluster |
| `tests/test_dashboard_probe.py` | The probe list, run under node: `?probe=` extends it to a node provisioned while the page is open, a bare port means loopback, and anything that is not `host:port` is dropped rather than fetched twice a second forever |
| `tests/test_dashboard_adopt.py` | Membership adoption, run under node: members are picked up from the configuration they replicate (so a page reload keeps them), while a peer-only address like `raft-node-4:8000` is never adopted into a card the browser cannot reach |
| `tests/test_dashboard_discover.py` | The discovery tick against a stubbed, deliberately out-of-order network: cards are ordered by the probe list rather than by who answered first, and no address is ever adopted twice |
| `tests/test_dashboard_flood.py` | The flood panel's wording and tint, run under node: failures are named and go amber even when the run completed, and a cancelled flood never reads as done |
| `tests/test_dashboard_keys.py` | The state machine's key display, run under node: numeric runs sort as numbers (`k8` before `k79` before `k80`), and a scroll position survives the 500 ms card rebuild |
| `tests/test_build_stamp.py` | Build stamps: `ui` tracks the file per request, `srv` stays frozen at import, injection leaves no placeholder, and a mixed-build cluster is named |
| `tests/test_log_repair.py` | Accelerated §5.3 backtracking: the conflict hint is advice and every jump still lands on an ordinary consistency check, and the 6224 round trips (~52 min) a wiped follower used to need collapse to two |
| `tests/test_gap_repair.py` | The two ways to fall behind, which cost wildly different amounts: a strict prefix rejoins in one round trip with or without the hint, while a wiped log is the regime that actually exercises the repair walk |
| `tests/test_append_batching.py` | The AppendEntries batch cap and the re-fire that pays for it — either half alone is worse than neither, so both are pinned here |
| `tests/test_commit_scan.py` | The sparse commit scan: same index the exhaustive walk would commit, without the O(N²) `term_at()` cost a burst used to pay |
| `tests/test_config_lookup.py` | `last_config()` stays on the partial index, because it is on the write path and unindexed it scanned 2.475 ms against 15,000 entries |
| `scripts/smoke.sh` | End-to-end check against a running compose cluster |
| `scripts/clean_start_check.sh` | THE clean-start gate: the compose path works from absolutely clean state |
| `scripts/run_local.sh` | Starts nodes 2–3 locally (plus idle 4–5) so node-1 can run under the IDE debugger |
| `scripts/node_up.sh` | Provisions one more staged process mid-demo — the run-local analogue of `kubectl scale` |
| `scripts/debug_node.py` | The IDE debug target for node-1 (a script, not `-m uvicorn` — see its docstring) |
| `Dockerfile` | `python:3.14-slim`, non-root user, uvicorn entrypoint |
| `docker-compose.yml` | Three voting nodes on 8001–8003 plus an idle learner on 8004, healthchecks, named volumes |
| `k8s/raftkv.yaml` | StatefulSet + headless Service (stable peer DNS) for kind |
| `docs/` | Architecture, Raft walkthrough, failure modes, overview |

## Test matrix

| Layer | File | What it proves |
|---|---|---|
| Rule units (sync, no timers) | `tests/test_election_rules.py`, `tests/test_append_entries.py`, `tests/test_apply_loop.py` | Each Raft receiver rule in isolation: vote restriction, one vote per term, consistency check, conflict-only truncation, timer-reset discipline, commit-index monotonicity, and an applier that drains the committed prefix in order without stalling on a no-op |
| Mockito-stubbed flows | `tests/test_election_flow.py`, `tests/test_replication.py` | Candidate and leader async flows with stubbed peers: quorum counting, stale-reply rejection, step-down on higher term, commit rule, and three followers at three nextIndex values each getting their own slice |
| Foundation units | `tests/test_models.py`, `tests/test_config.py`, `tests/test_logging_setup.py`, `tests/test_storage.py`, `tests/test_transport.py` | Shapes validate, timing ratios enforced, logs redact values, storage survives reopen atomically |
| Simulated-network scenarios | `tests/test_simulation.py` | Whole-cluster behavior under real timers: stable election, failover + rejoin, partitioned leader cannot commit and is repaired on heal, catch-up, restart durability |
| API contract | `tests/test_api.py` | HTTP surface: KV round-trip, follower 503 + `leader_id`, validation errors, structured 500s, `/logs` |
| Injected failure | `tests/test_admin_failure.py`, `tests/test_partition.py`, `tests/test_reset.py` | The failure-injection controls do what they claim: a crashed node refuses every surface except the dashboard and `/admin/recover`; a partition is cut in both directions and a partitioned leader commits nothing; reset destroys exactly the state Figure 2 requires to be durable, and keeps the membership that a revert would turn into a second quorum |
| Membership (§6) | `tests/test_learner.py`, `tests/test_joint_consensus.py`, `tests/test_membership_growth.py` | A learner is replicated to but never counted for quorum; promotion runs through joint consensus needing separate majorities of both configurations; and a live cluster grown 3 → 4 → 5 keeps committing throughout, then tolerates the failures its new size promises |
| Concurrency under load | `tests/test_flood.py`, `tests/test_flood_endpoint.py` | Many client writes genuinely in flight at once: distinct keys, contention on one key, and non-`set` commands through the applier — with every node agreeing on the winner, decided by log order rather than send order. Plus the `/admin/flood` contract the dashboard drives it through: start returns immediately, progress is pollable, refusals are documented. And the accounting holds under a failing write: one unexpected error is isolated into a `failed` count instead of abandoning the rest of the burst |
| Dashboard logic | `tests/test_dashboard_leader.py`, `tests/test_dashboard_staged.py`, `tests/test_dashboard_flood.py`, `tests/test_dashboard_keys.py` | The four pieces of UI logic that can misreport the cluster. The leader pick: with a stale leader and the real one both self-reporting LEADER, the panel picks by highest term (not address order), routes writes there, and calls an isolated leader's writes unavailable. The member split: a running process in nobody's configuration is drawn as staged rather than as a broken member, and a *crashed* node stays a member — reclassifying it would shrink the quorum denominator at exactly the moment the cluster is surviving that failure. The flood summary: a run that finished having timed out most of its writes is tinted amber and names the failures, rather than reading as a clean sweep. The key display: numeric runs sort as numbers, so a flood's keys read in sequence instead of `k1, k10, k100, k11, k2`, and a scroll position survives the 500 ms rebuild that would otherwise reset it twice a second. All four sliced out of the shipped HTML and executed under node; skipped if node is absent |
| Build provenance | `tests/test_build_stamp.py` | The stamp describes the bytes it claims to: `ui` is recomputed per request (never cached), `srv` is frozen at import, the served page carries its own hash with no placeholder left behind, `/build` answers even while crashed, and nodes on different builds report `MIXED` rather than agreement |
| Performance regressions | `tests/test_log_repair.py`, `tests/test_gap_repair.py`, `tests/test_append_batching.py`, `tests/test_commit_scan.py`, `tests/test_config_lookup.py` | The four costs that were measured rather than guessed, each pinned so it cannot come back: one-index backtracking, the unbounded batch, the O(N²) commit walk, and the unindexed configuration lookup |
| Compose smoke | `scripts/smoke.sh` | The real containers do all of the above end to end |

## Make targets

- `make install` — `uv sync --all-extras`
- `make test` — full pytest suite
- `make lint` — ruff over src and tests
- `make demo` — build + start the three-node compose cluster, dashboard on :8001
- `make down` — stop the cluster and delete its volumes
- `make smoke` — end-to-end smoke test against the running cluster
- `make clean-start-check` — the clean-start gate: everything works from clean state
- `make run-local` — nodes 2–3 locally (plus idle 4–5); you run node-1 under the debugger.
  Replication, growth, failover, partitions and load all run against this one cluster
  without shelling out to `docker` or `kubectl`; see [Running locally](#running-locally)
- `make node-up N=6` — provision one more process mid-demo, the run-local analogue of
  `kubectl scale`. It comes up **staged**: attach and promote it from the dashboard
- `make demo-reset` — **destructive**: stops local nodes and deletes `data/` and `logs/`.
  The way back to a known-good starting state between runs
- `make k8s-demo` — kind cluster + StatefulSet variant. **Run this first**: the other
  `k8s-*` targets need the cluster it creates, and refuse with one line if it is absent
  (kubectl's own failure here is a `localhost:8080 connection refused` that reads like a
  broken command rather than a missing cluster)
- `make k8s-forward` — port-forward pods to localhost 8001–8005 (auto-reconnects when a pod is replaced)
- `make k8s-scale N=5` — provision more pods. They come up **staged**: a replica is a process, not a member, so attach and promote them from the dashboard to actually grow the cluster
- `make k8s-down` — delete the kind cluster

## Docs

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — modules, concurrency model, SQLite schema, component + deployment diagrams
- [docs/RAFT.md](docs/RAFT.md) — plain-language Raft walkthrough with sequence diagrams
- [docs/FAILURE_MODES.md](docs/FAILURE_MODES.md) — handled failures and deliberate omissions, as tables
- [docs/OVERVIEW.md](docs/OVERVIEW.md) — how this addresses the assignment; decision log with evidence
