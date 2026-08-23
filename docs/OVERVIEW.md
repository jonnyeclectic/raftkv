# Overview — what this project sets out to do

**The goal:** implement a distributed-systems algorithm, then demonstrate how it works,
where it fails, how those failures are mitigated, and the engineering practices around
it — testing and observability.

**Where each part is answered:**

| Goal | Artifact that answers it |
|---|---|
| How it works | [RAFT.md](RAFT.md) (plain-language walkthrough + sequence diagrams) and the dashboard — three cards electing a leader in real time |
| Where it fails | [FAILURE_MODES.md](FAILURE_MODES.md) table 1 (handled, each with a way to reproduce it) and table 2 (deliberately omitted, each with its cost and production fix) |
| How it's mitigated | Quorum commit, persistence-before-reply, conflict-only repair — each mapped to the test that proves it (README test matrix) |
| Testing | Four layers: rule units (no timers) → mockito-stubbed flows → simulated-network partition/failover/divergence scenarios → compose smoke; see the README test matrix |
| Observability | JSON logs (rotating files + per-node ring buffer at `/logs`), the dashboard merging all nodes' feeds, `/state` with full metrics on every node |

## Decision log (evidence + confidence)

| # | Decision | Alternatives rejected | Evidence | Confidence |
|---|----------|----------------------|----------|------------|
| D1 | **Raft** (leader election + log replication only) | Paxos (famously hard to teach), 2PC (not fault-tolerant consensus), CRDTs (no total order) | Raft's explicit design goal is understandability (Ongaro & Ousterhout, raft.github.io/raft.pdf §1) — and an implementation written to be read and reviewed rewards explainability | High |
| D2 | Scope the initial cut to elections + replication: omit snapshots (§7), membership changes (§6), PreVote, leader no-op on election win (thesis §6.4), ReadIndex — **name each in FAILURE_MODES.md with why-cut + production path** | Implementing all of them up front (weeks of work, more bug surface, violates "no bugs") | MIT 6.824 scopes labs the same way; Students' Guide flags snapshot/lastApplied subtleties as top bug farms | High |
| D2a | Of that cut list, **put §6 membership changes and the election-win no-op back in**; leave snapshots, PreVote and ReadIndex out (D20 later put PreVote back, for a reason D2a could not have known) | Staying with the original scope | Growing a running cluster turned out to be the capability worth having, and the no-op is a hard precondition for it rather than an optimisation — a leader holding an inherited uncommitted config entry cannot tell whether it is committed. Both are implemented and tested; see FAILURE_MODES.md table 1b and `tests/test_joint_consensus.py`. The three left out stayed out because their absence is either invisible at this scale (snapshots, ReadIndex) or *worth* showing — PreVote's absence is what makes a rejoining node force one needless election, which this build surfaces rather than hides | High |
| D3 | **3-node cluster**, quorum = 2 | 5 nodes (more moving parts, same lessons) | Smallest cluster demonstrating majority quorum, failover, and partition safety | High |
| D4 | **Python 3.14**, image `python:3.14-slim` | 3.13 (older), 3.15 (pre-release Oct 2026) | 3.14.7 current stable (python.org 2026-08-05); fastapi 0.141.1, pydantic 2.13.4, pydantic-core 2.48.0 ship cp314 wheels (PyPI, verified 2026-08-13) | High |
| D5 | **FastAPI + HTTP/JSON between nodes**; `httpx.AsyncClient` for RPCs | gRPC (toolchain + codegen noise, opaque on the wire), raw TCP (reinventing framing) | FastAPI is the chosen web framework here; JSON RPCs are curl-able and debugger-friendly against a running cluster | High |
| D6 | **SQLite (stdlib `sqlite3`)** per node: Raft persistent state (`currentTerm`, `votedFor`, `log[]`) **and** the applied KV state machine | Postgres (a second distributed system to operate), SQLAlchemy (ORM adds nothing over 5 SQL statements) | Raft *requires* durable state before replying to RPCs (Fig. 2, §5.2) — so the database is load-bearing, not decorative. Stdlib = zero deps | High |
| D7 | **mockito 2.0.4 + pytest-mockito 0.0.6.post1** for unit mocks | unittest.mock (mockito is a fixed requirement of this build) | mockito 2.0 (Apr 2026) adds first-class async stubbing: `thenReturn` on async callables stays awaitable (CHANGES.txt, PR #107); CI-tested on 3.8–3.14 | High |
| D8 | Unit tests drive Raft **rule methods directly** (no timers); integration tests run real timers over an **in-memory transport** with partition/crash controls | Fake asyncio clock (most bug-prone kind of test infra); sleeps-and-hope | Mirrors MIT 6.824 harness + Eli Bendersky's RPC-proxy pattern; asyncio's single loop makes sync handlers atomic | High |
| D9 | Dashboard = **one static HTML page, vanilla JS, polling `GET /state`** on every node every 500 ms | React/Vite (build step = clean-start risk), WebSockets/SSE (state fits in a poll) | Zero build step; polling degrades gracefully when a node dies (card goes red), which is exactly the state the page exists to surface | High |
| D10 | Logging: stdlib `logging` + JSON formatter + `RotatingFileHandler` (1 MB × 5) + in-memory ring buffer served at `GET /logs`; dashboard merges all nodes' feeds = **centralized view** | structlog (dep for 15 lines of formatter), ELK/Loki (an ops stack for a three-node cluster) | Rolling files: stdlib. Centralization at this scale = aggregation endpoint + docker compose stdout | High |
| D11 | **PII redaction at the logging layer**: log keys and metadata, never values; enforced by a test | Logging payloads (a compliance liability) | A replicated store whose values may carry personal or health data sits under regimes like GDPR and the US state health-privacy statutes (WA My Health My Data, Nevada SB 370), under which logs themselves become audit surfaces. Redaction therefore belongs at the layer nothing can route around, not in a convention each call site is trusted to follow | High |
| D12 | Docker Compose is the primary runtime; **K8s StatefulSet + kind** included | K8s-only (heavier to stand up locally) | StatefulSet gives stable identities (`node-1.raftkv-hl`) that consensus peers need | High |
| D13 | Default timing: heartbeat 500 ms, election timeout 1.5–3 s (config-overridable; tests use 30/100–200 ms) | Paper's 150–300 ms (invisible to humans) | Paper §5.2 requires randomized timeouts ≫ broadcast time; scaling up preserves the ratio and makes elections observable | High |
| D14 | Writes: client discovers leader via `/state`; non-leader returns **503 + leader_id**; dashboard auto-routes | 307 redirects (Docker-internal hostnames are unreachable from the browser, so the redirect fails in the one environment the dashboard runs in) | Clean-start reliability beats cleverness; production answer (smart client / redirects) documented | Medium |
| D15 | Accelerated `conflictIndex`/`conflictTerm` backtracking (§5.3) | Naive one-index `nextIndex` decrement — what this shipped as, until it was measured | "Slow only on long logs, irrelevant at this scale" turned out to be wrong *at* this scale: `/admin/flood` makes a 6000-entry log in six seconds, and a node reset behind one then needed ~52 min of round trips to rejoin (measured 2026-08-16, live cluster). The hint costs two optional integers and makes that two round trips | High |
| D16 | A **partial index** on the log's configuration entries | Caching the live configuration beside the log; leaving the scan alone | `last_config()` is on the write path by design — membership is a view over the log, which is what makes truncation revert it for free — so its cost is multiplied by concurrency. Unindexed it scanned: 2.475 ms against a 15,000-entry log, so 1000 concurrent writes spent ~2.5 s of event-loop time and the leader was voted out for being busy. The index preserves the design (still read from the log) and removes the cost: 0.0021 ms measured live, and the 2000-concurrent burst went from *every write timing out* to 0.5 s clean | High |
| D17 | **One required status check** (`gate`), computed by a job that inspects every other job's result, rather than requiring the eight jobs individually | Listing each job as a required check in branch protection | GitHub reports a **skipped** job as a *passing* required check, so a required check that can be skipped fails open — and looks identical to passing while doing it. One un-skippable aggregator holds the policy in a reviewed file (which jobs must be green, which may skip and why) instead of in a settings page no diff ever shows. It is also what lets `codeql` and `dependency-review` degrade cleanly on a private repo, where code scanning needs an entitlement this one may not have | High |
| D18 | The expensive gates run on a **cron, not on every pull request**: `clean-start-check` from a cold runner, plus the suite on macOS | Putting both on the pull-request gate; or trusting a remembered "run it the night before" | The no-cache rebuild re-answers a question the cached build already answered, and the real-timer election tests are the most timing-sensitive thing in the suite — a slower runner is exactly where a flaky gate would come from, and a flaky gate teaches people to re-run rather than to read. Nightly costs nobody's attention and means the answer is known by morning, which is what a manual pre-release run was only ever approximating | Medium |
| D19 | The cluster **provisions its own nodes on demand** (`POST /admin/spawn-node`), and nothing above node-3 starts by itself | Shipping two idle nodes on every runtime (what this did before); making the dashboard call out to Docker or Kubernetes | The distinction that matters is *a replica is a process, not a cluster member* — and pre-starting the spare capacity quietly erased it: the staged row was already populated at startup, so the distinction had to be taken on trust. Provisioning on demand makes the two steps two visible steps, removes the cap at five, and costs one endpoint. It is the sharpest surface in the build, so it is bounded (`RAFT_PROVISION_MAX`), carries its own flag rather than riding on `RAFT_ADMIN_ENABLED`, builds a constant argv from one pydantic-bounded integer with no shell anywhere, and refuses inside a container — where the child would share the node's network namespace and advertise an address resolving to each peer's own loopback. Compose therefore keeps its two staged nodes declared but dormant behind a profile (`docker compose up -d raft-node-4` starts one) and Kubernetes keeps `kubectl scale`; `tests/test_provision.py` asserts all four properties | High |
| D20 | **CheckQuorum and PreVote, shipped as one change** (thesis §6.2, §9.6) | Shipping CheckQuorum alone, which is half the code; or leaving both out, as D2a decided | CheckQuorum answers a question the partition scenario invites and had no answer to: a partitioned leader kept saying `role: leader` for the whole partition, because every mechanism that deposes a leader arrives in a *message* and a partitioned one receives none. But CheckQuorum alone is a regression wearing a fix's clothes — resigning turns a quiet stale leader into a candidate, and a candidate that can never reach a quorum burns a term per election timeout, so it rejoins tens of terms ahead and that term alone deposes a leader that was serving fine. D2a's reasoning ("PreVote's absence is worth showing") held only while nothing else made the absence expensive; CheckQuorum does, and the thesis pairs them for exactly this reason. Building both also surfaced a bug neither half has alone — a leader's election timer is stale for its whole tenure by design, so the naive lease let a healthy incumbent grant the straw poll that unseated it; invisible to every stubbed unit test, caught by a live three-node one (table 1c) | High |

## Prior art

Python Raft implementations exist — PySyncObj, raftos, zatt among them. This build is
not trying to compete as a library; it differentiates by being **Figure-2-faithful**
(every persistence and receiver rule traceable to the paper), **asyncio-native**
(synchronous rule methods atomic on one event loop — no locks), **simulation-tested**
(deterministic partition/failover/divergence scenarios over an in-memory transport),
and **operable** (dashboard, debugger script, and clean-start gate are first-class
deliverables, not afterthoughts).

## Security

Observability never leaks payloads: the logging layer refuses to log a KV **value** —
keys and metadata only — and the guarantee is enforced by
`tests/test_logging_setup.py::test_log_event_refuses_pii_value` at the unit level and
`tests/test_api.py::test_logs_endpoint_and_pii_redaction` at the HTTP surface. All
SQL in `storage.py` is parameterized (`?` placeholders throughout — no string-built
queries). The container runs as a non-root user (uid 1000) on `python:3.14-slim`, and
the Kubernetes variant adds a read-only root filesystem with explicit writable mounts.
The static security scans are clean: Snyk Code reports zero issues over
`src/` (an earlier Snyk pass flagged two DOM-XSS paths in the dashboard's
string-templated rendering, fixed by rebuilding it with DOM APIs —
`textContent`/`replaceChildren` — so remote node state is never interpreted as HTML),
and a bandit cross-check reports zero medium/high findings — its four
low-severity notes are the two stdlib `random` calls that implement Raft's randomized
election timeouts (deliberately non-cryptographic, per paper §5.2) and two internal
asserts.
The one endpoint that runs a program, `POST /admin/spawn-node`, is built so that the
claim "no caller-supplied string reaches a command line" is checkable rather than
asserted: the argv is a constant assembled from `sys.executable` and a single integer
that pydantic has already bounded to 4–99, dispatched through
`asyncio.create_subprocess_exec` (argv) rather than `create_subprocess_shell` (a string),
with no shell and no interpolation anywhere in the path. It is bounded by
`RAFT_PROVISION_MAX`, carries its own `RAFT_PROVISION_ENABLED` flag so a deployment can
keep the failure controls without accepting process spawning, and refuses outright inside
a container. `tests/test_provision.py` asserts each of those rather than trusting the
comment above them.
Dependencies are pinned to exact versions verified on PyPI (2026-08-13) and locked via
`uv.lock`; the container image installs from `requirements.lock` (a hash-checked
export of the same resolution), so transitive versions cannot drift at image build
time either. CI gates every push on ruff lint, the full test suite, and the compose
smoke test.
