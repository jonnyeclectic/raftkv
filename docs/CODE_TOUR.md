# Code tour

Where to start reading, which paths carry the argument, and where to put a breakpoint.

This is the navigation document. The others answer different questions, and this one
deliberately does not repeat them: [ARCHITECTURE.md](ARCHITECTURE.md) is the
module-by-module reference with diagrams, [RAFT.md](RAFT.md) is the algorithm in plain
language plus the seven correctness rules, and [FAILURE_MODES.md](FAILURE_MODES.md) is
where it breaks. What follows is the map between them: entrypoints, call paths, and the
debugger.

Citations here name a symbol, never a line number — `raft.py::submit` rather than
`raft.py:625`. That is not a style preference. Line numbers in prose rot on the first
edit to the file they point at, silently, and `tests/test_docs_matrix.py` can check a
symbol against the tree the same way it already checks a cited test name.

## 1. The shape of it

One process = one FastAPI app = one `RaftNode` = one SQLite file. Three of them agree on
an ordered list of commands, and each applies that list in order, so all three end up
holding the same key-value state. Everything here is either that list, a way to agree on
it, or a way to watch it happen.

The structural fact worth knowing before anything else: **the dependency direction is
strict.** `raft.py` does not know HTTP exists — it talks to a `storage.py::Storage` and a
`transport.py::Transport`, the latter a `Protocol`. `app.py::create_app` is the only
place a real one of each is wired into a node. That is what lets the failure suite run
real elections in-process over `transport.py::MemoryTransport`, with no network and no
sleeps.

Reading order for someone seeing this for the first time:

| Order | File | Why here |
|---|---|---|
| 1 | `models.py` | The vocabulary. `models.py::LogEntry`, `models.py::Command`, `models.py::ClusterConfig`, `models.py::NoOp` — every later file is written in these terms |
| 2 | `docs/RAFT.md` | The algorithm and the seven rules, before meeting them as code |
| 3 | `raft.py` | The deliverable. Its module docstring names the seven rules as the review checklist |
| 4 | `storage.py` | Short, and it is where "persistence before reply" stops being a slogan |
| 5 | `app.py` | The wiring, and every surface the dashboard drives |

## 2. Entrypoints

Three ways in, for three different purposes.

| Entrypoint | Started by | Use |
|---|---|---|
| `debug_node.py::main` | an IDE run configuration | **The debugger path.** One node under the debugger, breakpoints one click away throughout |
| `app.py::create_app` | `uvicorn --factory` | How compose and k8s start a node. Same app object, no debugger |
| `tests/conftest.py` | the `cluster` fixture | N nodes in one process over `MemoryTransport`. Where failures are actually reasoned about |

`scripts/debug_node.py` carries the pasteable run configuration in its module docstring,
and that docstring is *input* rather than documentation — it is what gets pasted into the
IDE dialog, so two of its strings are pinned by
`tests/test_debug_entrypoint.py::test_the_pasteable_config_block_names_no_ambiguous_address`
and `tests/test_debug_entrypoint.py::test_the_pasteable_config_block_sets_an_advertise_address`.

**Node-1 runs default timings.** The asymmetry that makes it lead deterministically lives
in `scripts/run_local.sh`, which starts nodes 2 and 3 at `RAFT_ELECTION_MIN=4
RAFT_ELECTION_MAX=6`; node-1's 1.5–3 s timeout always fires first. Do not stretch node-1
instead — under that inversion its timer never fires first, so it cannot win an election,
and a heartbeat longer than the peers' timeout costs it any leadership it is handed.
The header comment in `scripts/run_local.sh` has the full reasoning.

### What happens on boot

| Step | Where | What it establishes |
|---|---|---|
| 1 | `config.py::from_env` | Parses `RAFT_*`. `config.py::_sane_timing` validates the ratios here, so a cluster that would churn elections refuses to boot instead |
| 2 | `app.py::create_app` → `storage.py::Storage` | SQLite open, WAL, `synchronous=FULL`, schema applied — including the partial index in `storage.py::_SCHEMA` |
| 3 | `raft.py::RaftNode` constructor | Term, vote and `last_applied` read off disk. Role starts FOLLOWER always; `commit_index` starts at `last_applied`, since everything applied was committed |
| 4 | `raft.py::_reload_config` | Membership derived from the newest configuration entry in the log, falling back to `raft.py::_bootstrap_config` only while the log holds none |
| 5 | `app.py::lifespan` → `raft.py::start` | The three loops begin: `raft.py::_election_timer_loop`, `raft.py::_replication_loop`, `raft.py::_apply_loop`. Each gets `raft.py::_on_task_death`, because a silently dead loop is the worst state a consensus member can be in |

## 3. Five paths through the code

Nearly every question about this system is one of these five. Trace them once in order
and `raft.py` stops being a wall of text.

### A write

| Step | Where | Note |
|---|---|---|
| 1 | `app.py::put_key` | Wraps the body in a `Command` with a fresh `request_id` |
| 2 | `raft.py::submit` | Leader check, append, **then register the waiter before re-checking commitment** — otherwise a single-node cluster applies the entry before anyone is listening |
| 3 | `raft.py::_advance_commit_index` | Nothing commits yet on a multi-node cluster; nobody has acked |
| 4 | `raft.py::_replication_loop` | `_replicate_now` is set, so the loop cuts its heartbeat wait short |
| 5 | `raft.py::_append_to_peer` | At most `models.py::MAX_ENTRIES_PER_APPEND` entries per peer. `match_index` comes from the arguments *sent*, never from `storage.py::last_log_index` |
| 6 | `raft.py::_advance_commit_index` | A majority has it now — commit, but only for a current-term entry (§5.4.2, Figure 8) |
| 7 | `raft.py::_apply_loop` | The single applier, strictly in order. `storage.py::apply` writes the KV row and `last_applied` in one transaction, then the waiter resolves |
| 8 | `raft.py::submit` returns | Checks the applied entry's `request_id`. If another leader filled that index, raise `NotLeaderError` rather than lie to the client |

### An election

| Step | Where | Note |
|---|---|---|
| 1 | `raft.py::_election_timer_loop` | Skips if not a voter *according to the log*, skips if leader, else checks the randomized deadline |
| 2 | `raft.py::_start_election` | Candidate, term++, self-vote, **persisted before anything is sent**, timer reset |
| 3 | `transport.py::request_vote` | Fan-out. The inner helper returns *who* answered, not just the answer — joint agreement is decided by which rosters the granters belong to, so an anonymous tally cannot work |
| 4 | `raft.py::handle_request_vote` | On each peer: observe term, check voter status, the §5.4.1 up-to-date comparison, one-vote-per-term, persist, reply |
| 5 | the stale-reply guard in `raft.py::_start_election` | If role or term moved while awaiting, stop |
| 6 | `raft.py::_become_leader` | Re-derive config, append a `NoOp` from this term, seed `next_index` / `match_index`, heartbeat immediately |

`raft.py::campaign` is the same path on demand — `POST /admin/campaign`, via
`app.py::admin_campaign`. It is a deliberately minimal stand-in for leadership transfer
(thesis §3.10): no TimeoutNow RPC, just an operator telling a chosen node to stop waiting
for its timer. Nothing that makes an election safe is bypassed, so the worst a press can do is
burn a term and lose.

### Repair after divergence

| Step | Where | Note |
|---|---|---|
| 1 | the consistency check in `raft.py::handle_append_entries` | Does `storage.py::term_at` at `prev_log_index` match what the leader claims? |
| 2 | `raft.py::_consistency_failure` | Reject, and say *where to resume*: the first missing index, or the disputed term and where our run of it starts |
| 3 | `raft.py::_next_index_after_rejection` | Jump — then clamp. The jump is speed; **the clamp is termination** |
| 4 | conflict-only truncation in `raft.py::handle_append_entries` | Never chop entries that already match, or a delayed reordered append un-commits applied state |
| 5 | the re-fire in `raft.py::_append_to_peer` | Still behind after a capped batch? Go again now, not at the next heartbeat |

Both halves are measured rather than argued: `tests/test_log_repair.py` and
`tests/test_gap_repair.py` pin the hint, `tests/test_append_batching.py` fails if either
the cap or the re-fire goes missing.

### Membership

| Step | Where | Note |
|---|---|---|
| 1 | `app.py::admin_spawn_node` → `provision.py::spawn` | Starts a *process*. Not a Raft operation, and deliberately not leader-only — what comes back is in nobody's configuration |
| 2 | `raft.py::add_learner` | A configuration entry putting it in `learners`. No joint consensus, precisely because quorum does not move |
| 3 | `raft.py::_reject_unless_ready_to_reconfigure` | Four preconditions, each a real failure if skipped. This is where a 409 comes from |
| 4 | the catch-up gate in `raft.py::promote_learner` | A learner behind the committed prefix is refused, and the error says by how much — which turns "denied" into "wait" |
| 5 | `raft.py::promote_learner` | Appends C-old,new. From the moment it is **appended** — not committed — `raft.py::_has_agreement` requires separate majorities of both sets |
| 6 | `raft.py::_maybe_leave_joint` | Once the joint entry commits, append C-new. That commit is the only trigger |

### Failure injection

| Step | Where | Note |
|---|---|---|
| — | `raft.py::crash` | Stops the loops and drops volatile state, but keeps the process so `/admin/recover` stays reachable. Durable state untouched, so `raft.py::recover` *proves* persistence rather than asserting it |
| — | `raft.py::set_blocked` + `app.py::refuse_if_partitioned` | A partition cut in **both** directions. A one-way cut is a delay, not a partition |
| — | `raft.py::reset` | Destroys durable state. Membership survives as a **term-0** entry, so any real leader's index 1 conflicts with it and the ordinary repair walk removes it |
| — | `app.py::admin_timing` | Live timing changes, validated as a whole `NodeConfig` before any field moves, so the §5.2 ratios hold mid-flight. Runtime-only; a restart restores the env-derived config |

## 4. The pieces that carry the argument

Nine mechanisms. If you hold these, the rest of the file is bookkeeping.

1. **The log is the system** (`models.py::LogEntry`). Consensus is agreement on an
   *order*, not on a value. 1-indexed with an implicit term-0 sentinel at index 0, so
   `prev_log_index=0` needs no special case: `storage.py::term_at` returns 0 there and
   `None` for a missing index, and that difference is load-bearing in the consistency
   check. Configuration entries share the log with client commands so ordering is total.
2. **Terms are the clock** (`raft.py::_observe_term`). A higher term in any request *or
   response* deposes the node, before the handler's own logic runs. This is why the
   dashboard can honestly show two nodes labelled LEADER: Raft guarantees one leader per
   *term*, not one leader, and the stale one commits nothing.
3. **Persistence before reply** (`storage.py::save_term_and_vote`). Fig. 2, §5.2. A node
   that votes, crashes and forgets can vote twice in one term. `storage.py::apply` writes
   the KV row and `last_applied` in one transaction, which is what makes apply
   exactly-once across restarts. `raft.py::reset` exists to demonstrate the inverse.
4. **The election restriction** (`raft.py::handle_request_vote`). Vote only for a
   candidate at least as up to date, comparing `(last_log_term, last_log_index)`
   lexicographically — term first. Any two majorities overlap, so a winner has convinced
   someone who saw every committed entry. One comparison, and it is the whole safety
   argument.
5. **Commit counts only current-term entries** (`raft.py::_advance_commit_index`).
   Figure 8. Older entries commit transitively. `raft.py::_commit_candidates` is the
   performance-critical half: `acked` is a step function of the candidate index, so only
   distinct `match_index` values plus `last_log_index` are worth testing.
6. **Membership is a view over the log** (`raft.py::_reload_config`, `storage.py::last_config`).
   Never a cache beside it, which is what makes a truncated configuration entry revert
   for free. Read `config.voters`, never `cfg.peers`. The price is a query on the write
   path — see §6 below.
7. **Joint consensus, one voter at a time** (`raft.py::_has_agreement`). Separate
   majorities of both sets, never a majority of the union. Adding to an *n*-server
   cluster gives `n + 2` over a union of `n + 1`; adding two at once gives 0 and the
   guarantee disappears. An empty voter set is vacuously a majority in
   `raft.py::_is_majority`, which is what lets the non-joint case share the code path.
8. **The stale-reply guards** (in `raft.py::_start_election`, `raft.py::_append_to_peer`,
   `raft.py::submit`). Any async method resuming after an `await` re-validates role and
   term before acting. `MemoryTransport` answers in the same event-loop turn, so these
   were executed constantly and exercised barely until `tests/chaos.py` started delaying
   *responses* — the delay is the point, not the drops.
9. **A dead loop must reach the health surface** (`raft.py::degraded`, `app.py::healthz`).
   `_apply_loop` dies on one storage error while `_replication_loop` survives, because it
   only reads — so the node keeps heartbeating, no follower can elect around it, and
   every write 504s forever. Pinned by
   `tests/test_api.py::test_healthz_reports_503_once_a_background_loop_has_died`.

## 5. Breakpoints

node-1 is already the node running under the debugger, so none of this needs setup.
Serve the dashboard from **node-2** — node-1's own page stops updating the moment you sit
on a frame.

A pause under ~4 s is free, because nodes 2/3 campaign only after 4–6 s of silence. Past
that, **the debugger is the failure injection** — worth doing on purpose rather than by
accident.

### Leader paths — node-1 must be leading

| Breakpoint | Shows | Watch |
|---|---|---|
| `raft.py::handle_request_vote` | §5.4.1 and Fig. 2 durability in one frame | `req.term`, `self.current_term`, `self.voted_for`, `self._last_log_position()`, `self.is_voter` |
| `raft.py::_advance_commit_index` | The quorum count stepping as a write commits. **Leader-only**, which is why the precondition matters | `self._commit_candidates()`, `candidate_index`, `self.match_index`, `acked`, `self.config.voters`, `self.quorum` |
| `raft.py::submit` | The whole client lifecycle from one stack: append, register waiter, nudge, await | `cmd.request_id`, `self._waiters`, `self.commit_index`, `self.last_applied` |

### Follower paths — hand leadership away first

Swap the roles from a terminal rather than waiting for a timeout, using the two tempo
controls (`app.py::admin_campaign` and `app.py::admin_timing`):

```bash
curl -sX POST 127.0.0.1:8002/admin/campaign      # node-2 takes the term, immediately
curl -sX POST 127.0.0.1:8001/admin/timing -H 'content-type: application/json' \
     -d '{"election_timeout_min": 60, "election_timeout_max": 120}'      # park node-1
```

Node-2's heartbeats now hit node-1's follower breakpoints twice a second, and the parked
timeout means a resumed node-1 does not campaign over a pause it slept through. Swapping
back is symmetric and works *while parked*, because `raft.py::campaign` does not wait for
a timer.

| Breakpoint | Shows | Watch |
|---|---|---|
| the consistency check in `raft.py::handle_append_entries` | Rule 5. Most valuable while a returning follower is repaired: rejection, hint, jump | `req.prev_log_index`, `req.prev_log_term`, `self.storage.term_at(req.prev_log_index)`, `req.leader_commit` |
| `raft.py::_next_index_after_rejection` | The §5.3 hint being computed and then clamped — the answer to "what if a peer lies?" | `resp.conflict_index`, `resp.conflict_term`, `proposed` before the clamp |
| `storage.py::save_term_and_vote` | Persistence before reply. Here the **call stack** is the interesting pane, not the variables: it names which of the three durability sites you are in | caller: `_observe_term`, `handle_request_vote`, or `_start_election` |

### Holding the joint configuration still

C-old,new is too fast to see on the dashboard — it commits in under 5 ms and the page
polls at 500 ms. The debugger can hold it anyway.

Break in `raft.py::_maybe_leave_joint` on the line that builds the final configuration,
*before* `raft.py::_leader_append` runs, then press **promote to voter**. The node stops
inside the joint configuration with both halves populated:

```
self.config.joint        -> True
self.config.old_voters   -> the three original voters
self.config.voters       -> those three plus the promoted node
self.quorum              -> majority of C-new
```

Note what the dashboard reports meanwhile: node-1 unreachable, because it is stopped.
The joint state is real and it is in the watch window, not on the page.

### Breakpoints to avoid on a running cluster

`raft.py::_replication_loop` and `raft.py::_append_to_peer` fire every heartbeat to every
peer; you will spend the session clicking Resume. Make them conditional
(`peer_id == "node-4"`) or leave them alone. `raft.py::_apply_loop` stops the single
applier, so everything behind it queues and every in-flight write walks toward its 504 —
genuinely interesting as the `degraded` story from the inside, but do it on purpose.

## 6. Where the reasoning is written down

Four things in this build were measured rather than guessed, and each answers a different
question about how it behaves under stress. The full workings are in
[FAILURE_MODES.md](FAILURE_MODES.md); what follows is where to look.

- **The throughput ceiling, and how it was found.** Two plausible
  fixes came first — an O(N²) commit walk and an unbounded replication batch — and
  neither moved it. What broke it open was measuring the leader's *silence* directly:
  1733 ms against a 1500 ms election floor. The cause was an unindexed `last_config()` on
  the write path, put there by mechanism 6 above. `tests/test_config_lookup.py` and
  `tests/test_commit_scan.py`.
- **What the adversarial review caught.** Table 1c — four bugs it found after
  the feature looked finished, including a `reset()` that reverted membership to the
  startup voter set and could therefore acknowledge a write and lose it.
  `tests/test_reset.py::test_reset_cannot_shrink_a_grown_cluster_into_a_second_quorum`.
- **Where it fails.** Table 2 — every omission named with what breaks at scale and
  the production fix. The sharpest remaining one is linearizable reads: CheckQuorum now
  bounds how long a partitioned leader can claim the role, but bounded is not linearizable,
  and a leader holding quorum still has no proof *at the moment of the read* that it was
  not deposed a millisecond ago. ReadIndex is the fix; CheckQuorum was its precondition.
- **Why CheckQuorum shipped with PreVote.** Alone it is not a fix — it converts a silent
  stale leader into one that burns a term per election timeout
  and disrupts a healthy leader on heal. PreVote shipped in the same change for that
  reason (thesis §9.6), and the pair is why a partitioned node now returns at the term it
  left with. `tests/test_check_quorum.py`, `tests/test_pre_vote.py`.
