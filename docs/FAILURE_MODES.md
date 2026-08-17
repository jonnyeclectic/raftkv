# Failure modes

The core question — *where does it fail, and how is that mitigated?* — answered
as two tables: the failures this build handles, and the failures it deliberately does
not (each with what breaks at scale and the production fix).

## Table 1 — handled failures

| Failure | Behavior | Why it's safe | Reproduce it |
|---|---|---|---|
| Leader crash | New election within one timeout; writes resume | Election restriction (§5.4.1) means only an up-to-date node wins | `docker compose stop <leader>` while watching the dashboard |
| Follower crash | No client impact (quorum 2/3 holds) | Majority commit rule | `docker compose stop <follower>`, keep writing |
| Minority-partitioned leader | Accepts but never commits; deposed on heal; uncommitted suffix truncated | Commit needs quorum (§5.4.2); higher term wins (§5.1) | `tests/test_simulation.py::test_partitioned_leader_cannot_commit_then_steps_down`; live: **cut links** on the leader's card, write a key, watch it never commit |
| Split vote | Re-election with new randomized timeouts | §5.2 randomization makes repeat collisions improbable | watch the `elections_started` metric during churn |
| Node restart | Rejoins with term/vote/log intact; missed entries backfilled | Fig. 2 persistence before reply; log consistency check repairs (§5.3) | `docker compose restart <node>`; `tests/test_simulation.py::test_term_vote_and_data_survive_restart` |
| Log divergence after failover | Conflicting suffix truncated, leader's log replayed | Conflict-only truncation (§5.3) | asserted in the partition test above (the doomed entry is replaced in place); unit: `tests/test_append_entries.py::test_conflict_truncates_suffix` |
| Slow/lossy network | Elections re-fire; replication retries every heartbeat | Timeout ratios (heartbeat << election) per §5.2 | throttle with `docker network` tooling |
| Client retries a timed-out write | Duplicate possible (at-least-once) | request_id detects index reuse but NOT cross-index duplicates — see omissions | §8: exactly-once needs client sessions |

## Table 1b — previously omitted, now implemented

| Was missing | What it does now |
|---|---|
| Cluster membership change (§6) | Joint consensus. A learner is promoted to a voting member through a C-old,new entry that requires separate majorities of both configurations, then a C-new entry once that commits. One voter at a time. See [ARCHITECTURE.md](ARCHITECTURE.md) and `tests/test_joint_consensus.py` |
| Leader no-op on election win (thesis §6.4) | Every new leader appends a no-op from its own term. It flushes entries a previous leader committed but never applied, and committing it is the precondition for any membership change |
| Catch-up before promotion (thesis §4.2.1) | `promote_learner()` refuses while the learner is behind the committed prefix, and the 409 reports the gap. A learner's lag is free because nobody counts it; the instant it becomes a voter that lag is subtracted from the cluster's fault tolerance |

## Table 1c — bugs an adversarial review found, and what fixed them

Four of these were found by pointing agents at the implementation with instructions to
produce two leaders or a lost commit, rather than to check that it worked. Three more
findings were investigated and refuted. The last row was found by running the load
generator and reading its own output sceptically. Each row is now a regression test.

| Bug | Why it was dangerous | Fix |
|---|---|---|
| `reset()` reverted membership to the **startup** voter set | After growth to five, node-1/2/3 boot naming only each other, so a reset left them a 3-voter configuration with quorum 2. Two resets on the minority side of a partition elected a second leader, committed a write, **acknowledged it to the client**, and lost it on repair. A 2-of-5 partition is the exact failure five voters exist to survive | Membership is re-seeded as a term-0 log entry, so it survives the wipe but loses to any real leader's index 1. `membership=bootstrap` opts into the revert and is only sound applied to every node at once. `tests/test_reset.py::test_reset_cannot_shrink_a_grown_cluster_into_a_second_quorum` |
| A promoted-then-reset voter demoted itself to a learner | Same root cause: it silently withheld votes while still being counted, deadlocking elections | Same fix; `is_voter` was already derived from the log rather than the startup flag |
| An undialable peer raised `KeyError`, not `TransportError` | It escaped every `except TransportError` and **killed the election timer permanently**. The node sat as a follower forever, campaigning never again, with nothing logged | Both transports raise `TransportError` for an unknown or address-less peer. `RAFT_ADVERTISE` is now set on bootstrap nodes too, since an empty one is what put an undialable voter in the configuration. `tests/test_membership_growth.py::test_an_undialable_voter_does_not_kill_the_election_loop` |
| `/admin/flood` raised `NameError` on every call | `FloodRunner` was imported but never constructed. The feature had no test, so nothing said so | Constructed in `create_app()` and cancelled on shutdown. `tests/test_flood_endpoint.py` |
| One failed write abandoned the whole flood, and the panel called it green | The generator's per-write `try` caught only `NotLeaderError` and `TimeoutError`, and `asyncio.gather` propagates the first exception while abandoning every task queued behind it. So one unexpected error — a SQLite `database is locked` under contention being the realistic one — stranded `done` below `total` permanently. Worse, the dashboard tinted by `timeout \|\| not_leader`, both of which were zero, so a burst that never ran rendered as a clean green sweep. A load generator that silently under-reports load is worse than no load generator | Every write settles into a counter and the worker raises nothing but `CancelledError`; unexpected errors land in a new `failed` count with `last_error`, `gather` takes `return_exceptions=True` as a second line of defence, and the panel tints `failed` **red** (not the amber that means "Raft under strain") and calls a short burst `INCOMPLETE`. `tests/test_flood_endpoint.py`, `tests/test_dashboard_flood.py` |

Refuted after investigation, and left alone: a learner reporting `quorum 1` with an
empty voter set (the value is never read), a leader not stepping down on an equal-term
AppendEntries (unreachable without a double vote, which durable voting prevents), and a
stale `match_index` surviving `_become_leader` (requires truncating below an index a
peer already acked).

## Two leaders on the dashboard

Cut the leader's links and the panel shows two nodes labelled LEADER at once. That is
not a safety violation and not a bug: Raft guarantees **one leader per term**, not one
leader. The isolated node still leads term *N* and the new one leads term *N+1*, and
both report honestly — the stale leader cannot learn it was deposed until a message
reaches it. It commits nothing in the meantime (quorum is unreachable), and `_observe_term`
demotes it on the first RPC after heal.

What *was* a bug is how the dashboard chose between them. `states` is keyed by address,
and the old code took the first entry self-reporting `role === "leader"` — so the pick
was made by node number, and a stale leader won whenever it sorted first. It then drove
three things at once:

- the **header chip**, naming the wrong leader;
- the **topology edges**, which are drawn leader→peer, so the graph showed a dead
  leader's severed links while the real leader's live ones went unrendered;
- **`leaderAddr()`**, which is where *set on leader* and *add learner* post — landing
  those writes on the one node in the cluster guaranteed to time out uncommitted.

`leaderEntry()` now picks the **highest term**, and all three read through it.
`quorumReach()` replaced the `writes` chip's headcount: in a clean partition every node
is up, so `nodes up >= quorum` called an isolated leader "accepting". Reachability counts
the leader plus peers on an uncut link, in **both** directions — `set_blocked` skips
outbound RPCs in `raft.py` and `refuse_if_partitioned` drops inbound ones in `app.py`, so
either side naming the other severs the link. Pinned by `tests/test_dashboard_leader.py`.

The topology edge set is leader-centred by design (`followers never talk to each other,
so a full mesh would be a lie`), so it re-centres on the new leader when leadership moves.
An edge appearing between two nodes after a heal is that re-centring, not a new network
path — the link was always there; it was only ever drawn from whichever node was leading.

## Table 2 — deliberate omissions

Each was cut on purpose, with eyes open. "§" cites the Raft paper; "thesis" is Ongaro's
dissertation.

| Omission | What breaks without it at scale | The production fix |
|---|---|---|
| Log compaction / snapshots (§7) | The log grows without bound: disk fills, restart replay time grows linearly forever | Snapshot the state machine at an index, discard the log prefix, ship snapshots to lagging peers via the InstallSnapshot RPC |
| PreVote (thesis §9.6) | A node rejoining from a partition carries an inflated term and forces one needless election, disrupting a healthy leader — our partition demo shows exactly this disruption | PreVote round: ask "would you vote for me?" without incrementing the term; only real candidates disturb the cluster |
| Linearizable reads (§8) | Follower reads can be stale; even leader reads can be stale across a partition (a deposed leader serving reads it thinks it still owns) | ReadIndex protocol or leader leases; or route reads through the log |
| Client sessions for exactly-once (§8) | A client that retries a timed-out write can apply it twice (at-least-once today; `request_id` only detects a log index reused by a different leader, not cross-index duplicates) | Client sessions with per-client serial numbers; the state machine deduplicates before applying |
| Accelerated nextIndex backoff (Students' Guide) | Catch-up of a far-behind follower takes one RPC round-trip per missing entry — minutes on long logs | Follower returns conflictIndex/conflictTerm; leader jumps `nextIndex` whole terms at a time |
| TLS/mTLS between nodes | RPCs cross the network in plaintext: any on-path attacker can read or forge votes and entries; medical-grade deployments would require encryption in transit | mTLS between peers (SPIFFE/cert-manager identities), TLS on the client API |
| Batching and flow control on the write path | A large burst of concurrent writes degrades super-linearly and can starve the heartbeat — measured below | Batch client commands into one append and one fsync; cap in-flight AppendEntries per follower; apply backpressure at the API rather than accepting unbounded concurrent submits |

## The throughput ceiling, measured

`/admin/flood` exists to make this reproducible rather than theoretical. Driven from the
dashboard against the real three-node cluster over HTTP, on an otherwise idle machine:

| Writes, all at once | Wall clock | Outcome |
|---|---|---|
| 200 | 0.1 s | all committed, 2381/s |
| 500 | 0.5 s | all committed, 1111/s |
| 1000 | 1.7 s | all committed, 572/s |
| 2000 | 10.9 s | **all 2000 commit-timeout**, one election, 0/s |

Throughput *falls* as the burst grows — 2381/s at 200, 572/s at 1000 — which is the
super-linear cost showing up before the cliff does. Between 1000 and 2000 it goes over.

Quote these numbers, not the ones an earlier revision of this file carried. Those were
measured in-process over `MemoryTransport` while two other jobs saturated the CPU, and
they put the cliff at 500 — five times pessimistic. The mechanism was right; the numbers
described the machine, not the system.

The mechanism is not the network and not fsync — it is `_advance_commit_index()`. It scans
the log downward from `last_log_index` to `commit_index`, one SQLite `term_at()` query per
index, and it runs once per `submit()`. While a burst is uncommitted that scan is
O(pending), so a burst of N costs O(N²) queries: instrumented, 200 concurrent writes issue
**20,315** `term_at()` calls against 203 appends. Those queries are synchronous, on the
same event loop that owes every follower a heartbeat, so past some burst size the
heartbeat is late, the followers elect around a leader that is merely busy, and every
in-flight write fails at once.

Two things follow that are worth saying out loud. The cliff is **load-dependent, not a
fixed number**: 500 concurrent writes commit cleanly in 0.5 s on an idle machine and time
out entirely when the CPU is contended — same code, same cluster. That is why
`tests/test_flood.py` asserts invariants and never latency or a success count, and it is
why the table above names the conditions it was measured under.

And the failure is **safe**. Measured immediately after the 2000-write burst timed out
every single client:

```
node-1 follower  term 2  log 2702  commit 2702  applied 2702  1000 keys
node-2 follower  term 2  log 2702  commit 2702  applied 2702  1000 keys
node-3 LEADER    term 2  log 2702  commit 2702  applied 2702  1000 keys   -> all agree
```

Nothing lost, nothing misordered, no node left behind: the writes were *refused*. The term
moved 1 → 2 because the starved heartbeat did cost the leader its leadership, which is the
mechanism above showing up as an election rather than as corruption. A surviving value for
a contended key is still the one the log ordered last. Degradation under overload is a
liveness problem here, not a safety one — and that distinction is the whole point of
having the flood.

One nuance the counters hide: a client `timeout` does **not** mean the entry never
committed. Those 2000 writes were appended and the survivors committed after the client
had already given up at `commit_timeout`. That is the at-least-once story in the omissions
table above, seen live rather than argued.

## When a learner can safely be promoted

"It has caught up" is necessary but not sufficient, and it is not the interesting half.
The hazard is not the learner's log — it is the **quorum change itself**.

**1. Change one server at a time.** Membership changes are unsafe in general because
C_old and C_new can have *disjoint* majorities during the transition, electing two
leaders in the same term (§6, Figure 10). Going 3 → 5: a majority of C_old is 2 and a
majority of C_new is 3, and 2 + 3 = 5 = |C_new|, so `{A,B}` and `{C,D,E}` can each
elect. Adding a **single** server makes that impossible: 3 → 4 gives majorities of 2
and 3, and 2 + 3 = 5 > 4 = |C_new|, so by pigeonhole every old majority intersects
every new one. This is why single-server changes need no joint consensus.

**2. Catch up first, measured in rounds — not bytes.** Promoting a lagging node raises
the quorum requirement while contributing nothing to it, so availability *drops* at the
moment you add capacity. Ongaro's rule (thesis §4.2.1): replicate to the learner for a
fixed number of rounds and promote only if the final round completed within one
election timeout. That is the property actually needed — the new voter must be able to
answer inside an election cycle — and it is what `matchIndex`-based thresholds
approximate.

**3. The new leader must commit an entry from its own term first.** A leader may hold
an uncommitted configuration entry from a previous leader without knowing it. Committing
a no-op on election win resolves the ambiguity before any config change is started.

**4. One change at a time.** Do not begin a new configuration change until the previous
one has committed, or the configurations overlap.

**5. Configuration applies on *append*, not on commit.** A server uses the latest config
in its log regardless of commitment, because that config may be needed for the very
election that commits it. The consequence is that truncating a config entry reverts the
configuration too.

### How this repo implements it

All five points above are implemented; each maps to a named guard.

1. **Configuration is a log entry.** `ClusterConfig` is a variant of `LogEntry.command`,
   so membership replicates, survives failover, and reverts if truncated.
   `_reload_config()` derives the live configuration from the log after *every* log
   mutation — it is a view, never a cache kept beside it.
2. **A no-op on election win.** `_become_leader()` appends one from its own term, and
   `_reject_unless_ready_to_reconfigure()` gates every membership change on committing
   it (point 3 above).
3. **A catch-up gate** (`promote_learner`, point 2 above) and a **one-at-a-time guard**
   (the symmetric-difference check, point 1 above).

The catch-up gate holds the learner to the **committed prefix** rather than to the
paper's round-timing heuristic. That is the property which actually bounds the stall —
what remains after it is the uncommitted tail, itself bounded by in-flight writes — and
unlike a timing measurement it is exact and reproducible, which matters more for behaviour
that has to be the same on a loaded laptop as on an idle one.

Attaching a learner still needs none of this, and that remains the point: quorum never
moves, so there is no transition to get wrong.
