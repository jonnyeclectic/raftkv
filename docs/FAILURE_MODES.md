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
| An index on the configuration lookup | `last_config()` sits on the write path — membership is a view over the log, so `_reload_config()` runs after every log mutation — and it was an unindexed scan. At 15,000 entries that is 2.475 ms per append, so 1000 concurrent writes spent ~2.5 s of event-loop time and the leader was voted out for being busy. A partial index over just the configuration entries makes it 0.0021 ms and changes no semantics. See "The throughput ceiling" below and `tests/test_config_lookup.py` |
| CheckQuorum (thesis §6.2) | A leader that has not heard back from a majority within one `election_timeout_max` resigns. Raft deposes a leader by *message*, and a partitioned one receives none — so its own silence is the only signal available to it. Contact is any RPC response (a rejection still proves a reachable peer), a joint transition needs a majority of **both** halves, and a single-voter cluster never resigns. `tests/test_check_quorum.py` |
| PreVote (thesis §9.6) | A node polls "would you vote for me at term+1?" before running, incrementing nothing and persisting nothing, and only becomes a candidate if a majority says yes. A node inside one `election_timeout_min` of its leader refuses to help depose it, and a leader refuses for as long as it leads. `tests/test_pre_vote.py` |
| Accelerated log backtracking (§5.3) | On a consistency-check rejection the follower reports where to resume — the first index it is missing, or the term it disagrees on and where its run of that term starts — and the leader jumps instead of stepping. See "Repairing a far-behind follower" below and `tests/test_log_repair.py` |
| A bound on the `AppendEntries` payload | `MAX_ENTRIES_PER_APPEND = 512`. The batch was every outstanding entry, to every follower, every round: at 2000 pending that is ~195 KiB per peer and ~33 ms of event-loop time per round across four followers, re-read out of SQLite and re-serialised until the burst drained. The cap ships with the rule that pays for it — a successful append that leaves a follower behind re-fires replication immediately, so catch-up costs `ceil(behind / 512)` *rounds* and not that many heartbeats. `tests/test_append_batching.py` fails if either half is removed |

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
| A node whose background loop died stayed leader and kept reporting healthy | `_apply_loop` dies on any storage error — a full disk is one `sqlite3.OperationalError` out of `storage.apply()` — while `_replication_loop` survives, because it only *reads*. So the node kept heartbeating, which means no follower could elect around it, with `commit_index` advancing past a `last_applied` that would never move again and every client write 504ing at `commit_timeout`. Permanently. Nothing recovered from it unaided: the cluster cannot depose a peer that is still sending AppendEntries, `/healthz` answered 200 so k8s never restarted the pod and the Service kept routing to it, and the only trace was a single `logger.error` that had already scrolled past. No adversary, no partition, no scale — one disk hiccup. Found by two independent audits of this repo, which is itself the point: it is invisible from inside the algorithm, because the algorithm is behaving correctly | `RaftNode.degraded` names the dead loop and `/healthz` answers 503 while it is set, so the node fails its probe instead of silently wedging the cluster. `k8s/raftkv.yaml` gains a `livenessProbe` — readiness alone only removes it from the Service, and this state needs a *restart* — set deliberately slacker than readiness, because taking a node out of rotation is cheap and killing it costs an election. Cancellation is explicitly not degraded, since `stop()` cancels every loop. `tests/test_api.py::test_healthz_reports_503_once_a_background_loop_has_died` |
| A healthy leader granted the pre-votes that unseated it | Found live while building PreVote, and invisible to every unit test that stubs the peers. The lease that stops a follower disturbing a working leader was written as "I know a leader AND have heard from it recently" — but a leader's own election timer is stale for its entire tenure *by design* (`_become_follower` resets it precisely because it has been), so the elapsed-time test read a perfectly healthy incumbent as one nobody had heard from. It therefore granted the straw poll of the first follower whose timer drifted, that follower won its poll on the incumbent's own vote, and the real election deposed it. A mechanism whose entire purpose is preventing needless elections was causing them — and only in a three-node cluster over real timers, which is why it took a live test to see | A leader holds the lease unconditionally: `_check_quorum` is the sole authority on when a leadership ends. `tests/test_pre_vote.py::test_a_leader_refuses_every_poll_for_as_long_as_it_leads` |
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
| Linearizable reads (§8) | Follower reads can be stale, and so can leader reads. CheckQuorum now bounds the *duration* — a leader that reaches no majority resigns within one election timeout instead of serving reads it no longer owns for the length of the partition — but bounded is not linearizable. Inside that window a partitioned leader still answers from a state machine the cluster has moved past, and a leader that legitimately holds quorum still has no proof at the moment of the read that it has not just been deposed | ReadIndex: on a read, record `commit_index`, confirm leadership with one round of heartbeats, then answer once `last_applied` reaches it. Leader leases are the faster variant and buy latency at the cost of a clock assumption. CheckQuorum is the precondition for both, and it is what shipped first |
| Client sessions for exactly-once (§8) | A client that retries a timed-out write can apply it twice (at-least-once today; `request_id` is minted server-side and only detects a log index reused by a different leader, so it cannot dedupe a retry even in principle). **"Applied twice" understates it under concurrency: the retry is a lost update.** A writes `balance=100`, the 200 is lost; B writes `balance=200` and is acknowledged; A's retry lands at a later index and reverts it to 100 — B's *acknowledged* write is gone, with no partition and no crash | Client sessions with per-client serial numbers; the state machine deduplicates inside the same transaction as the apply. Accepting a client-supplied id is the easy half; session expiry is the hard half |
| TLS/mTLS between nodes | RPCs cross the network in plaintext: any on-path attacker can read or forge votes and entries; medical-grade deployments would require encryption in transit | mTLS between peers (SPIFFE/cert-manager identities), TLS on the client API |
| Flow control on the write path | Past ~1000 concurrent client writes the leader loses its term and the whole burst reports `commit_timeout` — measured below. The same volume sent in smaller batches is fine, so it is concurrency the server has no way to refuse. The one piece of this that *is* implemented is the outbound half: the `AppendEntries` payload is capped (table 1b). Nothing caps what comes *in* | Batch client commands into one append and one fsync, so N concurrent writes cost one durable write rather than N; apply backpressure at the API — a bounded submit queue, or a 429 — rather than accepting unbounded concurrent submits and discovering the limit as a lost election |

## Repairing a far-behind follower, measured

A follower whose log has diverged — or been wiped by `/admin/reset` — is repaired by the
leader walking `nextIndex` back until the consistency check passes, then shipping the rest.
The paper allows the naive walk (one index per rejection) and the Students' Guide calls it
correct but slow "on long logs". This repo shipped the naive walk on the reasoning that a
short-lived cluster never builds a long log.

`/admin/flood` builds one in six seconds. Measured live on 2026-08-16, on the running
five-node cluster: a 3000-write mixed flood left the log at **6224 entries**; resetting
node-2 from the dashboard then produced this:

| | Naive walk (what shipped) | With the §5.3 hint |
|---|---|---|
| Rejections reaching one follower | ~2 per second (heartbeat-paced) | same |
| Round trips to repair 6224 entries | 6224 | 2 |
| Wall clock | **~52 minutes** | under a second |
| What the operator sees meanwhile | an empty state machine, indefinitely | it fills |

The failure is safe — the node holds a single term-0 configuration entry and commits
nothing — but *safe* and *broken* look identical on a dashboard, and 52 minutes is longer
than anyone will wait. That is what moved this from an acknowledged optimization to a fix.

The hint itself is two optional integers on `AppendEntriesResponse`, and it is advice
rather than authority: every jump still lands on an ordinary consistency check, and the
leader clamps the result so it can never rise or stand still. A wrong hint therefore costs
one round trip; it cannot corrupt a log, and it cannot hang the repair. A peer that sends
no hint at all — older code, or the stale-term rejection, which carries none — falls back
to the one-index walk, so the optimization never became a requirement.

## The throughput ceiling, measured

`/admin/flood` exists to make this reproducible rather than theoretical — and it earned
its keep: driving it and reading the output sceptically is what found the bug below.

There **was** a ceiling. Past ~1000 concurrent writes the leader lost its term and the
whole burst reported `commit_timeout`. Measured 2026-08-16 on a five-voter cluster over
HTTP, rediscovering the leader before every run:

| Writes all at once | Before | After |
|---|---|---|
| 200 | 0.5 s, all committed | 0.1 s, all committed |
| 700 | 1.4 s, all committed | 0.2 s, all committed |
| 1000 | **7.1 s, all timed out, leadership lost** | 0.2 s, all committed |
| 1500 | **8.8 s, all timed out, leadership lost** | 0.4 s, all committed |
| 2000 | **10.2 s, all timed out, leadership lost** | 0.5 s, all committed |

The term did not move once across the whole "after" column — no elections at all. The
largest burst the endpoint permits, 5000 writes with 2000 in flight, now finishes in
**2.0 s at 2524 writes/second** with every write committed.

### The bug was a missing index

The visible mechanism was never in doubt: the leader goes quiet, followers elect around a
node that is merely busy, and every in-flight write dies at once. Measuring a follower's
`append_entries_received` during a 1000-write burst caught it directly — **1733 ms with
nothing arriving**, against an election timeout whose floor is 1500 ms.

What blocked the loop was `last_config()`. Membership is a *view over the log* rather than
a cache beside it, so `_reload_config()` runs after every log mutation and `last_config()`
runs with it — putting that query on the write path, once per `submit()`. It was an
unindexed scan:

```
log  1,000 entries -> 0.175 ms   x1000 concurrent = 0.17 s
log  5,000 entries -> 0.823 ms   x1000 concurrent = 0.82 s
log 15,000 entries -> 2.475 ms   x1000 concurrent = 2.48 s   <- past the election floor
```

2.48 s predicted, 1733 ms measured. The cost is O(log length x concurrency), which is why
the cliff appeared to *move* between measurement sessions: the log grew from 3,000 to
nearly 16,000 entries over an afternoon of testing and the ceiling fell underneath it.

The fix is a partial index over just the configuration entries (`storage.py`, `_SCHEMA`).
On the live 15,923-entry log: **2.475 ms → 0.0021 ms**, and the query plan changes from
`SCAN log` to `SCAN log USING INDEX log_config`. No semantics move — the configuration is
still derived from the log, so truncating a config entry still reverts to the previous one
for free. Leader silence under the maximum burst is now **494 ms**, which is just the
heartbeat interval. `tests/test_config_lookup.py`.

### Two wrong answers came first, and they were worth having

Before the index, two other costs were found, fixed, and **did not move the cliff**:

- `_advance_commit_index()` walked every index from the log end down to `commit_index`,
  one query each, once per `submit()` — 4901 `term_at()` calls to cross one 4900-entry
  gap. Now flat (`tests/test_commit_scan.py`).
- The `AppendEntries` batch was unbounded: ~195 KiB per peer and ~33 ms of loop time per
  round at 2000 pending. Now capped at 512 (`tests/test_append_batching.py`).

Both are real improvements and neither was the answer. What they bought was the elimination
of the replication path as a suspect, which is what forced the question "where does the
loop actually go" — and instrumenting *that* rather than optimising the next plausible
thing is what found the index in one measurement. An earlier revision of this file asserted
the O(N²) commit scan **was** the cause. It was not, and profiling said so plainly: during
a burst the event loop sat idle in `kqueue` rather than executing Raft code.

### A measurement pitfall worth writing down

The first numbers taken for this section were wrong, in a way worth repeating because it
looks like a result. The harness resolved the leader **once** and then posted every run to
that address. The first big burst costs the leader its leadership, so runs two and three
went to a follower and read *its* flood counters — which never ran — producing an
identical `ok=0 timeout=999 not_leader=1001` for every subsequent row. A stable-looking
number repeated across runs is exactly what a real ceiling would look like.

Rediscover the leader before every run. `not_leader` climbing is the tell that you did not.

### The ceiling has not been removed, only moved

Nothing here makes the write path unbounded. The endpoint caps a burst at 5000 writes and
2000 in flight, and those are the largest numbers anyone has run against it — the cluster
absorbs them, so where it breaks next is *unmeasured*, not *nonexistent*. The remaining
omission in the table above stands: there is still no batching of client commands into one
append and one fsync, and still no backpressure at the API. A slower disk, a contended CPU,
or a burst past what `/admin/flood` will issue can all still starve a heartbeat.

That is also why `tests/test_flood.py` asserts invariants — convergence, `last_applied <=
commit_index <= last_log_index`, acked writes surviving — and never latency or a success
count. The same burst that commits in 0.5 s on an idle machine can time out entirely when
the CPU is contended, same code, same cluster.

### When it did break, it broke safely

Worth keeping, because it is the property that mattered while the ceiling existed and the
one that will matter again past whatever the next limit is. Measured on the pre-index
build, immediately after a 2000-at-once burst timed out every single client and cost the
leader its term:

```
node-2  log 15017  commit 15017  applied 15017
node-3  log 15017  commit 15017  applied 15017
node-4  log 15017  commit 15017  applied 15017   -> all agree
node-5  log 15017  commit 15017  applied 15017
```

Nothing lost, nothing misordered, no node left behind: the writes were *refused*, and the
election that took the leader down is the overload showing up as a leadership change
rather than as corruption. A surviving value for a contended key is still the one the log
ordered last — verified separately with the `overwrite` workload, where 1000 concurrent
writes to a single key left all five nodes agreeing on `v999`. Degradation under overload
is a liveness problem here, not a safety one, and that distinction is the whole point of
having the flood.

One nuance the counters hide: a client `timeout` does **not** mean the entry never
committed. Every one of those 2000 writes reported `commit_timeout` to its client, and the
log still ended at 15017 with all four nodes applied to it — they were appended and
committed after the clients had already given up at `commit_timeout`. That is the
at-least-once story in the omissions table above, seen live rather than argued.

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
