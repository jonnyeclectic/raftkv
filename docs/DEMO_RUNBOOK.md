# Demo runbook

The presentation script. ≈25 minutes of material plus Q&A; see [If the room is
short](#if-the-room-is-short) for the 15-minute cut.

**One cluster, started once, before the call — `make run-local` with node-1 under the IDE
debugger.** Everything below runs against it: replication, growth, failover, partitions,
load, and the debugger set-piece. Nothing here shells out to `docker` or `kubectl`.

That is deliberate. An earlier shape of this script opened on `make demo` (compose) and
switched to `make run-local` at the debugger section, which meant tearing down a working
cluster and rebuilding it in front of the panel. Both bind ports 8001–8003, so a slow
teardown leaves two clusters answering to the same node IDs and the dashboard shows a mix
of them. The Docker path still exists and still works; it is a credential to mention, not
a thing to do live.

## 1. T-24h and T-1h

- `make clean-start-check` — **both** the night before and the morning of. This is the
  compose path, and it is the "works on a machine that has never seen this project"
  credential: it rebuilds `--no-cache` from clean, smokes, and tears down. Clean starts
  are where live demos classically die. Stop any `run-local`/debugger nodes first — the
  check refuses while anything else holds 8001–8003, because a local node's specific
  `127.0.0.1` bind wins over compose's wildcard forward and smoke would otherwise test
  the wrong cluster (the symptom: a false `no failover leader`). Run it with the machine
  otherwise idle: the full test suite starving the VM mid-smoke produces the same failure
  honestly.
- `make demo-reset && make run-local` — then rehearse the actual script below on the
  actual cluster. `demo-reset` deletes every database and log, which is the point: a
  half-grown 5-voter cluster left over from the last rehearsal is exactly the surprise you
  do not want in the opening thirty seconds.
- Push the final state to GitHub and confirm CI is green — `gate` is the single context
  that decides, and it fails if any job it depends on was skipped rather than passed. Then
  open the repo on github.com and check that the README screenshot and every mermaid
  diagram actually render there.
- **Confirm the recruiter/panel has the repo link** — they review the code before the call.
- Close other Docker projects; `make down` anything stale. Then confirm the ports are
  genuinely free: `lsof -nP -i :8001-8006 -sTCP:LISTEN` should print nothing.

## 2. Start the cluster (before the call, not during it)

```bash
make demo-reset      # DESTRUCTIVE: kills local nodes, deletes data/ and logs/
make run-local       # nodes 2 and 3 only — nothing above node-3 starts by itself
```

Then start node-1 yourself from the IDE, so a breakpoint is one click away all the way
through:

```
script: scripts/debug_node.py        <- a SCRIPT target, not `module: uvicorn`
env:    RAFT_NODE_ID=node-1 RAFT_DB_PATH=data/node-1.db RAFT_LOG_DIR=logs
        RAFT_PEERS=node-2=127.0.0.1:8002,node-3=127.0.0.1:8003
        RAFT_ADVERTISE=127.0.0.1:8001
        RAFT_PORT=8001
working directory: the repo root
```

Open **http://127.0.0.1:8002/** — node-2's copy of the dashboard. Serve it from a node you
are *not* debugging: node-1's page stops updating the moment you sit on a breakpoint, which
is confusing in exactly the section where the breakpoint is the point.

Four things in that setup are load-bearing, and each has already cost time here:

- **A `script:` target, not `module: uvicorn`.** PyCharm/IntelliJ 2024.3 resolves module
  targets through `pkgutil.get_loader()`, which Python 3.14 removed; pydevd swallows the
  error and reports the misleading `No module named uvicorn`. It *runs* fine and only fails
  to *debug*, which is what makes it confusing. `scripts/debug_node.py` explains the
  mechanism.
- **`127.0.0.1`, never `localhost`.** localhost resolves to 127.0.0.1 *or* `::1`. A compose
  cluster publishes on both while a local node binds only the first, so the same string can
  reach two different clusters. This is why the dashboard polls the literal address.
- **`RAFT_ADVERTISE`, even on a bootstrap node.** A node writes its own address into the
  configuration entry, that entry replicates, and an empty one leaves a later-joined member
  holding a voter it can never dial. The failure surfaces several steps later, in section 5.
- **The timing asymmetry lives in `run_local.sh`, not in this config.** Node-1 runs default
  timings; `run_local.sh` starts nodes 2 and 3 with `RAFT_ELECTION_MIN=4
  RAFT_ELECTION_MAX=6`. That is what makes node-1 the leader *deterministically* — its
  1.5–3 s timeout always fires before the peers' 4–6 s — and what buys the debugger section
  its headroom: a pause under ~4 s costs nothing, and a longer one is section 8's set-piece.
  Do not stretch node-1's own timers instead: an earlier cut did (60–120 s on node-1 only),
  under which node-1 could never win an election — its timer never fired first — nor hold
  leadership, since its 5 s heartbeat outlasted the peers' 3 s timeout.

**Start node-1 before or alongside `make run-local`, not after.** Whoever's election timer
fires first takes term 1, and a node-1 that boots thirty seconds late arrives to find
node-2 already leading. Nothing is broken when that happens and section 8's precondition
has a remedy for it (**reset all nodes**), but the opening reads better when the card you
are about to debug is the green one.

> **Restart node-1 after any Python change.** `GET /` re-reads `dashboard.html` from disk on
> every request, so UI edits show up on a browser reload — but `app.py`, `raft.py` and
> `models.py` are imported once at process start. The failure mode is quiet: the dashboard
> grows a new button while the server still 404s the endpoint behind it. Seen exactly once,
> on `/admin/partition`. The masthead's `ui`/`srv` stamps exist to catch it — `srv` is
> frozen at import, so a stale process shows a stale hash.

## 3. Opening (2 min)

Dashboard is up, three cards, one green. Narrate the election: randomized timeouts break the
symmetry, the term counter ticks to 1, one card turns green and the other two follow it. The
topology graph draws edges outward from the leader only, because that is the real direction
of data flow.

Note what is **not** on screen: there is no staged row. Three cards, three voters, and no
spare capacity idling anywhere. Say so now and collect on it in section 5 — the cluster is
about to start its own fourth process in front of them, and the row that appears will matter
precisely because it was not there a minute earlier.

## 4. Replication (2 min)

Write `temp=72` from the CONTROL panel (Enter, or **set on leader**; the status line goes
green with the node it committed through). Point at log length / commit / applied advancing
on all three cards, then open a node's **state machine** section to show the key actually
landed.

`curl 127.0.0.1:8002/kv/temp` for a follower read of the replicated value — the answer to
"is that real, or is the UI faking it?" The response names the node that served it, so a
follower answering is visible in the payload rather than taken on trust.

## 5. Growing the cluster, and why odd sizes win (5 min)

This is the section the staged row was built for. `kubectl scale` and `docker compose up`
both provision a **process**; membership is a configuration entry in the replicated log, so
a started node is not a member until a leader appends one. Growing a Raft cluster is two
steps where growing a stateless Deployment is one, and the staged row is what step one looks
like.

**Provision.** Press **provision node** in the CONTROL panel. The dashboard asks the leader
to start another raftkv process; roughly a second later node-4 appears in a staged row that
did not exist before, dashed and greyed, labelled *running · empty voter set · nobody
replicates to it*.

Now say the line, while it is on screen and nothing has joined anything:

> That process is up, healthy, and answering. It is not in the cluster. Nothing replicates
> to it, it never campaigns, and the other three do not know it exists.

That is the whole point of the section, and it lands harder here than a pre-started spare
would: the node was created by the button they just watched, and it *still* bought nothing.
Quorum is unchanged at 2 of 3.

**Attach.** Press **attach as learner** on node-4. It joins *non-voting*: quorum stays
2-of-3, so no configuration change is in flight and no joint consensus is needed. Watch its
log length climb from 0 to match the others — the ordinary `nextIndex` walk, the same repair
a returning follower gets, and fast even after a flood: the follower tells the leader where
to resume (§5.3) instead of being walked back an index at a time, and the leader then ships
the backlog in batches, re-firing immediately after each one rather than waiting out a
heartbeat. It moves out of the staged row into the cluster.

**Promote.** Press **promote to voter** on node-4. The strip goes from `quorum 2 of 3
voting` to `3 of 4 voting`.

**Do not promise the audience the joint phase on screen — it is too fast to see.** The page
*can* render it, but on a healthy local cluster C-old,new commits in single-digit
milliseconds while the dashboard polls at 500 ms, so in practice it goes straight from one
number to the other. Pointing at a strip that never changes is a bad thirty seconds on
stage.

Point at the **event feed** instead, with the `membership` filter chip on. That is durable,
timestamped, and shows both halves:

```
config_committed
config_joint       node-4      <- C-old,new appended
learner_added      node-4
```

Then say what those two lines mean: from the instant `config_joint` is appended — not
committed, *appended* — every decision needs a majority of the old configuration **and** a
majority of the new one. There is therefore no instant at which the two memberships could
elect separate leaders. `config_committed` is the leader appending C-new once the joint
entry commits, which is the only thing that ends the transition.

The narrow window is itself the honest point, and worth saying: joint consensus is a
correctness mechanism, not a phase you operate in. It exists to make an unsafe instant
impossible, and on a healthy cluster that instant is a few milliseconds long.

**Now stop and read the numbers.** Four voters: quorum 3, tolerates 1 failure. Three voters:
quorum 2, tolerates 1 failure. **The fourth machine bought nothing.** Kill a node to show
it, then revive it. Then press **provision node** again for node-5, attach and promote it:
five voters, quorum 3, tolerates 2. That is the point — every even size is dominated by the
odd size below it, which is why real clusters are 3, 5 or 7 and never 4.

The button has no special relationship with 4 and 5, and saying so is worth ten seconds:
press it a third time and node-6 appears, a fourth and node-7 does, up to
`RAFT_PROVISION_MAX` (12 by default). Nothing about the two steps changes as the cluster
grows, and **one voter at a time** stays the rule no matter how many processes are staged —
the overlap arithmetic that makes joint consensus safe is what disappears at two.

### The same thing from a terminal (optional, ~1 min)

If someone suspects the button is browser magic, it is one HTTP call and there is a terminal
equivalent:

```bash
make node-up N=6      # node-6 on 127.0.0.1:8006, staged
```

It prints the two ways to bring it in, and the distinction between them is the whole point:

- **Attach it directly** — type `127.0.0.1:8006` into **attach by address** in the CONTROL
  panel. One step, no query string, and it lands in the cluster as a learner.
- **Or see it sit in the staged row first** — reopen the dashboard at
  `http://127.0.0.1:8002/?probe=8004,8005,8006` (the script prints this URL) and attach it
  from its own card. `?probe=` **replaces** the default list rather than adding to it, so
  keep 8004 and 8005 in it or those two stop being watched. The default is short on purpose:
  every address on it is a fetch twice a second for the whole session.

Only *staged* nodes need probing at all. Once a node is in the configuration the page adopts
it automatically — the configuration is a replicated log entry carrying each member's
address, so any node will hand it over in `/state`. That is what makes a page reload safe
mid-demo: a promoted node-4, or a node-6 you attached by address, comes back on its own
without the query string.

Either way it arrives *non-voting*, and **promote to voter** is still a separate press. That
is the honest shape of the operation: `make node-up` and `kubectl scale` do exactly the same
amount of it.

**Scaling down is not symmetric, and this build does not implement it.** Removing a voter is
another configuration change — it has to leave the configuration before its process leaves,
or the quorum it was counted in shrinks under a cluster that has not agreed to shrink.
Killing a promoted node's process is a *failure*, which the next section demonstrates on
purpose.

Three things to have ready for the follow-up:

1. **Why one at a time?** Adding two at once (3→5) gives majorities of 2 and 3 over a union
   of 5, which can be disjoint. One at a time always overlaps by at least one.
2. **Why does promote sometimes answer 409?** Four preconditions: a change already in
   flight, the previous one uncommitted, this leader has not yet committed an entry from its
   own term, or **the learner has not caught up** (the message reports how many committed
   entries it is behind). All four are "wait a beat", not "no". The last is worth saying out
   loud: a learner's lag is free because nobody counts it, and promoting it converts that
   lag directly into lost fault tolerance.
3. **Where does the learner's address come from?** Its own `/state`, not the address the
   browser used to reach it — `advertise_addr`. A browser-visible address and a
   peer-dialable one are not always the same, which is why `RAFT_ADVERTISE` is in the
   debugger config in section 2.

## 6. Failover (3 min)

Press **kill** on the leader's card. The node keeps its process — that is how the button,
which becomes **revive** in place, stays reachable — but it 503s every Raft RPC, `/state`,
`/healthz` and the KV API, so its peers experience an ordinary crash.

Narrate: the red card, the election in the merged log feed, the new leader's card turning
green. Write again to prove availability with a node down. Press **revive**: it rejoins as a
follower, sees the higher term, and catches up (log length converges).

> Doing this by killing the process instead — `pkill`, or `docker compose stop` on the
> compose path — demonstrates the same Raft behaviour but costs you the node for the rest of
> the demo. The button is reversible; that is the only reason it exists.

## 7. Partition, the failure a crash cannot show (3 min)

Press **cut links** on the leader's card. It stays up, still green, still calling itself
leader. Now write a key: the entry appears in its log, `commit` never moves, and the client
times out at 504 `commit_timeout`. Say the line out loud: *commit needs a majority, and it
can no longer reach one.* Meanwhile the other nodes elect a leader at a higher term.

A cut is bidirectional on purpose — outbound in `raft.py` (`node.blocked`), inbound in
`app.py` (`refuse_if_partitioned`). A one-way cut is a delay, not a partition, and makes
Raft look broken in ways real networks are not.

Press **heal all links**. The old leader sees the higher term in the next RPC, steps down,
and its uncommitted suffix is truncated and replaced. That truncation is the moment worth
pausing on, and it is worth *showing* rather than asserting: `curl` the key you wrote during
the partition and watch it 404 on the node that accepted it. It is the only reason two
leaders in different terms cannot both be believed.

**reset** on a single card wipes that node's term, vote, log and applied state. Worth doing
once deliberately mid-term: the node comes back able to vote again in a term it already
voted in, which is exactly the double-vote that Figure 2's durability requirement exists to
prevent. It is the fastest way to show why the database is part of the algorithm.

Note the asymmetry between the two reset controls, because it is a genuine safety property
and a good answer to "what did you get wrong?": a **per-node** reset keeps that node's
membership, while **reset all nodes** reverts it. Reverting one node alone would hand it the
voter set its process booted with — after growing to five, a *subset* of the real one, and
therefore a second quorum able to elect and commit on its own. Sent to every node at once,
nobody is left holding the superseded set, so the cluster re-forms at its original size.
This was a real lost-write bug here, found by adversarial review after the feature looked
finished; see [FAILURE_MODES.md](FAILURE_MODES.md) table 1c.

**reset all nodes** is also how you get back to the opening state without leaving the
browser — useful if you grew the cluster in section 5 and want three cards again. It reverts
membership and wipes the keyspace; the node-4 and node-5 *processes* keep running, so the
staged row comes back with them. Only `make demo-reset` removes those.

## 8. Debugger (5 min)

No setup: node-1 has been running under the debugger since before the call. That is the
whole reason this script does not open on compose.

Precondition: node-1 is the leader. On a three-card cluster started in the order section 2
gives, it is — its default 1.5–3 s election timeout beats nodes 2/3's stretched 4–6 s every
time. If a failover in section 6 or 7 moved the leadership, **reset all nodes** — or **kill**
the current leader — and node-1 wins the next election within ~3 s.

Breakpoints:

- `RaftNode.handle_request_vote` — inspect an incoming vote request mid-flight (term,
  lastLog comparison, the persisted vote).
- `RaftNode._advance_commit_index` — leader-only, which is why the precondition above
  matters: step the quorum count as a write commits.

A short pause is free: nodes 2/3 campaign only after 4–6 s of leader silence, so sit on a
frame, inspect, resume — leadership intact. Then sit past ~4–6 s deliberately: the dashboard
— served by node-2, which is why it keeps updating — shows node-1 unreachable and a new
election. **The debugger IS the failure injection.** Resume: node-1 sees the higher term in
the next RPC and steps down, live — a second failover without touching a control. One press
of **kill** on the interim leader (or **reset all nodes**) puts node-1 back in front for
section 9.

To step node-1's **follower** paths instead — the consistency check in
`handle_append_entries`, the vote rules in `handle_request_vote` — swap the roles from a
terminal, with the two tempo controls:

```bash
curl -sX POST 127.0.0.1:8002/admin/campaign      # node-2 takes the term, immediately
curl -sX POST 127.0.0.1:8001/admin/timing -H 'content-type: application/json' \
     -d '{"heartbeat_interval": 0.5, "election_timeout_min": 60, "election_timeout_max": 120}'
```

Node-2 now leads — its heartbeats hit node-1's follower breakpoints twice a second — and the
parked timeout means a resumed node-1 does not campaign over a pause it slept through.
Swapping back is symmetric, and works *while parked*, because campaign does not wait for a
timer:

```bash
curl -sX POST 127.0.0.1:8001/admin/campaign      # node-1 takes the term back
curl -sX POST 127.0.0.1:8001/admin/timing -H 'content-type: application/json' \
     -d '{"heartbeat_interval": 0.5, "election_timeout_min": 1.5, "election_timeout_max": 3.0}'
```

A timing update is validated whole against the startup ratio rules (§5.2), so a typo cannot
configure a cluster that elects around its own leader forever — send a heartbeat longer than
the election floor and the node answers 409 with the rule it broke, rather than accepting
it. Both knobs are runtime-only; a restart restores the env-derived config.

## 9. Load, and where it stops holding (3 min)

The **load generator** panel, right-hand column. Everything here is one control away.

1. **`mixed`, 200 writes, 50 at once** → **run flood**. Bar fills green, summary reads
   `mixed done · 200/200 · 200 committed · …/s`. These are real client writes through the
   same `submit()` the HTTP API calls, replicated and applied on every node. Watch the
   cards' commit index climb together.
2. **`overwrite`, 200 writes, 50 at once.** Same volume, one key. Every node shows the same
   surviving value, and the winner is decided by **log order** rather than by the order a
   client sent them in — the honest answer to "what happens when two clients race".

   **Read the value off the cards; do not predict it.** An earlier draft of this script
   promised the survivor would *not* be the last write sent, which is not something the demo
   can promise: rehearsed 2026-08-17, `v199` won four runs out of four. At 50 in flight the
   two orders usually agree, and saying otherwise hands the room a wrong prediction to
   catch. The point that always holds is the one worth making — consensus picks exactly one
   winner and every node agrees on it.
3. **Switch the workload to `distinct`, set writes to 5000 and at-once to 2000** — the
   largest burst the endpoint allows (`MAX_TOTAL` / `MAX_CONCURRENCY` in `flood.py`) — and
   run it. Every write commits, the term counter does not move, and all the commit indexes
   climb together. Then open any card's **state machine** and scroll: 5000 keys, `k0000`
   through `k4999`, in order.

   **Narrate the invariant, read the rate off the panel.** The term staying put is the
   claim worth making and it is a property; the throughput number is a measurement of
   whatever machine you are on, and the same burst that commits in half a second on an idle
   laptop can time out entirely on a contended one. That is why `tests/test_flood.py`
   asserts convergence and acked-write survival and never a rate or a success count.

   Set the workload deliberately here. The panel defaults to `mixed`, which reuses a
   keyspace of eight, so the burst commits 5000 entries and the key list barely moves —
   rehearsed, and it makes the state machine look like nothing happened. `distinct` is the
   one that fills it.

   Then tell the story behind that number, because it is the best one in the demo and it is
   a debugging story rather than a Raft one. **This burst used to take the cluster down.**
   Past ~1000 concurrent writes the leader lost its term and every write reported
   `commit_timeout`.

   The cause was a **missing index**. Membership in Raft is a view over the log rather than
   a cache beside it — that is what makes a truncated configuration entry revert for free —
   so `_reload_config()` runs after every log mutation, which puts its `last_config()` query
   on the write path, once per write. Unindexed it was a full scan, so it cost 2.475 ms
   against a 15,000-entry log; a thousand concurrent writes spent ~2.5 s of event-loop time
   in it, the heartbeat never went out, and the followers elected around a leader that was
   merely busy. A partial index over the configuration entries takes it to 0.0021 ms.

   The part worth saying out loud: **two plausible fixes came first and neither worked.** An
   O(N²) commit scan and an unbounded replication batch were both real, both fixed, and both
   left the cliff exactly where it was. What broke it open was measuring the leader's silence
   directly instead of optimising the next likely-looking thing. If someone asks how you
   debug a distributed system, this is the answer, and the wrong turns are the interesting
   half.

If the room wants the failure itself rather than the fix, the mechanism is still one
`RAFT_ELECTION_MIN` away: shorten the election timeout and the same burst starves the
heartbeat again. Numbers, the instrumented costs, the two wrong answers and the measurement
pitfall that produced a whole table of fake results are in
[FAILURE_MODES.md](FAILURE_MODES.md) § "The throughput ceiling, measured".

The reassuring half, and worth saying explicitly: **nothing is lost or misordered.** The
writes are *refused*. Every node still converges on the same state. This is a liveness
failure under overload, not a safety one — which is exactly the distinction the whole
project is about. And the ceiling has not been removed, only moved: 5000 at 2000 is simply
the largest burst anyone has run, so where it breaks next is unmeasured rather than absent.

## 10. Where it fails (5 min)

Walk [FAILURE_MODES.md](FAILURE_MODES.md) table 2. The framing: every row was cut on
purpose, each has a named production fix, and one of them (PreVote) is visible in this very
demo when a revived node forces an election. Table 2 also carries flow control on the write
path — the omission section 9 just demonstrated. If asked why *that* one is still open when
the rest of the load story got fixed: the outbound half is done (the `AppendEntries` payload
is capped, table 1b), and nothing at all bounds what arrives at the API. Refusing a client is
a product decision as much as an engineering one, and this build would rather time out
honestly than pretend to a limit it never measured.

Table 1c is the one to volunteer if nobody asks what went wrong: bugs an adversarial review
found *after* the feature looked finished, including the reset lost-write from section 7.

## If the room is short

Cut, in this order:

1. Section 9 step 2 (`overwrite`) — steps 1 and 3 carry the argument alone.
2. Section 5's "the same thing from a terminal" — optional by design.
3. Section 7's single-node reset — the partition itself is the point.
4. Section 6 — section 8's breakpoint produces a failover for free.

That leaves ≈15 minutes with every distinct idea still in it.

## 11. Q&A parking lot

- **Why not Paxos?** Understandability was Raft's explicit design goal — and explainability
  is the point of this exercise.
- **Why HTTP, not gRPC?** Observability of the demo: every RPC is curl-able and readable on
  the wire.
- **Why SQLite?** Raft *requires* stable storage before replying to RPCs — the database is
  the algorithm, not an accessory.
- **Does it run on Kubernetes?** Yes — `make k8s-demo` brings up a kind cluster with the
  same image as a StatefulSet, and `make k8s-scale N=5` provisions staged pods exactly like
  `make node-up` does locally (a pod above the bootstrap ordinal boots as a learner with no
  peers). Show it from a screenshot or after the call; do not create a cluster live.
- **Figure 8** — have the whiteboard sketch ready (why commit counts only current-term
  entries).
- **Company ties** — fault tolerance under partial failure, PII-safe observability, and the
  replicated log as a durable audit trail; make these live, matched to where the
  conversation goes.
