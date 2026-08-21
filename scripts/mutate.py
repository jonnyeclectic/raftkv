#!/usr/bin/env python3
# ruff: noqa: E501 -- the anchors below must match src byte-for-byte; wrapping breaks them
"""Targeted mutation testing for raftkv.

Not random mutation: each entry below is a bug a competent person could plausibly
introduce, and most correspond to an invariant this repo explicitly warns about. A
mutation that SURVIVES the suite is a coverage hole -- the suite is asserting on a regime
where that code cannot fail.

Usage: uv run python mutate.py [--only N]
"""

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src" / "raftkv"

# (label, file, old, new)
MUTATIONS = [
    # ---- quorum / majority arithmetic ----
    ("quorum: majority -> half", "raft.py",
     "return len(self.config.voters) // 2 + 1",
     "return len(self.config.voters) // 2"),
    ("_is_majority: >= -> >", "raft.py",
     "return len(acked & voters.keys()) >= len(voters) // 2 + 1",
     "return len(acked & voters.keys()) > len(voters) // 2 + 1"),
    ("joint agreement: AND -> OR (union majority)", "raft.py",
     "return (self._is_majority(self.config.voters, acked)\n                and self._is_majority(self.config.old_voters, acked))",
     "return (self._is_majority(self.config.voters, acked)\n                or self._is_majority(self.config.old_voters, acked))"),
    ("empty voter set counts as agreement", "raft.py",
     "        if not self.config.voters:\n            return False",
     "        if not self.config.voters:\n            return True"),
    # floor(N/2)+1 -> ceil(N/2). IDENTICAL for every odd N: at three voters both give two,
    # so no three-node test can tell them apart. At four they give three and two, and two
    # of four is exactly half -- both halves of a 2-2 split would hold a "majority" and
    # elect a leader each.
    #
    # Already caught before tests/test_election_quorum_by_size.py existed, and by tests that
    # are not about elections at all: test_commit_advances_only_as_far_as_a_majority_holds,
    # test_promote_endpoint_round_trip and test_an_undialable_voter_does_not_kill_the_
    # election_loop all reach four voters incidentally, on the commit and membership paths.
    # That is coverage by luck rather than by intent -- it would evaporate the day those
    # tests were rewritten at three nodes -- which is what the named even-N election tests
    # are for. Kept here as a mutation because "which test catches this" should be a
    # question with an answer.
    ("majority: floor(N/2)+1 -> ceil(N/2)  [only wrong for EVEN N]", "raft.py",
     "return len(acked & voters.keys()) >= len(voters) // 2 + 1",
     "return len(acked & voters.keys()) >= (len(voters) + 1) // 2"),
    ("quorum: floor(N/2)+1 -> ceil(N/2)  [reported number only]", "raft.py",
     "return len(self.config.voters) // 2 + 1",
     "return (len(self.config.voters) + 1) // 2"),
    # Half counts as a majority. Unlike the ceil swap above this is wrong at EVERY size,
    # odd included: 2 of 5 would elect, and so would the other 3, in the same term.
    ("majority: >= half+1 -> >= half  [half counts as a majority]", "raft.py",
     "return len(acked & voters.keys()) >= len(voters) // 2 + 1",
     "return len(acked & voters.keys()) >= len(voters) // 2"),

    # ---- Figure 8 / commit rule ----
    ("commit: drop the current-term restriction", "raft.py",
     "            if self.storage.term_at(candidate_index) != self.current_term:\n                break",
     "            if False:\n                break"),
    ("commit candidates: drop last_log_index", "raft.py",
     "        if last > self.commit_index:\n            steps.add(last)",
     "        if False:\n            steps.add(last)"),

    # ---- AppendEntries receiver rules ----
    ("stale term: < -> <=", "raft.py",
     "        if req.term < self.current_term:  # stale leader",
     "        if req.term <= self.current_term:  # stale leader"),
    ("consistency check: always pass", "raft.py",
     "        if self.storage.term_at(req.prev_log_index) != req.prev_log_term:\n            return self._consistency_failure(req.prev_log_index)",
     "        if False:\n            return self._consistency_failure(req.prev_log_index)"),
    ("leaderCommit: trust it beyond what we verified", "raft.py",
     "            self.commit_index = max(self.commit_index, min(req.leader_commit, last_new_entry))",
     "            self.commit_index = max(self.commit_index, req.leader_commit)"),
    # Drop the monotonicity guard on the RESULT — the pre-fix code. A heartbeat whose
    # prev_log_index sits below commit_index (empty entries, high leaderCommit) then drops
    # commit_index backwards, stranding last_applied above it. Fig. 2 says commitIndex only
    # increases; etcd's commitTo guards the same way. See FAILURE_MODES.md table 1c.
    ("commit: monotonicity guard on the result removed", "raft.py",
     "            self.commit_index = max(self.commit_index, min(req.leader_commit, last_new_entry))",
     "            self.commit_index = min(req.leader_commit, last_new_entry)"),

    # ---- matchIndex discipline (Students' Guide) ----
    ("matchIndex from our log, not what we sent", "raft.py",
     "            self.match_index[peer_id] = prev_index + len(entries)",
     "            self.match_index[peer_id] = self.storage.last_log_index()"),

    # ---- §5.3 repair ----
    ("nextIndex clamp removed (can rise)", "raft.py",
     "        return max(1, min(proposed, current - 1))",
     "        return max(1, proposed)"),

    # ---- persistence before reply (Fig 2 §5.2) ----
    ("vote not persisted before replying", "raft.py",
     "        self.storage.save_term_and_vote(self.current_term, req.candidate_id)  # rule 1: disk first",
     "        pass  # MUTANT: vote not persisted"),
    # Mutate the ORDER, not the presence: adopt the higher term in memory BEFORE persisting
    # it. The write still happens, so a healthy node looks fine — but a write that raises
    # (a value SQLite cannot hold arrives on the unauthenticated port) now leaves the term
    # in memory and not on disk, free to vote twice after a restart. test_wire_bounds.py's
    # persist-ordering tests drive a failing write and assert memory and disk still agree.
    ("observe_term: mutate memory before persisting", "raft.py",
     "            self.storage.save_term_and_vote(term, None)  # rule 1: persist first, literally\n            self.current_term = term\n            self.voted_for = None",
     "            self.current_term = term\n            self.voted_for = None\n            self.storage.save_term_and_vote(term, None)"),

    # ---- re-election commit safety (§5.4.2 / §5.4.3) ----
    # Append the no-op BEFORE resetting match_index — the State Machine Safety bug. The
    # no-op's commit scan then counts a prior tenure's match_index; if a truncation left a
    # stale value at exactly the no-op's index, it commits an entry only this node holds.
    ("become_leader: no-op appended before match_index is reset", "raft.py",
     "        self.match_index = {p: 0 for p in self._replication_targets()}\n        self._noop_index = self._leader_append(NoOp())",
     "        self._noop_index = self._leader_append(NoOp())\n        self.match_index = {p: 0 for p in self._replication_targets()}"),

    # ---- wire bounds on the unauthenticated RPC surface ----
    # Remove the ceiling on AppendEntries.term. A term of 2**63 then reaches _observe_term,
    # which assigns it to current_term before the SQLite write raises OverflowError.
    ("wire: AppendEntries.term ceiling removed", "models.py",
     "    term: int = Field(ge=0, le=MAX_WIRE_INT)\n    leader_id: str = Field(min_length=1, max_length=MAX_ID_LEN)",
     "    term: int\n    leader_id: str = Field(min_length=1, max_length=MAX_ID_LEN)"),

    # ---- membership ----
    ("learners leak into the voter set", "raft.py",
     "        return len(self.config.voters) // 2 + 1",
     "        return (len(self.config.voters) + len(self.config.learners)) // 2 + 1"),
    ("config not reloaded after append", "raft.py",
     "        self._reload_config()  # a client command never changes membership, but the",
     "        pass  # MUTANT: no reload"),

    # ---- apply loop ----
    ("apply loop: stall on non-command entries", "raft.py",
     "                    self.storage.advance_applied(index)",
     "                    pass  # MUTANT: stalls"),

    # ---- storage ----
    ("term_at(0) sentinel returns None", "storage.py",
     "        if idx == 0:\n            return 0  # sentinel",
     "        if idx == 0:\n            return None  # MUTANT"),
    ("config lookup index dropped", "storage.py",
     "CREATE INDEX IF NOT EXISTS log_config ON log(idx)\n    WHERE json_extract(command, '$.op') = 'config';",
     ""),
    ("entries_from ignores its limit", "storage.py",
     "        if limit is not None and limit <= 0:\n            return []",
     "        limit = None\n        if limit is not None and limit <= 0:\n            return []"),

    # ---- transport aliasing ----
    ("HttpTransport aliases the peer dict", "transport.py",
     "dict(peers)", "peers"),

    # ---- HTTP contract (app.py) ----
    ("not-leader 503 -> 500", "app.py",
     'status_code=503, content={"error": "not_leader", "leader_id": exc.leader_id}',
     'status_code=500, content={"error": "not_leader", "leader_id": exc.leader_id}'),
    ("not-leader response drops leader_id", "app.py",
     'content={"error": "not_leader", "leader_id": exc.leader_id}',
     'content={"error": "not_leader"}'),
    ("commit timeout 504 -> 200", "app.py",
     'return JSONResponse(status_code=504, content={"error": "commit_timeout"})',
     'return JSONResponse(status_code=200, content={"error": "commit_timeout"})'),
    ("missing key 404 -> 200", "app.py",
     'raise HTTPException(status_code=404, detail="key not found")',
     'return {"key": key, "value": None}'),
    ("crashed node answers normally", "app.py",
     'raise HTTPException(status_code=503, detail="node is down (simulated crash)")',
     'return None'),
    ("partition is one-way (inbound allowed)", "app.py",
     'raise HTTPException(status_code=503, detail=f"link to {peer_id} is partitioned")',
     'return None'),

    # ---- flood accounting (flood.py) ----
    ("flood: CancelledError becomes an outcome", "flood.py",
     "                except asyncio.CancelledError:",
     "                except asyncio.CancelledError if False else _Never:"),
    ("flood: unexpected error not counted", "flood.py",
     '                self._state[outcome] += 1\n                self._state["done"] += 1',
     '                if outcome != "failed":\n                    self._state[outcome] += 1\n                self._state["done"] += 1'),
    ("flood: done not incremented on failure", "flood.py",
     '                self._state["done"] += 1',
     '                if outcome == "ok":\n                    self._state["done"] += 1'),

    # ---- provisioning bounds (provision.py) ----
    ("provision: explicit ordinal skips the cap window", "provision.py",
     "    elif ordinal >= FIRST_ORDINAL + max_nodes:",
     "    elif False:"),

    # ---- CheckQuorum (thesis §6.2) ----
    ("checkquorum: a leader never resigns", "raft.py",
     "        if self._has_agreement(reachable):\n            return",
     "        if True:\n            return"),
    ("checkquorum: union majority, not both halves", "raft.py",
     "        if self._has_agreement(reachable):",
     "        if self._is_majority(self.config.voters, reachable):"),
    ("checkquorum: contact not seeded on winning", "raft.py",
     "        self._last_contact = {p: now for p in self._replication_targets()}",
     "        self._last_contact = {}"),
    ("checkquorum: only a SUCCESSFUL append counts as contact", "raft.py",
     "        self._last_contact[peer_id] = time.monotonic()\n        self._observe_term(resp.term)",
     "        if resp.success:\n            self._last_contact[peer_id] = time.monotonic()\n        self._observe_term(resp.term)"),

    # ---- PreVote (thesis §9.6) ----
    ("prevote: a straw poll observes the term after all", "raft.py",
     "        if req.pre_vote:\n            return self._handle_pre_vote(req)  # BEFORE _observe_term; see there",
     "        if False:\n            return self._handle_pre_vote(req)"),
    ("prevote: a leader no longer holds its own lease", "raft.py",
     "        lease_intact = self.role is Role.LEADER or (\n            self.leader_id is not None",
     "        lease_intact = False or (\n            self.leader_id is not None"),
    ("prevote: lease applies even with no leader to protect", "raft.py",
     "        lease_intact = self.role is Role.LEADER or (\n            self.leader_id is not None",
     "        lease_intact = self.role is Role.LEADER or (\n            True"),
    ("prevote: campaign routed through the poll it must bypass", "raft.py",
     "        await self._start_election(pre_vote=False)",
     "        await self._start_election()"),
    # The bug this pair was extracted from, found live on a four-voter cluster: the lease
    # reads the ELECTION TIMER, which a node's own campaign resets before it polls. Each
    # candidate then cites its own campaign as proof the leader is alive and refuses every
    # other candidate. Three of four voters up, identical logs, no leader for ~45 s.
    ("prevote: lease reads the election timer, not leader contact", "raft.py",
     "            and time.monotonic() - self._last_heard_from_leader\n            < self.cfg.election_timeout_min",
     "            and time.monotonic() - self._last_reset < self.cfg.election_timeout_min"),
    ("prevote: leader contact never recorded", "raft.py",
     '        self._last_heard_from_leader = time.monotonic()',
     '        pass  # MUTANT: contact not recorded'),
    ("prevote: failed poll does not back off", "raft.py",
     "            self._reset_election_timer()\n            role_before, term_before = self.role, self.current_term",
     "            pass  # MUTANT: no backoff\n            role_before, term_before = self.role, self.current_term"),

    # ---- the dashboard's ballot and log automata ----
    # These draw two of Raft's guarantees, so a bug here misteaches the guarantee to
    # whoever is reading the panel to find out what the cluster did. Pinned by
    # tests/test_dashboard_state_machines.py, which runs the shipped block under node.
    ("ballot: straw poll pinned under UNVOTED instead of looping where it stands", "static/dashboard.html",
     'rows.push({from: state, to: state, term: e.term, ts: e.ts,\n                 cause: e.event === "pre_vote_failed"',
     'rows.push({from: "unvoted", to: "unvoted", term: e.term, ts: e.ts,\n                 cause: e.event === "pre_vote_failed"'),
    # One vote per term (§5.2) is what makes two leaders in a term impossible. A dead check
    # here is silent: the panel keeps drawing a perfectly ordinary ballot.
    ("ballot: a second vote in one term is never compared", "static/dashboard.html",
     "if (castTo !== null && castTo !== e.candidate) {",
     "if (false) {"),
    ("ballot: a term change does not clear that term's vote", "static/dashboard.html",
     "      castTo = null;\n    }\n    if (e.term != null) term = e.term;",
     "      castTo = castTo;\n    }\n    if (e.term != null) term = e.term;"),
    # A follower never logs commit_advanced -- handle_append_entries moves commit_index
    # silently -- so `applied` is its only evidence. Without it the truncation check below
    # is dead on every node except the leader, which is where it matters least.
    ("log: applied no longer proves the entry was committed", "static/dashboard.html",
     "if (e.index != null) proven = Math.max(proven, e.index);",
     "if (false) proven = Math.max(proven, e.index);"),
    ("log: reset leaves the committed watermark standing", "static/dashboard.html",
     '      proven = 0;\n      add("appended", "truncated", "log destroyed by reset", e, 0);',
     '      add("appended", "truncated", "log destroyed by reset", e, 0);'),
    # commit_index NAMES a committed entry, so cutting FROM that index erases it. `<` lets
    # the single worst case through while every other case still reports correctly.
    ("log: truncation exactly at the committed index allowed through", "static/dashboard.html",
     "if (e.from_index != null && e.from_index <= proven) {",
     "if (e.from_index != null && e.from_index < proven) {"),

    # The two detectors above are worth nothing if the row they build is never appended:
    # every number on the panel stays correct and the alarm simply never fires.
    ("panel: the log's violation row is computed and dropped", "static/dashboard.html",
     "  if (bad) wrap.append(bad);\n  return wrap;\n}\n/* --- end automaton rendering",
     "  return wrap;\n}\n/* --- end automaton rendering"),
    ("panel: the log automaton lights the box the BALLOT would light", "static/dashboard.html",
     "  const now = logStage(s);\n  const {rows, violations} = logTransitions(lastLines, s.node_id);",
     "  const now = ballotState(s.voted_for, s.node_id);\n  const {rows, violations} = logTransitions(lastLines, s.node_id);"),

    # ---- runtime tempo controls ----
    ("campaign: learner allowed to campaign", "raft.py",
     '        if not self.is_voter:\n            raise ValueError("this node is a learner; learners never campaign (§6)")',
     "        if False:\n            raise ValueError('unreachable')"),
]


def run(label, path, old, new, idx):
    f = SRC / path
    src = f.read_text()
    if old not in src:
        return (idx, label, "SKIP", "anchor not found")
    backup = src
    f.write_text(src.replace(old, new, 1))
    try:
        r = subprocess.run(
            ["uv", "run", "pytest", "-x", "-q"],
            cwd=ROOT, capture_output=True, text=True,
            env={**os.environ, "UV_CACHE_DIR": os.environ.get("TMPDIR", "/tmp") + "/uv-cache"},
            timeout=900,
        )
        out = (r.stdout + r.stderr)
        if r.returncode == 0:
            return (idx, label, "SURVIVED", "suite still green")
        first = ""
        for line in out.splitlines():
            if line.startswith("FAILED") or "assert" in line.lower():
                first = line.strip()[:90]
                break
        return (idx, label, "caught", first)
    except subprocess.TimeoutExpired:
        return (idx, label, "caught", "suite hung/timeout")
    finally:
        f.write_text(backup)


if __name__ == "__main__":
    only = None
    if "--only" in sys.argv:
        only = int(sys.argv[sys.argv.index("--only") + 1])
    results = []
    for i, (label, path, old, new) in enumerate(MUTATIONS):
        if only is not None and i != only:
            continue
        res = run(label, path, old, new, i)
        results.append(res)
        print(f"[{res[0]:2d}] {res[2]:9s} {res[1]}", flush=True)
        if res[2] == "SURVIVED":
            print("       ^^^ COVERAGE HOLE", flush=True)
    print("\n==== summary ====")
    for idx, label, status, note in results:
        if status in ("SURVIVED", "SKIP"):
            print(f"  {status:9s} [{idx}] {label}  ({note})")
    caught = sum(1 for r in results if r[2] == "caught")
    print(f"\ncaught {caught}/{len(results)}")
