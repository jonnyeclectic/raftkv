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
     "            self.commit_index = min(req.leader_commit, last_new_entry)",
     "            self.commit_index = req.leader_commit"),

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
     "        self.storage.save_term_and_vote(self.current_term, req.candidate_id)  # rule 1",
     "        pass  # MUTANT: vote not persisted"),

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
    ("prevote: failed poll does not back off", "raft.py",
     "            self._reset_election_timer()\n            role_before, term_before = self.role, self.current_term",
     "            pass  # MUTANT: no backoff\n            role_before, term_before = self.role, self.current_term"),

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
