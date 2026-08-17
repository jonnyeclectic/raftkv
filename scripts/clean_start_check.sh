#!/usr/bin/env bash
# The cold-start gate: prove the cluster comes up from absolutely clean state.
# A build that only works on the machine that built it is the classic failure mode,
# and it is invisible on that machine. Run this before trusting a fresh checkout;
# .github/workflows/nightly.yml runs the same script on a schedule.
set -euo pipefail
cd "$(dirname "$0")/.."
echo "== clean start: removing containers, volumes, local images =="
docker compose down --volumes --remove-orphans --rmi local 2>/dev/null || true
echo "== checking nothing else holds the cluster ports =="
# After our own `down`, any remaining listener on 8001-8003 is a foreign cluster — usually
# `make run-local` or the IDE debug node. Its specific 127.0.0.1 bind beats compose's
# wildcard forward, so smoke would test the WRONG cluster: "kill the leader" stops a
# container while the local leader keeps answering, and the check fails with
# "no failover leader" 30 seconds later instead of naming the real problem now.
BUSY=$(lsof -nP -iTCP:8001-8003 -sTCP:LISTEN 2>/dev/null || true)
if [ -n "$BUSY" ]; then
  echo "FAIL: ports 8001-8003 are already in use, so smoke would test the wrong cluster:" >&2
  echo "$BUSY" >&2
  echo "Stop the local nodes first: pkill -f 'raftkv.app' (or make demo-reset, which also deletes data/ and logs/)." >&2
  exit 1
fi
echo "== building from scratch (no cache) =="
docker compose build --no-cache
echo "== starting and waiting for health =="
docker compose up -d --wait
./scripts/smoke.sh
docker compose down --volumes
echo "== CLEAN START PASS: the compose path works from scratch =="
