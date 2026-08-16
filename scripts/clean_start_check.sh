#!/usr/bin/env bash
# The cold-start gate: prove the cluster comes up from absolutely clean state.
# A build that only works on the machine that built it is the classic failure mode,
# and it is invisible on that machine. Run this before trusting a fresh checkout;
# .github/workflows/nightly.yml runs the same script on a schedule.
set -euo pipefail
cd "$(dirname "$0")/.."
echo "== clean start: removing containers, volumes, local images =="
docker compose down --volumes --remove-orphans --rmi local 2>/dev/null || true
echo "== building from scratch (no cache) =="
docker compose build --no-cache
echo "== starting and waiting for health =="
docker compose up -d --wait
./scripts/smoke.sh
docker compose down --volumes
echo "== CLEAN START PASS: the compose path works from scratch =="
