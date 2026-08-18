#!/usr/bin/env bash
# Provision ONE more raftkv process at runtime, without touching the running cluster.
# This is the run-local analogue of `kubectl scale --replicas=N`, and it is deliberately
# only half of growing the cluster: it starts a PROCESS. The process comes up as a
# learner with no peers, which means an empty voter set, which means it is in nobody's
# configuration and nobody replicates to it. Membership is a log entry, so a leader has
# to append one -- that is the "attach" half, and it happens from the dashboard.
#
#   scripts/node_up.sh 6        -> node-6 on 127.0.0.1:8006
#
# Detached on purpose: run_local.sh holds its children and kills them all on exit, so a
# node started from this script has to outlive the shell that ran it.
set -euo pipefail
cd "$(dirname "$0")/.."

N="${1:?usage: node_up.sh <ordinal>   e.g. node_up.sh 6}"
case "$N" in
  ''|*[!0-9]*) echo "error: ordinal must be a number, got '$N'" >&2; exit 1 ;;
esac
if [ "$N" -lt 4 ]; then
  echo "error: nodes 1-3 are the bootstrap cluster; they are started by run_local.sh" >&2
  echo "       and by the IDE debugger, and they boot as VOTERS with a peer list." >&2
  exit 1
fi

PORT=$((8000 + N))
ID="node-$N"
ADDR="127.0.0.1:$PORT"

if command -v lsof >/dev/null && lsof -nP -i ":$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "error: port $PORT is already in use — $ID may already be running." >&2
  lsof -nP -i ":$PORT" -sTCP:LISTEN >&2
  exit 1
fi

mkdir -p data logs
RAFT_NODE_ID="$ID" RAFT_DB_PATH="data/$ID.db" RAFT_LOG_DIR=logs \
RAFT_LEARNER=1 RAFT_ADVERTISE="$ADDR" \
  nohup uv run uvicorn --factory raftkv.app:create_app --port "$PORT" \
  >> "logs/$ID.stdout" 2>&1 &

# Wait for it to answer rather than printing instructions for a node that is still
# importing: the next thing anyone does is type this address into the dashboard.
for _ in $(seq 1 40); do
  if curl -fsS -m 1 "http://$ADDR/healthz" >/dev/null 2>&1; then
    cat <<EOF

$ID is up on $ADDR — STAGED: a learner with no peers, in nobody's configuration.
A replica is a process, not a cluster member. To make it one, open the dashboard on an
address that is probing this one:

  http://127.0.0.1:8002/?probe=8004,8005,$PORT

$ID then sits in the staged row, and both steps are on its own card:

  attach as learner   (quorum does not move, so no joint consensus)
  promote to voter    (joint consensus, one voter at a time)

Stop it again (only safe while it is still STAGED — once it is a voter, killing the
process is a failure, not a scale-down): pkill -f "create_app --port $PORT"
EOF
    exit 0
  fi
  sleep 0.25
done

echo "error: $ID did not answer on $ADDR within 10s; see logs/$ID.stdout" >&2
exit 1
