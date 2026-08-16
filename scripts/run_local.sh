#!/usr/bin/env bash
# Starts nodes 2 and 3 locally. Run node-1 YOURSELF under the IDE debugger:
#   script: scripts/debug_node.py   <- a SCRIPT target; `module: uvicorn` cannot be
#                                      debugged on py3.14 (see that file's docstring)
#   env: RAFT_NODE_ID=node-1 RAFT_DB_PATH=data/node-1.db RAFT_LOG_DIR=logs
#        RAFT_PEERS=node-2=127.0.0.1:8002,node-3=127.0.0.1:8003 RAFT_PORT=8001
#        RAFT_ADVERTISE=127.0.0.1:8001
# Dashboard: http://127.0.0.1:8002/  (node-1's card goes red while you sit on a breakpoint)
#            Literal address, not localhost: this is the one path where BOTH a local
#            node and a compose container can be listening on the same port number.
set -euo pipefail
cd "$(dirname "$0")/.."

# Refuse to start on top of another cluster. `make demo` publishes these same ports, so
# running both leaves two three-node clusters that BOTH answer to node-1/2/3: the
# dashboard then shows a mix, terms leap (a fresh node jumping t1 -> t22 is the tell),
# and it reads as a consensus bug when it is really two clusters wearing one name.
for port in 8001 8002 8003 8004 8005; do
  if command -v lsof >/dev/null && lsof -nP -i ":$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "error: port $port is already in use. Stop the other cluster first:" >&2
    echo "         make down          # compose" >&2
    echo "         pkill -f raftkv.app  # a stray local node or debugger" >&2
    lsof -nP -i ":$port" -sTCP:LISTEN >&2
    exit 1
  fi
done

mkdir -p data logs
trap 'kill 0' EXIT
# RAFT_ADVERTISE on the bootstrap nodes too: each node writes its OWN address into
# the configuration, and an empty one gives a later-joined member a voter it cannot
# dial -- so it can never collect that vote.
RAFT_NODE_ID=node-2 RAFT_DB_PATH=data/node-2.db RAFT_LOG_DIR=logs \
RAFT_ADVERTISE=127.0.0.1:8002 \
RAFT_PEERS="node-1=127.0.0.1:8001,node-3=127.0.0.1:8003" \
  uv run uvicorn --factory raftkv.app:create_app --port 8002 &
RAFT_NODE_ID=node-3 RAFT_DB_PATH=data/node-3.db RAFT_LOG_DIR=logs \
RAFT_ADVERTISE=127.0.0.1:8003 \
RAFT_PEERS="node-1=127.0.0.1:8001,node-2=127.0.0.1:8002" \
  uv run uvicorn --factory raftkv.app:create_app --port 8003 &
# node-4 idles as a learner with NO peers: it never campaigns and nobody knows it
# exists until the dashboard's "add node (learner)" button attaches it to the leader.
RAFT_NODE_ID=node-4 RAFT_DB_PATH=data/node-4.db RAFT_LOG_DIR=logs \
RAFT_LEARNER=1 RAFT_ADVERTISE=127.0.0.1:8004 \
  uv run uvicorn --factory raftkv.app:create_app --port 8004 &
# node-5 idles too: promoting node-4 then node-5 grows the cluster 3 -> 4 -> 5, which
# is what makes the "why odd sizes" point concrete rather than arithmetic.
RAFT_NODE_ID=node-5 RAFT_DB_PATH=data/node-5.db RAFT_LOG_DIR=logs \
RAFT_LEARNER=1 RAFT_ADVERTISE=127.0.0.1:8005 \
  uv run uvicorn --factory raftkv.app:create_app --port 8005 &
wait
