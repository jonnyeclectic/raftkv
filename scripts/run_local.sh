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
    echo "         make down                    # compose" >&2
    echo "         pkill -f 'raftkv.app'        # stray run-local nodes" >&2
    echo "         pkill -f 'debug_node.py'     # the IDE debugger node (or press Stop in the IDE)" >&2
    lsof -nP -i ":$port" -sTCP:LISTEN >&2
    exit 1
  fi
done

mkdir -p data logs
trap 'kill 0' EXIT
# RAFT_ADVERTISE on the bootstrap nodes too: each node writes its OWN address into
# the configuration, and an empty one gives a later-joined member a voter it cannot
# dial -- so it can never collect that vote.
#
# Stretched ELECTION timeouts on nodes 2/3 ONLY -- node-1 (the IDE debugger node) runs
# defaults. The asymmetry is deliberate, not an accident: node-1's 1.5-3 s timeout
# always fires before these 4-6 s ones, so it wins every election it contests, and a
# breakpoint pause on node-1 under ~4 s costs nothing while a longer one makes nodes 2/3
# elect around it within an observable 4-6 s -- under this layout the debugger itself is
# the failure injection. Heartbeats stay at the 0.5 s default everywhere, so whoever
# leads, the cluster feels live. The first cut of this stretched node-1 itself (60-120 s),
# under which node-1 could never win an election -- its own timer never fired first --
# nor hold leadership, since a 5 s heartbeat outlasts the peers' 3 s timeout.
RAFT_NODE_ID=node-2 RAFT_DB_PATH=data/node-2.db RAFT_LOG_DIR=logs \
RAFT_ADVERTISE=127.0.0.1:8002 \
RAFT_ELECTION_MIN=4 RAFT_ELECTION_MAX=6 \
RAFT_PEERS="node-1=127.0.0.1:8001,node-3=127.0.0.1:8003" \
  uv run uvicorn --factory raftkv.app:create_app --port 8002 &
RAFT_NODE_ID=node-3 RAFT_DB_PATH=data/node-3.db RAFT_LOG_DIR=logs \
RAFT_ADVERTISE=127.0.0.1:8003 \
RAFT_ELECTION_MIN=4 RAFT_ELECTION_MAX=6 \
RAFT_PEERS="node-1=127.0.0.1:8001,node-2=127.0.0.1:8002" \
  uv run uvicorn --factory raftkv.app:create_app --port 8003 &
# NOTHING above node-3 starts here, and that is the point. This used to launch node-4 and
# node-5 idle so a membership change had something to attach, which collapsed the two
# steps into one: the staged row was already populated at startup, so "a replica is a
# process, not a cluster member" was never visible as a distinction. Now the row starts
# EMPTY and the dashboard's `provision node` button starts a process on demand --
# node-4 first, then 5, 6, ... up to RAFT_PROVISION_MAX (see src/raftkv/provision.py).
# You must scale before you can attach, which is the true shape of the operation and now
# the shape the UI has.
#
# `make node-up N=4` does the same thing from a terminal, and `docker-compose.yml` still
# ships nodes 4 and 5 pre-declared because a container cannot spawn its own siblings
# (provision.py refuses there, and says why).
wait
