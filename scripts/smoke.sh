#!/usr/bin/env bash
# End-to-end smoke test against a running `docker compose up` cluster:
# leader election -> write -> replication -> leader kill -> failover -> convergence.
set -euo pipefail
cd "$(dirname "$0")/.."
NODES=(localhost:8001 localhost:8002 localhost:8003)

state() { curl -sf --max-time 2 "http://$1/state" 2>/dev/null || true; }
field() { python3 -c "import sys, json; print(json.load(sys.stdin).get('$1') or '')" 2>/dev/null || true; }

leader_addr() {
  for addr in "${NODES[@]}"; do
    if [ "$(state "$addr" | field role)" = "leader" ]; then echo "$addr"; return 0; fi
  done
  return 1
}

check_value() { [ "$(curl -sf "http://$1/kv/$2" 2>/dev/null | field value)" = "$3" ]; }

wait_for() {  # wait_for <description> <function> [args...]
  local desc=$1; shift
  for _ in $(seq 1 60); do
    if "$@" >/dev/null 2>&1; then echo "  ok: $desc"; return 0; fi
    sleep 0.5
  done
  echo "FAIL: timed out waiting for $desc" >&2
  exit 1
}

echo "1/5 waiting for a leader"
LEADER=""
for _ in $(seq 1 60); do
  LEADER=$(leader_addr || true)
  [ -n "$LEADER" ] && break
  sleep 0.5
done
[ -n "$LEADER" ] || { echo "FAIL: no leader elected" >&2; exit 1; }
echo "  ok: leader $LEADER"

echo "2/5 writing smoke=v1 via leader $LEADER"
curl -sf -X PUT "http://$LEADER/kv/smoke" -H 'content-type: application/json' \
  -d '{"value": "v1"}' >/dev/null

echo "3/5 verifying replication to every node"
for addr in "${NODES[@]}"; do
  wait_for "smoke=v1 on $addr" check_value "$addr" smoke v1
done

echo "4/5 killing the leader and writing through the new one"
case "$LEADER" in
  localhost:8001) SVC=raft-node-1 ;;
  localhost:8002) SVC=raft-node-2 ;;
  localhost:8003) SVC=raft-node-3 ;;
  *) echo "FAIL: unmapped leader address $LEADER" >&2; exit 1 ;;
esac
docker compose stop -t 1 "$SVC" >/dev/null
NEW_LEADER=""
for _ in $(seq 1 60); do
  CAND=$(leader_addr || true)
  if [ -n "$CAND" ] && [ "$CAND" != "$LEADER" ]; then NEW_LEADER=$CAND; break; fi
  sleep 0.5
done
[ -n "$NEW_LEADER" ] || { echo "FAIL: no failover leader" >&2; exit 1; }
echo "  ok: new leader $NEW_LEADER"
curl -sf -X PUT "http://$NEW_LEADER/kv/smoke" -H 'content-type: application/json' \
  -d '{"value": "v2"}' >/dev/null

echo "5/5 restarting old leader and verifying convergence"
docker compose start "$SVC" >/dev/null
for addr in "${NODES[@]}"; do
  wait_for "smoke=v2 on $addr" check_value "$addr" smoke v2
done
echo "SMOKE PASS"
