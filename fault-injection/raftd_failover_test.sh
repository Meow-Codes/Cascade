#!/usr/bin/env bash
# kill -9 the Raft leader of a REAL 3-process raftd cluster while a C++ broker
# is heartbeating against it, then check: new leader elected, no committed
# data lost, writes accepted, the C++ client failed over, and the killed
# node recovers from its own disk and catches up.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib_common.sh"
source "$(dirname "${BASH_SOURCE[0]}")/lib_raftd.sh"

SCENARIO="Raft cluster failover (raftd, kill -9 leader)"
BROKER_PID=""
cleanup() {
    [ -n "$BROKER_PID" ] && kill "$BROKER_PID" 2>/dev/null || true
    stop_cluster
}
trap cleanup EXIT
fail() { record_result "$SCENARIO" "FAIL" "$1"; echo "FAIL: $1"; echo "logs: $CLUSTER_DIR (removed on exit; rerun with CLUSTER_DIR set to keep)"; exit 1; }

build_go_tools
start_cluster || fail "cluster did not form (3 voters)"
echo "cluster up: leader = n$(leader_node)"

# C++ broker registers + heartbeats against all three endpoints
"$REPO_ROOT/build/tools/broker_demo" 9100 "$GRPC_ENDPOINTS" cpp-broker-failover > "$CLUSTER_DIR/broker.log" 2>&1 &
BROKER_PID=$!
sleep 3
"$CTL_BIN" -addr "$GRPC_ENDPOINTS" brokers | grep -q 'id=cpp-broker-failover .*alive=true' \
    || fail "C++ broker did not register"
"$CTL_BIN" -addr "$GRPC_ENDPOINTS" topics | grep -q 'name=demo-' \
    || fail "C++ broker's topic was not replicated"

# Leader redirect: a client that only knows a FOLLOWER must still succeed
OLD_LEADER=$(leader_node) || fail "no leader"
FOLLOWER=""
for n in 1 2 3; do [ "$n" != "$OLD_LEADER" ] && FOLLOWER=$n && break; done
"$CTL_BIN" -addr "127.0.0.1:5005$FOLLOWER" create-topic via-follower 1 >/dev/null \
    || fail "write via follower was not redirected to the leader"

# Kill the leader
KILL_MS=$(date +%s%3N)
kill -9 "${NODE_PID[$OLD_LEADER]}"
wait "${NODE_PID[$OLD_LEADER]}" 2>/dev/null || true
echo "killed leader n$OLD_LEADER"

NEW_LEADER=""
for _ in $(seq 1 150); do
    NEW_LEADER=$(leader_node || true)
    [ -n "$NEW_LEADER" ] && [ "$NEW_LEADER" != "$OLD_LEADER" ] && break
    NEW_LEADER=""
    sleep 0.1
done
[ -n "$NEW_LEADER" ] || fail "no new leader within 15s"
ELECTION_MS=$(( $(date +%s%3N) - KILL_MS ))
echo "new leader n$NEW_LEADER after ${ELECTION_MS}ms"

# Writes work, earlier committed topics survived
"$CTL_BIN" -addr "$GRPC_ENDPOINTS" create-topic post-failover 1 >/dev/null || fail "write rejected after failover"
TOPICS=$("$CTL_BIN" -addr "$GRPC_ENDPOINTS" topics)
for want in 'name=demo-' 'name=via-follower' 'name=post-failover'; do
    echo "$TOPICS" | grep -q "$want" || fail "topic matching '$want' missing after failover (data loss)"
done

# The C++ client must have failed over and kept heartbeating
sleep 4
BROKERS=$("$CTL_BIN" -addr "$GRPC_ENDPOINTS" brokers)
echo "$BROKERS" | grep -q 'id=cpp-broker-failover .*alive=true' || fail "C++ broker not alive after failover"
LAST_HB=$(echo "$BROKERS" | sed -n 's/^id=cpp-broker-failover .*last_heartbeat_unix_ms=\([0-9]*\).*/\1/p')
{ [ -n "$LAST_HB" ] && [ "$LAST_HB" -gt "$KILL_MS" ]; } || fail "C++ broker stopped heartbeating after leader loss"

# Restart the killed node from its own data dir (no -bootstrap / -join)
start_node "$OLD_LEADER"
CAUGHT_UP=0
for _ in $(seq 1 150); do
    L=$(leader_node || true)
    if [ -n "$L" ]; then
        A=$(metric "$OLD_LEADER" cascade_raft_applied_index || true)
        C=$(metric "$L" cascade_raft_commit_index || true)
        if [ -n "$A" ] && [ -n "$C" ] && [ $((C - A)) -le 5 ]; then CAUGHT_UP=1; break; fi
    fi
    sleep 0.2
done
[ "$CAUGHT_UP" = "1" ] || fail "restarted node n$OLD_LEADER did not catch up"
grep -q "recovered raft state" "$CLUSTER_DIR/n$OLD_LEADER.log" || fail "restarted node did not recover from disk"
"$CTL_BIN" -addr "127.0.0.1:5005$OLD_LEADER" topics | grep -q 'name=post-failover' \
    || fail "restarted node is missing a topic committed while it was down"

record_result "$SCENARIO" "PASS" "leader n$OLD_LEADER -> n$NEW_LEADER in ${ELECTION_MS}ms; C++ broker kept heartbeating; no topic lost; killed node recovered from disk and caught up"
echo "PASS: $SCENARIO (election took ${ELECTION_MS}ms)"