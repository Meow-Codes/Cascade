#!/usr/bin/env bash
# Helpers for running a local 3-node raftd cluster.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RAFTD_BIN="$REPO_ROOT/build/raftd"
CTL_BIN="$REPO_ROOT/build/cascadectl"
CLUSTER_DIR="${CLUSTER_DIR:-/tmp/cascade_raftd_$$}"
GRPC_ENDPOINTS="127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053"

declare -A NODE_PID

build_go_tools() {
    (cd "$REPO_ROOT/go" && go build -o "$RAFTD_BIN" ./cmd/raftd && go build -o "$CTL_BIN" ./cmd/cascadectl)
}

# start_node <1|2|3> [extra raftd flags]   (ports: raft 700N, grpc 5005N, metrics 920N)
start_node() {
    local n="$1"; shift
    mkdir -p "$CLUSTER_DIR/n$n"
    "$RAFTD_BIN" -id "n$n" -raft-addr "127.0.0.1:700$n" -grpc-addr "127.0.0.1:5005$n" \
        -metrics-addr "127.0.0.1:920$n" -data-dir "$CLUSTER_DIR/n$n" "$@" \
        > "$CLUSTER_DIR/n$n.log" 2>&1 &
    NODE_PID[$n]=$!
}

# metric <node#> <metric name>  -> value of the first series with that name
metric() {
    curl -sf --max-time 1 "http://127.0.0.1:920$1/metrics" 2>/dev/null | awk -v m="$2" '
        index($1, m) == 1 && (length($1) == length(m) || substr($1, length(m)+1, 1) == "{") { print $NF; exit }'
}

# prints the node number of the live leader, or fails
leader_node() {
    local n
    for n in 1 2 3; do
        [ -n "${NODE_PID[$n]:-}" ] && kill -0 "${NODE_PID[$n]}" 2>/dev/null || continue
        if [ "$(metric "$n" cascade_raft_is_leader)" = "1" ]; then echo "$n"; return 0; fi
    done
    return 1
}

wait_for_leader() { # <timeout secs>
    local deadline=$((SECONDS + $1))
    while [ $SECONDS -lt $deadline ]; do
        leader_node >/dev/null && return 0
        sleep 0.2
    done
    return 1
}

wait_for_voters() { # <count> <timeout secs>
    local want=$1 deadline=$((SECONDS + $2)) l
    while [ $SECONDS -lt $deadline ]; do
        if l=$(leader_node) && [ "$(metric "$l" cascade_raft_num_peers)" = "$((want - 1))" ]; then return 0; fi
        sleep 0.3
    done
    return 1
}

start_cluster() {
    start_node 1 -bootstrap
    wait_for_leader 15 || return 1
    start_node 2 -join 127.0.0.1:50051
    start_node 3 -join 127.0.0.1:50051
    wait_for_voters 3 30
}

stop_cluster() {
    local n pid
    for n in 1 2 3; do
        pid="${NODE_PID[$n]:-}"
        [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    rm -rf "$CLUSTER_DIR"
}