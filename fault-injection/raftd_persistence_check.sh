#!/usr/bin/env bash
# Non-destructive persistence checker for a running (or restarted) raftd cluster.
# usage: raftd_persistence_check.sh snapshot | verify | brokers | redirect | dup <topic>
set -uo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CTL="$REPO_ROOT/build/cascadectl"
ENDPOINTS="127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053"
BASELINE="${BASELINE:-/tmp/cascade_persist_baseline.txt}"

metric() { # <node#> <metric name>
    curl -sf --max-time 1 "http://127.0.0.1:920$1/metrics" 2>/dev/null | awk -v m="$2" '
        index($1, m) == 1 && (length($1) == length(m) || substr($1, length(m)+1, 1) == "{") { print $NF; exit }'
}
node_up() { [ -n "$(metric "$1" cascade_raft_term)" ]; }

snapshot() {
    local tmp
    tmp=$(mktemp "${BASELINE}.XXXXXX") || { echo "cannot create temporary baseline"; exit 1; }
    if ! "$CTL" -addr "$ENDPOINTS" topics | sort > "$tmp"; then
        rm -f "$tmp"
        echo "cannot list topics"
        exit 1
    fi
    if ! mv "$tmp" "$BASELINE"; then
        rm -f "$tmp"
        echo "cannot save baseline to $BASELINE"
        exit 1
    fi
    echo "baseline saved: $(wc -l < "$BASELINE") topics -> $BASELINE"
}

# Every baseline topic must be present on EVERY live node, asked individually.
# Reads are served from each node's local FSM, so this checks what each node
# recovered from its own disk, not what the leader says.
verify() {
    [ -f "$BASELINE" ] || { echo "no baseline; run 'snapshot' first"; exit 1; }
    local rc=0 n got missing ok live queried
    live=0
    for n in 1 2 3; do
        if ! node_up "$n"; then echo "n$n: DOWN (skipped)"; continue; fi
        live=$((live + 1))
        ok=0; queried=0; missing=""
        for _ in $(seq 1 50); do # allow up to ~10s of replication lag
            if ! got=$("$CTL" -addr "127.0.0.1:5005$n" topics 2>/dev/null); then
                sleep 0.2
                continue
            fi
            queried=1
            got=$(printf '%s\n' "$got" | sort)
            missing=$(comm -23 "$BASELINE" <(printf '%s\n' "$got"))
            if [ -z "$missing" ]; then ok=1; break; fi
            sleep 0.2
        done
        printf 'n%s: term=%s commit=%s applied=%s leader=%s  ' "$n" \
            "$(metric "$n" cascade_raft_term)" "$(metric "$n" cascade_raft_commit_index)" \
            "$(metric "$n" cascade_raft_applied_index)" "$(metric "$n" cascade_raft_is_leader)"
        if [ "$ok" = 1 ]; then echo "PASS (all $(wc -l < "$BASELINE") baseline topics present)"
        elif [ "$queried" = 0 ]; then echo "FAIL (could not list topics)"; rc=1
        else printf 'FAIL, missing:\n%s\n' "$missing"; rc=1; fi
    done
    if [ "$live" = 0 ]; then echo "FAIL: no live nodes to verify"; rc=1; fi
    exit $rc
}

brokers() { "$CTL" -addr "$ENDPOINTS" brokers; }

# A write sent to a FOLLOWER must be redirected: proves the replicated
# node/gRPC-address registry survived the restart too.
redirect() {
    local f="" n
    for n in 1 2 3; do
        if node_up "$n" && [ "$(metric "$n" cascade_raft_is_leader)" = "0" ]; then f=$n; break; fi
    done
    [ -n "$f" ] || { echo "no live follower found"; exit 1; }
    if "$CTL" -addr "127.0.0.1:5005$f" create-topic "persist-redirect-$(date +%s%N)-$$" 1 >/dev/null; then
        echo "PASS: write via follower n$f was redirected to the leader"
    else
        echo "FAIL: write via follower n$f failed"; exit 1
    fi
}

# Write-path proof: re-creating an existing topic must be REJECTED.
dup() {
    local topic="$1"
    if ! grep -q "^name=$topic " "$BASELINE" 2>/dev/null; then
        echo "SKIP: '$topic' is not in the baseline, so it didn't exist before the restart and this check proves nothing."
        echo "      Create it, run 'snapshot', restart, then re-run."
        exit 3
    fi
    local out; out=$("$CTL" -addr "$ENDPOINTS" create-topic "$topic" 1 2>&1)
    if echo "$out" | grep -q "already exists"; then echo "PASS: '$topic' still exists (re-create rejected)"
    else echo "FAIL: '$topic' was not rejected: $out"; exit 1; fi
}

case "${1:-}" in
    snapshot) snapshot ;; verify) verify ;; brokers) brokers ;; redirect) redirect ;; dup) dup "${2:?topic name}" ;;
    *) echo "usage: $0 snapshot | verify | brokers | redirect | dup <topic>"; exit 2 ;;
esac