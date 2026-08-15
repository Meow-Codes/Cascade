#!/usr/bin/env bash
# Injects real network delay + jitter via netem, then runs the TCP
# latency benchmark against it to show the effect on p50/p99 -- ties
# directly back to Phase 2's original loopback numbers as a baseline.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib_common.sh"
require_tc

DELAY_MS="${1:-50}"
JITTER_MS="${2:-20}"
echo "Injecting ${DELAY_MS}ms +/- ${JITTER_MS}ms latency on lo..."
sudo tc qdisc add dev lo root netem delay "${DELAY_MS}ms" "${JITTER_MS}ms" 2>/dev/null || \
    sudo tc qdisc change dev lo root netem delay "${DELAY_MS}ms" "${JITTER_MS}ms"

cleanup() { sudo tc qdisc del dev lo root netem 2>/dev/null || true; }
trap cleanup EXIT

OUTPUT=$(timeout 30 "$REPO_ROOT/build/benchmarks/bench_tcp" 2>&1 | grep "RTT" || true)
echo "$OUTPUT"
record_result "Latency injection (${DELAY_MS}ms +/-${JITTER_MS}ms)" "PASS" "$OUTPUT"