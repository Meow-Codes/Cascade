#!/usr/bin/env bash
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "| Scenario | Result | Detail |" > "$SCRIPT_DIR/RESULTS.md"
echo "|---|---|---|" >> "$SCRIPT_DIR/RESULTS.md"

echo "=== Broker crash (WAL kill -9) ==="
bash "$SCRIPT_DIR/broker_crash_test.sh" 10

echo "=== Replica crash (Raft leader kill) ==="
bash "$SCRIPT_DIR/replica_crash_test.sh"

echo "=== Network partition ==="
bash "$SCRIPT_DIR/partition_test.sh"

echo "=== Consumer failure ==="
bash "$SCRIPT_DIR/consumer_failure_test.sh"

echo "=== Packet loss (requires sudo for tc) ==="
bash "$SCRIPT_DIR/packet_loss_test.sh" 10 || echo "packet loss test skipped/failed -- see above"

echo "=== Latency injection (requires sudo for tc) ==="
bash "$SCRIPT_DIR/latency_jitter_test.sh" 50 20 || echo "latency test skipped/failed -- see above"

echo "=== Memory pressure (requires cgroup v2) ==="
bash "$SCRIPT_DIR/memory_pressure_test.sh" || echo "memory pressure test skipped -- see above"

echo ""
echo "Results written to $SCRIPT_DIR/RESULTS.md"
cat "$SCRIPT_DIR/RESULTS.md"