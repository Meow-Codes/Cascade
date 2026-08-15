#!/usr/bin/env bash
# Wraps the existing Phase 3 kill_wal_test.sh into the standardized
# fault-injection suite format (multiple runs, aggregated result).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib_common.sh"

RUNS="${1:-10}"
FAIL_COUNT=0
for i in $(seq 1 "$RUNS"); do
    if ! "$REPO_ROOT/fault-injection/kill_wal_test.sh" "$REPO_ROOT/build" > /tmp/kill_run_$i.log 2>&1; then
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
done

if [ "$FAIL_COUNT" -eq 0 ]; then
    record_result "Broker crash (kill -9 mid-write, WAL)" "PASS" "${RUNS}/${RUNS} runs recovered with zero corruption"
else
    record_result "Broker crash (kill -9 mid-write, WAL)" "FAIL" "${FAIL_COUNT}/${RUNS} runs failed -- see /tmp/kill_run_*.log"
fi
echo "Broker crash test: $((RUNS - FAIL_COUNT))/${RUNS} passed"