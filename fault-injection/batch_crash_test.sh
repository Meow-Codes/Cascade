#!/usr/bin/env bash
# kill -9 a batch writer at random points; after recovery every ACKNOWLEDGED
# batch must be present and every recovered record intact.
# NOTE: a process kill leaves the page cache intact, so this validates
# recovery/CRC behavior and "no acked batch lost across a process crash".
# It cannot validate msync itself (that needs power-loss testing).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib_common.sh"

RUNS="${1:-10}"
BATCH="${2:-64}"
FAILS=0

for i in $(seq 1 "$RUNS"); do
    WORK=$(mktemp -d /tmp/cascade_batch_kill_XXXXXX)
    "$REPO_ROOT/build/tools/wal_batch_writer" "$WORK/data" "$BATCH" "$WORK/ack.bin" >/dev/null 2>&1 &
    PID=$!
    sleep "0.$(printf '%03d' $(( (RANDOM % 300) + 60 )))"
    kill -9 "$PID"
    wait "$PID" 2>/dev/null || true

    if ! "$REPO_ROOT/build/tools/wal_verifier" "$WORK/data" "$WORK/ack.bin" "$BATCH" > "$WORK/verify.log" 2>&1; then
        FAILS=$((FAILS + 1))
        cat "$WORK/verify.log"
    else
        grep -E 'Recovered next_offset|Acknowledged|tail is' "$WORK/verify.log" | tr '\n' ' '; echo
    fi
    rm -rf "$WORK"
done

if [ "$FAILS" -eq 0 ]; then
    record_result "Batch publish crash (kill -9, batch=$BATCH)" "PASS" "${RUNS}/${RUNS} runs: every acknowledged batch recovered, zero corrupt records; unacked tails may be partial (batches are not atomic)"
else
    record_result "Batch publish crash (kill -9, batch=$BATCH)" "FAIL" "${FAILS}/${RUNS} runs failed"
fi
echo "Batch crash test: $((RUNS - FAILS))/${RUNS} passed"
[ "$FAILS" -eq 0 ]