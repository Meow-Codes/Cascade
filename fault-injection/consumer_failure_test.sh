#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib_common.sh"

if "$REPO_ROOT/build/tools/consumer_stall_demo" > /tmp/consumer_stall.log 2>&1; then
    STALL_LINE=$(grep "stalled by backpressure" /tmp/consumer_stall.log)
    RESUME_LINE=$(grep "producer resumed" /tmp/consumer_stall.log)
    record_result "Consumer failure (stalled, never commits)" "PASS" "${STALL_LINE}; ${RESUME_LINE}"
else
    record_result "Consumer failure (stalled, never commits)" "FAIL" "see /tmp/consumer_stall.log"
fi
cat /tmp/consumer_stall.log