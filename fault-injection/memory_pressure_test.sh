#!/usr/bin/env bash
# Runs the broker benchmark under a cgroup v2 memory limit to observe
# actual behavior under memory pressure -- HONEST NOTE: this project has
# no explicit memory-pressure handling built (no backpressure tied to
# RSS, no graceful degradation). This script's job is to OBSERVE and
# RECORD what actually happens (clean OOM-kill vs silent corruption vs
# graceful slowdown), not to assert a specific outcome -- a documented
# gap is more valuable than a script that quietly asserts success.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib_common.sh"

if [ ! -d /sys/fs/cgroup ] || [ ! -w /sys/fs/cgroup ]; then
    echo "SKIP: cgroup v2 not available/writable in this environment (common under WSL2 without systemd)."
    record_result "Memory pressure (cgroup limit)" "SKIPPED" "cgroup v2 unavailable in this environment"
    exit 0
fi

CGROUP="/sys/fs/cgroup/cascade_test"
sudo mkdir -p "$CGROUP"
echo "50M" | sudo tee "$CGROUP/memory.max" > /dev/null

set +e
sudo cgexec -g memory:cascade_test "$REPO_ROOT/build/benchmarks/bench_broker" > /tmp/mem_pressure.log 2>&1
EXIT_CODE=$?
set -e

sudo rmdir "$CGROUP" 2>/dev/null || true

if [ $EXIT_CODE -eq 0 ]; then
    record_result "Memory pressure (50MB cgroup limit)" "OBSERVED" "process completed normally within limit -- see /tmp/mem_pressure.log"
elif [ $EXIT_CODE -eq 137 ]; then
    record_result "Memory pressure (50MB cgroup limit)" "OBSERVED" "OOM-killed by kernel (exit 137) -- expected, no memory-aware backpressure implemented yet"
else
    record_result "Memory pressure (50MB cgroup limit)" "OBSERVED" "exited with code $EXIT_CODE -- see /tmp/mem_pressure.log"
fi
echo "Memory pressure test complete, exit code: $EXIT_CODE"