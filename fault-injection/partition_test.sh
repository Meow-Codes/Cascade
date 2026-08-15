#!/usr/bin/env bash
# Wraps the existing Go raftnode partition test -- real split-brain
# prevention proof, reused rather than reimplemented at the bash level.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib_common.sh"

cd "$REPO_ROOT/go"
if go test ./raftnode_test/ -run TestPartition_IsolatedLeaderCannotCommitMajorityMakesProgress -v > /tmp/partition.log 2>&1; then
    record_result "Network partition (split-brain)" "PASS" "isolated leader rejected write; majority made progress; converged after heal"
else
    record_result "Network partition (split-brain)" "FAIL" "see /tmp/partition.log"
fi
cat /tmp/partition.log