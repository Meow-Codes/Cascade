#!/usr/bin/env bash
# Wraps the existing Go raftnode kill-leader test into the standardized
# suite -- reuses TestCluster_NewLeaderElectedAfterLeaderShutdownNoDataLoss
# rather than reimplementing the scenario in bash.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib_common.sh"

cd "$REPO_ROOT/go"
if go test ./raftnode_test/ -run TestCluster_NewLeaderElectedAfterLeaderShutdownNoDataLoss -v > /tmp/replica_crash.log 2>&1; then
    record_result "Replica crash (Raft leader kill)" "PASS" "new leader elected, zero committed data loss (see raftnode_test)"
else
    record_result "Replica crash (Raft leader kill)" "FAIL" "see /tmp/replica_crash.log"
fi
cat /tmp/replica_crash.log