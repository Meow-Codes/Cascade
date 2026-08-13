#!/usr/bin/env bash
# Proves the C++ broker <-> Go control plane gRPC integration end to end:
# starts the real Go control plane, runs the C++ registration/heartbeat
# tool against it, and checks the tool's own pass/fail exit code.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT/go"

echo "Starting Go control plane..."
go run ./cmd/controlplaned &
GO_PID=$!
sleep 1.5 # let it bind :50051

cleanup() { kill "$GO_PID" 2>/dev/null || true; }
trap cleanup EXIT

echo "Running C++ broker_register_tool against it..."
"$REPO_ROOT/build/tools/broker_register_tool" localhost:50051 cpp-broker-integration-test

RESULT=$?
if [ $RESULT -eq 0 ]; then
    echo "PASS: C++ <-> Go control plane integration verified."
else
    echo "FAIL: integration check failed."
fi
exit $RESULT