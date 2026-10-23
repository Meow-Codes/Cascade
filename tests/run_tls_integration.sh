#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$1"
TEST_BINARY="$2"
TEST_FILTER="ControlPlaneClientTLS.ConnectsSuccessfullyWithCorrectCACert"
SERVER_BINARY="$PROJECT_ROOT/build/controlplaned"
SERVER_PID=""

cleanup() {
    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}

trap cleanup EXIT INT TERM

cd "$PROJECT_ROOT/go"
go build -o "$SERVER_BINARY" ./cmd/controlplaned

"$SERVER_BINARY" \
    -cert "$PROJECT_ROOT/certs/server-cert.pem" \
    -key "$PROJECT_ROOT/certs/server-key.pem" &
SERVER_PID=$!

ready=0

for _ in $(seq 1 100); do
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "TLS control-plane server exited unexpectedly"
        wait "$SERVER_PID" || true
        exit 1
    fi

    if (echo > /dev/tcp/127.0.0.1/50051) >/dev/null 2>&1; then
        ready=1
        break
    fi

    sleep 0.1
done

if [[ "$ready" -ne 1 ]]; then
    echo "TLS control-plane server did not become ready"
    exit 1
fi

"$TEST_BINARY" --gtest_filter="$TEST_FILTER"
