#!/usr/bin/env bash

set -euo pipefail

BUILD_DIR="${1:-build}"

echo "========== Cascade Slow Consumer Fault Injection =========="

echo
echo "[1] Starting broker..."
"$BUILD_DIR/tools/broker_server" &
BROKER_PID=$!

sleep 1

echo
echo "[2] Starting slow consumer..."
"$BUILD_DIR/tools/slow_consumer" &
CONSUMER_PID=$!

sleep 1

echo
echo "[3] Starting fast producer..."
"$BUILD_DIR/tools/fast_producer"

echo
echo "[4] Waiting..."

sleep 5

echo
echo "[5] Stopping..."

kill $CONSUMER_PID || true
kill $BROKER_PID || true

wait $BROKER_PID 2>/dev/null || true
wait $CONSUMER_PID 2>/dev/null || true

echo
echo "Fault injection completed."