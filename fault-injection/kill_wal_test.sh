#!/usr/bin/env bash
# Fault injection: kill -9 a writer mid-append, then verify the WAL
# recovers cleanly with no corruption, per Phase 3's Definition of Done.
set -euo pipefail

BUILD_DIR="${1:-build}"
DIR="/tmp/cascade_kill_test_$$"
rm -rf "$DIR"
mkdir -p "$DIR"

echo "Starting writer against $DIR ..."
"$BUILD_DIR/tools/wal_kill_writer" "$DIR" &
PID=$!

# Randomize the kill delay a little across runs so repeated CI runs land
# at different points in the write cycle instead of always the same spot.
SLEEP_MS=$(( (RANDOM % 300) + 50 ))
python3 -c "import time; time.sleep($SLEEP_MS/1000)"

echo "Killing PID $PID (SIGKILL) after ${SLEEP_MS}ms..."
kill -9 "$PID"
wait "$PID" 2>/dev/null || true

echo "Verifying recovery..."
"$BUILD_DIR/tools/wal_verifier" "$DIR"
RESULT=$?

rm -rf "$DIR"
exit $RESULT