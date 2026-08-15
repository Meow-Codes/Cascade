#!/usr/bin/env bash
# Shared helpers for all fault-injection scripts.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_FILE="$REPO_ROOT/fault-injection/RESULTS.md"

record_result() {
    local scenario="$1" status="$2" detail="$3"
    echo "| $scenario | $status | $detail |" >> "$RESULTS_FILE"
}

has_tc() { command -v tc >/dev/null 2>&1; }
require_tc() {
    if ! has_tc; then
        echo "SKIP: 'tc' (iproute2) not found -- install with: sudo apt install iproute2"
        exit 77 # standard "skipped" exit code convention
    fi
}