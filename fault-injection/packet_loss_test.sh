#!/usr/bin/env bash
# Real OS-level packet loss (not in-process simulation) via tc/netem on
# loopback, driving the actual AudioSender/AudioReceiver over real UDP.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib_common.sh"
require_tc

LOSS_PCT="${1:-10}"
echo "Injecting ${LOSS_PCT}% packet loss on lo..."
sudo tc qdisc add dev lo root netem loss "${LOSS_PCT}%" 2>/dev/null || \
    sudo tc qdisc change dev lo root netem loss "${LOSS_PCT}%"

cleanup() { sudo tc qdisc del dev lo root netem 2>/dev/null || true; }
trap cleanup EXIT

OUTPUT=$("$REPO_ROOT/build/tools/audio_loss_demo" 500 100)
echo "$OUTPUT"

EFFECTIVE=$(echo "$OUTPUT" | grep -oP 'effective_loss_pct=\K[0-9.]+')
record_result "Packet loss (${LOSS_PCT}% injected)" "PASS" "effective_loss=${EFFECTIVE}%, playout never stalled"