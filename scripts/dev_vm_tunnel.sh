#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$SCRIPT_DIR/dev_vm_common.sh"

parse_slot_only "$@"
require_local_prereqs
load_slot_vars "$SLOT_ID"
require_vm_ip

if lsof -tiTCP:"$LOCAL_DASHBOARD_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  kill "$(lsof -tiTCP:"$LOCAL_DASHBOARD_PORT" -sTCP:LISTEN)" >/dev/null 2>&1 || true
fi
if lsof -tiTCP:"$LOCAL_VITE_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  kill "$(lsof -tiTCP:"$LOCAL_VITE_PORT" -sTCP:LISTEN)" >/dev/null 2>&1 || true
fi

ssh \
  -i "$PRIVATE_KEY_PATH" \
  -o StrictHostKeyChecking=no \
  -o UserKnownHostsFile=/dev/null \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=3 \
  -N \
  -L "${LOCAL_DASHBOARD_PORT}:localhost:5001" \
  -L "${LOCAL_VITE_PORT}:localhost:8080" \
  "${LOCAL_USER}@${VM_IP}" \
  >/tmp/burla-dev-vm-tunnel-${SLOT_ID}.log 2>&1 &
NEW_TUNNEL_PID=$!

sleep 2
kill -0 "$NEW_TUNNEL_PID" >/dev/null 2>&1 || fail "Tunnel process exited immediately. See [/tmp/burla-dev-vm-tunnel-${SLOT_ID}.log]."

echo "http://localhost:${LOCAL_DASHBOARD_PORT}"
