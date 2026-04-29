#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$SCRIPT_DIR/dev_vm_common.sh"

parse_slot_only "$@"
require_local_prereqs
load_state_vars "$SLOT_ID"
validate_loaded_state_for_slot

if [[ -n "${TUNNEL_PID:-}" ]] && kill -0 "$TUNNEL_PID" >/dev/null 2>&1; then
  kill "$TUNNEL_PID" >/dev/null 2>&1 || true
  wait "$TUNNEL_PID" >/dev/null 2>&1 || true
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

PATCH_JSON="$(
  TUNNEL_PID="$NEW_TUNNEL_PID" \
  DASHBOARD_URL="http://localhost:${LOCAL_DASHBOARD_PORT}" \
  python3 - <<'PY'
import json
import os

print(
    json.dumps(
        {
            "tunnel_pid": int(os.environ["TUNNEL_PID"]),
            "dashboard_url": os.environ["DASHBOARD_URL"],
        }
    )
)
PY
)"

merge_state_json "$STATE_PATH" "$PATCH_JSON" >/dev/null
echo "http://localhost:${LOCAL_DASHBOARD_PORT}"
