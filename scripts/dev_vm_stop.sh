#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$SCRIPT_DIR/dev_vm_common.sh"

parse_slot_only "$@"
require_local_prereqs

STATE_PATH="$(state_path_for_slot "$SLOT_ID")"
PROJECT_ID="$(project_id_for_slot "$SLOT_ID")"
ZONE="$DEFAULT_ZONE"
VM_NAME="$(vm_name_for_slot "$SLOT_ID")"
TUNNEL_PID=""
VM_IP=""
PRIVATE_KEY_PATH="$(private_key_path_for_slot "$SLOT_ID")"
LOCAL_USER="$(id -un)"

if [[ -f "$STATE_PATH" ]]; then
  load_state_vars "$SLOT_ID"
  PROJECT_ID="$(project_id_for_slot "$SLOT_ID")"
  ZONE="${ZONE:-$DEFAULT_ZONE}"
  VM_NAME="$(vm_name_for_slot "$SLOT_ID")"
  PRIVATE_KEY_PATH="${PRIVATE_KEY_PATH:-$(private_key_path_for_slot "$SLOT_ID")}"
  LOCAL_USER="${LOCAL_USER:-$(id -un)}"
fi

if vm_exists "$PROJECT_ID" "${ZONE:-$DEFAULT_ZONE}" "$VM_NAME"; then
  VM_IP="$(vm_external_ip "$PROJECT_ID" "${ZONE:-$DEFAULT_ZONE}" "$VM_NAME")"
fi

if [[ -n "${TUNNEL_PID:-}" ]] && kill -0 "$TUNNEL_PID" >/dev/null 2>&1; then
  kill "$TUNNEL_PID" >/dev/null 2>&1 || true
  wait "$TUNNEL_PID" >/dev/null 2>&1 || true
fi

# Let remote-dev clean up worker VMs before the reusable dev VM stops.
if [[ -n "${VM_IP:-}" ]] && [[ -n "${PRIVATE_KEY_PATH:-}" ]]; then
  shutdown_cmd="curl -fsS -m 60 -X POST http://localhost:5001/v1/cluster/shutdown -H 'Content-Type: application/json' -d '{}'"
  ssh_run "$shutdown_cmd" >/dev/null 2>&1 || true
fi

if vm_exists "$PROJECT_ID" "${ZONE:-$DEFAULT_ZONE}" "$VM_NAME"; then
  status="$(vm_status "$PROJECT_ID" "${ZONE:-$DEFAULT_ZONE}" "$VM_NAME")"
  if [[ "$status" == "RUNNING" ]]; then
    gcloud compute instances stop "$VM_NAME" --project "$PROJECT_ID" --zone "${ZONE:-$DEFAULT_ZONE}" --quiet >/dev/null
  fi
fi

if [[ -f "$STATE_PATH" ]]; then
  PATCH_JSON="$(
    python3 - <<'PY'
import json
from datetime import datetime, timezone

print(json.dumps({"tunnel_pid": None, "last_stopped_at": datetime.now(timezone.utc).isoformat()}))
PY
  )"
  merge_state_json "$STATE_PATH" "$PATCH_JSON" >/dev/null
fi

echo "Stopped VM for slot [$SLOT_ID]."
