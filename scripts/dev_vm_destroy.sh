#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$SCRIPT_DIR/dev_vm_common.sh"

parse_slot_and_destroy_flags "$@"
require_local_prereqs

if [[ "$DELETE_PROJECT" == "true" ]]; then
  fail "--delete-project is disabled for reusable dev VM slots. Stop VMs instead of deleting them."
fi

STATE_PATH="$(state_path_for_slot "$SLOT_ID")"
PROJECT_ID="$(project_id_for_slot "$SLOT_ID")"
ZONE="$DEFAULT_ZONE"
VM_NAME=""
TUNNEL_PID=""

if [[ -f "$STATE_PATH" ]]; then
  load_state_vars "$SLOT_ID"
  validate_loaded_state_for_slot
fi

if [[ -n "${TUNNEL_PID:-}" ]] && kill -0 "$TUNNEL_PID" >/dev/null 2>&1; then
  kill "$TUNNEL_PID" >/dev/null 2>&1 || true
  wait "$TUNNEL_PID" >/dev/null 2>&1 || true
fi

# Best-effort graceful shutdown so main_service deletes any worker nodes it
# created (no-op in local-dev, deletes real GCE workers in remote-dev).
if [[ -n "${VM_IP:-}" ]] && [[ -n "${PRIVATE_KEY_PATH:-}" ]]; then
  shutdown_cmd="curl -fsS -m 60 -X POST http://localhost:5001/v1/cluster/shutdown -H 'Content-Type: application/json' -d '{}'"
  ssh_run "$shutdown_cmd" >/dev/null 2>&1 || true
fi

if [[ -n "${VM_NAME:-}" ]] && vm_exists "$PROJECT_ID" "${ZONE:-$DEFAULT_ZONE}" "$VM_NAME"; then
  status="$(vm_status "$PROJECT_ID" "${ZONE:-$DEFAULT_ZONE}" "$VM_NAME")"
  if [[ "$status" == "RUNNING" ]]; then
    gcloud compute instances stop "$VM_NAME" --project "$PROJECT_ID" --zone "${ZONE:-$DEFAULT_ZONE}" --quiet >/dev/null
  elif [[ "$status" != "TERMINATED" ]]; then
    fail "VM [$VM_NAME] is [$status]; wait until it is RUNNING or TERMINATED before stopping slot [$SLOT_ID]."
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
