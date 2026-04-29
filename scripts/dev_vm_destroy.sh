#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$SCRIPT_DIR/dev_vm_common.sh"

parse_slot_and_destroy_flags "$@"
require_local_prereqs

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

if [[ -n "${VM_NAME:-}" ]] && [[ -n "${VM_IP:-}" ]] && [[ -n "${PRIVATE_KEY_PATH:-}" ]] && ssh_run "CLOUDSDK_CORE_PROJECT='$PROJECT_ID' gcloud compute instances delete '$VM_NAME' --zone '${ZONE:-$DEFAULT_ZONE}' --quiet" >/dev/null 2>&1; then
  :
elif [[ -n "${VM_NAME:-}" ]] && vm_exists "$PROJECT_ID" "${ZONE:-$DEFAULT_ZONE}" "$VM_NAME"; then
  delete_output="$(
    gcloud compute instances delete "$VM_NAME" --project "$PROJECT_ID" --zone "${ZONE:-$DEFAULT_ZONE}" --quiet 2>&1
  )" || {
    if [[ "$delete_output" == *"was not found"* ]]; then
      :
    else
      echo "$delete_output" >&2
      exit 1
    fi
  }
fi

rm -f "$STATE_PATH"

if [[ "$DELETE_PROJECT" == "true" ]] && project_exists "$PROJECT_ID"; then
  gcloud projects delete "$PROJECT_ID" --quiet >/dev/null
fi

echo "Destroyed resources for slot [$SLOT_ID]."
