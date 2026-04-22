#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$SCRIPT_DIR/dev_vm_common.sh"

parse_agent_and_destroy_flags "$@"
require_local_prereqs
require_agent_worktree_context "$AGENT_ID"

STATE_PATH="$(state_path_for_agent "$AGENT_ID")"
PROJECT_ID="$(project_id_for_agent "$AGENT_ID")"
ZONE="$DEFAULT_ZONE"
VM_NAME=""
TUNNEL_PID=""

if [[ -f "$STATE_PATH" ]]; then
  load_state_vars "$AGENT_ID"
  validate_loaded_state_against_current_context
fi

if [[ -n "${TUNNEL_PID:-}" ]] && kill -0 "$TUNNEL_PID" >/dev/null 2>&1; then
  kill "$TUNNEL_PID" >/dev/null 2>&1 || true
  wait "$TUNNEL_PID" >/dev/null 2>&1 || true
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

echo "Destroyed resources for agent [$AGENT_ID]."
