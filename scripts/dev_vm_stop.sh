#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$SCRIPT_DIR/dev_vm_common.sh"

parse_slot_only "$@"
require_local_prereqs

load_slot_vars "$SLOT_ID"

if vm_exists "$PROJECT_ID" "${ZONE:-$DEFAULT_ZONE}" "$VM_NAME"; then
  VM_IP="$(vm_external_ip "$PROJECT_ID" "${ZONE:-$DEFAULT_ZONE}" "$VM_NAME")"
fi

if lsof -tiTCP:"$LOCAL_DASHBOARD_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  kill "$(lsof -tiTCP:"$LOCAL_DASHBOARD_PORT" -sTCP:LISTEN)" >/dev/null 2>&1 || true
fi
if lsof -tiTCP:"$LOCAL_VITE_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  kill "$(lsof -tiTCP:"$LOCAL_VITE_PORT" -sTCP:LISTEN)" >/dev/null 2>&1 || true
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

echo "Stopped VM for slot [$SLOT_ID]."
