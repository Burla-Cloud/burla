#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$SCRIPT_DIR/dev_vm_common.sh"

parse_slot_only "$@"
require_local_prereqs
ensure_state_dir
ensure_slot_keypair "$SLOT_ID"

STATE_PATH="$(state_path_for_slot "$SLOT_ID")"
PROJECT_ID="$(project_id_for_slot "$SLOT_ID")"
ZONE="$DEFAULT_ZONE"
TIMESTAMP="$(timestamp_utc)"
NEW_VM_NAME="$(vm_name_for_slot "$SLOT_ID" "$TIMESTAMP")"
VM_NAME="$NEW_VM_NAME"
LOCAL_DASHBOARD_PORT="$(dashboard_port_for_slot "$SLOT_ID")"
LOCAL_VITE_PORT="$(vite_port_for_slot "$SLOT_ID")"
REMOTE_REPO_DIR="$DEFAULT_REMOTE_REPO_DIR"
REMOTE_LOG_PATH="$DEFAULT_REMOTE_LOG_PATH"
REMOTE_TMUX_SESSION="burla-dev-${SLOT_ID}"
LOCAL_USER="$(id -un)"
PRIVATE_KEY_PATH="$(private_key_path_for_slot "$SLOT_ID")"
PUBLIC_KEY_PATH="$(public_key_path_for_slot "$SLOT_ID")"
SSH_KEY_VALUE="$(python3 - "$PUBLIC_KEY_PATH" "$LOCAL_USER" <<'PY'
import pathlib
import sys

public_key = pathlib.Path(sys.argv[1]).read_text().strip()
username = sys.argv[2]
print(f"{username}:{public_key}")
PY
)"

if [[ -f "$STATE_PATH" ]]; then
  load_state_vars "$SLOT_ID"
  validate_loaded_state_for_slot
  existing_status="$(vm_status "$PROJECT_ID" "${ZONE:-$DEFAULT_ZONE}" "$VM_NAME")"
  if [[ "$existing_status" == "TERMINATED" ]]; then
    gcloud compute instances start "$VM_NAME" --project "$PROJECT_ID" --zone "${ZONE:-$DEFAULT_ZONE}" --quiet >/dev/null
    VM_IP="$(vm_external_ip "$PROJECT_ID" "${ZONE:-$DEFAULT_ZONE}" "$VM_NAME")"
    PATCH_JSON="$(
      VM_IP="$VM_IP" \
      python3 - <<'PY'
import json
import os

print(json.dumps({"vm_ip": os.environ["VM_IP"], "tunnel_pid": None}))
PY
    )"
    merge_state_json "$STATE_PATH" "$PATCH_JSON" >/dev/null
    print_state_file "$STATE_PATH"
    exit 0
  fi
  if [[ "$existing_status" == "RUNNING" ]]; then
    print_state_file "$STATE_PATH"
    exit 0
  fi
  if [[ -n "$existing_status" ]]; then
    fail "VM [$VM_NAME] is [$existing_status]; wait until it is RUNNING or TERMINATED before reusing slot [$SLOT_ID]."
  fi
  VM_NAME="$NEW_VM_NAME"
  VM_IP=""
fi

ensure_slot_project "$PROJECT_ID"
gcloud services enable run.googleapis.com --project "$PROJECT_ID" >/dev/null
if ! main_service_account_exists "$PROJECT_ID"; then
  fail "Slot [$SLOT_ID] is not prepared. Run [scripts/dev_vm_prepare_slot.sh --slot $SLOT_ID] first."
fi

ensure_artifact_repositories "$PROJECT_ID"
ensure_artifact_writer_role "$PROJECT_ID"

STARTUP_SCRIPT="$(mktemp)"
trap 'rm -f "$STARTUP_SCRIPT"' EXIT
render_startup_script "$LOCAL_USER" "$STARTUP_SCRIPT"

gcloud compute instances create "$VM_NAME" \
  --project "$PROJECT_ID" \
  --zone "$ZONE" \
  --machine-type "$DEFAULT_MACHINE_TYPE" \
  --image-project "$DEFAULT_IMAGE_PROJECT" \
  --image-family "$DEFAULT_IMAGE_FAMILY" \
  --boot-disk-size 100GB \
  --boot-disk-type pd-balanced \
  --service-account "$(main_service_service_account "$PROJECT_ID")" \
  --scopes https://www.googleapis.com/auth/cloud-platform \
  --labels "burla-agent-id=${AGENT_ID},burla-runtime=ephemeral-dev,burla-ephemeral=true" \
  --metadata "enable-oslogin=FALSE,ssh-keys=${SSH_KEY_VALUE}" \
  --metadata-from-file startup-script="$STARTUP_SCRIPT" \
  >/dev/null

VM_IP="$(gcloud compute instances describe "$VM_NAME" --project "$PROJECT_ID" --zone "$ZONE" --format='value(networkInterfaces[0].accessConfigs[0].natIP)')"

PATCH_JSON="$(
  AGENT_ID="$AGENT_ID" \
  SLOT_ID="$SLOT_ID" \
  PROJECT_ID="$PROJECT_ID" \
  VM_NAME="$VM_NAME" \
  ZONE="$ZONE" \
  REMOTE_REPO_DIR="$REMOTE_REPO_DIR" \
  LOCAL_DASHBOARD_PORT="$LOCAL_DASHBOARD_PORT" \
  LOCAL_VITE_PORT="$LOCAL_VITE_PORT" \
  DASHBOARD_URL="http://localhost:${LOCAL_DASHBOARD_PORT}" \
  REMOTE_LOG_PATH="$REMOTE_LOG_PATH" \
  REMOTE_TMUX_SESSION="$REMOTE_TMUX_SESSION" \
  LOCAL_USER="$LOCAL_USER" \
  VM_IP="$VM_IP" \
  PRIVATE_KEY_PATH="$PRIVATE_KEY_PATH" \
  PUBLIC_KEY_PATH="$PUBLIC_KEY_PATH" \
  python3 - <<'PY'
import json
import os

print(
    json.dumps(
        {
            "slot_id": os.environ["SLOT_ID"],
            "agent_id": os.environ["AGENT_ID"],
            "project_id": os.environ["PROJECT_ID"],
            "vm_name": os.environ["VM_NAME"],
            "zone": os.environ["ZONE"],
            "remote_repo_dir": os.environ["REMOTE_REPO_DIR"],
            "local_dashboard_port": int(os.environ["LOCAL_DASHBOARD_PORT"]),
            "local_vite_port": int(os.environ["LOCAL_VITE_PORT"]),
            "dashboard_url": os.environ["DASHBOARD_URL"],
            "tunnel_pid": None,
            "remote_log_path": os.environ["REMOTE_LOG_PATH"],
            "remote_tmux_session": os.environ["REMOTE_TMUX_SESSION"],
            "local_user": os.environ["LOCAL_USER"],
            "vm_ip": os.environ["VM_IP"],
            "private_key_path": os.environ["PRIVATE_KEY_PATH"],
            "public_key_path": os.environ["PUBLIC_KEY_PATH"],
        }
    )
)
PY
)"

merge_state_json "$STATE_PATH" "$PATCH_JSON"
