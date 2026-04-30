#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$SCRIPT_DIR/dev_vm_common.sh"

SLOT_ID=""
MIN_SLOT="${BURLA_DEV_VM_MIN_SLOT:-00}"
MAX_SLOT="${BURLA_DEV_VM_MAX_SLOT:-10}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --slot|--agent)
      SLOT_ID="$2"
      shift 2
      ;;
    --min-slot)
      MIN_SLOT="$2"
      shift 2
      ;;
    --max-slot)
      MAX_SLOT="$2"
      shift 2
      ;;
    *)
      fail "Unknown argument [$1]."
      ;;
  esac
done

validate_slot_id "$MIN_SLOT"
validate_slot_id "$MAX_SLOT"
require_local_prereqs

if [[ -z "$SLOT_ID" ]]; then
  for ((slot_num = 10#$MIN_SLOT; slot_num <= 10#$MAX_SLOT; slot_num += 1)); do
    candidate_slot="$(printf "%02d" "$slot_num")"
    if slot_is_available "$candidate_slot"; then
      SLOT_ID="$candidate_slot"
      break
    fi
  done
  [[ -n "$SLOT_ID" ]] || fail "No available dev VM slots from [$MIN_SLOT] to [$MAX_SLOT]."
else
  validate_slot_id "$SLOT_ID"
fi
AGENT_ID="$SLOT_ID"
ensure_state_dir
ensure_slot_keypair "$SLOT_ID"

STATE_PATH="$(state_path_for_slot "$SLOT_ID")"
PROJECT_ID="$(project_id_for_slot "$SLOT_ID")"
ZONE="$DEFAULT_ZONE"
VM_NAME="$(vm_name_for_slot "$SLOT_ID")"
LOCAL_DASHBOARD_PORT="$(dashboard_port_for_slot "$SLOT_ID")"
LOCAL_VITE_PORT="$(vite_port_for_slot "$SLOT_ID")"
REMOTE_REPO_DIR="$DEFAULT_REMOTE_REPO_DIR"
REMOTE_LOG_PATH="$DEFAULT_REMOTE_LOG_PATH"
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

ensure_slot_project "$PROJECT_ID"
gcloud services enable \
  run.googleapis.com \
  compute.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  secretmanager.googleapis.com \
  --project "$PROJECT_ID" \
  >/dev/null

if ! main_service_account_exists "$PROJECT_ID"; then
  for attempt in 1 2 3; do
    if DISABLE_BURLA_TELEMETRY=True CLOUDSDK_CORE_PROJECT="$PROJECT_ID" uv run --project "$CLIENT_PROJECT" burla install; then
      break
    fi
    if [[ "$attempt" -eq 3 ]]; then
      fail "burla install failed three times for project [$PROJECT_ID]."
    fi
    sleep $((attempt * 5))
  done
fi

ensure_artifact_repositories "$PROJECT_ID"
ensure_artifact_writer_role "$PROJECT_ID"
ensure_artifact_reader_role "$PROJECT_ID"

existing_status="$(vm_status "$PROJECT_ID" "$ZONE" "$VM_NAME" || true)"
if [[ "$existing_status" == "TERMINATED" ]]; then
  echo "Starting VM [$VM_NAME] in project [$PROJECT_ID]."
  gcloud compute instances start "$VM_NAME" --project "$PROJECT_ID" --zone "$ZONE" --quiet >/dev/null
elif [[ -z "$existing_status" ]]; then
  echo "Creating VM [$VM_NAME] in project [$PROJECT_ID]."
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
else
  echo "Using existing VM [$VM_NAME] with status [$existing_status]."
fi

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
            "local_user": os.environ["LOCAL_USER"],
            "vm_ip": os.environ["VM_IP"],
            "private_key_path": os.environ["PRIVATE_KEY_PATH"],
            "public_key_path": os.environ["PUBLIC_KEY_PATH"],
        }
    )
)
PY
)"

merge_state_json "$STATE_PATH" "$PATCH_JSON" >/dev/null
wait_for_vm_bootstrap
echo "Dev VM slot [$SLOT_ID] is ready."
echo "$PATCH_JSON" | python3 -m json.tool
