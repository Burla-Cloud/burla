#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$SCRIPT_DIR/dev_vm_common.sh"

parse_agent_only "$@"
require_local_prereqs
ensure_state_dir
ensure_agent_keypair "$AGENT_ID"

STATE_PATH="$(state_path_for_agent "$AGENT_ID")"
PROJECT_ID="$(project_id_for_agent "$AGENT_ID")"
ZONE="$DEFAULT_ZONE"
TIMESTAMP="$(timestamp_utc)"
VM_NAME="$(vm_name_for_agent "$AGENT_ID" "$TIMESTAMP")"
LOCAL_DASHBOARD_PORT="$(dashboard_port_for_agent "$AGENT_ID")"
LOCAL_VITE_PORT="$(vite_port_for_agent "$AGENT_ID")"
REMOTE_REPO_DIR="$DEFAULT_REMOTE_REPO_DIR"
REMOTE_LOG_PATH="$DEFAULT_REMOTE_LOG_PATH"
REMOTE_TMUX_SESSION="burla-local-dev-${AGENT_ID}"
LOCAL_USER="$(id -un)"
PRIVATE_KEY_PATH="$(private_key_path_for_agent "$AGENT_ID")"
PUBLIC_KEY_PATH="$(public_key_path_for_agent "$AGENT_ID")"
SSH_KEY_VALUE="$(python3 - "$PUBLIC_KEY_PATH" "$LOCAL_USER" <<'PY'
import pathlib
import sys

public_key = pathlib.Path(sys.argv[1]).read_text().strip()
username = sys.argv[2]
print(f"{username}:{public_key}")
PY
)"

if [[ -f "$STATE_PATH" ]]; then
  "$SCRIPT_DIR/dev_vm_destroy.sh" --agent "$AGENT_ID"
fi

if ! project_exists "$PROJECT_ID"; then
  gcloud projects create "$PROJECT_ID" \
    --name="$PROJECT_ID" \
    --organization="$DEFAULT_ORGANIZATION_ID" \
    >/dev/null
  gcloud beta billing projects link "$PROJECT_ID" \
    --billing-account="$DEFAULT_BILLING_ACCOUNT" \
    >/dev/null
fi

gcloud services enable run.googleapis.com --project "$PROJECT_ID" >/dev/null

if ! gcloud run services describe burla-main-service --project "$PROJECT_ID" --region "$DEFAULT_REGION" --quiet >/dev/null 2>&1; then
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

ensure_artifact_repository "$PROJECT_ID"

for attempt in 1 2 3; do
  if gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$(main_service_service_account "$PROJECT_ID")" \
    --role=roles/artifactregistry.writer \
    --condition=None \
    >/dev/null 2>&1; then
    break
  fi
  if [[ "$attempt" -eq 3 ]]; then
    fail "Failed to grant Artifact Registry writer role for project [$PROJECT_ID]."
  fi
  sleep $((attempt * 5))
done

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
  --labels "burla-agent-id=${AGENT_ID},burla-runtime=local-dev,burla-ephemeral=true" \
  --metadata "enable-oslogin=FALSE,ssh-keys=${SSH_KEY_VALUE}" \
  --metadata-from-file startup-script="$STARTUP_SCRIPT" \
  >/dev/null

VM_IP="$(gcloud compute instances describe "$VM_NAME" --project "$PROJECT_ID" --zone "$ZONE" --format='value(networkInterfaces[0].accessConfigs[0].natIP)')"

PATCH_JSON="$(
  AGENT_ID="$AGENT_ID" \
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
