#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CLIENT_PROJECT="$REPO_ROOT/client"
STATE_DIR="$REPO_ROOT/.cursor/dev-vm-state"

DEFAULT_ORGANIZATION_ID="${BURLA_DEV_VM_ORGANIZATION_ID:-1085197508222}"
DEFAULT_BILLING_ACCOUNT="${BURLA_DEV_VM_BILLING_ACCOUNT:-014651-7FBBE2-45278D}"
DEFAULT_REGION="${BURLA_DEV_VM_REGION:-us-central1}"
DEFAULT_ZONE="${BURLA_DEV_VM_ZONE:-us-central1-a}"
DEFAULT_MACHINE_TYPE="${BURLA_DEV_VM_MACHINE_TYPE:-e2-standard-8}"
DEFAULT_IMAGE_PROJECT="${BURLA_DEV_VM_IMAGE_PROJECT:-ubuntu-os-cloud}"
DEFAULT_IMAGE_FAMILY="${BURLA_DEV_VM_IMAGE_FAMILY:-ubuntu-2204-lts}"
DEFAULT_ARTIFACT_LOCATION="${BURLA_DEV_VM_ARTIFACT_LOCATION:-us}"
DEFAULT_ARTIFACT_REPOSITORY="${BURLA_DEV_VM_ARTIFACT_REPOSITORY:-burla-main-service}"
DEFAULT_REMOTE_REPO_DIR="${BURLA_DEV_VM_REMOTE_REPO_DIR:-/srv/burla}"
DEFAULT_REMOTE_LOG_PATH="${BURLA_DEV_VM_REMOTE_LOG_PATH:-/var/log/burla-local-dev.log}"
DEFAULT_BOOTSTRAP_READY_PATH="${BURLA_DEV_VM_BOOTSTRAP_READY_PATH:-/var/lib/burla-vm/bootstrap-ready}"
DEFAULT_PROJECT_PREFIX="${BURLA_DEV_VM_PROJECT_PREFIX:-burla-agent-}"
DEFAULT_VM_PREFIX="${BURLA_DEV_VM_VM_PREFIX:-burla-dev-vm-}"
DEFAULT_DASHBOARD_PORT_BASE=15000
DEFAULT_VITE_PORT_BASE=18000

fail() {
  echo "Error: $*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "Missing required command [$1]."
}

require_local_prereqs() {
  require_command gcloud
  require_command python3
  require_command scp
  require_command ssh
  require_command ssh-keygen
  require_command tar
  require_command uv
}

validate_agent_id() {
  local agent_id="$1"
  [[ "$agent_id" =~ ^[0-9]{2}$ ]] || fail "--agent must be a two-digit string like [01]."
}

parse_agent_only() {
  AGENT_ID=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --agent)
        AGENT_ID="$2"
        shift 2
        ;;
      *)
        fail "Unknown argument [$1]."
        ;;
    esac
  done

  [[ -n "$AGENT_ID" ]] || fail "--agent is required."
  validate_agent_id "$AGENT_ID"
}

parse_agent_and_python() {
  AGENT_ID=""
  PYTHON_VERSION=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --agent)
        AGENT_ID="$2"
        shift 2
        ;;
      --python)
        PYTHON_VERSION="$2"
        shift 2
        ;;
      *)
        fail "Unknown argument [$1]."
        ;;
    esac
  done

  [[ -n "$AGENT_ID" ]] || fail "--agent is required."
  [[ -n "$PYTHON_VERSION" ]] || fail "--python is required."
  validate_agent_id "$AGENT_ID"
}

parse_agent_and_destroy_flags() {
  AGENT_ID=""
  DELETE_PROJECT="false"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --agent)
        AGENT_ID="$2"
        shift 2
        ;;
      --delete-project)
        DELETE_PROJECT="true"
        shift
        ;;
      *)
        fail "Unknown argument [$1]."
        ;;
    esac
  done

  [[ -n "$AGENT_ID" ]] || fail "--agent is required."
  validate_agent_id "$AGENT_ID"
}

agent_number() {
  local agent_id="$1"
  printf '%d' "$((10#$agent_id))"
}

project_id_for_agent() {
  local agent_id="$1"
  echo "${DEFAULT_PROJECT_PREFIX}${agent_id}"
}

vm_name_for_agent() {
  local agent_id="$1"
  local timestamp="$2"
  echo "${DEFAULT_VM_PREFIX}${agent_id}-${timestamp}"
}

dashboard_port_for_agent() {
  local agent_id="$1"
  echo $((DEFAULT_DASHBOARD_PORT_BASE + $(agent_number "$agent_id")))
}

vite_port_for_agent() {
  local agent_id="$1"
  echo $((DEFAULT_VITE_PORT_BASE + $(agent_number "$agent_id")))
}

state_path_for_agent() {
  local agent_id="$1"
  echo "$STATE_DIR/${agent_id}.json"
}

private_key_path_for_agent() {
  local agent_id="$1"
  echo "$STATE_DIR/${agent_id}_ed25519"
}

public_key_path_for_agent() {
  local agent_id="$1"
  echo "$STATE_DIR/${agent_id}_ed25519.pub"
}

timestamp_utc() {
  date -u +%Y%m%dt%H%M%S
}

main_service_service_account() {
  local project_id="$1"
  echo "burla-main-service@${project_id}.iam.gserviceaccount.com"
}

ensure_state_dir() {
  mkdir -p "$STATE_DIR"
}

ensure_agent_keypair() {
  local agent_id="$1"
  local private_key_path
  local public_key_path
  private_key_path="$(private_key_path_for_agent "$agent_id")"
  public_key_path="$(public_key_path_for_agent "$agent_id")"

  if [[ -f "$private_key_path" && -f "$public_key_path" ]]; then
    return
  fi

  ssh-keygen -t ed25519 -N "" -C "burla-dev-vm-${agent_id}" -f "$private_key_path" >/dev/null
}

merge_state_json() {
  local state_path="$1"
  local patch_json="$2"

  python3 - "$state_path" "$patch_json" <<'PY'
import json
import pathlib
import sys

state_path = pathlib.Path(sys.argv[1])
patch = json.loads(sys.argv[2])
state = {}
if state_path.exists():
    state = json.loads(state_path.read_text())
state.update(patch)
state_path.parent.mkdir(parents=True, exist_ok=True)
state_path.write_text(json.dumps(state, indent=2) + "\n")
print(json.dumps(state, indent=2))
PY
}

load_state_vars() {
  local agent_id="$1"
  local state_path
  state_path="$(state_path_for_agent "$agent_id")"
  [[ -f "$state_path" ]] || fail "State file [$state_path] not found."

  eval "$(
    python3 - "$state_path" <<'PY'
import json
import pathlib
import shlex
import sys

state = json.loads(pathlib.Path(sys.argv[1]).read_text())
for key, value in state.items():
    shell_key = key.upper()
    if value is None:
        rendered = ""
    else:
        rendered = str(value)
    print(f"{shell_key}={shlex.quote(rendered)}")
PY
  )"
  STATE_PATH="$state_path"
}

print_state_file() {
  local state_path="$1"
  python3 - "$state_path" <<'PY'
import json
import pathlib
import sys

state = json.loads(pathlib.Path(sys.argv[1]).read_text())
print(json.dumps(state, indent=2))
PY
}

project_exists() {
  local project_id="$1"
  gcloud projects describe "$project_id" --format='value(projectId)' >/dev/null 2>&1
}

vm_exists() {
  local project_id="$1"
  local zone="$2"
  local vm_name="$3"
  gcloud compute instances describe "$vm_name" --project "$project_id" --zone "$zone" >/dev/null 2>&1
}

ensure_artifact_repository() {
  local project_id="$1"
  if gcloud artifacts repositories describe "$DEFAULT_ARTIFACT_REPOSITORY" --project "$project_id" --location "$DEFAULT_ARTIFACT_LOCATION" >/dev/null 2>&1; then
    return
  fi

  gcloud services enable artifactregistry.googleapis.com --project "$project_id" >/dev/null
  gcloud artifacts repositories create "$DEFAULT_ARTIFACT_REPOSITORY" \
    --project "$project_id" \
    --location "$DEFAULT_ARTIFACT_LOCATION" \
    --repository-format docker \
    --description "Burla main service images" \
    >/dev/null
}

remote_bash_command() {
  local remote_body="$1"
  python3 - "$remote_body" <<'PY'
import shlex
import sys

print("bash -lc " + shlex.quote(sys.argv[1]))
PY
}

ssh_run() {
  local remote_body="$1"
  local remote_cmd
  remote_cmd="$(remote_bash_command "$remote_body")"
  ssh \
    -i "$PRIVATE_KEY_PATH" \
    -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null \
    -o ServerAliveInterval=30 \
    -o ServerAliveCountMax=3 \
    "${LOCAL_USER}@${VM_IP}" \
    "$remote_cmd"
}

scp_to_vm() {
  local local_path="$1"
  local remote_path="$2"

  scp \
    -i "$PRIVATE_KEY_PATH" \
    -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null \
    "$local_path" \
    "${LOCAL_USER}@${VM_IP}:${remote_path}"
}

render_startup_script() {
  local local_user="$1"
  local output_path="$2"

  DEV_VM_USER="$local_user" \
  REMOTE_REPO_DIR="$DEFAULT_REMOTE_REPO_DIR" \
  REMOTE_LOG_PATH="$DEFAULT_REMOTE_LOG_PATH" \
  BOOTSTRAP_READY_PATH="$DEFAULT_BOOTSTRAP_READY_PATH" \
  python3 - "$SCRIPT_DIR/dev_vm_startup.sh" "$output_path" <<'PY'
import os
import pathlib
import sys

template = pathlib.Path(sys.argv[1]).read_text()
rendered = template.replace("__DEV_VM_USER__", os.environ["DEV_VM_USER"])
rendered = rendered.replace("__REMOTE_REPO_DIR__", os.environ["REMOTE_REPO_DIR"])
rendered = rendered.replace("__REMOTE_LOG_PATH__", os.environ["REMOTE_LOG_PATH"])
rendered = rendered.replace("__BOOTSTRAP_READY_PATH__", os.environ["BOOTSTRAP_READY_PATH"])
pathlib.Path(sys.argv[2]).write_text(rendered)
PY
}
