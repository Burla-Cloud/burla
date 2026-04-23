#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$SCRIPT_DIR/dev_vm_common.sh"

parse_agent_only "$@"
require_local_prereqs
require_agent_worktree_context "$AGENT_ID"
load_state_vars "$AGENT_ID"
validate_loaded_state_against_current_context

VM_EXISTS="false"
REMOTE_SESSION_RUNNING="false"
TUNNEL_RUNNING="false"
DASHBOARD_REACHABLE="false"
HEALTH="missing"
RUNNING_MODE=""

if ssh_run "true" >/dev/null 2>&1; then
  VM_EXISTS="true"
  HEALTH="vm_created"
fi

if [[ -n "${TUNNEL_PID:-}" ]] && kill -0 "$TUNNEL_PID" >/dev/null 2>&1; then
  TUNNEL_RUNNING="true"
fi

if [[ "$VM_EXISTS" == "true" ]] && ssh_run "tmux has-session -t '$REMOTE_TMUX_SESSION'" >/dev/null 2>&1; then
  REMOTE_SESSION_RUNNING="true"
  HEALTH="main_service_running"
fi

# Detect which mode main_service was started in by inspecting its container env.
# Absent container -> empty string; IN_LOCAL_DEV_MODE=True present -> local-dev; else remote-dev.
if [[ "$VM_EXISTS" == "true" ]]; then
  inspect_cmd="docker inspect main_service --format '{{range .Config.Env}}{{println .}}{{end}}' 2>/dev/null || true"
  container_env="$(ssh_run "$inspect_cmd" 2>/dev/null || true)"
  if [[ -n "$container_env" ]]; then
    if echo "$container_env" | grep -q '^IN_LOCAL_DEV_MODE=True$'; then
      RUNNING_MODE="local-dev"
    else
      RUNNING_MODE="remote-dev"
    fi
  fi
fi

if [[ "$TUNNEL_RUNNING" == "true" ]] && curl -sf "$DASHBOARD_URL" >/dev/null 2>&1; then
  DASHBOARD_REACHABLE="true"
  HEALTH="dashboard_reachable"
fi

AGENT_ID="$AGENT_ID" \
PROJECT_ID="$PROJECT_ID" \
VM_NAME="$VM_NAME" \
ZONE="$ZONE" \
REMOTE_REPO_DIR="$REMOTE_REPO_DIR" \
LOCAL_DASHBOARD_PORT="$LOCAL_DASHBOARD_PORT" \
LOCAL_VITE_PORT="$LOCAL_VITE_PORT" \
DASHBOARD_URL="$DASHBOARD_URL" \
TUNNEL_PID="${TUNNEL_PID:-}" \
REMOTE_LOG_PATH="$REMOTE_LOG_PATH" \
REMOTE_TMUX_SESSION="$REMOTE_TMUX_SESSION" \
BRANCH_NAME="${BRANCH_NAME:-}" \
TASK_SLUG="${TASK_SLUG:-}" \
WORKTREE_PATH="${WORKTREE_PATH:-}" \
PRIMARY_CHECKOUT_PATH="${PRIMARY_CHECKOUT_PATH:-}" \
LOCAL_USER="$LOCAL_USER" \
VM_IP="$VM_IP" \
VM_EXISTS="$VM_EXISTS" \
REMOTE_SESSION_RUNNING="$REMOTE_SESSION_RUNNING" \
TUNNEL_RUNNING="$TUNNEL_RUNNING" \
DASHBOARD_REACHABLE="$DASHBOARD_REACHABLE" \
HEALTH="$HEALTH" \
RUNNING_MODE="$RUNNING_MODE" \
LAST_STARTED_MODE="${LAST_STARTED_MODE:-}" \
python3 - <<'PY'
import json
import os

running_mode = os.environ["RUNNING_MODE"] or None
last_started_mode = os.environ["LAST_STARTED_MODE"] or None
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
            "tunnel_pid": int(os.environ["TUNNEL_PID"]) if os.environ.get("TUNNEL_PID") else None,
            "remote_log_path": os.environ["REMOTE_LOG_PATH"],
            "remote_tmux_session": os.environ["REMOTE_TMUX_SESSION"],
            "branch_name": os.environ["BRANCH_NAME"],
            "task_slug": os.environ["TASK_SLUG"],
            "worktree_path": os.environ["WORKTREE_PATH"],
            "primary_checkout_path": os.environ["PRIMARY_CHECKOUT_PATH"],
            "local_user": os.environ["LOCAL_USER"],
            "vm_ip": os.environ["VM_IP"],
            "vm_exists": os.environ["VM_EXISTS"] == "true",
            "remote_session_running": os.environ["REMOTE_SESSION_RUNNING"] == "true",
            "tunnel_running": os.environ["TUNNEL_RUNNING"] == "true",
            "dashboard_reachable": os.environ["DASHBOARD_REACHABLE"] == "true",
            "running_mode": running_mode,
            "last_started_mode": last_started_mode,
            "health": os.environ["HEALTH"],
        },
        indent=2,
    )
)
PY
