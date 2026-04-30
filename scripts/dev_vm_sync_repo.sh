#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$SCRIPT_DIR/dev_vm_common.sh"

SOURCE_PATH=""
parse_slot_only_args=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --source)
      SOURCE_PATH="$2"
      shift 2
      ;;
    *)
      parse_slot_only_args+=("$1")
      shift
      ;;
  esac
done

parse_slot_only "${parse_slot_only_args[@]}"
require_local_prereqs
load_state_vars "$SLOT_ID"
validate_loaded_state_for_slot

if [[ -z "$SOURCE_PATH" ]]; then
  SOURCE_PATH="$(current_git_toplevel)"
fi
SOURCE_PATH="$(cd "$SOURCE_PATH" && pwd)"
SOURCE_TOPLEVEL="$(git -C "$SOURCE_PATH" rev-parse --show-toplevel 2>/dev/null)" || fail "Source path [$SOURCE_PATH] is not inside a git checkout."
[[ "$SOURCE_TOPLEVEL" == "$SOURCE_PATH" ]] || fail "Source path [$SOURCE_PATH] must be a git checkout root, got [$SOURCE_TOPLEVEL]."

REMOTE_BODY="$(cat <<EOF
sudo mkdir -p '$REMOTE_REPO_DIR'
sudo chown -R "\$(id -un):\$(id -gn)" '$REMOTE_REPO_DIR'
EOF
)"

ssh_run "$REMOTE_BODY" >/dev/null

rsync \
  -az \
  --delete \
  --exclude='.git/' \
  --exclude='.cursor/dev-vm-state/' \
  --exclude='.gstack/' \
  --exclude='.DS_Store' \
  --exclude='__pycache__/' \
  --exclude='.pytest_cache/' \
  --exclude='.mypy_cache/' \
  --exclude='.ruff_cache/' \
  --exclude='.venv/' \
  --exclude='client/.venv/' \
  --exclude='main_service/frontend/node_modules/' \
  --exclude='main_service/frontend/dist/' \
  --exclude='main_service/frontend/build/' \
  --exclude='main_service/src/main_service/static/assets/' \
  --exclude='_node_auth/' \
  --exclude='_shared_workspace/' \
  --exclude='_worker_service_python_env/' \
  --exclude='_python_version_marker/' \
  -e "ssh -i '$PRIVATE_KEY_PATH' -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" \
  "$SOURCE_PATH/" \
  "${LOCAL_USER}@${VM_IP}:${REMOTE_REPO_DIR}/" \
  >/dev/null
echo "Synced repo to [$VM_NAME:$REMOTE_REPO_DIR]."

SOURCE_PATCH_JSON="$(source_git_metadata_json "$SOURCE_PATH")"
merge_state_json "$STATE_PATH" "$SOURCE_PATCH_JSON" >/dev/null

# Ship git-ignored frontend env file (Syncfusion license, etc.) so
# `make build-frontend` bakes VITE_* vars into the bundle. Prefer the
# worktree copy; fall back to the primary checkout. Silent skip if neither
# exists.
FRONTEND_ENV_PATH="main_service/frontend/.env.local"
LOCAL_ENV_FILE=""
for candidate in \
  "$SOURCE_PATH/$FRONTEND_ENV_PATH" \
  "$(primary_checkout_path)/$FRONTEND_ENV_PATH"; do
  if [[ -f "$candidate" ]]; then
    LOCAL_ENV_FILE="$candidate"
    break
  fi
done

if [[ -n "$LOCAL_ENV_FILE" ]]; then
  scp_to_vm "$LOCAL_ENV_FILE" "$REMOTE_REPO_DIR/$FRONTEND_ENV_PATH" >/dev/null
  echo "Copied [$FRONTEND_ENV_PATH] from [$LOCAL_ENV_FILE] to VM."
fi
