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

SYNC_ARCHIVE="$(mktemp "/tmp/burla-dev-vm-${AGENT_ID}-XXXXXX.tgz")"
REMOTE_ARCHIVE="burla-dev-vm-${AGENT_ID}.tgz"
trap 'rm -f "$SYNC_ARCHIVE"' EXIT

COPYFILE_DISABLE=1 tar -czf "$SYNC_ARCHIVE" \
  --exclude='.git' \
  --exclude='.pytest_cache' \
  --exclude='.cursor/dev-vm-state' \
  --exclude='main_service/frontend/node_modules' \
  --exclude='main_service/frontend/dist' \
  --exclude='main_service/frontend/build' \
  --exclude='_shared_workspace' \
  --exclude='_worker_service_python_env' \
  --exclude='_node_auth' \
  --exclude='__pycache__' \
  --exclude='*/__pycache__' \
  -C "$REPO_ROOT" .

scp_to_vm "$SYNC_ARCHIVE" "~/${REMOTE_ARCHIVE}" >/dev/null

# Scratch dirs that `make local-dev` bind-mounts into running node
# containers. If we `rm -rf` the whole repo dir, those dirs get
# recreated at a fresh inode and the containers' mounts go stale —
# the mount still resolves but every write inside the container
# returns "Directory nonexistent", and every subsequent POST /jobs/{id}
# 500s on `NODE_AUTH_CREDENTIALS_PATH.write_text()`. Preserve the
# scratch dirs in place so their inodes survive the sync.
REMOTE_BODY="$(cat <<EOF
sudo mkdir -p '$REMOTE_REPO_DIR'
sudo find '$REMOTE_REPO_DIR' -mindepth 1 -maxdepth 1 \\
  ! -name '_node_auth' \\
  ! -name '_shared_workspace' \\
  ! -name '_worker_service_python_env' \\
  -exec rm -rf {} +
sudo tar xzf "\$HOME/${REMOTE_ARCHIVE}" -C '$REMOTE_REPO_DIR'
sudo chown -R "\$(id -un):\$(id -gn)" '$REMOTE_REPO_DIR'
rm -f "\$HOME/${REMOTE_ARCHIVE}"
EOF
)"

ssh_run "$REMOTE_BODY" >/dev/null
echo "Synced repo to [$VM_NAME:$REMOTE_REPO_DIR]."

# Ship git-ignored frontend env file (Syncfusion license, etc.) so
# `make build-frontend` bakes VITE_* vars into the bundle. Prefer the
# worktree copy; fall back to the primary checkout. Silent skip if neither
# exists. /srv/burla is user-owned after the chown above so plain scp works.
FRONTEND_ENV_PATH="main_service/frontend/.env.local"
LOCAL_ENV_FILE=""
for candidate in \
  "$CURRENT_WORKTREE_PATH/$FRONTEND_ENV_PATH" \
  "$PRIMARY_CHECKOUT_PATH/$FRONTEND_ENV_PATH"; do
  if [[ -f "$candidate" ]]; then
    LOCAL_ENV_FILE="$candidate"
    break
  fi
done

if [[ -n "$LOCAL_ENV_FILE" ]]; then
  scp_to_vm "$LOCAL_ENV_FILE" "$REMOTE_REPO_DIR/$FRONTEND_ENV_PATH" >/dev/null
  echo "Copied [$FRONTEND_ENV_PATH] from [$LOCAL_ENV_FILE] to VM."
fi
