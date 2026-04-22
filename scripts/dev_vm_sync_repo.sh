#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$SCRIPT_DIR/dev_vm_common.sh"

parse_agent_only "$@"
require_local_prereqs
load_state_vars "$AGENT_ID"

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

REMOTE_BODY="$(cat <<EOF
sudo rm -rf '$REMOTE_REPO_DIR'
sudo mkdir -p '$REMOTE_REPO_DIR'
sudo tar xzf "\$HOME/${REMOTE_ARCHIVE}" -C '$REMOTE_REPO_DIR'
sudo chown -R "\$(id -un):\$(id -gn)" '$REMOTE_REPO_DIR'
rm -f "\$HOME/${REMOTE_ARCHIVE}"
EOF
)"

ssh_run "$REMOTE_BODY" >/dev/null
echo "Synced repo to [$VM_NAME:$REMOTE_REPO_DIR]."
