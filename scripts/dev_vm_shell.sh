#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$SCRIPT_DIR/dev_vm_common.sh"

parse_slot_only "$@"
require_local_prereqs
load_state_vars "$SLOT_ID"
validate_loaded_state_for_slot

exec ssh \
  -i "$PRIVATE_KEY_PATH" \
  -o StrictHostKeyChecking=no \
  -o UserKnownHostsFile=/dev/null \
  -t \
  "${LOCAL_USER}@${VM_IP}" \
  "cd '$REMOTE_REPO_DIR' && exec bash -l"
