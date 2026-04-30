#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$SCRIPT_DIR/dev_vm_common.sh"

parse_slot_only "$@"
require_local_prereqs
load_slot_vars "$SLOT_ID"
require_vm_ip

exec ssh \
  -i "$PRIVATE_KEY_PATH" \
  -o StrictHostKeyChecking=no \
  -o UserKnownHostsFile=/dev/null \
  -t \
  "${LOCAL_USER}@${VM_IP}" \
  "cd '$REMOTE_REPO_DIR' && exec bash -l"
