#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$SCRIPT_DIR/dev_vm_common.sh"

parse_slot_only "$@"
require_local_prereqs
load_state_vars "$SLOT_ID"
validate_loaded_state_for_slot

MAX_ATTEMPTS="${BURLA_DEV_VM_WAIT_ATTEMPTS:-120}"
SLEEP_SECONDS="${BURLA_DEV_VM_WAIT_SLEEP_SECONDS:-5}"
REMOTE_BODY="test -f '$DEFAULT_BOOTSTRAP_READY_PATH' && command -v docker >/dev/null && command -v gcloud >/dev/null && command -v uv >/dev/null && command -v node >/dev/null && command -v npm >/dev/null && command -v tmux >/dev/null && docker info >/dev/null 2>&1"

for ((attempt = 1; attempt <= MAX_ATTEMPTS; attempt += 1)); do
  if ssh_run "$REMOTE_BODY" >/dev/null 2>&1; then
    echo "VM [$VM_NAME] is reachable and bootstrapped."
    exit 0
  fi
  sleep "$SLEEP_SECONDS"
done

fail "VM [$VM_NAME] never became reachable after $MAX_ATTEMPTS attempts."
