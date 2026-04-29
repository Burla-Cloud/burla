#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$SCRIPT_DIR/dev_vm_common.sh"

SOURCE_PATH=""
MIN_SLOT="${BURLA_DEV_VM_MIN_SLOT:-00}"
MAX_SLOT="${BURLA_DEV_VM_MAX_SLOT:-10}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --source)
      SOURCE_PATH="$2"
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
require_command git
require_command python3
ensure_state_dir

if [[ -z "$SOURCE_PATH" ]]; then
  SOURCE_PATH="$(current_git_toplevel)"
fi
SOURCE_PATH="$(cd "$SOURCE_PATH" && pwd)"
SOURCE_TOPLEVEL="$(git -C "$SOURCE_PATH" rev-parse --show-toplevel 2>/dev/null)" || fail "Source path [$SOURCE_PATH] is not inside a git checkout."
[[ "$SOURCE_TOPLEVEL" == "$SOURCE_PATH" ]] || fail "Source path [$SOURCE_PATH] must be a git checkout root, got [$SOURCE_TOPLEVEL]."

SOURCE_JSON="$(source_git_metadata_json "$SOURCE_PATH")"
ACQUIRED_SLOT=""
ACQUIRED_LOCK_PATH=""

for ((slot_num = 10#$MIN_SLOT; slot_num <= 10#$MAX_SLOT; slot_num += 1)); do
  slot_id="$(printf "%02d" "$slot_num")"
  lock_path="$(lock_path_for_slot "$slot_id")"
  if [[ -f "$lock_path" ]]; then
    continue
  fi

  LOCK_JSON="$(
    SLOT_ID="$slot_id" \
    SOURCE_JSON="$SOURCE_JSON" \
    SHELL_PID="$$" \
    python3 - <<'PY'
import json
import os
from datetime import datetime, timezone

source = json.loads(os.environ["SOURCE_JSON"])
lock = {
    "slot_id": os.environ["SLOT_ID"],
    "pid": int(os.environ["SHELL_PID"]),
    "created_at": datetime.now(timezone.utc).isoformat(),
    "source_path": source["last_synced_source_path"],
    "source_branch": source["last_synced_branch"],
    "source_commit": source["last_synced_commit"],
    "source_dirty": source["last_synced_dirty"],
}
print(json.dumps(lock))
PY
  )"
  python3 - "$lock_path" "$LOCK_JSON" <<'PY'
import pathlib
import sys

lock_path = pathlib.Path(sys.argv[1])
lock_path.parent.mkdir(parents=True, exist_ok=True)
lock_path.write_text(sys.argv[2] + "\n")
PY
  ACQUIRED_SLOT="$slot_id"
  ACQUIRED_LOCK_PATH="$lock_path"
  break
done

[[ -n "$ACQUIRED_SLOT" ]] || fail "No unlocked dev VM slots available from [$MIN_SLOT] to [$MAX_SLOT]."

SLOT_ID="$ACQUIRED_SLOT" LOCK_PATH="$ACQUIRED_LOCK_PATH" python3 - <<'PY'
import json
import os

print(
    json.dumps(
        {
            "slot_id": os.environ["SLOT_ID"],
            "lock_path": os.environ["LOCK_PATH"],
        },
        indent=2,
    )
)
PY
