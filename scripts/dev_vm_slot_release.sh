#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$SCRIPT_DIR/dev_vm_common.sh"

parse_slot_only "$@"
LOCK_PATH="$(lock_path_for_slot "$SLOT_ID")"
RELEASED="false"

if [[ -f "$LOCK_PATH" ]]; then
  rm -f "$LOCK_PATH"
  RELEASED="true"
fi

SLOT_ID="$SLOT_ID" LOCK_PATH="$LOCK_PATH" RELEASED="$RELEASED" python3 - <<'PY'
import json
import os

print(
    json.dumps(
        {
            "slot_id": os.environ["SLOT_ID"],
            "lock_path": os.environ["LOCK_PATH"],
            "released": os.environ["RELEASED"] == "true",
        },
        indent=2,
    )
)
PY
