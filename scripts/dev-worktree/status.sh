#!/usr/bin/env bash
set -euo pipefail

THIS_SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$THIS_SCRIPT_DIR/../dev_vm_common.sh"

parse_agent_task_and_base "$@"
require_command git
require_primary_checkout_context

BRANCH_NAME="$(branch_name_for_task "$AGENT_ID" "$TASK_SLUG")"
WORKTREE_PATH="$(worktree_path_for_task "$AGENT_ID" "$TASK_SLUG")"
BRANCH_EXISTS="false"
WORKTREE_EXISTS="false"
PATH_IS_WORKTREE="false"
WORKTREE_BRANCH=""
WORKTREE_DIRTY="false"

if git show-ref --verify --quiet "refs/heads/$BRANCH_NAME"; then
  BRANCH_EXISTS="true"
fi

if [[ -d "$WORKTREE_PATH" ]]; then
  WORKTREE_EXISTS="true"
  if git -C "$WORKTREE_PATH" rev-parse --show-toplevel >/dev/null 2>&1; then
    PATH_IS_WORKTREE="true"
    WORKTREE_BRANCH="$(git -C "$WORKTREE_PATH" branch --show-current)"
    if [[ -n "$(git -C "$WORKTREE_PATH" status --short)" ]]; then
      WORKTREE_DIRTY="true"
    fi
  fi
fi

AGENT_ID="$AGENT_ID" \
TASK_SLUG="$TASK_SLUG" \
BRANCH_NAME="$BRANCH_NAME" \
WORKTREE_PATH="$WORKTREE_PATH" \
PRIMARY_CHECKOUT_PATH="$PRIMARY_CHECKOUT_PATH" \
BRANCH_EXISTS="$BRANCH_EXISTS" \
WORKTREE_EXISTS="$WORKTREE_EXISTS" \
PATH_IS_WORKTREE="$PATH_IS_WORKTREE" \
WORKTREE_BRANCH="$WORKTREE_BRANCH" \
WORKTREE_DIRTY="$WORKTREE_DIRTY" \
python3 - <<'PY'
import json
import os

print(
    json.dumps(
        {
            "agent_id": os.environ["AGENT_ID"],
            "task_slug": os.environ["TASK_SLUG"],
            "branch_name": os.environ["BRANCH_NAME"],
            "worktree_path": os.environ["WORKTREE_PATH"],
            "primary_checkout_path": os.environ["PRIMARY_CHECKOUT_PATH"],
            "branch_exists": os.environ["BRANCH_EXISTS"] == "true",
            "worktree_exists": os.environ["WORKTREE_EXISTS"] == "true",
            "path_is_worktree": os.environ["PATH_IS_WORKTREE"] == "true",
            "worktree_branch": os.environ["WORKTREE_BRANCH"],
            "worktree_dirty": os.environ["WORKTREE_DIRTY"] == "true",
        },
        indent=2,
    )
)
PY
