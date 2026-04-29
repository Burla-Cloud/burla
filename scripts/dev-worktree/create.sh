#!/usr/bin/env bash
set -euo pipefail

THIS_SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$THIS_SCRIPT_DIR/../dev_vm_common.sh"

parse_worktree_task_and_base "$@"
require_command git
require_primary_checkout_context

WORKTREE_PATH="$(worktree_path_for_task "$TASK_SLUG")"
WORKTREE_EXISTS="false"
BRANCH_EXISTS="false"
CREATED_WORKTREE="false"

if git show-ref --verify --quiet "refs/heads/$BRANCH_NAME"; then
  BRANCH_EXISTS="true"
fi

if [[ -d "$WORKTREE_PATH" ]]; then
  EXISTING_TOPLEVEL="$(git -C "$WORKTREE_PATH" rev-parse --show-toplevel 2>/dev/null)" || fail "Path [$WORKTREE_PATH] exists but is not a git worktree."
  EXISTING_BRANCH="$(git -C "$WORKTREE_PATH" branch --show-current)"
  [[ "$EXISTING_TOPLEVEL" == "$WORKTREE_PATH" ]] || fail "Worktree path [$WORKTREE_PATH] resolves to [$EXISTING_TOPLEVEL]."
  [[ "$EXISTING_BRANCH" == "$BRANCH_NAME" ]] || fail "Worktree [$WORKTREE_PATH] is on branch [$EXISTING_BRANCH], expected [$BRANCH_NAME]."
  WORKTREE_EXISTS="true"
else
  mkdir -p "$(dirname "$WORKTREE_PATH")"
  if [[ "$BRANCH_EXISTS" == "true" ]]; then
    git worktree add "$WORKTREE_PATH" "$BRANCH_NAME" >/dev/null
  else
    git rev-parse --verify "$BASE_REF" >/dev/null 2>&1 || fail "Base ref [$BASE_REF] was not found."
    git worktree add -b "$BRANCH_NAME" "$WORKTREE_PATH" "$BASE_REF" >/dev/null
    BRANCH_EXISTS="true"
  fi
  WORKTREE_EXISTS="true"
  CREATED_WORKTREE="true"
fi

TASK_SLUG="$TASK_SLUG" \
BASE_REF="$BASE_REF" \
BRANCH_NAME="$BRANCH_NAME" \
WORKTREE_PATH="$WORKTREE_PATH" \
PRIMARY_CHECKOUT_PATH="$PRIMARY_CHECKOUT_PATH" \
WORKTREE_EXISTS="$WORKTREE_EXISTS" \
BRANCH_EXISTS="$BRANCH_EXISTS" \
CREATED_WORKTREE="$CREATED_WORKTREE" \
python3 - <<'PY'
import json
import os

print(
    json.dumps(
        {
            "task_slug": os.environ["TASK_SLUG"],
            "base_ref": os.environ["BASE_REF"],
            "branch_name": os.environ["BRANCH_NAME"],
            "worktree_path": os.environ["WORKTREE_PATH"],
            "primary_checkout_path": os.environ["PRIMARY_CHECKOUT_PATH"],
            "branch_exists": os.environ["BRANCH_EXISTS"] == "true",
            "worktree_exists": os.environ["WORKTREE_EXISTS"] == "true",
            "created_worktree": os.environ["CREATED_WORKTREE"] == "true",
        },
        indent=2,
    )
)
PY
