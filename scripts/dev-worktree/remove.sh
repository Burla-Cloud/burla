#!/usr/bin/env bash
set -euo pipefail

THIS_SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$THIS_SCRIPT_DIR/../dev_vm_common.sh"

AGENT_ID=""
TASK_SLUG=""
DELETE_BRANCH="false"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --agent)
      AGENT_ID="$2"
      shift 2
      ;;
    --task)
      TASK_SLUG="$2"
      shift 2
      ;;
    --delete-branch)
      DELETE_BRANCH="true"
      shift
      ;;
    *)
      fail "Unknown argument [$1]."
      ;;
  esac
done

[[ -n "$AGENT_ID" ]] || fail "--agent is required."
[[ -n "$TASK_SLUG" ]] || fail "--task is required."
validate_agent_id "$AGENT_ID"
validate_task_slug "$TASK_SLUG"
require_command git
require_primary_checkout_context

BRANCH_NAME="$(branch_name_for_task "$AGENT_ID" "$TASK_SLUG")"
WORKTREE_PATH="$(worktree_path_for_task "$AGENT_ID" "$TASK_SLUG")"
REMOVED_WORKTREE="false"
DELETED_BRANCH="false"

if [[ -d "$WORKTREE_PATH" ]]; then
  git worktree remove --force "$WORKTREE_PATH" >/dev/null
  REMOVED_WORKTREE="true"
fi

if [[ "$DELETE_BRANCH" == "true" ]] && git show-ref --verify --quiet "refs/heads/$BRANCH_NAME"; then
  git branch -D "$BRANCH_NAME" >/dev/null
  DELETED_BRANCH="true"
fi

AGENT_ID="$AGENT_ID" \
TASK_SLUG="$TASK_SLUG" \
BRANCH_NAME="$BRANCH_NAME" \
WORKTREE_PATH="$WORKTREE_PATH" \
DELETE_BRANCH="$DELETE_BRANCH" \
REMOVED_WORKTREE="$REMOVED_WORKTREE" \
DELETED_BRANCH="$DELETED_BRANCH" \
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
            "delete_branch_requested": os.environ["DELETE_BRANCH"] == "true",
            "removed_worktree": os.environ["REMOVED_WORKTREE"] == "true",
            "deleted_branch": os.environ["DELETED_BRANCH"] == "true",
        },
        indent=2,
    )
)
PY
