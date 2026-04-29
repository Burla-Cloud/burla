#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$SCRIPT_DIR/dev_vm_common.sh"

parse_slot_and_python "$@"
require_local_prereqs
require_command zsh
load_state_vars "$SLOT_ID"
validate_loaded_state_for_slot

uv python install "$PYTHON_VERSION" >/dev/null 2>&1
uv python pin --project "$CLIENT_PROJECT" "$PYTHON_VERSION" >/dev/null 2>&1
uv sync --project "$CLIENT_PROJECT" --group dev >/dev/null 2>&1

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

PROMPT_PREFIX="${PYTHON_VERSION}-vm-${AGENT_ID}"
ZSHRC_PATH="$TMP_DIR/.zshrc"

python3 - "$ZSHRC_PATH" "$PROMPT_PREFIX" "$DASHBOARD_URL" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
prompt_prefix = sys.argv[2]
dashboard_url = sys.argv[3]
path.write_text(
    f'PROMPT="({prompt_prefix}) %c %% "\n'
    f'export BURLA_CLUSTER_DASHBOARD_URL={dashboard_url}\n'
)
PY

ZDOTDIR="$TMP_DIR" uv run --project "$CLIENT_PROJECT" --group dev zsh -i
