#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$SCRIPT_DIR/dev_vm_common.sh"

parse_agent_only "$@"
require_local_prereqs
require_agent_worktree_context "$AGENT_ID"
load_state_vars "$AGENT_ID"
validate_loaded_state_against_current_context

REMOTE_BODY="$(cat <<EOF
cat > /tmp/burla-start-local-dev.sh <<'INNER'
#!/usr/bin/env bash
set -euo pipefail
CLOUDSDK_CORE_PROJECT='$PROJECT_ID' gcloud auth configure-docker us-docker.pkg.dev --quiet >/dev/null
cd '$REMOTE_REPO_DIR/main_service'
make build-frontend
test -f .frontend_last_built_at.txt || printf '%s' "$(date +%s)" > .frontend_last_built_at.txt
CLOUDSDK_CORE_PROJECT='$PROJECT_ID' make image
tmux kill-session -t '$REMOTE_TMUX_SESSION' >/dev/null 2>&1 || true
docker rm -f main_service >/dev/null 2>&1 || true
: > '$REMOTE_LOG_PATH'
tmux new-session -d -s '$REMOTE_TMUX_SESSION' "cd '$REMOTE_REPO_DIR' && CLOUDSDK_CORE_PROJECT='$PROJECT_ID' make local-dev >> '$REMOTE_LOG_PATH' 2>&1"
INNER
bash /tmp/burla-start-local-dev.sh
rm -f /tmp/burla-start-local-dev.sh
tmux has-session -t '$REMOTE_TMUX_SESSION'
EOF
)"

ssh_run "$REMOTE_BODY" >/dev/null
echo "Started local-dev on [$VM_NAME] in tmux session [$REMOTE_TMUX_SESSION]."
