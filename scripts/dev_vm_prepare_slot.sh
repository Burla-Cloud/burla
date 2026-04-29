#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$SCRIPT_DIR/dev_vm_common.sh"

parse_slot_only "$@"
require_local_prereqs
ensure_state_dir

PROJECT_ID="$(project_id_for_slot "$SLOT_ID")"

ensure_slot_project "$PROJECT_ID"
gcloud services enable \
  run.googleapis.com \
  compute.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  secretmanager.googleapis.com \
  --project "$PROJECT_ID" \
  >/dev/null

if ! gcloud run services describe burla-main-service --project "$PROJECT_ID" --region "$DEFAULT_REGION" --quiet >/dev/null 2>&1; then
  for attempt in 1 2 3; do
    if DISABLE_BURLA_TELEMETRY=True CLOUDSDK_CORE_PROJECT="$PROJECT_ID" uv run --project "$CLIENT_PROJECT" burla install; then
      break
    fi
    if [[ "$attempt" -eq 3 ]]; then
      fail "burla install failed three times for project [$PROJECT_ID]."
    fi
    sleep $((attempt * 5))
  done
fi

ensure_artifact_repositories "$PROJECT_ID"
ensure_artifact_writer_role "$PROJECT_ID"

SLOT_ID="$SLOT_ID" PROJECT_ID="$PROJECT_ID" python3 - <<'PY'
import json
import os

print(
    json.dumps(
        {
            "slot_id": os.environ["SLOT_ID"],
            "project_id": os.environ["PROJECT_ID"],
            "prepared": True,
        },
        indent=2,
    )
)
PY
