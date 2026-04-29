#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$SCRIPT_DIR/dev_vm_common.sh"

cat >&2 <<'EOF'
dev_vm_start.sh no longer starts local-dev or remote-dev.

Start the VM with:
  scripts/dev_vm_create.sh --slot <id>

Then SSH into the VM and run the desired command directly:
  cd /srv/burla
  make local-dev

or:
  cd /srv/burla
  make remote-dev
EOF
exit 1
