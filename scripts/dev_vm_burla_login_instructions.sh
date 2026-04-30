#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/dev_vm_common.sh
source "$SCRIPT_DIR/dev_vm_common.sh"

parse_slot_only "$@"
require_local_prereqs
load_state_vars "$SLOT_ID"
validate_loaded_state_for_slot

echo "1. Make sure the tunnel is running:"
echo "   scripts/dev_vm_tunnel.sh --slot $SLOT_ID"
echo
echo "2. Open [$DASHBOARD_URL] in the GStack browser."
echo
echo "3. Click Start in the dashboard so the cluster starts with the current mode/config."
echo
echo "4. In another terminal, SSH into the VM and run:"
echo "   ssh -i \"$PRIVATE_KEY_PATH\" \"$LOCAL_USER@$VM_IP\""
echo "   cd /srv/burla"
echo "   uv run --project ./client --group dev burla login --no_browser=True"
echo
echo "5. Open the printed login URL in the same GStack browser session and click authorize."
echo
echo "6. Verify credentials:"
echo "   scripts/dev_vm_status.sh --slot $SLOT_ID"
