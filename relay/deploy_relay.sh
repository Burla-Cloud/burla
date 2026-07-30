#!/bin/bash
# Deploys one Burla relay VM (frps + auth plugin) into a Burla-owned GCP
# project. Run once per region; point <region>.<subdomain-host> (or the apex
# and *.<subdomain-host>) at the printed static IP.
#
# Usage:
#   ./deploy_relay.sh --project burla-prod --region us-central1 \
#       --subdomain-host relay.burla.dev [--backend-url https://backend.burla.dev]
set -euo pipefail

PROJECT=""
REGION=""
SUBDOMAIN_HOST=""
BACKEND_URL="https://backend.burla.dev"
FRP_VERSION="v0.70.1"

while [[ $# -gt 0 ]]; do
    case "$1" in
    --project) PROJECT="$2"; shift 2 ;;
    --region) REGION="$2"; shift 2 ;;
    --subdomain-host) SUBDOMAIN_HOST="$2"; shift 2 ;;
    --backend-url) BACKEND_URL="$2"; shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done
[[ -n "$PROJECT" && -n "$REGION" && -n "$SUBDOMAIN_HOST" ]] || {
    echo "Required: --project, --region, --subdomain-host" >&2
    exit 1
}

VM_NAME="burla-relay-$REGION"
ZONE="$REGION-a"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

gcloud compute addresses create "$VM_NAME" \
    --project="$PROJECT" --region="$REGION" 2>/dev/null || true
STATIC_IP=$(gcloud compute addresses describe "$VM_NAME" \
    --project="$PROJECT" --region="$REGION" --format='value(address)')

# 443 = SNI-routed tunnel traffic, 7000 = frpc control connections.
gcloud compute firewall-rules create burla-relay-firewall \
    --project="$PROJECT" --direction=INGRESS --priority=1000 --network=default \
    --action=ALLOW --rules=tcp:443,tcp:7000 --target-tags=burla-relay \
    2>/dev/null || true

STARTUP_SCRIPT=$(mktemp)
trap 'rm -f "$STARTUP_SCRIPT"' EXIT
{
    cat <<HEADER
#!/bin/bash
set -euo pipefail
mkdir -p /etc/burla-relay
cat > /etc/burla-relay/auth_plugin.py <<'PLUGIN_EOF'
HEADER
    cat "$SCRIPT_DIR/auth_plugin.py"
    cat <<'MIDDLE'
PLUGIN_EOF
cat > /etc/burla-relay/frps.toml <<'FRPS_EOF'
MIDDLE
    cat "$SCRIPT_DIR/frps.toml"
    cat <<FOOTER
FRPS_EOF
docker network create burla-relay || true
docker rm -f burla-relay-auth burla-relay-frps || true
docker run -d --restart=always --network=burla-relay --name=burla-relay-auth \\
    -v /etc/burla-relay/auth_plugin.py:/app/auth_plugin.py:ro \\
    -e BURLA_BACKEND_URL="$BACKEND_URL" \\
    -w /app python:3.13-slim bash -c \\
    "pip install --quiet fastapi uvicorn requests && uvicorn auth_plugin:app --host 0.0.0.0 --port 9000"
docker run -d --restart=always --network=burla-relay --name=burla-relay-frps \\
    -p 443:443 -p 7000:7000 \\
    -v /etc/burla-relay/frps.toml:/etc/frp/frps.toml:ro \\
    -e FRP_SUBDOMAIN_HOST="$SUBDOMAIN_HOST" \\
    -e FRP_AUTH_PLUGIN_ADDR="burla-relay-auth:9000" \\
    fatedier/frps:$FRP_VERSION -c /etc/frp/frps.toml
FOOTER
} > "$STARTUP_SCRIPT"

if gcloud compute instances describe "$VM_NAME" \
    --project="$PROJECT" --zone="$ZONE" >/dev/null 2>&1; then
    gcloud compute instances add-metadata "$VM_NAME" \
        --project="$PROJECT" --zone="$ZONE" \
        --metadata-from-file=startup-script="$STARTUP_SCRIPT"
    gcloud compute instances reset "$VM_NAME" --project="$PROJECT" --zone="$ZONE"
else
    gcloud compute instances create "$VM_NAME" \
        --project="$PROJECT" --zone="$ZONE" \
        --machine-type=e2-standard-4 \
        --address="$STATIC_IP" \
        --tags=burla-relay \
        --boot-disk-size=20GB \
        --image-family=cos-stable \
        --image-project=cos-cloud \
        --metadata-from-file=startup-script="$STARTUP_SCRIPT"
fi

echo ""
echo "Relay deployed. Point DNS at the static IP:"
echo "  *.$SUBDOMAIN_HOST  A  $STATIC_IP"
echo "  $SUBDOMAIN_HOST    A  $STATIC_IP"
