#!/bin/bash
# Deploys one Burla relay VM (frps + auth plugin) to an AWS account.
# Run once per region; point *.<subdomain-host> and <subdomain-host> at the
# printed elastic IP.
#
# Usage:
#   ./deploy_relay_aws.sh --profile burla-test --region us-east-1 \
#       --subdomain-host test.relay.burla.dev \
#       [--backend-url https://test.backend.burla.dev] [--instance-type t3.small]
set -euo pipefail

PROFILE=""
REGION="us-east-1"
SUBDOMAIN_HOST=""
BACKEND_URL="https://backend.burla.dev"
INSTANCE_TYPE="t3.small"
FRP_VERSION="v0.70.1"

while [[ $# -gt 0 ]]; do
    case "$1" in
    --profile) PROFILE="$2"; shift 2 ;;
    --region) REGION="$2"; shift 2 ;;
    --subdomain-host) SUBDOMAIN_HOST="$2"; shift 2 ;;
    --backend-url) BACKEND_URL="$2"; shift 2 ;;
    --instance-type) INSTANCE_TYPE="$2"; shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done
[[ -n "$PROFILE" && -n "$SUBDOMAIN_HOST" ]] || {
    echo "Required: --profile, --subdomain-host" >&2
    exit 1
}

AWS() { aws --profile "$PROFILE" --region "$REGION" "$@"; }
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

EXISTING=$(AWS ec2 describe-instances \
    --filters "Name=tag:Name,Values=burla-relay" \
    "Name=instance-state-name,Values=pending,running,stopping,stopped" \
    --query "Reservations[0].Instances[0].InstanceId" --output text)
if [[ "$EXISTING" != "None" && -n "$EXISTING" ]]; then
    echo "A burla-relay instance already exists: $EXISTING" >&2
    echo "Terminate it first to redeploy (user-data only runs on first boot)." >&2
    exit 1
fi

SG_ID=$(AWS ec2 describe-security-groups \
    --filters "Name=group-name,Values=burla-relay" \
    --query "SecurityGroups[0].GroupId" --output text)
if [[ "$SG_ID" == "None" || -z "$SG_ID" ]]; then
    SG_ID=$(AWS ec2 create-security-group --group-name burla-relay \
        --description "Burla relay (443 = SNI tunnel traffic, 7000 = frpc control)" \
        --query GroupId --output text)
fi
# 443 = client/browser connections routed by SNI, 7000 = frpc tunnels dialing in.
AWS ec2 authorize-security-group-ingress --group-id "$SG_ID" \
    --protocol tcp --port 443 --cidr 0.0.0.0/0 2>/dev/null || true
AWS ec2 authorize-security-group-ingress --group-id "$SG_ID" \
    --protocol tcp --port 7000 --cidr 0.0.0.0/0 2>/dev/null || true

ALLOCATION_ID=$(AWS ec2 describe-addresses \
    --filters "Name=tag:Name,Values=burla-relay" \
    --query "Addresses[0].AllocationId" --output text)
if [[ "$ALLOCATION_ID" == "None" || -z "$ALLOCATION_ID" ]]; then
    ALLOCATION_ID=$(AWS ec2 allocate-address --domain vpc \
        --tag-specifications "ResourceType=elastic-ip,Tags=[{Key=Name,Value=burla-relay}]" \
        --query AllocationId --output text)
fi
STATIC_IP=$(AWS ec2 describe-addresses --allocation-ids "$ALLOCATION_ID" \
    --query "Addresses[0].PublicIp" --output text)

AMI_ID=$(AWS ssm get-parameter \
    --name /aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id \
    --query "Parameter.Value" --output text)

USER_DATA=$(mktemp)
trap 'rm -f "$USER_DATA"' EXIT
{
    cat <<HEADER
#!/bin/bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y docker.io
systemctl enable --now docker
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
docker rm -f burla-relay-auth burla-relay-frps || true
docker run -d --restart=always --network=host --name=burla-relay-auth \\
    -v /etc/burla-relay/auth_plugin.py:/app/auth_plugin.py:ro \\
    -e BURLA_BACKEND_URL="$BACKEND_URL" \\
    -w /app python:3.13-slim bash -c \\
    "pip install --quiet fastapi uvicorn requests && uvicorn auth_plugin:app --host 127.0.0.1 --port 9000"
docker run -d --restart=always --network=host --name=burla-relay-frps \\
    -v /etc/burla-relay/frps.toml:/etc/frp/frps.toml:ro \\
    -e FRP_SUBDOMAIN_HOST="$SUBDOMAIN_HOST" \\
    -e FRP_AUTH_PLUGIN_ADDR="127.0.0.1:9000" \\
    fatedier/frps:$FRP_VERSION -c /etc/frp/frps.toml
FOOTER
} > "$USER_DATA"

INSTANCE_ID=$(AWS ec2 run-instances \
    --image-id "$AMI_ID" \
    --instance-type "$INSTANCE_TYPE" \
    --security-group-ids "$SG_ID" \
    --user-data "file://$USER_DATA" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=burla-relay}]" \
    --query "Instances[0].InstanceId" --output text)

AWS ec2 wait instance-exists --instance-ids "$INSTANCE_ID" 2>/dev/null || true
AWS ec2 wait instance-running --instance-ids "$INSTANCE_ID"
AWS ec2 associate-address --instance-id "$INSTANCE_ID" \
    --allocation-id "$ALLOCATION_ID" --allow-reassociation >/dev/null

echo ""
echo "Relay instance $INSTANCE_ID deployed. Point DNS at the elastic IP:"
echo "  *.$SUBDOMAIN_HOST  A  $STATIC_IP"
echo "  $SUBDOMAIN_HOST    A  $STATIC_IP"
