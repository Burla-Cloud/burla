#!/bin/bash
# Deploys (or updates) one Burla relay VM (frps + auth plugin) in an AWS
# account. Safe to run unconditionally (release CI does): the relay config is
# fingerprinted, and a relay already running the current config is left alone.
# When the config changed, a replacement VM is booted first and the elastic IP
# only moves once the new frps is accepting connections, so tunnels see a
# seconds-long blip rather than minutes of downtime. Point *.<subdomain-host>
# and <subdomain-host> at the printed elastic IP (one-time DNS setup).
#
# Usage:
#   ./deploy_relay_aws.sh --region us-east-1 --subdomain-host relay.burla.dev \
#       [--profile burla-prod] [--backend-url https://backend.burla.dev] \
#       [--instance-type t3.small]
#
# Without --profile the default AWS credential chain is used (CI role).
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
[[ -n "$SUBDOMAIN_HOST" ]] || {
    echo "Required: --subdomain-host" >&2
    exit 1
}

AWS() { aws ${PROFILE:+--profile "$PROFILE"} --region "$REGION" "$@"; }
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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

# The user-data embeds everything the VM's behavior derives from (plugin code,
# frps config, frp version, backend URL, subdomain host), so its hash + the
# instance type decide whether the running relay is already up to date. The
# AMI is deliberately not included: the "current Ubuntu" alias changes every
# few weeks and would churn the relay for nothing (unattended-upgrades patches
# the running VM).
FINGERPRINT=$(cat "$USER_DATA" <(echo "$INSTANCE_TYPE") \
    | openssl dgst -sha256 -r | cut -c1-16)

OLD_INSTANCE=$(AWS ec2 describe-instances \
    --filters "Name=tag:Name,Values=burla-relay" \
    "Name=instance-state-name,Values=pending,running,stopping,stopped" \
    --query "Reservations[0].Instances[0].InstanceId" --output text)
[[ "$OLD_INSTANCE" == "None" ]] && OLD_INSTANCE=""
if [[ -n "$OLD_INSTANCE" ]]; then
    OLD_FINGERPRINT=$(AWS ec2 describe-tags \
        --filters "Name=resource-id,Values=$OLD_INSTANCE" \
        "Name=key,Values=burla-relay-fingerprint" \
        --query "Tags[0].Value" --output text)
    if [[ "$OLD_FINGERPRINT" == "$FINGERPRINT" ]]; then
        echo "Relay $OLD_INSTANCE already runs this config (fingerprint $FINGERPRINT)."
        exit 0
    fi
    echo "Relay $OLD_INSTANCE runs old config; replacing it."
fi

INSTANCE_ID=$(AWS ec2 run-instances \
    --image-id "$AMI_ID" \
    --instance-type "$INSTANCE_TYPE" \
    --security-group-ids "$SG_ID" \
    --user-data "file://$USER_DATA" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=burla-relay},{Key=burla-relay-fingerprint,Value=$FINGERPRINT}]" \
    --query "Instances[0].InstanceId" --output text)

AWS ec2 wait instance-exists --instance-ids "$INSTANCE_ID" 2>/dev/null || true
AWS ec2 wait instance-running --instance-ids "$INSTANCE_ID"

# Wait for frps to accept connections on the new VM's temporary public IP
# before pointing the elastic IP (i.e. live tunnels) at it.
BOOT_IP=$(AWS ec2 describe-instances --instance-ids "$INSTANCE_ID" \
    --query "Reservations[0].Instances[0].PublicIpAddress" --output text)
echo "Waiting for frps on $BOOT_IP:7000 (docker install takes a few minutes) ..."
for attempt in $(seq 1 120); do
    if python3 -c "import socket; socket.create_connection(('$BOOT_IP', 7000), 3)" 2>/dev/null; then
        break
    fi
    if [[ "$attempt" == 120 ]]; then
        echo "frps never came up on $INSTANCE_ID; leaving the old relay in place." >&2
        exit 1
    fi
    sleep 5
done

AWS ec2 associate-address --instance-id "$INSTANCE_ID" \
    --allocation-id "$ALLOCATION_ID" --allow-reassociation >/dev/null

if [[ -n "$OLD_INSTANCE" ]]; then
    AWS ec2 terminate-instances --instance-ids "$OLD_INSTANCE" >/dev/null
    echo "Terminated old relay $OLD_INSTANCE."
fi

echo ""
echo "Relay instance $INSTANCE_ID deployed. Point DNS at the elastic IP:"
echo "  *.$SUBDOMAIN_HOST  A  $STATIC_IP"
echo "  $SUBDOMAIN_HOST    A  $STATIC_IP"
