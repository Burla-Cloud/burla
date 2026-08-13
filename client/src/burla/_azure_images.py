import hashlib

PUBLIC_GALLERY_NAME = "Burla-2c8c1432-e210-4810-a6c6-a26a3da1658e"
PUBLIC_IMAGE_NAME = "burla-node-cpu-v2"
_PUBLIC_IMAGE_REGION_ORDER = (
    "eastus",
    "eastus2",
    "centralus",
    "northcentralus",
    "southcentralus",
    "westcentralus",
    "westus",
    "westus2",
    "westus3",
    "canadacentral",
    "canadaeast",
    "brazilsouth",
    "northeurope",
    "westeurope",
    "uksouth",
    "ukwest",
    "francecentral",
    "germanywestcentral",
    "swedencentral",
    "switzerlandnorth",
    "norwayeast",
    "italynorth",
    "polandcentral",
    "spaincentral",
    "eastasia",
    "southeastasia",
    "japaneast",
    "japanwest",
    "koreacentral",
    "centralindia",
    "southindia",
    "australiaeast",
    "uaenorth",
    "southafricanorth",
    "qatarcentral",
    "israelcentral",
)
PUBLIC_IMAGE_REGIONS = tuple(
    region for region in _PUBLIC_IMAGE_REGION_ORDER if region != "westus"
)
PUBLIC_IMAGE_REGIONS_PER_VERSION = 5
_PUBLIC_IMAGE_REGION_VERSION_OFFSETS = {
    "westcentralus": 201,
    "westus": 202,
    "westus2": 203,
    "westus3": 204,
    "canadacentral": 205,
}

NODE_IMAGE_SETUP_SCRIPT = """#!/bin/bash
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y docker.io git jq curl psmisc build-essential fuse3
systemctl enable docker

# The deployed shared workspace uses blobfuse2 with the node identity.
curl -fsSL https://packages.microsoft.com/config/ubuntu/22.04/packages-microsoft-prod.deb -o /tmp/pms.deb
dpkg -i /tmp/pms.deb
apt-get update
apt-get install -y blobfuse2
grep -q '^user_allow_other' /etc/fuse.conf || echo user_allow_other >> /etc/fuse.conf

# Keep the Python runtime aligned with the AWS and GCP node images.
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:$PATH"
uv python install 3.13
ln -sf "$(uv python find 3.13)" /usr/local/bin/python3
ln -sf /usr/local/bin/python3 /usr/local/bin/python

# A warm environment keeps node boot time independent of package download time.
mkdir -p /opt && cd /opt
git clone --depth 1 --branch main https://github.com/Burla-Cloud/burla.git --no-checkout
cd burla
git sparse-checkout init --cone
git sparse-checkout set node_service client
git checkout main
uv venv /opt/burla/.venv --python 3.13 --seed
echo 'export UV_PROJECT_ENVIRONMENT=/opt/burla/.venv' >> /root/.bashrc
UV_PROJECT_ENVIRONMENT=/opt/burla/.venv uv pip install ./node_service cloudpickle tblib

# EC2 and Azure lack GCE shutdown-script metadata, so node_service needs notice.
cat > /etc/systemd/system/burla-shutdown-hook.service <<'EOF'
[Unit]
Description=Tell burla node_service this VM is shutting down
DefaultDependencies=no
Before=shutdown.target

[Service]
Type=oneshot
ExecStart=/usr/bin/curl -s -X POST http://localhost:8081/shutdown
TimeoutStartSec=10

[Install]
WantedBy=shutdown.target
EOF
systemctl enable burla-shutdown-hook

# Generalization makes one source image safe to clone into every subscription.
/usr/bin/python3 /usr/sbin/waagent -deprovision+user -force
shutdown -h now
"""

NODE_IMAGE_HASH = hashlib.sha256(NODE_IMAGE_SETUP_SCRIPT.encode()).hexdigest()[:12]


def public_node_image_version(region: str) -> str:
    if region not in PUBLIC_IMAGE_REGIONS:
        raise ValueError(f"Burla's public Azure image is unavailable in {region}.")
    group = _PUBLIC_IMAGE_REGION_ORDER.index(region) // PUBLIC_IMAGE_REGIONS_PER_VERSION
    offset = _PUBLIC_IMAGE_REGION_VERSION_OFFSETS.get(region, group)
    components = [
        int(NODE_IMAGE_HASH[index : index + 4], 16) for index in range(0, 12, 4)
    ]
    # Small regional versions avoid Azure's CRP-PIR failures on large updates.
    components[-1] = components[-1] * 100 + offset
    return ".".join(str(component) for component in components)


def public_node_image_id(region: str) -> str:
    return (
        f"/CommunityGalleries/{PUBLIC_GALLERY_NAME}"
        f"/Images/{PUBLIC_IMAGE_NAME}/Versions/{public_node_image_version(region)}"
    )
