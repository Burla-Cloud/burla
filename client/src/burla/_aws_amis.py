import hashlib

# Pin discovery to Burla's production account so similarly named public images
# cannot be substituted by another publisher.
PUBLIC_AMI_OWNER_ID = "018789813546"

_NODE_AMI_SETUP_SCRIPT = """#!/bin/bash
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y docker.io git jq curl psmisc build-essential
systemctl enable docker

# mountpoint-s3 (mounts the shared-workspace bucket at /workspace/shared)
curl -fsSL https://s3.amazonaws.com/mountpoint-s3-release/latest/x86_64/mount-s3.deb -o /tmp/mount-s3.deb
apt-get install -y /tmp/mount-s3.deb

# uv + python 3.13 (mirrors the GCP disk image)
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:$PATH"
uv python install 3.13
ln -sf "$(uv python find 3.13)" /usr/local/bin/python3
ln -sf /usr/local/bin/python3 /usr/local/bin/python

# node_service repo + pre-warmed venv so node boots are fast
mkdir -p /opt && cd /opt
git clone --depth 1 --branch main https://github.com/Burla-Cloud/burla.git --no-checkout
cd burla
git sparse-checkout init --cone
git sparse-checkout set node_service client
git checkout main
uv venv /opt/burla/.venv --python 3.13 --seed
echo 'export UV_PROJECT_ENVIRONMENT=/opt/burla/.venv' >> /root/.bashrc
UV_PROJECT_ENVIRONMENT=/opt/burla/.venv uv pip install ./node_service cloudpickle tblib

# EC2 has no shutdown-script metadata like GCE; this unit tells the
# node_service the VM is going away (spot interruption, manual stop).
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
"""

# Driver stack comes from Ubuntu's archive, all built from one SRU cycle so
# the kernel modules (precompiled + signed for this exact aws kernel, no
# DKMS, so no GPU is needed to build this image), userspace libs, and fabric
# manager stay version-locked. FM matters: NVSwitch machines (p4d.*,
# p5.48xlarge) refuse to run CUDA unless a fabric manager exactly matching
# the driver is running. NVIDIA's own CUDA repo can't serve this: it dropped
# classic fabricmanager packaging after driver branch 575 (580+ only ships
# Blackwell's nvlink5). GL/X libs are skipped (headless), decode/encode are
# kept so containers requesting the `video` capability still start. The
# toolkit (not in Ubuntu's archive) registers the `nvidia` docker runtime
# the workers' host_config asks for.
_GPU_AMI_EXTRA_SETUP = """
apt-get install -y \
  linux-modules-nvidia-580-server-$(uname -r) \
  nvidia-headless-no-dkms-580-server \
  nvidia-utils-580-server \
  libnvidia-decode-580-server \
  libnvidia-encode-580-server \
  nvidia-fabricmanager-580
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg
curl -fsSL https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' > /etc/apt/sources.list.d/nvidia-container-toolkit.list
apt-get update
apt-get install -y nvidia-container-toolkit
nvidia-ctk runtime configure --runtime=docker
systemctl enable nvidia-fabricmanager
"""

_AMI_SETUP_FINISH = """
# Every clone needs its own cloud-init state and system identity.
cloud-init clean --logs --machine-id

# Powering off signals the installer that setup is complete.
shutdown -h now
"""


def node_ami_setup_script(gpu: bool) -> str:
    gpu_part = _GPU_AMI_EXTRA_SETUP if gpu else ""
    return _NODE_AMI_SETUP_SCRIPT + gpu_part + _AMI_SETUP_FINISH


# Nodes fetch the requested Burla code at boot, so releases reuse an image until
# its actual machine setup changes.
def node_ami_hash(gpu: bool) -> str:
    return hashlib.sha256(node_ami_setup_script(gpu).encode()).hexdigest()[:12]


def node_ami_name_prefix(gpu: bool) -> str:
    variant = "gpu" if gpu else "nogpu"
    return f"burla-node-{variant}-{node_ami_hash(gpu)}"


def public_node_ami_id(ec2, gpu: bool) -> str:
    response = ec2.describe_images(
        Filters=[
            {"Name": "owner-id", "Values": [PUBLIC_AMI_OWNER_ID]},
            {"Name": "architecture", "Values": ["x86_64"]},
            {"Name": "name", "Values": [f"{node_ami_name_prefix(gpu)}-*"]},
            {"Name": "is-public", "Values": ["true"]},
            {"Name": "state", "Values": ["available"]},
        ],
    )
    images = sorted(
        response["Images"], key=lambda image: image["CreationDate"], reverse=True
    )
    if not images:
        variant = "GPU" if gpu else "CPU"
        raise RuntimeError(
            f"Burla has no public {variant} node image in {ec2.meta.region_name}."
        )
    return images[0]["ImageId"]
