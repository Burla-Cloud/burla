#!/usr/bin/env bash
set -euo pipefail

LOG_PATH="/var/log/burla-vm-bootstrap.log"
READY_PATH="__BOOTSTRAP_READY_PATH__"
DEV_VM_USER="__DEV_VM_USER__"
REMOTE_REPO_DIR="__REMOTE_REPO_DIR__"
REMOTE_LOG_PATH="__REMOTE_LOG_PATH__"

mkdir -p "$(dirname "$LOG_PATH")"
exec > >(tee -a "$LOG_PATH") 2>&1

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y ca-certificates curl git gnupg jq make python3 python3-pip python3-venv rsync software-properties-common tar tmux

install -m 0755 -d /etc/apt/keyrings

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg
DOCKER_REPO="deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable"
echo "$DOCKER_REPO" > /etc/apt/sources.list.d/docker.list

curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /etc/apt/keyrings/google-cloud-cli.gpg
chmod a+r /etc/apt/keyrings/google-cloud-cli.gpg
GCLOUD_REPO="deb [signed-by=/etc/apt/keyrings/google-cloud-cli.gpg] https://packages.cloud.google.com/apt cloud-sdk main"
echo "$GCLOUD_REPO" > /etc/apt/sources.list.d/google-cloud-cli.list

curl -fsSL https://deb.nodesource.com/setup_20.x | bash -

apt-get update
apt-get install -y containerd.io docker-buildx-plugin docker-ce docker-ce-cli docker-compose-plugin google-cloud-cli nodejs
systemctl enable --now docker

curl -LsSf https://astral.sh/uv/install.sh | sh
install -m 0755 /root/.local/bin/uv /usr/local/bin/uv
install -m 0755 /root/.local/bin/uvx /usr/local/bin/uvx

id -u "$DEV_VM_USER" >/dev/null 2>&1 || useradd --create-home --shell /bin/bash "$DEV_VM_USER"
usermod -aG docker "$DEV_VM_USER"

mkdir -p "$REMOTE_REPO_DIR"
chown -R "$DEV_VM_USER:$DEV_VM_USER" "$REMOTE_REPO_DIR"

mkdir -p "$(dirname "$REMOTE_LOG_PATH")"
touch "$REMOTE_LOG_PATH"
chown "$DEV_VM_USER:$DEV_VM_USER" "$REMOTE_LOG_PATH"

mkdir -p "$(dirname "$READY_PATH")"
touch "$READY_PATH"
