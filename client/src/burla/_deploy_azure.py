"""
`burla deploy --cloud=azure`.

Mirrors the AWS deploy: managed identities + roles, network, a blob
shared-workspace container, a node managed image (the Azure twin of the AWS
AMI), and one small always-on head VM running main_service.

Everything lives in one resource group (`burla`). Azure has no cross-cloud
equivalent of "credential-less VM that terminates on poweroff", so unlike AWS
every node VM carries the `burla-node` user-assigned identity, whose custom
role only allows deleting burla resources (see providers/azure.py).
"""

import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from time import sleep, time
from urllib.parse import urlparse

import requests

from burla import _BURLA_BACKEND_URL, _BURLA_NODE_SOURCE_REF, __version__
from burla._helpers import run_command, VerboseCalledProcessError
from burla._deploy import (
    RELAY_HOST,
    RELAY_SERVER_ADDR,
    RELAY_SERVER_PORT,
    FRP_VERSION,
    head_install_spec,
)
from burla._reporting import log_telemetry

RESOURCE_GROUP = "burla"
HEAD_VM_SIZE = "Standard_B2s"
IMAGE_BUILDER_VM_SIZE = "Standard_D2s_v5"
UBUNTU_IMAGE_URN = "Canonical:0001-com-ubuntu-server-jammy:22_04-lts-gen2:latest"
NODE_SELF_DELETE_ROLE = "Burla Node Self Delete"
SHARED_WORKSPACE_CONTAINER = "shared-workspace"

_RESOURCE_PROVIDERS = (
    "Microsoft.Compute",
    "Microsoft.Network",
    "Microsoft.Storage",
    "Microsoft.ManagedIdentity",
)


def _az(cmd: str, parse_json: bool = True, raise_error: bool = True):
    result = run_command(f"az {cmd}", raise_error=raise_error)
    if result.returncode != 0:
        return None
    stdout = result.stdout.decode().strip()
    if parse_json and stdout:
        return json.loads(stdout)
    return stdout


def _azure_subscription() -> tuple[str, str]:
    """(subscription_id, subscription_name) for the CLI's active subscription."""
    account = _az("account show")
    return account["id"], account["name"]


def _azure_region() -> str:
    import os

    region = os.environ.get("AZURE_REGION")
    if region:
        return region
    configured = _az("config get defaults.location", raise_error=False)
    if configured and configured.get("value"):
        return configured["value"]
    return "eastus"


def _azure_ownership_payload() -> dict:
    subscription_id, _ = _azure_subscription()
    access_token = _az(
        'account get-access-token --resource "https://management.azure.com/" '
        "--query accessToken --output tsv",
        parse_json=False,
    )
    return {
        "cloud": "azure",
        "subscription_id": subscription_id,
        "access_token": access_token,
    }


def _storage_account_name(subscription_id: str) -> str:
    # Storage account names: 3-24 chars, lowercase alphanumeric, globally
    # unique. 16 hex chars of the subscription id is unique enough in practice.
    return f"burla{subscription_id.replace('-', '')[:16]}"


# ------------------------------------------------------------------ prep
# Everything below is idempotent and shared with client-hosted mode
# (_prepare_azure in _local_head.py), which needs the same network, node
# identity, and node image before it can boot its first VM.


def register_resource_providers():
    """Fresh subscriptions have Compute/Network/Storage unregistered, and the
    SDK (unlike the az CLI) never auto-registers them."""
    for namespace in _RESOURCE_PROVIDERS:
        state = _az(
            f"provider show --namespace {namespace} "
            "--query registrationState --output tsv",
            parse_json=False,
            raise_error=False,
        )
        if state != "Registered":
            run_command(f"az provider register --namespace {namespace} --wait")


def ensure_resource_group(region: str):
    # The group's location is only metadata (resources inside it can live in
    # any region), and re-creating with a different location is an error.
    exists = _az(f"group exists --name {RESOURCE_GROUP}", parse_json=True)
    if not exists:
        run_command(f"az group create --name {RESOURCE_GROUP} --location {region}")


def ensure_network(region: str):
    """VNet with a nodes subnet + a head subnet, each behind an NSG that only
    admits VNet-internal traffic. Clients reach nodes + dashboard through the
    relay (VMs dial out to it), so nothing is open to the internet."""
    vnet = f"burla-{region}"
    node_nsg = f"burla-cluster-node-{region}"
    head_nsg = f"burla-head-{region}"

    existing = _az(
        f"network vnet show --resource-group {RESOURCE_GROUP} --name {vnet}",
        raise_error=False,
    )
    if existing:
        return

    run_command(
        f"az network nsg create --resource-group {RESOURCE_GROUP} "
        f"--name {node_nsg} --location {region}"
    )
    run_command(
        f"az network nsg rule create --resource-group {RESOURCE_GROUP} "
        f"--nsg-name {node_nsg} --name allow-node-port --priority 100 "
        "--access Allow --direction Inbound --protocol Tcp "
        "--source-address-prefixes VirtualNetwork --destination-port-ranges 8080"
    )
    run_command(
        f"az network nsg create --resource-group {RESOURCE_GROUP} "
        f"--name {head_nsg} --location {region}"
    )
    run_command(
        f"az network nsg rule create --resource-group {RESOURCE_GROUP} "
        f"--nsg-name {head_nsg} --name allow-node-to-head --priority 100 "
        "--access Allow --direction Inbound --protocol Tcp "
        "--source-address-prefixes VirtualNetwork --destination-port-ranges 8443"
    )
    run_command(
        f"az network vnet create --resource-group {RESOURCE_GROUP} --name {vnet} "
        f"--location {region} --address-prefixes 10.0.0.0/16"
    )
    run_command(
        f"az network vnet subnet create --resource-group {RESOURCE_GROUP} "
        f"--vnet-name {vnet} --name nodes --address-prefixes 10.0.0.0/20 "
        f"--network-security-group {node_nsg}"
    )
    run_command(
        f"az network vnet subnet create --resource-group {RESOURCE_GROUP} "
        f"--vnet-name {vnet} --name head --address-prefixes 10.0.16.0/28 "
        f"--network-security-group {head_nsg}"
    )


def _ensure_role_assignment(principal_id: str, role: str, scope: str):
    """Idempotent role assignment with retries: a just-created identity takes
    a few seconds to become assignable (PrincipalNotFound)."""
    for attempt in range(6):
        result = run_command(
            f'az role assignment create --assignee-object-id {principal_id} '
            f'--assignee-principal-type ServicePrincipal --role "{role}" '
            f'--scope "{scope}"',
            raise_error=False,
        )
        stderr = result.stderr.decode()
        if result.returncode == 0 or "RoleAssignmentExists" in stderr:
            return
        if "PrincipalNotFound" not in stderr:
            raise VerboseCalledProcessError("az role assignment create", result.stderr)
        sleep(10)
    raise Exception(f"Role assignment for {principal_id} never became possible.")


def ensure_node_identity(subscription_id: str) -> dict:
    """The burla-node user-assigned identity + its self-delete role. Returns
    {"id", "principalId", "clientId"}."""
    identity = _az(
        f"identity create --resource-group {RESOURCE_GROUP} --name burla-node"
    )

    existing_role = _az(
        f'role definition list --name "{NODE_SELF_DELETE_ROLE}" '
        f'--scope "/subscriptions/{subscription_id}"'
    )
    if not existing_role:
        role_definition = {
            "Name": NODE_SELF_DELETE_ROLE,
            "Description": (
                "Lets a burla node VM delete itself (and its NIC/IP/disk) "
                "when its cluster head is gone."
            ),
            "Actions": [
                "Microsoft.Compute/virtualMachines/delete",
                "Microsoft.Compute/disks/delete",
                "Microsoft.Network/networkInterfaces/delete",
                "Microsoft.Network/publicIPAddresses/delete",
            ],
            "AssignableScopes": [f"/subscriptions/{subscription_id}"],
        }
        with tempfile.NamedTemporaryFile("w", suffix=".json") as role_file:
            json.dump(role_definition, role_file)
            role_file.flush()
            run_command(f"az role definition create --role-definition {role_file.name}")

    scope = f"/subscriptions/{subscription_id}/resourceGroups/{RESOURCE_GROUP}"
    _ensure_role_assignment(identity["principalId"], NODE_SELF_DELETE_ROLE, scope)
    return identity


# ------------------------------------------------------------------ node image

_NODE_IMAGE_SETUP_SCRIPT = """#!/bin/bash
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y docker.io git jq curl psmisc build-essential fuse3
systemctl enable docker

# blobfuse2 (mounts the shared-workspace container at /workspace/shared)
curl -fsSL https://packages.microsoft.com/config/ubuntu/22.04/packages-microsoft-prod.deb -o /tmp/pms.deb
dpkg -i /tmp/pms.deb
apt-get update
apt-get install -y blobfuse2
grep -q '^user_allow_other' /etc/fuse.conf || echo user_allow_other >> /etc/fuse.conf

# uv + python 3.13 (mirrors the AWS/GCP node images)
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:$PATH"
uv python install 3.13
ln -sf "$(uv python find 3.13)/python" /usr/local/bin/python3 || true
ln -sf /usr/local/bin/python3 /usr/local/bin/python || true

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

# Azure has no shutdown-script metadata like GCE; this unit tells the
# node_service the VM is going away (spot eviction, manual stop).
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

# Generalize so a managed image can be captured from this VM; powering off
# signals the installer that setup is complete.
waagent -deprovision+user -force
shutdown -h now
"""

# The image only bakes docker, uv, and a warm node_service venv; nodes
# git-fetch the code they actually run at boot. So it is keyed by this
# script's content rather than by the burla version: releasing must never
# trigger a 10-minute rebuild.
NODE_IMAGE_HASH = hashlib.sha256(_NODE_IMAGE_SETUP_SCRIPT.encode()).hexdigest()[:12]


def _throwaway_ssh_key(directory: str) -> str:
    """Azure VMs require an admin SSH key; nobody ever logs into burla VMs,
    so mint one in a temp dir instead of touching ~/.ssh."""
    key_path = Path(directory) / "burla_builder_key"
    subprocess.run(
        ["ssh-keygen", "-t", "rsa", "-b", "2048", "-N", "", "-q", "-f", str(key_path)],
        check=True,
    )
    return str(key_path) + ".pub"


def ensure_node_image(spinner, region: str) -> str:
    """Build the burla node managed image (docker + git repo + uv + blobfuse2)
    if this region has no image matching the current setup script. Takes ~10
    minutes, and only happens when that script changes, not on every release."""
    images = (
        _az(f"image list --resource-group {RESOURCE_GROUP}", raise_error=False) or []
    )
    for image in sorted(images, key=lambda i: i["name"], reverse=True):
        tags = image.get("tags") or {}
        is_match = tags.get("burla-node-image-hash") == NODE_IMAGE_HASH
        if is_match and image["location"] == region:
            spinner.text = f"Node image ... using existing {image['name']}."
            spinner.ok("✓")
            return image["id"]

    spinner.text = (
        "Building node image (takes ~10 minutes, only when the image changes) ... "
    )
    spinner.start()

    builder_name = f"burla-image-builder-{int(time())}"
    with tempfile.TemporaryDirectory() as tmp:
        custom_data_path = Path(tmp) / "setup.sh"
        custom_data_path.write_text(_NODE_IMAGE_SETUP_SCRIPT)
        public_key_path = _throwaway_ssh_key(tmp)
        run_command(
            f"az vm create --resource-group {RESOURCE_GROUP} --name {builder_name} "
            f"--location {region} --image {UBUNTU_IMAGE_URN} "
            f"--size {IMAGE_BUILDER_VM_SIZE} --admin-username burla "
            f"--ssh-key-values {public_key_path} "
            f"--vnet-name burla-{region} --subnet nodes --nsg '' "
            f"--public-ip-sku Standard "
            f"--os-disk-delete-option Delete --nic-delete-option Delete "
            f"--custom-data {custom_data_path}"
        )

    try:
        # Azure's apt mirror has been observed serving packages at ~50 KB/s;
        # two hours comfortably covers even that.
        deadline = time() + 7200
        power_state = None
        while time() < deadline:
            power_state = _az(
                f"vm get-instance-view --resource-group {RESOURCE_GROUP} "
                f"--name {builder_name} --query \"instanceView.statuses"
                '[?starts_with(code, \'PowerState/\')].code | [0]" --output tsv',
                parse_json=False,
                raise_error=False,
            )
            if power_state == "PowerState/stopped":
                break
            sleep(15)
        if power_state != "PowerState/stopped":
            spinner.fail("✗")
            raise Exception(
                f"Image builder VM {builder_name} never stopped (state={power_state}). "
                "Check its boot diagnostics, then delete it and re-run deploy."
            )

        run_command(
            f"az vm deallocate --resource-group {RESOURCE_GROUP} --name {builder_name}"
        )
        run_command(
            f"az vm generalize --resource-group {RESOURCE_GROUP} --name {builder_name}"
        )
        image_name = f"burla-node-nogpu-{NODE_IMAGE_HASH}-{int(time())}"
        image = _az(
            f"image create --resource-group {RESOURCE_GROUP} --name {image_name} "
            # Explicit location: without it the image lands in the resource
            # group's home region, which fails when nodes run elsewhere.
            f"--location {region} "
            f"--source {builder_name} --hyper-v-generation V2 "
            f"--tags burla-node-image=true burla-node-image-hash={NODE_IMAGE_HASH} "
            f"burla-version={__version__}"
        )
    finally:
        run_command(
            f"az vm delete --resource-group {RESOURCE_GROUP} --name {builder_name} --yes",
            raise_error=False,
        )
        # --nic-delete-option covers the NIC; the auto-created public IP is
        # not attached to the VM resource and needs its own delete.
        run_command(
            f"az network public-ip delete --resource-group {RESOURCE_GROUP} "
            f"--name {builder_name}PublicIP",
            raise_error=False,
        )

    spinner.text = f"Building node image ... Done ({image_name})."
    spinner.ok("✓")
    return image["id"]


# ------------------------------------------------------------------ deploy


def _head_setup_commands(
    project_id: str,
    subscription_id: str,
    region: str,
    dashboard_hostname: str,
    cluster_id_token: str,
    account_name: str,
    head_identity_client_id: str,
    storage_account: str,
) -> list[str]:
    node_source_ref = _BURLA_NODE_SOURCE_REF
    install_spec = head_install_spec()
    relay_subdomain = f"head--{project_id}"
    return [
        "set -eu",
        "export DEBIAN_FRONTEND=noninteractive",
        "apt-get update",
        "apt-get install -y docker.io",
        "systemctl enable --now docker",
        "mkdir -p /var/lib/burla/tls /var/lib/burla/caddy /etc/burla",
        "docker pull python:3.13",
        "docker pull caddy:2.10.2-alpine",
        f'CLUSTER_ID_TOKEN="{cluster_id_token}"',
        "docker rm -f burla-main-service burla-head-caddy burla-head-frpc || true",
        (
            "docker run -d --restart=always --network=host --name=burla-main-service "
            "-v /var/lib/burla:/var/lib/burla "
            f'-e PROJECT_ID="{project_id}" '
            '-e CLUSTER_ID_TOKEN="$CLUSTER_ID_TOKEN" '
            "-e CLOUD_PROVIDER=azure "
            f'-e AZURE_SUBSCRIPTION_ID="{subscription_id}" '
            f'-e AZURE_RESOURCE_GROUP="{RESOURCE_GROUP}" '
            f'-e AZURE_REGION="{region}" '
            # Which managed identity DefaultAzureCredential should use.
            f'-e AZURE_CLIENT_ID="{head_identity_client_id}" '
            f'-e CLOUD_ACCOUNT_NAME="{account_name}" '
            "-e BIND_HOST=127.0.0.1 "
            "-e PORT=5001 "
            "-e INTERNAL_TLS_PORT=8443 "
            "-e HISTORY_DB_PATH=/var/lib/burla/history.db "
            f'-e SHARED_WORKSPACE_BUCKET="{storage_account}" '
            f'-e BURLA_BACKEND_URL="{_BURLA_BACKEND_URL}" '
            f'-e BURLA_RELAY_HOST="{RELAY_HOST}" '
            f'-e BURLA_RELAY_SERVER_ADDR="{RELAY_SERVER_ADDR}" '
            f'-e BURLA_RELAY_SERVER_PORT="{RELAY_SERVER_PORT}" '
            f'-e BURLA_NODE_SOURCE_REF="{node_source_ref}" '
            "python:3.13 "
            f"sh -c 'pip install --no-cache-dir \"{install_spec}\" "
            "&& exec python -m uvicorn main_service:app "
            "--host 127.0.0.1 --port 5001 --workers 1 --timeout-keep-alive 60'"
        ),
        (
            "until curl --fail --silent http://127.0.0.1:5001/version >/dev/null; "
            "do sleep 1; done"
        ),
        "rm -rf /etc/burla/Caddyfile",
        (
            "cat > /etc/burla/Caddyfile <<EOF\n"
            f"{dashboard_hostname} {{\n"
            "  reverse_proxy 127.0.0.1:5001\n"
            "}\n"
            ":8443 {\n"
            "  tls /etc/burla/tls/head.pem /etc/burla/tls/head.key\n"
            "  reverse_proxy 127.0.0.1:5001\n"
            "}\n"
            "EOF"
        ),
        (
            "docker run -d --restart=always --network=host --name=burla-head-caddy "
            "-v /etc/burla/Caddyfile:/etc/caddy/Caddyfile:ro "
            "-v /var/lib/burla/tls/head.pem:/etc/burla/tls/head.pem:ro "
            "-v /var/lib/burla/tls/head.key:/etc/burla/tls/head.key:ro "
            "-v /var/lib/burla/caddy:/data "
            "caddy:2.10.2-alpine caddy run --config /etc/caddy/Caddyfile --adapter caddyfile"
        ),
        (
            "cat > /etc/burla/frpc.toml <<EOF\n"
            f'serverAddr = "{RELAY_SERVER_ADDR}"\n'
            f"serverPort = {RELAY_SERVER_PORT}\n"
            "loginFailExit = false\n"
            f'user = "{project_id}"\n'
            'metadatas.token = "$CLUSTER_ID_TOKEN"\n'
            "transport.poolCount = 4\n"
            "\n"
            "[[proxies]]\n"
            f'name = "{relay_subdomain}"\n'
            'type = "https"\n'
            'localIP = "127.0.0.1"\n'
            "localPort = 443\n"
            f'subdomain = "{relay_subdomain}"\n'
            "EOF"
        ),
        "chmod 600 /etc/burla/frpc.toml",
        (
            "docker run -d --restart=always --network=host --name=burla-head-frpc "
            "-v /etc/burla/frpc.toml:/etc/frp/frpc.toml:ro "
            f"fatedier/frpc:v{FRP_VERSION} -c /etc/frp/frpc.toml"
        ),
    ]


def deploy_azure(spinner):
    log_telemetry("Somebody is running `burla deploy --cloud=azure`!")

    spinner.text = "Checking for az CLI ... "
    spinner.start()
    if shutil.which("az") is None:
        spinner.fail("✗")
        msg = "Error: The Azure CLI is not installed or not in your PATH.\n"
        msg += "Please install it from: https://learn.microsoft.com/cli/azure/install-azure-cli"
        print(msg, file=sys.stderr)
        log_telemetry("User does not have the az CLI installed.")
        sys.exit(1)

    subscription_id, account_name = _azure_subscription()
    region = _azure_region()
    # Cluster id: what backend.burla.dev and the dashboard know this cluster
    # as. Keeps the GUID's dashes: the longest relay label this produces
    # (burla-node-xxxxxxxx--azure-<36 char guid>) is exactly the 63-char DNS
    # limit, so nothing may ever be added to this format.
    project_id = f"azure-{subscription_id}"
    spinner.text = f"Checking for az CLI ... Using subscription {account_name} in {region}."
    spinner.ok("✓")
    log_telemetry("Installer has az CLI and is logged in.", project_id=project_id)

    spinner.text = "Preparing subscription (providers, resource group, network) ... "
    spinner.start()
    register_resource_providers()
    ensure_resource_group(region)
    ensure_network(region)
    spinner.text = "Preparing subscription (providers, resource group, network) ... Done."
    spinner.ok("✓")

    storage_account = _storage_account_name(subscription_id)
    _create_storage(spinner, storage_account, region)
    node_identity, head_identity = _create_identities(
        spinner, subscription_id, storage_account
    )
    cluster_id_token = _register_cluster_and_save_token(spinner, project_id, region)
    ensure_node_image(spinner, region)
    dashboard_url = _deploy_head_vm(
        spinner,
        project_id,
        subscription_id,
        region,
        cluster_id_token,
        account_name,
        head_identity,
        storage_account,
    )

    headers = {"Authorization": f"Bearer {cluster_id_token}"}
    url = f"{_BURLA_BACKEND_URL}/v1/clusters/{project_id}/version"
    response = requests.put(url, json={"version": __version__}, headers=headers)
    response.raise_for_status()

    # Point this machine's client at the freshly deployed cluster.
    from burla._auth import save_deployed_cluster_config

    save_deployed_cluster_config("azure", project_id, cluster_id_token, dashboard_url)

    msg = f"\nSuccessfully deployed Burla v{__version__} on Azure!\n"
    msg += f"Quickstart:\n"
    msg += f"  1. Open your new cluster dashboard: {dashboard_url}\n"
    msg += f'  2. Hit "⏻ Start" to boot some machines.\n'
    msg += f"  3. Run `burla login` to connect your laptop to the cluster.\n"
    msg += f"  4. Import and call `remote_parallel_map`!\n\n"
    msg += f"Don't hesitate to E-Mail jake@burla.dev, thank you for using Burla!"
    spinner.write(msg)

    log_telemetry("Burla successfully deployed on Azure!", project_id=project_id)


def _create_storage(spinner, storage_account: str, region: str):
    spinner.text = "Creating storage account ... "
    spinner.start()
    result = run_command(
        f"az storage account create --name {storage_account} "
        f"--resource-group {RESOURCE_GROUP} --location {region} "
        "--sku Standard_LRS --kind StorageV2 --min-tls-version TLS1_2",
        raise_error=False,
    )
    stderr = result.stderr.decode()
    already_exists = "already taken" in stderr or "AlreadyExists" in stderr
    if result.returncode != 0 and not already_exists:
        raise VerboseCalledProcessError("az storage account create", result.stderr)

    account_key = _az(
        f"storage account keys list --account-name {storage_account} "
        f"--resource-group {RESOURCE_GROUP} --query [0].value --output tsv",
        parse_json=False,
    )
    run_command(
        f"az storage container create --name {SHARED_WORKSPACE_CONTAINER} "
        f"--account-name {storage_account} --account-key {account_key}"
    )
    run_command(
        f"az storage cors clear --services b --account-name {storage_account} "
        f"--account-key {account_key}"
    )
    run_command(
        f"az storage cors add --services b --methods GET HEAD POST PUT DELETE "
        f"--origins '*' --allowed-headers '*' "
        f"--exposed-headers 'Content-Type,Content-Length,Location' "
        f"--max-age 3600 --account-name {storage_account} --account-key {account_key}"
    )
    suffix = "Storage account already exists." if already_exists else "Done."
    spinner.text = f"Creating storage account ... {suffix}"
    spinner.ok("✓")


def _create_identities(spinner, subscription_id: str, storage_account: str):
    spinner.text = "Creating managed identities and roles ... "
    spinner.start()

    node_identity = ensure_node_identity(subscription_id)
    head_identity = _az(
        f"identity create --resource-group {RESOURCE_GROUP} --name burla-main-service"
    )

    resource_group_scope = (
        f"/subscriptions/{subscription_id}/resourceGroups/{RESOURCE_GROUP}"
    )
    storage_scope = (
        f"{resource_group_scope}/providers/Microsoft.Storage"
        f"/storageAccounts/{storage_account}"
    )
    # Contributor over the burla resource group covers everything the head
    # does: boot/delete node VMs (and assign them the burla-node identity),
    # list images, read the network.
    _ensure_role_assignment(
        head_identity["principalId"], "Contributor", resource_group_scope
    )
    _ensure_role_assignment(
        head_identity["principalId"], "Storage Blob Data Contributor", storage_scope
    )
    _ensure_role_assignment(
        node_identity["principalId"], "Storage Blob Data Contributor", storage_scope
    )

    spinner.text = "Creating managed identities and roles ... Done."
    spinner.ok("✓")
    return node_identity, head_identity


def _register_cluster_and_save_token(spinner, project_id, region):
    from burla._deploy import AuthError
    from burla._local_head import LocalHeadError, get_or_register_cluster_token

    spinner.text = "Registering cluster ... "
    spinner.start()

    try:
        cluster_id_token = get_or_register_cluster_token("azure", project_id, region)
    except LocalHeadError:
        spinner.fail("✗")
        raise AuthError()

    # ensure deployer is authorized
    installer_email = _az(
        "account show --query user.name --output tsv",
        parse_json=False,
        raise_error=False,
    )
    if installer_email and "@" in installer_email:
        headers = {"Authorization": f"Bearer {cluster_id_token}"}
        users_url = f"{_BURLA_BACKEND_URL}/v1/clusters/{project_id}/users"
        requests.post(users_url, json={"new_user": installer_email}, headers=headers)
    else:
        msg = "Could not infer your email from your Azure identity. After deploying, "
        msg += "run `burla login` to authorize yourself against this cluster."
        spinner.write(msg)

    spinner.text = "Registering cluster ... Done."
    spinner.ok("✓")
    return cluster_id_token


def _run_head_update(head_name: str, commands: list[str]):
    """Azure's SSM equivalent: the walinuxagent-backed run-command."""
    with tempfile.NamedTemporaryFile("w", suffix=".sh") as script_file:
        script_file.write("\n".join(commands) + "\n")
        script_file.flush()
        result = _az(
            f"vm run-command invoke --resource-group {RESOURCE_GROUP} "
            f"--name {head_name} --command-id RunShellScript "
            f"--scripts @{script_file.name}"
        )
    messages = [entry.get("message", "") for entry in result.get("value", [])]
    combined = "\n".join(messages)
    if "[stderr]" in combined and "error" in combined.lower():
        raise Exception(f"Head update failed:\n{combined[-3000:]}")


def _deploy_head_vm(
    spinner,
    project_id: str,
    subscription_id: str,
    region: str,
    cluster_id_token: str,
    account_name: str,
    head_identity: dict,
    storage_account: str,
) -> str:
    spinner.text = "Deploying burla-main-service instance ... "
    spinner.start()

    head_name = "burla-main-service"
    existing = _az(
        f"vm show --resource-group {RESOURCE_GROUP} --name {head_name}",
        raise_error=False,
    )

    from burla._deploy import _register_dashboard, _shutdown_cluster_for_upgrade

    dashboard_url = f"https://head--{project_id}.{RELAY_HOST}"
    if existing:
        _shutdown_cluster_for_upgrade(project_id, cluster_id_token, dashboard_url)

    run_command(
        f"az network public-ip create --resource-group {RESOURCE_GROUP} "
        f"--name {head_name}-pip --location {region} --sku Standard "
        "--allocation-method Static"
    )
    public_ip = _az(
        f"network public-ip show --resource-group {RESOURCE_GROUP} "
        f"--name {head_name}-pip --query ipAddress --output tsv",
        parse_json=False,
    )
    _register_dashboard(
        project_id,
        cluster_id_token,
        public_ip,
        _azure_ownership_payload(),
        dashboard_url,
    )

    commands = _head_setup_commands(
        project_id,
        subscription_id,
        region,
        urlparse(dashboard_url).hostname,
        cluster_id_token,
        account_name,
        head_identity["clientId"],
        storage_account,
    )
    if existing:
        power_state = _az(
            f"vm get-instance-view --resource-group {RESOURCE_GROUP} "
            f"--name {head_name} --query \"instanceView.statuses"
            '[?starts_with(code, \'PowerState/\')].code | [0]" --output tsv',
            parse_json=False,
            raise_error=False,
        )
        if power_state != "PowerState/running":
            run_command(
                f"az vm start --resource-group {RESOURCE_GROUP} --name {head_name}"
            )
        _run_head_update(head_name, commands)
    else:
        with tempfile.TemporaryDirectory() as tmp:
            custom_data_path = Path(tmp) / "setup.sh"
            custom_data_path.write_text("#!/bin/bash\n" + "\n".join(commands) + "\n")
            public_key_path = _throwaway_ssh_key(tmp)
            run_command(
                f"az vm create --resource-group {RESOURCE_GROUP} --name {head_name} "
                f"--location {region} --image {UBUNTU_IMAGE_URN} --size {HEAD_VM_SIZE} "
                f"--admin-username burla --ssh-key-values {public_key_path} "
                f"--vnet-name burla-{region} --subnet head --nsg '' "
                f"--public-ip-address {head_name}-pip "
                f"--assign-identity {head_identity['id']} "
                f"--os-disk-delete-option Delete --nic-delete-option Delete "
                f"--custom-data {custom_data_path}"
            )

    spinner.text = (
        "Deploying burla-main-service instance ... waiting for it to serve traffic ..."
    )
    start = time()
    while time() - start < 600:
        try:
            response = requests.get(
                f"{dashboard_url}/version",
                headers={"Authorization": f"Bearer {cluster_id_token}"},
                timeout=3,
            )
            expected_version = response.json().get("version") == __version__
            expected_project = response.json().get("project") == project_id
            if response.status_code == 200 and expected_version and expected_project:
                break
        except (requests.RequestException, ValueError):
            pass
        sleep(5)
    else:
        spinner.fail("✗")
        raise Exception(f"burla-main-service never became reachable at {dashboard_url}")

    spinner.text = "Deploying burla-main-service instance ... Done."
    spinner.ok("✓")
    return dashboard_url
