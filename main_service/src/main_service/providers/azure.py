import base64
import json
import os
import threading
from time import time

from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from burla._azure_images import public_node_image_id

from main_service import CLUSTER_NAME, IN_CLIENT_HOSTED_MODE, cluster_state
from main_service.providers import NoCapacity

# Allocation errors worth reporting as NoCapacity; anything else is a real
# error. Quota errors (OperationNotAllowed) are deliberately excluded: waiting
# never fixes a quota of zero.
_CAPACITY_ERROR_CODES = (
    "AllocationFailed",
    "ZonalAllocationFailed",
    "OverconstrainedAllocationRequest",
    "OverconstrainedZonalAllocationRequest",
    "SkuNotAvailable",
)

# Fixed container inside the shared-workspace storage account; the config's
# single bucket-name string holds only the storage account name on Azure.
SHARED_WORKSPACE_CONTAINER = "shared-workspace"

_instance_resource_groups: dict[str, str] = {}
_credential = None
_shutdown_token = None
_shutdown_token_lock = threading.Lock()
DELETE_LEASE_REFRESH_SEC = 4 * 60
_DELETE_LEASE_EXECUTION_SEC = 2 * 60


def _default_credential():
    global _credential
    if _credential is None:
        _credential = DefaultAzureCredential()
    return _credential


def _resource_group_from_id(resource_id: str) -> str:
    parts = resource_id.strip("/").split("/")
    return parts[parts.index("resourceGroups") + 1]


def _azure_delete_lease(instance_name: str, resource_group: str) -> dict:
    """A short-lived caller token gives an orphan a bounded deletion window.

    Azure ARM tokens cannot be downscoped. The token stays root-only on the
    user's own VM and expires in about an hour.
    """
    global _shutdown_token
    with _shutdown_token_lock:
        if (
            _shutdown_token is None
            or _shutdown_token.expires_on - time() <= DELETE_LEASE_REFRESH_SEC
        ):
            _shutdown_token = _default_credential().get_token(
                "https://management.azure.com/.default"
            )
        token = _shutdown_token

    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
    vm_id = (
        f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
        f"/providers/Microsoft.Compute/virtualMachines/{instance_name}"
    )
    return {
        "access_token": token.token,
        "delete_at": token.expires_on - _DELETE_LEASE_EXECUTION_SEC,
        "expires_at": token.expires_on,
        "vm_id": vm_id,
    }


def azure_delete_lease(instance_name: str, saved_resource_group: str | None) -> dict:
    resource_group = _instance_resource_groups.get(instance_name)
    if resource_group is None:
        provider = AzureProvider()
        resource_group = provider._instance_resource_group(
            instance_name, saved_resource_group
        )
    if resource_group is None:
        raise ResourceNotFoundError(f"Azure VM {instance_name} was not found.")
    return _azure_delete_lease(instance_name, resource_group)


def _ssh_public_key() -> str:
    """Azure requires an admin SSH key on Linux VMs. Nobody ever logs in, so
    generate a fresh keypair per head process and discard the private half."""
    global _cached_ssh_public_key
    if _cached_ssh_public_key is None:
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        _cached_ssh_public_key = (
            key.public_key()
            .public_bytes(
                serialization.Encoding.OpenSSH, serialization.PublicFormat.OpenSSH
            )
            .decode()
        )
    return _cached_ssh_public_key


_cached_ssh_public_key = None


class AzureProvider:
    """Azure VMs launched from Burla's public Community Gallery image."""

    def __init__(self):
        self.subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
        self.resource_group = os.environ.get("AZURE_RESOURCE_GROUP")
        self.subnet_id = os.environ.get("AZURE_SUBNET_ID")
        credential = _default_credential()
        self.compute = ComputeManagementClient(credential, self.subscription_id)
        self.network = NetworkManagementClient(credential, self.subscription_id)

    def _resource_id(
        self, resource_group: str, provider: str, type_name: str, name: str
    ) -> str:
        return (
            f"/subscriptions/{self.subscription_id}/resourceGroups/{resource_group}"
            f"/providers/{provider}/{type_name}/{name}"
        )

    def _image_reference(self, region: str) -> dict:
        image_id = os.environ.get("BURLA_NODE_IMAGE") or public_node_image_id(region)
        if image_id.lower().startswith("/communitygalleries/"):
            return {"communityGalleryImageId": image_id}
        return {"id": image_id}

    def _create_node_subnet(self, region: str) -> tuple[str, str]:
        resource_group = self.resource_group
        vnet_name = f"burla-{region}"
        try:
            self.network.virtual_networks.get(resource_group, vnet_name)
        except ResourceNotFoundError:
            self.network.virtual_networks.begin_create_or_update(
                resource_group,
                vnet_name,
                {
                    "location": region,
                    "properties": {
                        "addressSpace": {"addressPrefixes": ["10.0.0.0/16"]}
                    },
                },
            ).result()

        nsg = self.network.network_security_groups.begin_create_or_update(
            resource_group,
            f"burla-cluster-node-{region}",
            {"location": region},
        ).result()
        subnet = self.network.subnets.begin_create_or_update(
            resource_group,
            vnet_name,
            "nodes",
            {
                "properties": {
                    "addressPrefix": "10.0.0.0/20",
                    "networkSecurityGroup": {"id": nsg.id},
                },
            },
        ).result()
        return resource_group, subnet.id

    def _placement(self, region: str) -> tuple[str, str]:
        if self.subnet_id:
            network_resource_group = _resource_group_from_id(self.subnet_id)
            parts = self.subnet_id.strip("/").split("/")
            vnet_name = parts[parts.index("virtualNetworks") + 1]
            vnet = self.network.virtual_networks.get(network_resource_group, vnet_name)
            if vnet.location.lower() != region.lower():
                raise Exception(
                    f"Azure subnet {self.subnet_id} is in {vnet.location}, "
                    f"but AZURE_REGION is {region}."
                )
            resource_group = self.resource_group or network_resource_group
            return resource_group, self.subnet_id

        candidates = []
        for vnet in self.network.virtual_networks.list_all():
            if vnet.location.lower() != region.lower():
                continue
            for subnet in vnet.subnets or []:
                has_outbound_access = (
                    subnet.default_outbound_access is not False
                    or subnet.nat_gateway is not None
                )
                if not subnet.delegations and has_outbound_access:
                    candidates.append(subnet.id)

        if not candidates:
            if self.resource_group:
                return self._create_node_subnet(region)
            raise Exception(
                f"No accessible Azure subnet was found in {region}. "
                "Set AZURE_SUBNET_ID to an existing outbound-capable subnet."
            )

        def preference(subnet_id: str) -> tuple[int, str]:
            value = subnet_id.lower()
            if f"/virtualnetworks/burla-{region.lower()}/subnets/nodes" in value:
                return 0, value
            if value.endswith("/subnets/default"):
                return 1, value
            if value.endswith("/subnets/nodes"):
                return 2, value
            return 3, value

        subnet_id = min(candidates, key=preference)
        resource_group = self.resource_group or _resource_group_from_id(subnet_id)
        return resource_group, subnet_id

    def create_instance(
        self,
        instance_name: str,
        machine_type: str,
        region: str,
        disk_size: int,
        spot: bool,
        num_gpus: int,
        port: int,
        startup_script: str,
        shutdown_script: str,
        on_log,
        needs_cloud_credentials: bool = False,
    ) -> tuple[str | None, str, str]:
        """Returns (public_ip, private_ip, resource_group)."""
        if num_gpus > 0:
            raise Exception(
                "GPU nodes are not supported on Azure yet (the burla node image "
                "does not include GPU drivers)."
            )
        if IN_CLIENT_HOSTED_MODE and needs_cloud_credentials:
            raise Exception("Azure shared workspaces require a deployed Burla cluster.")

        resource_group, subnet_id = self._placement(region)
        _instance_resource_groups[instance_name] = resource_group
        if IN_CLIENT_HOSTED_MODE:
            lease = _azure_delete_lease(instance_name, resource_group)
            lease_b64 = base64.b64encode(json.dumps(lease).encode()).decode()
            startup_script = startup_script.replace(
                "set -Eeuo pipefail\n",
                "set -Eeuo pipefail\n"
                "mkdir -p /etc/burla\n"
                "install -m 600 /dev/null /etc/burla/azure-delete-lease.json\n"
                f'echo "{lease_b64}" | base64 -d '
                "> /etc/burla/azure-delete-lease.json\n",
                1,
            )
        on_log(f"Attempting to provision {machine_type} in region: {region}")

        # All request bodies are raw ARM JSON (camelCase + properties
        # envelope): the SDK passes dicts through to the wire unconverted.
        tags = {"burla-cluster-node": "true", "burla-cluster-id": CLUSTER_NAME}

        public_ip = None
        if not IN_CLIENT_HOSTED_MODE:
            public_ip = self.network.public_ip_addresses.begin_create_or_update(
                resource_group,
                f"{instance_name}-pip",
                {
                    "location": region,
                    "sku": {"name": "Standard"},
                    "properties": {"publicIPAllocationMethod": "Static"},
                    "tags": tags,
                },
            ).result()

        ip_configuration = {"subnet": {"id": subnet_id}}
        if public_ip is not None:
            ip_configuration["publicIPAddress"] = {
                "id": public_ip.id,
                "properties": {"deleteOption": "Delete"},
            }
        nic_poller = self.network.network_interfaces.begin_create_or_update(
            resource_group,
            f"{instance_name}-nic",
            {
                "location": region,
                "tags": tags,
                "properties": {
                    "ipConfigurations": [
                        {
                            "name": "primary",
                            "properties": ip_configuration,
                        }
                    ]
                },
            },
        )
        nic = nic_poller.result()

        vm_properties = {
            "hardwareProfile": {"vmSize": machine_type},
            "storageProfile": {
                "imageReference": self._image_reference(region),
                "osDisk": {
                    "createOption": "FromImage",
                    # The Ubuntu 22.04 base image has a 30 GB OS disk and
                    # Azure refuses to shrink below the image's size.
                    "diskSizeGB": max(disk_size, 30),
                    "managedDisk": {"storageAccountType": "StandardSSD_LRS"},
                    "deleteOption": "Delete",
                },
            },
            "osProfile": {
                "computerName": instance_name,
                "adminUsername": "burla",
                "linuxConfiguration": {
                    "disablePasswordAuthentication": True,
                    "ssh": {
                        "publicKeys": [
                            {
                                "path": "/home/burla/.ssh/authorized_keys",
                                "keyData": _ssh_public_key(),
                            }
                        ]
                    },
                },
                "customData": base64.b64encode(startup_script.encode()).decode(),
            },
            "networkProfile": {
                "networkInterfaces": [
                    {"id": nic.id, "properties": {"deleteOption": "Delete"}}
                ]
            },
        }
        if spot:
            vm_properties["priority"] = "Spot"
            vm_properties["evictionPolicy"] = "Delete"
            vm_properties["billingProfile"] = {"maxPrice": -1}

        vm_parameters = {
            "location": region,
            # Several dev clusters boot nodes into one subscription, so every
            # destructive lookup filters on the cluster tag.
            "tags": tags,
            "properties": vm_properties,
        }
        if not IN_CLIENT_HOSTED_MODE:
            vm_parameters["identity"] = {
                "type": "UserAssigned",
                "userAssignedIdentities": {
                    self._resource_id(
                        resource_group,
                        "Microsoft.ManagedIdentity",
                        "userAssignedIdentities",
                        "burla-node",
                    ): {}
                },
            }

        try:
            vm_poller = self.compute.virtual_machines.begin_create_or_update(
                resource_group, instance_name, vm_parameters
            )
            vm_poller.result()
        except HttpResponseError as error:
            code = getattr(error.error, "code", "")
            if code in _CAPACITY_ERROR_CODES:
                on_log(f"No available capacity for {machine_type} in region: {region}")
                msg = f"INSUFFICIENT_INSTANCE_CAPACITY: region {region} currently has "
                msg += f"no available capacity for VM size {machine_type} ({code})\n"
                raise NoCapacity(msg)
            raise

        private_ip = nic.ip_configurations[0].private_ip_address
        on_log(
            f"Successfully provisioned {machine_type} in region: {region}\n"
            "Waiting for startup script ..."
        )
        public_ip_address = public_ip.ip_address if public_ip is not None else None
        return public_ip_address, private_ip, resource_group

    def _instance_resource_group(
        self, instance_name: str, saved_resource_group: str | None
    ) -> str | None:
        if instance_name in _instance_resource_groups:
            return _instance_resource_groups[instance_name]

        # Heads upgraded from 1.6.2 persisted an Azure region in `zone`, while
        # new heads persist the resource group. Validate the saved value before
        # using it so legacy nodes still get deleted from their real group.
        candidates = dict.fromkeys(
            candidate
            for candidate in (saved_resource_group, self.resource_group, "burla")
            if candidate
        )
        for resource_group in candidates:
            resources = (
                (
                    self.compute.virtual_machines.get,
                    instance_name,
                ),
                (
                    self.network.network_interfaces.get,
                    f"{instance_name}-nic",
                ),
                (
                    self.network.public_ip_addresses.get,
                    f"{instance_name}-pip",
                ),
            )
            for get_resource, resource_name in resources:
                try:
                    get_resource(resource_group, resource_name)
                    _instance_resource_groups[instance_name] = resource_group
                    return resource_group
                except ResourceNotFoundError:
                    pass

        vms = (
            self.compute.virtual_machines.list(self.resource_group)
            if self.resource_group
            else self.compute.virtual_machines.list_all()
        )
        for vm in vms:
            if vm.name == instance_name:
                resource_group = _resource_group_from_id(vm.id)
                _instance_resource_groups[instance_name] = resource_group
                return resource_group
        resources = (
            (
                self.network.network_interfaces.list_all(),
                f"{instance_name}-nic",
            ),
            (
                self.network.public_ip_addresses.list_all(),
                f"{instance_name}-pip",
            ),
        )
        for items, resource_name in resources:
            for resource in items:
                if resource.name == resource_name:
                    resource_group = _resource_group_from_id(resource.id)
                    _instance_resource_groups[instance_name] = resource_group
                    return resource_group
        return None

    def delete_instance(self, instance_name: str, zone: str | None = None):
        resource_group = self._instance_resource_group(instance_name, zone)
        if resource_group is None:
            return
        try:
            poller = self.compute.virtual_machines.begin_delete(
                resource_group, instance_name, force_deletion=True
            )
            poller.result()
        except ResourceNotFoundError:
            pass
        # The VM delete cascades NIC -> public IP -> disk via their
        # delete_options, but a create that failed before the VM existed
        # leaves the NIC/IP behind - sweep those directly.
        try:
            self.network.network_interfaces.begin_delete(
                resource_group, f"{instance_name}-nic"
            ).result()
        except ResourceNotFoundError:
            pass
        try:
            self.network.public_ip_addresses.begin_delete(
                resource_group, f"{instance_name}-pip"
            ).result()
        except ResourceNotFoundError:
            pass
        _instance_resource_groups.pop(instance_name, None)

    def existing_instances(self, instance_names: list[str], region: str) -> set[str]:
        """Which of these instances still exist, tag-scoped to this cluster."""
        wanted = set(instance_names)
        vms = (
            self.compute.virtual_machines.list(self.resource_group)
            if self.resource_group
            else self.compute.virtual_machines.list_all()
        )
        found = set()
        for vm in vms:
            in_cluster = (vm.tags or {}).get("burla-cluster-id") == CLUSTER_NAME
            if vm.name in wanted and in_cluster:
                found.add(vm.name)
        return found

    def delete_stopped_instances(self):
        """Sweep Burla nodes whose in-VM deletion could not finish."""
        vms = (
            self.compute.virtual_machines.list(self.resource_group)
            if self.resource_group
            else self.compute.virtual_machines.list_all()
        )
        for vm in vms:
            if not vm.name.startswith("burla-node-"):
                continue
            tags = vm.tags or {}
            if tags.get("burla-cluster-node") != "true":
                continue
            if tags.get("burla-cluster-id") != CLUSTER_NAME:
                continue
            resource_group = _resource_group_from_id(vm.id)
            instance_view = self.compute.virtual_machines.instance_view(
                resource_group, vm.name
            )
            power_states = [
                status.code
                for status in instance_view.statuses or []
                if status.code and status.code.startswith("PowerState/")
            ]
            if "PowerState/stopped" in power_states:
                self.delete_instance(vm.name, resource_group)

        for nic in self.network.network_interfaces.list_all():
            tags = nic.tags or {}
            if (
                not nic.name.startswith("burla-node-")
                or not nic.name.endswith("-nic")
                or tags.get("burla-cluster-node") != "true"
                or tags.get("burla-cluster-id") != CLUSTER_NAME
                or nic.virtual_machine is not None
            ):
                continue
            instance_name = nic.name.removesuffix("-nic")
            node = cluster_state.get_node(instance_name)
            if node and node.get("status") == "BOOTING":
                continue
            self.delete_instance(instance_name, _resource_group_from_id(nic.id))

        for public_ip in self.network.public_ip_addresses.list_all():
            tags = public_ip.tags or {}
            if (
                not public_ip.name.startswith("burla-node-")
                or not public_ip.name.endswith("-pip")
                or tags.get("burla-cluster-node") != "true"
                or tags.get("burla-cluster-id") != CLUSTER_NAME
                or public_ip.ip_configuration is not None
            ):
                continue
            instance_name = public_ip.name.removesuffix("-pip")
            node = cluster_state.get_node(instance_name)
            if node and node.get("status") == "BOOTING":
                continue
            self.delete_instance(instance_name, _resource_group_from_id(public_ip.id))

    def mount_shared_workspace_script(self, bucket_name: str) -> str:
        # bucket_name is the storage account; blobfuse2 authenticates with the
        # node's managed identity (mode: msi).
        return f"""
        mkdir -p /workspace/shared /var/cache/blobfuse2
        cat > /etc/burla/blobfuse2.yaml <<'BLOBFUSE_EOF'
allow-other: true
file_cache:
  path: /var/cache/blobfuse2
azstorage:
  type: block
  account-name: {bucket_name}
  container: {SHARED_WORKSPACE_CONTAINER}
  mode: msi
BLOBFUSE_EOF
        blobfuse2 mount /workspace/shared --config-file=/etc/burla/blobfuse2.yaml
        """
