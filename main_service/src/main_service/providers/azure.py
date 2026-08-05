import base64
import os
from time import sleep

from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient

from main_service import CLUSTER_NAME
from main_service.providers import NoCapacity

RESOURCE_GROUP = os.environ.get("AZURE_RESOURCE_GROUP", "burla")

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
    """Azure twin of the AWS/GCP providers. Nodes are VMs built from the burla
    node managed image (see client/src/burla/_deploy_azure.py), tagged
    `burla-cluster-node`, in the `burla` resource group.

    Every node carries the `burla-node` user-assigned identity: unlike AWS
    (poweroff terminates) and GCP (guest attribute + head reaper), a guest
    poweroff on Azure leaves the VM in a "stopped" state that still bills for
    compute, so nodes must call the ARM API to delete themselves when the head
    is gone. The identity's role only allows deleting VMs/NICs/IPs/disks
    inside the burla resource group.
    """

    def __init__(self):
        self.subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
        self.region = os.environ.get("AZURE_REGION", "eastus")
        credential = DefaultAzureCredential()
        self.compute = ComputeManagementClient(credential, self.subscription_id)
        self.network = NetworkManagementClient(credential, self.subscription_id)

    def _resource_id(self, provider: str, type_name: str, name: str) -> str:
        return (
            f"/subscriptions/{self.subscription_id}/resourceGroups/{RESOURCE_GROUP}"
            f"/providers/{provider}/{type_name}/{name}"
        )

    def _image_id(self, region: str) -> str:
        """Newest burla node managed image in this region. Like the AWS AMI it
        is just a warm base image (nodes git-fetch the code they run at boot),
        so it is not keyed by burla version."""
        override = os.environ.get("BURLA_NODE_IMAGE")
        if override:
            return override
        images = [
            image
            for image in self.compute.images.list_by_resource_group(RESOURCE_GROUP)
            if (image.tags or {}).get("burla-node-image") == "true"
            and image.location == region
            and image.provisioning_state == "Succeeded"
        ]
        if not images:
            raise Exception(
                f"No burla node image found in region {region}. "
                "Run `burla deploy --cloud azure` to build one, or set BURLA_NODE_IMAGE."
            )
        # Image names end in a unix timestamp, so newest sorts last.
        return sorted(images, key=lambda image: image.name)[-1].id

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
    ) -> tuple[str, str, str]:
        """Create the VM (regional allocation; Azure spreads across zones
        itself). Returns (public_ip, private_ip, region)."""
        if num_gpus > 0:
            raise Exception(
                "GPU nodes are not supported on Azure yet (the burla node image "
                "does not include GPU drivers)."
            )

        try:
            subnet = self.network.subnets.get(RESOURCE_GROUP, f"burla-{region}", "nodes")
        except ResourceNotFoundError:
            raise Exception(
                f"Burla's network in region {region} was not found. "
                "Run `burla deploy --cloud azure` (or boot once via the client) to create it."
            )

        image_id = self._image_id(region)
        on_log(f"Attempting to provision {machine_type} in region: {region}")

        public_ip_poller = self.network.public_ip_addresses.begin_create_or_update(
            RESOURCE_GROUP,
            f"{instance_name}-pip",
            {
                "location": region,
                "sku": {"name": "Standard"},
                "public_ip_allocation_method": "Static",
                "tags": {"burla-cluster-node": "true", "burla-cluster-id": CLUSTER_NAME},
            },
        )
        public_ip = public_ip_poller.result()

        nic_poller = self.network.network_interfaces.begin_create_or_update(
            RESOURCE_GROUP,
            f"{instance_name}-nic",
            {
                "location": region,
                "ip_configurations": [
                    {
                        "name": "primary",
                        "subnet": {"id": subnet.id},
                        "public_ip_address": {
                            "id": public_ip.id,
                            # Deleting the NIC takes the public IP with it.
                            "properties": {"delete_option": "Delete"},
                        },
                    }
                ],
                "tags": {"burla-cluster-node": "true", "burla-cluster-id": CLUSTER_NAME},
            },
        )
        nic = nic_poller.result()

        vm_parameters = {
            "location": region,
            "tags": {
                "burla-cluster-node": "true",
                # Several dev clusters boot nodes into one subscription, so
                # every destructive lookup filters on this.
                "burla-cluster-id": CLUSTER_NAME,
            },
            "hardware_profile": {"vm_size": machine_type},
            "storage_profile": {
                "image_reference": {"id": image_id},
                "os_disk": {
                    "create_option": "FromImage",
                    "disk_size_gb": disk_size,
                    "managed_disk": {"storage_account_type": "StandardSSD_LRS"},
                    "delete_option": "Delete",
                },
            },
            "os_profile": {
                "computer_name": instance_name,
                "admin_username": "burla",
                "linux_configuration": {
                    "disable_password_authentication": True,
                    "ssh": {
                        "public_keys": [
                            {
                                "path": "/home/burla/.ssh/authorized_keys",
                                "key_data": _ssh_public_key(),
                            }
                        ]
                    },
                },
                "custom_data": base64.b64encode(startup_script.encode()).decode(),
            },
            "network_profile": {
                "network_interfaces": [
                    {"id": nic.id, "properties": {"delete_option": "Delete"}}
                ]
            },
            # Self-deletion identity (see class docstring). The same identity
            # carries the shared-workspace storage role, so unlike AWS/GCP it
            # is attached whether or not the filesystem is enabled.
            "identity": {
                "type": "UserAssigned",
                "user_assigned_identities": {
                    self._resource_id(
                        "Microsoft.ManagedIdentity", "userAssignedIdentities", "burla-node"
                    ): {}
                },
            },
        }
        if spot:
            vm_parameters["priority"] = "Spot"
            vm_parameters["eviction_policy"] = "Delete"
            vm_parameters["billing_profile"] = {"max_price": -1}

        try:
            vm_poller = self.compute.virtual_machines.begin_create_or_update(
                RESOURCE_GROUP, instance_name, vm_parameters
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
        return public_ip.ip_address, private_ip, region

    def delete_instance(self, instance_name: str, zone: str | None = None):
        try:
            poller = self.compute.virtual_machines.begin_delete(
                RESOURCE_GROUP, instance_name, force_deletion=True
            )
            poller.result()
        except ResourceNotFoundError:
            pass
        # The VM delete cascades NIC -> public IP -> disk via their
        # delete_options, but a create that failed before the VM existed
        # leaves the NIC/IP behind - sweep those directly.
        try:
            self.network.network_interfaces.begin_delete(
                RESOURCE_GROUP, f"{instance_name}-nic"
            ).result()
        except ResourceNotFoundError:
            pass
        try:
            self.network.public_ip_addresses.begin_delete(
                RESOURCE_GROUP, f"{instance_name}-pip"
            ).result()
        except ResourceNotFoundError:
            pass

    def delete_stopped_instances(self):
        """Nodes delete themselves through the ARM API (their identity exists
        for exactly this), so a healthy self-delete leaves nothing behind.
        This sweeps the failure mode where that call never landed and the node
        fell back to poweroff: on Azure that leaves the VM "stopped" but NOT
        deallocated, which still bills for compute.

        Deallocated VMs are left alone: a guest can't deallocate, so that
        state only comes from a person stopping the VM via the portal/CLI, and
        deleting those would destroy a user's node behind their back.
        Deliberately not scoped to this cluster, like the GCP reaper: a
        stopped burla node wants to be gone no matter which head notices.
        """
        for vm in self.compute.virtual_machines.list(RESOURCE_GROUP):
            if not vm.name.startswith("burla-node-"):
                continue
            if (vm.tags or {}).get("burla-cluster-node") != "true":
                continue
            instance_view = self.compute.virtual_machines.instance_view(
                RESOURCE_GROUP, vm.name
            )
            power_states = [
                status.code
                for status in instance_view.statuses or []
                if status.code and status.code.startswith("PowerState/")
            ]
            if "PowerState/stopped" in power_states:
                self.delete_instance(vm.name)

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
