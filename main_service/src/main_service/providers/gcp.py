from google.api_core.exceptions import NotFound, ServiceUnavailable, Conflict
from google.api_core.retry import Retry, if_transient_error
from google.cloud.compute_v1 import (
    AttachedDisk,
    NetworkInterface,
    AttachedDiskInitializeParams,
    Metadata,
    Items,
    AccessConfig,
    ServiceAccount,
    Tags,
    InstancesClient,
    Instance,
    Scheduling,
    MachineTypesClient,
    AggregatedListMachineTypesRequest,
)

from main_service import PROJECT_ID
from main_service.providers import NoCapacity, InstanceDeletedMidBoot

# Retries GCE API calls (unary RPCs and polling done by ExtendedOperation.result()) on
# transient network errors, e.g. requests.exceptions.ConnectionError from
# "Remote end closed connection". Operation-level errors like ZONE_RESOURCE_POOL_EXHAUSTED
# are set on the future via set_exception and not raised from _refresh, so this retry
# does not hide them.
GCE_TRANSIENT_RETRY = Retry(predicate=if_transient_error)


class GCPProvider:
    def __init__(self):
        self.instance_client = InstancesClient()
        self.machine_types_client = MachineTypesClient()

    def disk_image(self, machine_type: str) -> str:
        if machine_type.startswith("n4"):
            return "projects/burla-prod/global/images/burla-node-nogpu-2"
        if machine_type.startswith("a2") or machine_type.startswith("a3"):
            return "projects/burla-prod/global/images/burla-node-gpu-2"
        raise ValueError(f"Invalid machine type: {machine_type}")

    def zones_supporting_machine_type(self, region_name: str, machine_type_name: str):
        name_filter = f"name={machine_type_name}"
        request = AggregatedListMachineTypesRequest(
            project=PROJECT_ID, filter=name_filter
        )
        zone_generator = self.machine_types_client.aggregated_list(
            request=request, retry=GCE_TRANSIENT_RETRY
        )
        for zone, matches in zone_generator:
            if matches.machine_types and zone.startswith(f"zones/{region_name}"):
                yield zone.split("/")[1]

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
    ) -> tuple[str, str, str]:
        """Create the VM, iterating zones on capacity exhaustion.
        Returns (external_ip, internal_ip, zone)."""
        disk_params = AttachedDiskInitializeParams(
            source_image=self.disk_image(machine_type), disk_size_gb=disk_size
        )
        disk = AttachedDisk(auto_delete=True, boot=True, initialize_params=disk_params)

        network_name = "global/networks/default"
        access_config = AccessConfig(name="External NAT", type="ONE_TO_ONE_NAT")
        network_interface = NetworkInterface(
            name=network_name, access_configs=[access_config]
        )

        can_live_migrate = (not spot) and num_gpus == 0
        if spot:
            scheduling = Scheduling(
                provisioning_model="SPOT",
                instance_termination_action="DELETE",
                on_host_maintenance="TERMINATE",
                automatic_restart=False,
            )
        elif can_live_migrate:
            scheduling = Scheduling(
                provisioning_model="STANDARD",
                on_host_maintenance="MIGRATE",
                automatic_restart=False,
            )
        else:
            scheduling = Scheduling(
                provisioning_model="STANDARD",
                on_host_maintenance="TERMINATE",
                automatic_restart=False,
            )

        access_anything_scope = "https://www.googleapis.com/auth/cloud-platform"
        service_account = ServiceAccount(
            email=f"{_project_number()}-compute@developer.gserviceaccount.com",
            scopes=[access_anything_scope],
        )

        metadata_items = [
            Items(key="startup-script", value=startup_script),
            Items(key="shutdown-script", value=shutdown_script),
        ]

        zones = list(self.zones_supporting_machine_type(region, machine_type))
        if not zones:
            msg = f"None of the zones in region {region} "
            raise Exception(msg + f"support the machine type {machine_type}.")

        exhausted_zones = []
        instance_created = False
        for zone in zones:
            on_log(f"Attempting to provision {machine_type} in zone: {zone}")
            try:
                instance = Instance(
                    name=instance_name,
                    machine_type=f"zones/{zone}/machineTypes/{machine_type}",
                    disks=[disk],
                    network_interfaces=[network_interface],
                    service_accounts=[service_account],
                    metadata=Metadata(items=metadata_items),
                    tags=Tags(items=["burla-cluster-node"]),
                    scheduling=scheduling,
                )
                kw = dict(project=PROJECT_ID, zone=zone, instance_resource=instance)
                operation = self.instance_client.insert(**kw, retry=GCE_TRANSIENT_RETRY)
                operation.result(retry=GCE_TRANSIENT_RETRY)
                instance_created = True
                break
            except (
                ServiceUnavailable
            ):  # not enough instances in this zone, try next zone.
                exhausted_zones.append(zone)
                on_log(f"No available capacity for {machine_type} in zone: {zone}")
            except Conflict:
                raise InstanceDeletedMidBoot(
                    f"Node {instance_name} deleted while starting."
                )

        if not instance_created:
            msg = f"ZONE_RESOURCE_POOL_EXHAUSTED: {exhausted_zones} currently have no "
            msg += f"available capacity for VM {machine_type}\n"
            raise NoCapacity(msg)

        kw = dict(project=PROJECT_ID, zone=zone, instance=instance_name)
        instance_info = self.instance_client.get(**kw, retry=GCE_TRANSIENT_RETRY)
        external_ip = instance_info.network_interfaces[0].access_configs[0].nat_i_p
        internal_ip = instance_info.network_interfaces[0].network_i_p
        on_log(
            f"Successfully provisioned {machine_type} in zone: {zone}\nWaiting for startup script ..."
        )
        return external_ip, internal_ip, zone

    def delete_instance(self, instance_name: str, zone: str | None = None):
        if zone is None:
            zone = self._find_zone(instance_name)
            if zone is None:
                return
        try:
            kwargs = dict(project=PROJECT_ID, zone=zone, instance=instance_name)
            self.instance_client.delete(**kwargs, retry=GCE_TRANSIENT_RETRY)
        except (NotFound, ValueError):
            pass  # these errors mean it was already deleted.

    def _find_zone(self, instance_name: str) -> str | None:
        response = self.instance_client.aggregated_list(project=PROJECT_ID)
        for _, vms_in_zone in response:
            for vm in getattr(vms_in_zone, "instances", []):
                if vm.name == instance_name:
                    return vm.zone.split("/")[-1]
        return None

    def mount_shared_workspace_script(self, bucket_name: str) -> str:
        return f"""
        mkdir -p /workspace/shared /var/cache/gcsfuse
        gcsfuse \\
            --client-protocol=http2 \\
            --metadata-cache-ttl-secs=1 \\
            --cache-dir=/var/cache/gcsfuse \\
            {bucket_name} /workspace/shared
        """


_cached_project_number = None


def _project_number() -> str:
    global _cached_project_number
    if _cached_project_number is None:
        from google.cloud import resourcemanager_v3

        client = resourcemanager_v3.ProjectsClient()
        project = client.get_project(name=f"projects/{PROJECT_ID}")
        _cached_project_number = project.name.split("/")[-1]
    return _cached_project_number
