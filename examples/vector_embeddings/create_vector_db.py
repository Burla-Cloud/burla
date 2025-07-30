import google.auth
from google.cloud import compute_v1
from google.cloud.compute_v1 import (
    Instance,
    AttachedDisk,
    AttachedDiskInitializeParams,
    AccessConfig,
    NetworkInterface,
    Metadata,
    Items,
)

_, project_id = google.auth.default()
zone = "us-central1-a"
instance_name = "qdrant-server"

startup_script = """#!/bin/bash
apt-get update
apt-get install -y docker.io
mkdir -p /qdrant_data
docker run -d -p 6333:6333 -v /qdrant_data:/qdrant/storage qdrant/qdrant
"""

disk_image = "projects/ubuntu-os-cloud/global/images/family/ubuntu-2204-lts"
disk_type = f"zones/{zone}/diskTypes/hyperdisk-balanced"
disk_params = AttachedDiskInitializeParams(
    source_image=disk_image, disk_size_gb=100, disk_type=disk_type
)

access_config = AccessConfig(type_="ONE_TO_ONE_NAT", name="External NAT")
network_interface = NetworkInterface(name="global/networks/default", access_configs=[access_config])

instance = Instance(
    name=instance_name,
    machine_type=f"zones/{zone}/machineTypes/n4-standard-80",
    disks=[AttachedDisk(boot=True, auto_delete=True, initialize_params=disk_params)],
    network_interfaces=[network_interface],
    metadata=Metadata(items=[Items(key="startup-script", value=startup_script)]),
)

instance_client = compute_v1.InstancesClient()
op = instance_client.insert(project=project_id, zone=zone, instance_resource=instance)
op.result()

new_instance = instance_client.get(project=project_id, zone=zone, instance=instance_name)
internal_ip = new_instance.network_interfaces[0].network_i_p
print(f"Qdrant server is ready. Internal IP: http://{internal_ip}:6333")
