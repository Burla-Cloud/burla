from google.cloud import compute_v1

project_id = "YOUR_PROJECT_ID"
zone = "us-central1-a"
instance_name = "qdrant-server"

startup_script = """#!/bin/bash
apt-get update
apt-get install -y docker.io
mkdir -p /qdrant_data
docker run -d -p 6333:6333 -v /qdrant_data:/qdrant/storage qdrant/qdrant
"""


def create_instance(project_id, zone, instance_name, startup_script):
    instance_client = compute_v1.InstancesClient()
    disk = compute_v1.AttachedDisk(
        boot=True,
        auto_delete=True,
        initialize_params=compute_v1.AttachedDiskInitializeParams(
            source_image="projects/ubuntu-os-cloud/global/images/family/ubuntu-2204-lts",
            disk_size_gb=50,
            disk_type=f"zones/{zone}/diskTypes/pd-ssd",
        ),
    )
    network_interface = compute_v1.NetworkInterface(
        name="global/networks/default",
        access_configs=[compute_v1.AccessConfig(type_="ONE_TO_ONE_NAT", name="External NAT")],
    )
    metadata = compute_v1.Metadata(
        items=[compute_v1.Items(key="startup-script", value=startup_script)]
    )
    instance = compute_v1.Instance(
        name=instance_name,
        machine_type=f"zones/{zone}/machineTypes/e2-standard-2",
        disks=[disk],
        network_interfaces=[network_interface],
        metadata=metadata,
    )
    op = instance_client.insert(
        project=project_id,
        zone=zone,
        instance_resource=instance,
    )
    op.result()  # Wait for completion


create_instance(project_id, zone, instance_name, startup_script)
