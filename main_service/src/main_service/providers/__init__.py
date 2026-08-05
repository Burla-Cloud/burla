"""
Compute provider abstraction. One implementation per cloud; everything above
this layer (node lifecycle, growth, dashboards) is cloud-agnostic.

A provider knows how to:
- create a VM that runs the burla node_service (create_instance)
- delete a VM (delete_instance)

`create_instance` blocks until the VM exists and returns its external IP.
Providers raise `NoCapacity` when every zone/AZ in the region is out of
capacity for the machine type.
"""

from main_service import CLOUD_PROVIDER, IN_LOCAL_DEV_MODE


class NoCapacity(Exception):
    pass


class InstanceDeletedMidBoot(Exception):
    pass


def get_provider():
    if IN_LOCAL_DEV_MODE:
        from main_service.providers.local_docker import LocalDockerProvider

        return LocalDockerProvider()
    if CLOUD_PROVIDER == "aws":
        from main_service.providers.aws import AWSProvider

        return AWSProvider()
    if CLOUD_PROVIDER == "azure":
        from main_service.providers.azure import AzureProvider

        return AzureProvider()
    from main_service.providers.gcp import GCPProvider

    return GCPProvider()
