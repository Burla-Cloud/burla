from google.cloud import compute_v1
from typing import List


def zones_supporting_machine_type(project_id: str, region_name: str, machine_type_name: str):
    client = compute_v1.MachineTypesClient()
    request = compute_v1.AggregatedListMachineTypesRequest(
        project=project_id, filter=f"name={machine_type_name}"
    )

    supported_zone_names: List[str] = []
    for location, scoped_list in client.aggregated_list(request=request):
        if not location.startswith("zones/") or not scoped_list.machine_types:
            continue
        zone_name = location.split("/")[1]
        if zone_name.startswith(region_name + "-"):
            supported_zone_names.append(zone_name)
    return supported_zone_names


# example
if __name__ == "__main__":
    print(
        zones_supporting_machine_type(
            project_identifier="burla-test",
            region_name="us-central1",
            machine_type_name="a3-highgpu-1g",
        )
    )
