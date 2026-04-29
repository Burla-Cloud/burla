import docker
from datetime import datetime, timezone
from time import time
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from google.cloud.firestore_v1.base_query import FieldFilter
from google.cloud.compute_v1 import InstancesClient, MachineTypesClient
from concurrent.futures import ThreadPoolExecutor

from main_service import (
    DB,
    IN_LOCAL_DEV_MODE,
    LOCAL_DEV_CONFIG,
    NODES_CACHE,
    _nodes_cache_lock,
    get_logger,
    get_auth_headers,
    get_add_background_task_function,
)
from main_service.node import Container, Node
from main_service.helpers import Logger, log_telemetry
from main_service.quota import (
    INSTANCE_BUCKET,
    N4_CPU_BUCKET,
    active_machine_types_for_region,
    cap_boot_machine_types,
    n4_cpu_count,
    quota_status,
)

router = APIRouter()
MAX_GROW_CPUS = 2560
LOCAL_DEV_MAX_GROW_CPUS = 4

# Nodes booted by /v1/cluster/grow always get a short inactivity timeout
# regardless of the cluster-config value, so a burst-scaled job doesn't leave
# expensive hardware sitting idle long after the job finishes.
GROW_INACTIVITY_SHUTDOWN_TIME_SEC = 60

# Priced n4-standard sizes the dashboard exposes, largest first. n4-standard-48
# is intentionally omitted to match `main_service/frontend/src/types/constants.ts`
# (pricing isn't defined for it), so grow never provisions an unpriced size.
N4_STANDARD_SIZES_DESCENDING = (80, 64, 32, 16, 8, 4, 2)


def _machine_type_cpu_count(machine_type: str) -> int:
    if machine_type.startswith("n4-standard-") and machine_type.split("-")[-1].isdigit():
        return int(machine_type.split("-")[-1])
    return 1


def _pack_n4_standard_machines(num_cpus: int) -> list[str]:
    """
    Pick n4-standard machine types that cover `num_cpus`, greedily using as
    many of the largest size as possible and covering any remainder with the
    smallest size that fits. e.g. 95 -> [n4-standard-80, n4-standard-16].
    """
    machines = []
    largest = N4_STANDARD_SIZES_DESCENDING[0]
    remaining = num_cpus
    while remaining >= largest:
        machines.append(f"n4-standard-{largest}")
        remaining -= largest
    if remaining > 0:
        for size in reversed(N4_STANDARD_SIZES_DESCENDING):
            if size >= remaining:
                machines.append(f"n4-standard-{size}")
                break
    return machines


def _pack_n4_standard_machines_up_to(num_cpus: int, min_size: int = 2) -> list[str]:
    machines = []
    remaining = num_cpus
    for size in [size for size in N4_STANDARD_SIZES_DESCENDING if size >= min_size]:
        while remaining >= size:
            machines.append(f"n4-standard-{size}")
            remaining -= size
    return machines


def _configured_machine_types(config: dict) -> list[str]:
    machine_types = []
    for node_spec in config["Nodes"]:
        machine_types.extend([node_spec["machine_type"]] * node_spec["quantity"])
    return machine_types


def _requested_machine_types(
    config: dict, n_nodes_to_add: int = None, node_machine_types: list[str] = None
) -> list[str]:
    if node_machine_types is not None:
        return node_machine_types
    if n_nodes_to_add is not None:
        return [config["Nodes"][0]["machine_type"]] * n_nodes_to_add
    return _configured_machine_types(config)


def _active_machine_types(gcp_region: str) -> list[str]:
    with _nodes_cache_lock:
        nodes = list(NODES_CACHE.values())
    return active_machine_types_for_region(nodes, gcp_region)


def _quota_exceeded(region: str, caps: list[dict]):
    raise HTTPException(
        status_code=400,
        detail={
            "error_code": "quota_exceeded",
            "region": region,
            "caps": caps,
            "message": "No machines can boot without exceeding this project's GCP quota.",
        },
    )


def _n4_quota_warning(
    requested_cpus: int,
    allowed_machine_types: list[str],
    gcp_region: str,
    active_machine_types: list[str],
) -> dict:
    status = quota_status(N4_CPU_BUCKET, gcp_region, active_machine_types)
    allowed_cpus = sum(n4_cpu_count(machine_type) for machine_type in allowed_machine_types)
    return {
        "type": "quota_capped",
        "machine_type": "n4-standard",
        "region": gcp_region,
        "requested": requested_cpus,
        "allowed": allowed_cpus,
        "count_unit": "vCPUs",
        "limit": status["limit"],
        "used": status["used"],
        "available": status["available"],
        "quota": status["quota"],
        "units": status["units"],
    }


def _prepare_n4_cpu_grow_boot_plan(
    requested_cpus: int,
    region: str,
    active_machine_types: list[str],
    min_machine_size: int,
    raise_on_zero: bool,
) -> tuple[list[str], list[dict]]:
    requested_machine_types = _pack_n4_standard_machines(requested_cpus)
    requested_quota_cpus = sum(n4_cpu_count(machine_type) for machine_type in requested_machine_types)
    cpu_status = quota_status(N4_CPU_BUCKET, region, active_machine_types)
    instance_status = quota_status(INSTANCE_BUCKET, region, active_machine_types)
    if (
        requested_quota_cpus <= cpu_status["available"]
        and len(requested_machine_types) <= instance_status["available"]
    ):
        return requested_machine_types, []

    quota_limited_cpus = min(requested_cpus, cpu_status["available"])
    machine_types = _pack_n4_standard_machines_up_to(
        quota_limited_cpus,
        min_size=min_machine_size,
    )
    machine_types = machine_types[: instance_status["available"]]
    warnings = [
        _n4_quota_warning(
            requested_cpus=requested_cpus,
            allowed_machine_types=machine_types,
            gcp_region=region,
            active_machine_types=active_machine_types,
        )
    ]
    if raise_on_zero and not machine_types:
        _quota_exceeded(region, warnings)
    return machine_types, warnings


def _prepare_node_boot_plan(
    config: dict,
    requested_machine_types: list[str],
    active_machine_types: list[str] = None,
    raise_on_zero: bool = True,
    n4_requested_cpus: int = None,
    n4_min_machine_size: int = 2,
) -> tuple[list[str], list[dict]]:
    if IN_LOCAL_DEV_MODE:
        return requested_machine_types, []

    gcp_region = config["Nodes"][0]["gcp_region"]
    active_machine_types = (
        active_machine_types if active_machine_types is not None else _active_machine_types(gcp_region)
    )
    if n4_requested_cpus is not None:
        return _prepare_n4_cpu_grow_boot_plan(
            requested_cpus=n4_requested_cpus,
            region=gcp_region,
            active_machine_types=active_machine_types,
            min_machine_size=n4_min_machine_size,
            raise_on_zero=raise_on_zero,
        )

    quota_plan = cap_boot_machine_types(
        requested_machine_types,
        gcp_region,
        active_machine_types=active_machine_types,
    )
    if raise_on_zero and not quota_plan.machine_types and quota_plan.caps:
        _quota_exceeded(gcp_region, quota_plan.warnings)
    return quota_plan.machine_types, quota_plan.warnings


def _remove_local_dev_cluster_containers():
    if not IN_LOCAL_DEV_MODE:
        return

    docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
    for container in docker_client.containers(all=True):
        names = container["Names"]
        is_cluster_container = any(
            name.startswith("/node_") or name.startswith("/OLD--") or "worker" in name
            for name in names
        )
        if is_cluster_container:
            docker_client.remove_container(container["Id"], force=True)


def _shutdown_cluster(logger: Logger, auth_headers: dict):
    futures = []
    executor = ThreadPoolExecutor(max_workers=32)
    instance_client = InstancesClient()

    node_filter = FieldFilter("status", "in", ["READY", "BOOTING", "RUNNING"])
    active_nodes = list(DB.collection("nodes").where(filter=node_filter).stream())
    for node_snapshot in active_nodes:
        node = Node.from_snapshot(DB, logger, node_snapshot, auth_headers, instance_client)
        futures.append(executor.submit(node.delete))
    [future.result() for future in futures]
    executor.shutdown(wait=True)

    _remove_local_dev_cluster_containers()


def _current_local_dev_max_node_port():
    max_port = 8080
    node_filter = FieldFilter("status", "in", ["READY", "BOOTING", "RUNNING"])
    active_nodes = list(DB.collection("nodes").where(filter=node_filter).stream())
    for node_snapshot in active_nodes:
        host = str(node_snapshot.to_dict().get("host") or "")
        if ":" not in host:
            continue
        port = host.rsplit(":", 1)[-1]
        if port.isdigit():
            max_port = max(max_port, int(port))
    return max_port


def _get_cluster_config():
    """
    Returns the current cluster_config dict.

    Hot path: served from `CLUSTER_CONFIG_CACHE`, kept in sync by a firestore
    on_snapshot listener started in `lifespan`. Cold path (listener hasn't
    fired yet): one direct firestore read. The doc is guaranteed to exist
    by then - main_service seeds it synchronously at module import.
    """
    # Importing here to pick up the latest module-level value (the cache is
    # updated in place by the snapshot thread) and to avoid an import cycle
    # from __init__.py -> this module at import time.
    from main_service import CLUSTER_CONFIG_CACHE, _config_cache_lock

    if IN_LOCAL_DEV_MODE:
        return LOCAL_DEV_CONFIG
    with _config_cache_lock:
        cached = CLUSTER_CONFIG_CACHE
    if cached is not None:
        return cached

    # First-call fallback while the snapshot listener is still warming up.
    return DB.collection("cluster_config").document("cluster_config").get().to_dict()


def _start_nodes(
    logger: Logger,
    auth_headers: dict,
    config: dict,
    n_nodes_to_add: int = None,
    node_instance_names: list[str] = None,
    reserved_for_job: str = None,
    node_machine_types: list[str] = None,
    containers_override: list[dict] = None,
    inactivity_shutdown_time_sec_override: Optional[int] = None,
    quota_checked: bool = False,
    active_machine_types_for_quota: list[str] = None,
):
    if not quota_checked:
        requested_machine_types = _requested_machine_types(config, n_nodes_to_add, node_machine_types)
        node_machine_types, _warnings = _prepare_node_boot_plan(
            config,
            requested_machine_types,
            active_machine_types=active_machine_types_for_quota,
        )
        n_nodes_to_add = len(node_machine_types)

    node_service_port = _current_local_dev_max_node_port()
    futures = []
    executor = ThreadPoolExecutor(max_workers=32)
    instance_client = InstancesClient()
    machine_types_client = MachineTypesClient()

    def _add_node_logged(**node_start_kwargs):
        return Node.start(**node_start_kwargs).instance_name

    for node_spec in config["Nodes"]:
        quantity = node_spec["quantity"] if n_nodes_to_add is None else n_nodes_to_add
        spec_containers = containers_override or node_spec["containers"]
        for index in range(quantity):
            if IN_LOCAL_DEV_MODE:
                node_service_port += 1
            instance_name = None if node_instance_names is None else node_instance_names[index]
            machine_type = (
                node_machine_types[index]
                if node_machine_types is not None
                else node_spec["machine_type"]
            )
            inactivity_timeout = (
                inactivity_shutdown_time_sec_override
                if inactivity_shutdown_time_sec_override is not None
                else node_spec.get("inactivity_shutdown_time_sec")
            )
            node_start_kwargs = dict(
                db=DB,
                logger=logger,
                machine_type=machine_type,
                gcp_region=node_spec["gcp_region"],
                containers=[Container.from_dict(c) for c in spec_containers],
                auth_headers=auth_headers,
                instance_client=instance_client,
                machine_types_client=machine_types_client,
                service_port=node_service_port,
                sync_gcs_bucket_name=config["gcs_bucket_name"],
                as_local_container=IN_LOCAL_DEV_MODE,
                inactivity_shutdown_time_sec=inactivity_timeout,
                disk_size=node_spec.get("disk_size_gb"),
                instance_name=instance_name,
                reserved_for_job=reserved_for_job,
            )
            futures.append(executor.submit(_add_node_logged, **node_start_kwargs))
        if n_nodes_to_add is not None:
            break

    exec_results = [future.result() for future in futures]
    executor.shutdown(wait=True)
    node_instance_names = [result for result in exec_results if result is not None]

    # kill any local containers that shouldn't be running anymore
    if IN_LOCAL_DEV_MODE and n_nodes_to_add is None:
        docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
        node_ids = [name[11:] for name in node_instance_names]
        for container in docker_client.containers(all=True):
            name = container["Names"][0]
            is_main_service = name.startswith("/main_service")
            belongs_to_current_node = any([id_ in name for id_ in node_ids])
            if not (is_main_service or belongs_to_current_node):
                docker_client.remove_container(container["Id"], force=True)

    return node_instance_names


def _mark_running_jobs_with_lifecycle_event(event: str, message: str):
    """
    Runs synchronously in the restart/shutdown endpoints so clients see a
    definitive lifecycle signal via their firestore log listener before their
    nodes start going away and producing infrastructure errors.
    """
    status_filter = FieldFilter("status", "==", "RUNNING")
    running_jobs = list(DB.collection("jobs").where(filter=status_filter).stream())
    if not running_jobs:
        return
    timestamp = datetime.now(timezone.utc)
    log_doc = {
        "logs": [{"message": message, "timestamp": timestamp}],
        "timestamp": timestamp,
        "is_error": True,
        "event": event,
    }
    # The client raises the matching exception the moment it sees the bool on
    # /results (see Node._gather_results), so write it on the same update as the
    # status change. Writes happen before VM teardown in _shutdown_cluster, so
    # the doc is authoritative if a node vanishes mid-poll.
    extra = (
        {"cluster_restarted": True} if event == "cluster_restarted" else {"cluster_shutdown": True}
    )
    for job_snapshot in running_jobs:
        job_ref = job_snapshot.reference
        job_ref.collection("logs").add(log_doc)
        job_ref.update({"status": "CANCELED", **extra})


def _restart_cluster(logger: Logger, auth_headers: dict, node_machine_types: list[str] = None):
    start = time()

    _shutdown_cluster(logger, auth_headers)
    _remove_local_dev_cluster_containers()

    config = _get_cluster_config()
    node_count = len(node_machine_types) if node_machine_types is not None else config["Nodes"][0]["quantity"]
    machine_type = (
        node_machine_types[0]
        if node_machine_types
        else config["Nodes"][0]["machine_type"]
    )
    msg = f"Booting {node_count} {machine_type} nodes"
    log_telemetry(msg, severity="INFO")

    if node_machine_types is None:
        _start_nodes(logger, auth_headers, config)
    elif node_machine_types:
        _start_nodes(
            logger,
            auth_headers,
            config,
            n_nodes_to_add=len(node_machine_types),
            node_machine_types=node_machine_types,
            quota_checked=True,
        )

    duration = time() - start
    logger.log(f"Restarted after {duration//60}m {duration%60}s")


@router.post("/v1/cluster/restart")
def restart_cluster(
    logger: Logger = Depends(get_logger),
    auth_headers: dict = Depends(get_auth_headers),
    add_background_task=Depends(get_add_background_task_function),
):
    config = _get_cluster_config()
    gcp_region = config["Nodes"][0]["gcp_region"]
    requested_machine_types = _configured_machine_types(config)
    active_machine_types = _active_machine_types(gcp_region)
    # This endpoint is both Start and Restart. If nodes are already active,
    # _restart_cluster deletes them before booting the replacement batch.
    quota_active_machine_types = [] if active_machine_types else active_machine_types
    requested_machine_types, warnings = _prepare_node_boot_plan(
        config,
        requested_machine_types,
        active_machine_types=quota_active_machine_types,
    )

    _mark_running_jobs_with_lifecycle_event("cluster_restarted", "The cluster was restarted.")
    add_background_task(_restart_cluster, logger, auth_headers, requested_machine_types)
    return {"warnings": warnings} if warnings else {}


@router.post("/v1/cluster/shutdown")
async def shutdown_cluster(
    logger: Logger = Depends(get_logger),
    auth_headers: dict = Depends(get_auth_headers),
):
    start = time()

    _mark_running_jobs_with_lifecycle_event("cluster_shutdown", "The cluster was shut down.")
    log_telemetry("Cluster turned off.", severity="INFO")
    _shutdown_cluster(logger, auth_headers)

    duration = time() - start
    logger.log(f"Shut down after {duration//60}m {duration%60}s")
