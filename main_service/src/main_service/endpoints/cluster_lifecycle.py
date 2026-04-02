import docker
import math
from time import time

from fastapi import APIRouter, Depends, Request, HTTPException
from google.cloud.firestore_v1.base_query import FieldFilter
from google.cloud.compute_v1 import InstancesClient
from concurrent.futures import ThreadPoolExecutor

from main_service import (
    DB,
    IN_LOCAL_DEV_MODE,
    LOCAL_DEV_CONFIG,
    DEFAULT_CONFIG,
    get_logger,
    get_auth_headers,
    get_add_background_task_function,
)
from main_service.node import Container, Node
from main_service.helpers import Logger, log_telemetry

router = APIRouter()
MAX_GROW_CPUS = 2560
LOCAL_DEV_MAX_GROW_CPUS = 4


def _machine_type_cpu_count(machine_type: str) -> int:
    if machine_type.startswith("n4-standard-") and machine_type.split("-")[-1].isdigit():
        return int(machine_type.split("-")[-1])
    return 1


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

    if IN_LOCAL_DEV_MODE:
        docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
        for container in docker_client.containers():
            name = container["Names"][0]
            is_node_container = name.startswith("/node")
            is_worker_container = "worker" in name
            if is_node_container or is_worker_container:
                docker_client.remove_container(container["Id"], force=True)


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
    config_doc = DB.collection("cluster_config").document("cluster_config").get()
    if not config_doc.exists:
        config_doc.reference.set(DEFAULT_CONFIG)
        return DEFAULT_CONFIG
    return LOCAL_DEV_CONFIG if IN_LOCAL_DEV_MODE else config_doc.to_dict()


def _start_nodes(logger: Logger, auth_headers: dict, config: dict, n_nodes_to_add: int = None):
    node_service_port = _current_local_dev_max_node_port()
    futures = []
    executor = ThreadPoolExecutor(max_workers=32)

    def _add_node_logged(**node_start_kwargs):
        return Node.start(**node_start_kwargs).instance_name

    for node_spec in config["Nodes"]:
        quantity = node_spec["quantity"] if n_nodes_to_add is None else n_nodes_to_add
        for _ in range(quantity):
            if IN_LOCAL_DEV_MODE:
                node_service_port += 1
            node_start_kwargs = dict(
                db=DB,
                logger=logger,
                machine_type=node_spec["machine_type"],
                gcp_region=node_spec["gcp_region"],
                containers=[Container.from_dict(c) for c in node_spec["containers"]],
                auth_headers=auth_headers,
                service_port=node_service_port,
                sync_gcs_bucket_name=config["gcs_bucket_name"],
                as_local_container=IN_LOCAL_DEV_MODE,
                inactivity_shutdown_time_sec=node_spec.get("inactivity_shutdown_time_sec"),
                disk_size=node_spec.get("disk_size_gb"),
            )
            futures.append(executor.submit(_add_node_logged, **node_start_kwargs))
        if n_nodes_to_add is not None:
            break

    exec_results = [future.result() for future in futures]
    executor.shutdown(wait=True)
    node_instance_names = [result for result in exec_results if result is not None]

    if IN_LOCAL_DEV_MODE:
        # kill local containers that shouldn't be running anymore
        docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
        node_ids = [name[11:] for name in node_instance_names]
        for container in docker_client.containers(all=True):
            name = container["Names"][0]
            is_main_service = name.startswith("/main_service")
            belongs_to_current_node = any([id_ in name for id_ in node_ids])
            if not (is_main_service or belongs_to_current_node):
                docker_client.remove_container(container["Id"], force=True)
    return node_instance_names


def _active_cluster_cpus():
    total_cpus = 0
    node_filter = FieldFilter("status", "in", ["READY", "BOOTING", "RUNNING"])
    active_nodes = list(DB.collection("nodes").where(filter=node_filter).stream())
    for node_snapshot in active_nodes:
        machine_type = node_snapshot.to_dict().get("machine_type")
        total_cpus += _machine_type_cpu_count(machine_type)
    return total_cpus


def _restart_cluster(logger: Logger, auth_headers: dict):
    start = time()

    _shutdown_cluster(logger, auth_headers)

    config = _get_cluster_config()
    msg = f"Booting {config['Nodes'][0]['quantity']} {config['Nodes'][0]['machine_type']} nodes"
    log_telemetry(msg, severity="INFO")

    _start_nodes(logger, auth_headers, config)

    duration = time() - start
    logger.log(f"Restarted after {duration//60}m {duration%60}s")


@router.post("/v1/cluster/restart")
def restart_cluster(
    logger: Logger = Depends(get_logger),
    auth_headers: dict = Depends(get_auth_headers),
    add_background_task=Depends(get_add_background_task_function),
):
    add_background_task(_restart_cluster, logger, auth_headers)


@router.post("/v1/cluster/shutdown")
async def shutdown_cluster(
    logger: Logger = Depends(get_logger),
    auth_headers: dict = Depends(get_auth_headers),
):
    start = time()

    log_telemetry("Cluster turned off.", severity="INFO")
    _shutdown_cluster(logger, auth_headers)

    duration = time() - start
    logger.log(f"Shut down after {duration//60}m {duration%60}s")


@router.post("/v1/cluster/grow")
async def grow_cluster(
    request: Request,
    logger: Logger = Depends(get_logger),
    auth_headers: dict = Depends(get_auth_headers),
):
    request_json = await request.json()
    target_cpus = int(request_json.get("target_cpus", 0))
    max_cpu = LOCAL_DEV_MAX_GROW_CPUS if IN_LOCAL_DEV_MODE else MAX_GROW_CPUS
    target_cpus = min(target_cpus, max_cpu)

    config = _get_cluster_config()
    node_spec = config["Nodes"][0]
    cpu_per_node = _machine_type_cpu_count(node_spec["machine_type"])
    current_cpus = _active_cluster_cpus()
    missing_cpus = max(0, target_cpus - current_cpus)
    n_nodes_to_add = math.ceil(missing_cpus / cpu_per_node) if missing_cpus else 0

    if n_nodes_to_add > 0:
        _start_nodes(logger, auth_headers, config, n_nodes_to_add=n_nodes_to_add)

    return {
        "target_cpus": target_cpus,
        "current_cpus": current_cpus,
        "added_nodes": n_nodes_to_add,
    }
