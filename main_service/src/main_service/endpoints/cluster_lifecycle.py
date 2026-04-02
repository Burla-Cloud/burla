import docker
import requests
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
    PROJECT_ID,
    BURLA_BACKEND_URL,
    get_logger,
    get_add_background_task_function,
)
from main_service.node import Container, Node
from main_service.helpers import Logger

router = APIRouter()


def _require_auth(request: Request) -> dict:
    email = request.session.get("X-User-Email") or request.headers.get("X-User-Email")
    authorization = request.session.get("Authorization") or request.headers.get("Authorization")
    if not email or not authorization:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return {"Authorization": authorization, "X-User-Email": email}


def _machine_type_cpu_count(machine_type: str) -> int:
    if machine_type.startswith("n4-standard-") and machine_type.split("-")[-1].isdigit():
        return int(machine_type.split("-")[-1])
    return 1


def _active_node_snapshots():
    node_filter = FieldFilter("status", "in", ["READY", "BOOTING", "RUNNING"])
    return list(DB.collection("nodes").where(filter=node_filter).stream())


def _remove_local_containers(remove_worker_containers: bool):
    if not IN_LOCAL_DEV_MODE:
        return
    docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
    for container in docker_client.containers():
        name = container["Names"][0]
        is_node_container = name.startswith("/node")
        is_worker_container = "worker" in name
        if is_node_container or (remove_worker_containers and is_worker_container):
            docker_client.remove_container(container["Id"], force=True)


def _prune_local_dev_containers_to_active_nodes(node_instance_names: list[str]):
    if not IN_LOCAL_DEV_MODE:
        return
    docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
    node_ids = [name[11:] for name in node_instance_names]
    for container in docker_client.containers(all=True):
        name = container["Names"][0]
        is_main_service = name.startswith("/main_service")
        belongs_to_current_node = any([id_ in name for id_ in node_ids])
        if not (is_main_service or belongs_to_current_node):
            docker_client.remove_container(container["Id"], force=True)


def _delete_active_nodes(logger: Logger, auth_headers: dict, instance_client: InstancesClient):
    futures = []
    executor = ThreadPoolExecutor(max_workers=32)
    for node_snapshot in _active_node_snapshots():
        node = Node.from_snapshot(DB, logger, node_snapshot, auth_headers, instance_client)
        futures.append(executor.submit(node.delete))
    [future.result() for future in futures]
    executor.shutdown(wait=True)


def _current_local_dev_max_node_port():
    max_port = 8080
    for node_snapshot in _active_node_snapshots():
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
    return [result for result in exec_results if result is not None]


def _active_cluster_cpus():
    total_cpus = 0
    for node_snapshot in _active_node_snapshots():
        machine_type = node_snapshot.to_dict().get("machine_type")
        total_cpus += _machine_type_cpu_count(machine_type)
    return total_cpus


def _restart_cluster(request: Request, logger: Logger):
    start = time()
    instance_client = InstancesClient()
    auth_headers = _require_auth(request)
    _delete_active_nodes(logger, auth_headers, instance_client)
    _remove_local_containers(remove_worker_containers=False)
    config = _get_cluster_config()

    try:
        msg = f"Booting {config['Nodes'][0]['quantity']} {config['Nodes'][0]['machine_type']} nodes"
        payload = {"project_id": PROJECT_ID, "message": msg}
        requests.post(f"{BURLA_BACKEND_URL}/v1/telemetry/log/INFO", json=payload, timeout=1)
    except Exception:
        pass

    node_instance_names = _start_nodes(logger, auth_headers, config)
    _prune_local_dev_containers_to_active_nodes(node_instance_names)

    duration = time() - start
    logger.log(f"Restarted after {duration//60}m {duration%60}s")


@router.post("/v1/cluster/restart")
def restart_cluster(
    request: Request,
    logger: Logger = Depends(get_logger),
    add_background_task=Depends(get_add_background_task_function),
):
    add_background_task(_restart_cluster, request, logger)


@router.post("/v1/cluster/shutdown")
async def shutdown_cluster(request: Request, logger: Logger = Depends(get_logger)):
    start = time()
    instance_client = InstancesClient()
    auth_headers = _require_auth(request)

    try:
        payload = {"project_id": PROJECT_ID, "message": "Cluster turned off."}
        requests.post(f"{BURLA_BACKEND_URL}/v1/telemetry/log/INFO", json=payload, timeout=1)
    except Exception:
        pass

    _delete_active_nodes(logger, auth_headers, instance_client)
    _remove_local_containers(remove_worker_containers=True)

    duration = time() - start
    logger.log(f"Shut down after {duration//60}m {duration%60}s")


@router.post("/v1/cluster/grow")
async def grow_cluster(request: Request, logger: Logger = Depends(get_logger)):
    auth_headers = _require_auth(request)
    request_json = await request.json()
    target_cpus = int(request_json.get("target_cpus", 0))
    if target_cpus <= 0:
        raise HTTPException(status_code=400, detail="target_cpus must be > 0")

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
