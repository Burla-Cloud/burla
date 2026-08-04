import asyncio

from time import time

from fastapi import APIRouter, Depends
from concurrent.futures import ThreadPoolExecutor

from main_service import (
    IN_LOCAL_DEV_MODE,
    CLUSTER_NAME,
    LOCAL_DEV_CONFIG,
    LOCAL_DEV_NODE_PORT_BASE,
    get_logger,
    get_add_background_task_function,
)
from main_service import cluster_state, history
from main_service.node import Container, Node
from main_service.providers import get_provider
from main_service.helpers import Logger, log_telemetry

router = APIRouter()
MAX_GROW_CPUS = 2560
LOCAL_DEV_MAX_GROW_CPUS = 4

# Nodes booted by the grow path always get a short inactivity timeout
# regardless of the cluster-config value, so a burst-scaled job doesn't leave
# expensive hardware sitting idle long after the job finishes.
GROW_INACTIVITY_SHUTDOWN_TIME_SEC = 60


def _remove_local_dev_cluster_containers():
    if not IN_LOCAL_DEV_MODE:
        return
    import docker

    # Filter on this cluster's member label: matching by `node_`/`worker` name
    # prefix would delete every other local-dev cluster's containers, and
    # matching `burla-cluster` would delete this head too (it carries that label
    # so `make stop` gets it).
    docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
    containers = docker_client.containers(
        all=True, filters={"label": f"burla-cluster-member={CLUSTER_NAME}"}
    )
    for container in containers:
        docker_client.remove_container(container["Id"], force=True)


def _shutdown_cluster(logger: Logger):
    futures = []
    executor = ThreadPoolExecutor(max_workers=32)
    provider = get_provider()

    active_statuses = ("READY", "BOOTING", "RUNNING")
    active_nodes = [
        n for n in cluster_state.list_nodes() if n.get("status") in active_statuses
    ]
    for node_dict in active_nodes:
        node = Node.from_state(logger, node_dict, provider)
        futures.append(executor.submit(node.delete))
    [future.result() for future in futures]
    executor.shutdown(wait=True)

    # FAILED tombstones exist so clients polling a mid-boot node can see the
    # failure; once the whole cluster is torn down they're purely historical
    # (and history already has them), so drop them from live state.
    for node_dict in cluster_state.list_nodes():
        if node_dict.get("status") == "FAILED":
            cluster_state.remove_node(node_dict["instance_name"])

    _remove_local_dev_cluster_containers()


def _current_local_dev_max_node_port():
    # Node ports are published on the host and the client reaches nodes at
    # localhost:<port>, so each cluster needs its own non-overlapping block.
    max_port = LOCAL_DEV_NODE_PORT_BASE
    active_statuses = ("READY", "BOOTING", "RUNNING")
    for node in cluster_state.list_nodes():
        if node.get("status") not in active_statuses:
            continue
        host = str(node.get("host") or "")
        if ":" not in host:
            continue
        port = host.rsplit(":", 1)[-1]
        if port.isdigit():
            max_port = max(max_port, int(port))
    return max_port


def _get_cluster_config():
    if IN_LOCAL_DEV_MODE:
        return LOCAL_DEV_CONFIG
    return history.get_cluster_config()


def _start_nodes(
    logger: Logger,
    config: dict,
    n_nodes_to_add: int = None,
    node_instance_names: list[str] = None,
    reserved_for_job: str = None,
    node_machine_types: list[str] = None,
    containers_override: list[dict] = None,
    inactivity_shutdown_time_sec_override: int | None = None,
):
    node_service_port = _current_local_dev_max_node_port()
    futures = []
    executor = ThreadPoolExecutor(max_workers=32)
    provider = get_provider()

    def _add_node_logged(**node_start_kwargs):
        return Node.start(**node_start_kwargs).instance_name

    for node_spec in config["Nodes"]:
        quantity = node_spec["quantity"] if n_nodes_to_add is None else n_nodes_to_add
        spec_containers = containers_override or node_spec["containers"]
        for index in range(quantity):
            if IN_LOCAL_DEV_MODE:
                node_service_port += 1
            instance_name = (
                None if node_instance_names is None else node_instance_names[index]
            )
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
                logger=logger,
                machine_type=machine_type,
                region=node_spec["gcp_region"],
                containers=[Container.from_dict(c) for c in spec_containers],
                provider=provider,
                service_port=node_service_port,
                sync_bucket_name=config["gcs_bucket_name"],
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
        import docker

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
    definitive lifecycle signal (riding the next /results response from their
    nodes) before those nodes start going away and producing infrastructure
    errors.
    """
    running_job_ids = cluster_state.running_job_ids()
    if not running_job_ids:
        return
    timestamp = time()
    log_doc = {
        "logs": [{"message": message, "timestamp": timestamp}],
        "timestamp": timestamp,
        "is_error": True,
        "event": event,
    }
    extra = (
        {"cluster_restarted": True}
        if event == "cluster_restarted"
        else {"cluster_shutdown": True}
    )
    for job_id in running_job_ids:
        history.add_job_logs(job_id, [log_doc])
        cluster_state.update_job(job_id, {"status": "CANCELED", **extra})


def _restart_cluster(logger: Logger):
    start = time()

    _shutdown_cluster(logger)
    _remove_local_dev_cluster_containers()

    config = _get_cluster_config()
    msg = f"Booting {config['Nodes'][0]['quantity']} {config['Nodes'][0]['machine_type']} nodes"
    log_telemetry(msg, severity="INFO")

    _start_nodes(logger, config)

    duration = time() - start
    logger.log(f"Restarted after {duration//60}m {duration%60}s")


@router.post("/v1/cluster/restart")
def restart_cluster(
    logger: Logger = Depends(get_logger),
    add_background_task=Depends(get_add_background_task_function),
):
    _mark_running_jobs_with_lifecycle_event(
        "cluster_restarted", "The cluster was restarted."
    )
    add_background_task(_restart_cluster, logger)


@router.post("/v1/cluster/shutdown")
async def shutdown_cluster(
    logger: Logger = Depends(get_logger),
):
    start = time()

    _mark_running_jobs_with_lifecycle_event(
        "cluster_shutdown", "The cluster was shut down."
    )
    log_telemetry("Cluster turned off.", severity="INFO")
    await asyncio.to_thread(_shutdown_cluster, logger)

    duration = time() - start
    logger.log(f"Shut down after {duration//60}m {duration%60}s")
