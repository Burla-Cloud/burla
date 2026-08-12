import asyncio
import hashlib
import os
import tempfile

from time import time

from fastapi import APIRouter, Depends, HTTPException, Request
from concurrent.futures import ThreadPoolExecutor

from main_service import (
    IN_LOCAL_DEV_MODE,
    CLOUD_PROVIDER,
    CLUSTER_ID_TOKEN,
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
    from main_service.providers.local_docker import remove_container

    # Filter on this cluster's member label: matching by `node_` name prefix
    # would delete every other local-dev cluster's containers, and matching
    # `burla-cluster` would delete this head too (it carries that label so
    # `make stop` gets it). Workers live on each node's own docker daemon and
    # die with it.
    docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
    containers = docker_client.containers(
        all=True, filters={"label": f"burla-cluster-member={CLUSTER_NAME}"}
    )
    for container in containers:
        remove_container(docker_client, container["Id"])


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


def _taken_local_dev_node_ports():
    """Ports this cluster is already using. Node ports are published on the host
    and the client reaches nodes at localhost:<port>, so each cluster gets its
    own block starting at LOCAL_DEV_NODE_PORT_BASE. The state field covers a
    node from the moment Node.start records it (BOOTING nodes have no host yet);
    live containers cover a node deleted a moment ago that still holds its host
    port binding while docker tears it down."""
    import docker

    active_statuses = ("READY", "BOOTING", "RUNNING")
    ports = set()
    for node in cluster_state.list_nodes():
        if node.get("status") in active_statuses and node.get("port"):
            ports.add(node["port"])

    docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
    containers = docker_client.containers(
        filters={"label": f"burla-cluster-member={CLUSTER_NAME}"}
    )
    for container in containers:
        for port in container.get("Ports") or []:
            if port.get("PublicPort"):
                ports.add(port["PublicPort"])
    return ports


def _get_cluster_config():
    if IN_LOCAL_DEV_MODE:
        return LOCAL_DEV_CONFIG
    return history.get_cluster_config()


@router.post("/v1/local-dev/node-quantity/{quantity}")
def set_local_dev_node_quantity(quantity: int):
    """Test-only knob. Multi-node tests need more than the one node local-dev
    boots, and local-dev reads the quantity once at startup then forces it back
    to 1 on every settings write, so nothing else can change it at runtime."""
    if not IN_LOCAL_DEV_MODE:
        raise HTTPException(status_code=404, detail="local-dev clusters only")
    LOCAL_DEV_CONFIG["Nodes"][0]["quantity"] = quantity
    return {"quantity": quantity}


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
    # Lowest free port rather than highest+1, so a node slot is reused by the
    # node that replaces it. The heavy per-slot caches (inner docker image
    # store, worker python env) are keyed by port, and the test suite churns
    # nodes hard enough that ever-climbing ports would rebuild both every time.
    taken_ports = _taken_local_dev_node_ports() if IN_LOCAL_DEV_MODE else set()
    futures = []
    executor = ThreadPoolExecutor(max_workers=32)
    provider = get_provider()

    def _add_node_logged(**node_start_kwargs):
        return Node.start(**node_start_kwargs).instance_name

    for node_spec in config["Nodes"]:
        quantity = node_spec["quantity"] if n_nodes_to_add is None else n_nodes_to_add
        spec_containers = containers_override or node_spec["containers"]
        for index in range(quantity):
            node_service_port = LOCAL_DEV_NODE_PORT_BASE
            if IN_LOCAL_DEV_MODE:
                node_service_port = LOCAL_DEV_NODE_PORT_BASE + 1
                while node_service_port in taken_ports:
                    node_service_port += 1
                taken_ports.add(node_service_port)
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

    # There used to be a sweep here removing every cluster-member container this
    # call didn't just boot. It existed to clean up sibling-mode leftover worker
    # containers, which don't exist anymore (workers live inside their node),
    # and it killed live nodes: restarts run as background tasks and arrive back
    # to back from the test fixtures, and the earlier restart's sweep landed
    # after the later restart's nodes had booted and taken jobs, deleting them
    # mid-job. Shutdown already sweeps this cluster's containers by label.
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


# ------------------------------------------------------------ deploy migration
# A first `burla deploy` moves this machine's client-hosted history into the
# new deployed cluster: deploy pauses this head, snapshots its history db,
# and uploads the snapshot to the deployed head's import endpoint.


@router.post("/v1/cluster/pause_job_admission")
def pause_job_admission():
    if not cluster_state.pause_job_admission_if_idle():
        raise HTTPException(status_code=409, detail="A job is currently running.")


@router.post("/v1/cluster/resume_job_admission")
def resume_job_admission():
    cluster_state.resume_job_admission()


@router.post("/v1/cluster/import_history")
async def import_history(request: Request):
    # Cluster-token only: authorized dashboard users must not be able to
    # inject arbitrary history rows.
    if request.headers.get("Authorization") != f"Bearer {CLUSTER_ID_TOKEN}":
        raise HTTPException(status_code=403, detail="cluster token required")

    digest = hashlib.sha256()
    descriptor, snapshot_path = tempfile.mkstemp(suffix=".db")
    try:
        with os.fdopen(descriptor, "wb") as snapshot_file:
            async for chunk in request.stream():
                digest.update(chunk)
                snapshot_file.write(chunk)
        imported = await asyncio.to_thread(
            _import_history_snapshot, snapshot_path, digest.hexdigest()
        )
    finally:
        os.remove(snapshot_path)
    return {"imported": imported}


def _import_history_snapshot(snapshot_path: str, digest: str) -> bool:
    config = history.snapshot_cluster_config(snapshot_path)
    if config is not None:
        deployed_config = history.get_cluster_config()
        # The snapshot's config has no shared-workspace bucket (client-hosted
        # heads run without one), and on AWS its node region may be one this
        # deployment never prepared (no node AMI / security groups there).
        config["gcs_bucket_name"] = deployed_config.get("gcs_bucket_name")
        if CLOUD_PROVIDER == "aws":
            deployed_region = deployed_config["Nodes"][0]["gcp_region"]
            for node_spec in config["Nodes"]:
                node_spec["gcp_region"] = deployed_region
    return history.import_snapshot(snapshot_path, digest, config)
