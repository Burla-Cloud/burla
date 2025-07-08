import json
import asyncio
import docker
import requests
from time import time

from fastapi import APIRouter, Depends, Request, HTTPException
from google.cloud.firestore_v1 import FieldFilter
from google.cloud.compute_v1 import InstancesClient
from starlette.responses import StreamingResponse
from concurrent.futures import ThreadPoolExecutor

from main_service import (
    DB,
    IN_LOCAL_DEV_MODE,
    LOCAL_DEV_CONFIG,
    DEFAULT_CONFIG,
    PROJECT_ID,
    BURLA_BACKEND_URL,
    get_logger,
)
from main_service.node import Container, Node
from main_service.helpers import Logger

router = APIRouter()


@router.post("/v1/cluster/restart")
def restart_cluster(request: Request, logger: Logger = Depends(get_logger)):
    start = time()
    instance_client = InstancesClient()

    email = request.session.get("X-User-Email")
    authorization = request.session.get("Authorization")
    auth_headers = {"Authorization": authorization, "X-User-Email": email}

    try:
        json = {"project_id": PROJECT_ID, "message": "Someone turned the cluster on."}
        requests.post(f"{BURLA_BACKEND_URL}/v1/telemetry/log/INFO", json=json, timeout=1)
    except Exception:
        pass

    futures = []
    executor = ThreadPoolExecutor(max_workers=32)

    # delete all nodes
    node_filter = FieldFilter("status", "in", ["READY", "BOOTING", "RUNNING"])
    for node_snapshot in DB.collection("nodes").where(filter=node_filter).stream():
        node = Node.from_snapshot(DB, logger, node_snapshot, auth_headers, instance_client)
        futures.append(executor.submit(node.delete))

    # add nodes according to cluster_config doc
    def _add_node_logged(machine_type, containers, node_service_port, inactivity_time, disk_size):
        node = Node.start(
            db=DB,
            logger=logger,
            machine_type=machine_type,
            containers=containers,
            auth_headers=auth_headers,
            service_port=node_service_port,
            as_local_container=IN_LOCAL_DEV_MODE,  # <- start in a container if IN_LOCAL_DEV_MODE
            inactivity_shutdown_time_sec=inactivity_time,
            disk_size=disk_size,
            verbose=True,
        )
        return node.instance_name

    # remove any existing `node_service` containers if in IN_LOCAL_DEV_MODE
    # this has to be done before starting new node_services so ports are available
    if IN_LOCAL_DEV_MODE:
        docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
        for container in docker_client.containers():
            if container["Names"][0].startswith("/node"):
                docker_client.remove_container(container["Id"], force=True)

    # use separate cluster config if IN_LOCAL_DEV_MODE:
    config_doc = DB.collection("cluster_config").document("cluster_config").get()
    if not config_doc.exists:
        config_doc.reference.set(DEFAULT_CONFIG)
        config = DEFAULT_CONFIG
    else:
        config = LOCAL_DEV_CONFIG if IN_LOCAL_DEV_MODE else config_doc.to_dict()

    node_service_port = 8080  # <- must default to 8080 because only 8080 was opened in GCP firewall

    for node_spec in config["Nodes"]:
        for _ in range(node_spec["quantity"]):

            if IN_LOCAL_DEV_MODE:  # avoid trying to open same port on multiple local containers
                node_service_port += 1
            machine_type = node_spec["machine_type"]
            containers = [Container.from_dict(c) for c in node_spec["containers"]]
            inactivity_time = node_spec.get("inactivity_shutdown_time_sec")
            disk_size = node_spec.get("disk_size_gb")

            node_args = (machine_type, containers, node_service_port, inactivity_time, disk_size)
            future = executor.submit(_add_node_logged, *node_args)
            futures.append(future)

    # wait until all operations done
    exec_results = [future.result() for future in futures]
    node_instance_names = [result for result in exec_results if result is not None]
    executor.shutdown(wait=True)

    # remove any old containers created by old nodes (new nodes only responsible for their workers)
    if IN_LOCAL_DEV_MODE:
        node_ids = [name[11:] for name in node_instance_names]
        for container in docker_client.containers(all=True):
            name = container["Names"][0]
            is_main_service = name.startswith("/main_service")
            belongs_to_current_node = any([id in name for id in node_ids])
            if not (is_main_service or belongs_to_current_node):
                docker_client.remove_container(container["Id"], force=True)

    duration = time() - start
    logger.log(f"Restarted after {duration//60}m {duration%60}s")


@router.post("/v1/cluster/shutdown")
async def shutdown_cluster(request: Request, logger: Logger = Depends(get_logger)):
    start = time()
    instance_client = InstancesClient()

    email = request.session.get("X-User-Email")
    authorization = request.session.get("Authorization")
    auth_headers = {"Authorization": authorization, "X-User-Email": email}

    try:
        json = {"project_id": PROJECT_ID, "message": "Someone turned the cluster off."}
        requests.post(f"{BURLA_BACKEND_URL}/v1/telemetry/log/INFO", json=json, timeout=1)
    except Exception:
        pass

    futures = []
    executor = ThreadPoolExecutor(max_workers=32)

    # delete all nodes
    node_filter = FieldFilter("status", "in", ["READY", "BOOTING", "RUNNING"])
    for node_snapshot in DB.collection("nodes").where(filter=node_filter).stream():
        node = Node.from_snapshot(DB, logger, node_snapshot, auth_headers, instance_client)
        futures.append(executor.submit(node.delete))

    # remove any existing node/worker service containers if in IN_LOCAL_DEV_MODE
    if IN_LOCAL_DEV_MODE:
        docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
        for container in docker_client.containers():
            is_node_service_container = container["Names"][0].startswith("/node")
            is_worker_service_container = "worker" in container["Names"][0]
            if is_node_service_container or is_worker_service_container:
                docker_client.remove_container(container["Id"], force=True)

    [future.result() for future in futures]
    executor.shutdown(wait=True)

    duration = time() - start
    logger.log(f"Shut down after {duration//60}m {duration%60}s")


@router.get("/v1/cluster")
async def cluster_info(logger: Logger = Depends(get_logger)):
    queue = asyncio.Queue()
    current_loop = asyncio.get_running_loop()

    async def node_stream():

        def on_snapshot(query_snapshot, changes, read_time):
            for change in changes:
                doc_data = change.document.to_dict() or {}
                instance_name = doc_data.get("instance_name")

                if change.type.name == "REMOVED":
                    event_data = {"nodeId": instance_name, "deleted": True}
                else:
                    event_data = {
                        "nodeId": instance_name,
                        "status": doc_data.get("status"),
                        "type": doc_data.get("machine_type"),
                    }
                current_loop.call_soon_threadsafe(queue.put_nowait, event_data)
                logger.log(f"Node change detected: {event_data}")

        display_filter = FieldFilter("display_in_dashboard", "==", True)
        query = DB.collection("nodes").where(filter=display_filter)
        node_watch = query.on_snapshot(on_snapshot)

        try:
            while True:
                event = await queue.get()
                yield f"data: {json.dumps(event)}\n\n"
        finally:
            node_watch.unsubscribe()

    return StreamingResponse(node_stream(), media_type="text/event-stream")


@router.delete("/v1/cluster/{node_id}")
def delete_node(node_id: str):
    """deletes the firestore doc, used to dismiss failed nodes from dashboard"""

    node_doc = DB.collection("nodes").document(node_id).get()
    if node_doc.exists and node_doc.to_dict().get("status") == "FAILED":
        node_doc.reference.update({"display_in_dashboard": False})
    elif node_doc.exists:
        node_doc.reference.update({"status": "DELETED", "display_in_dashboard": False})
    else:
        raise HTTPException(status_code=404, detail=f"Node {node_id} not found")


@router.get("/v1/cluster/{node_id}/logs")
async def node_log_stream(node_id: str, logger: Logger = Depends(get_logger)):
    queue = asyncio.Queue()
    current_loop = asyncio.get_running_loop()

    def on_snapshot(query_snapshot, changes, read_time):
        for change in changes:
            doc_data = change.document.to_dict() or {}
            message = doc_data.get("msg")
            event = {"message": message}
            current_loop.call_soon_threadsafe(queue.put_nowait, event)

    logs_ref = DB.collection("nodes").document(node_id).collection("logs")
    watch = logs_ref.on_snapshot(on_snapshot)

    async def log_generator():
        try:
            while True:
                event = await queue.get()
                yield f"data: {json.dumps(event)}\n\n"
        finally:
            watch.unsubscribe()

    return StreamingResponse(log_generator(), media_type="text/event-stream")
