import json
import asyncio
import docker
import requests
from time import time
from datetime import datetime
import pytz
import textwrap

from fastapi import APIRouter, Depends, Request
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
    get_add_background_task_function,
)
from main_service.node import Node
from main_service.helpers import Logger

router = APIRouter()


@router.post("/v1/cluster/nodes")
def add_node(request: Request, logger: Logger = Depends(get_logger)):

    # use separate cluster config if IN_LOCAL_DEV_MODE:
    config_doc = DB.collection("cluster_config").document("cluster_config").get()
    if not config_doc.exists:
        config_doc.reference.set(DEFAULT_CONFIG)
        config = DEFAULT_CONFIG
    else:
        config = LOCAL_DEV_CONFIG if IN_LOCAL_DEV_MODE else config_doc.to_dict()

    request_json = request.json()
    total_cpus = request_json["total_cpus"]
    disk_size = request_json.get("disk_size", config["Nodes"][0]["disk_size_gb"])
    image_uri = request_json.get("image_uri", config["Nodes"][0]["containers"][0]["image"])
    gcp_region = request_json.get("gcp_region", config["Nodes"][0]["gcp_region"])
    inactivity_shutdown_time_sec = 180

    futures = []
    executor = ThreadPoolExecutor(max_workers=32)
    email = request.session.get("X-User-Email")
    authorization = request.session.get("Authorization")
    auth_headers = {"Authorization": authorization, "X-User-Email": email}

    def _add_node_logged(**node_start_kwargs):
        return Node.start(**node_start_kwargs).instance_name

    node_service_port = 8080
    for n_cpus in [80, 64, 48, 32, 16, 8, 4, 2, 1]:
        quantity = total_cpus // n_cpus
        machine_type = "n4-standard-2" if n_cpus == 1 else f"n4-standard-{n_cpus}"
        for _ in range(quantity):
            if IN_LOCAL_DEV_MODE:
                node_service_port += 1
            node_start_kwargs = dict(
                db=DB,
                logger=logger,
                machine_type=machine_type,
                gcp_region=gcp_region,
                containers=[image_uri],
                auth_headers=auth_headers,
                service_port=node_service_port,
                sync_gcs_bucket_name=config["gcs_bucket_name"],
                as_local_container=IN_LOCAL_DEV_MODE,  # start in a container if IN_LOCAL_DEV_MODE
                inactivity_shutdown_time_sec=inactivity_shutdown_time_sec,
                disk_size=disk_size,
            )
            future = executor.submit(_add_node_logged, **node_start_kwargs)
            futures.append(future)
            total_cpus -= quantity * n_cpus

    # wait until all operations done
    exec_results = [future.result() for future in futures]
    node_instance_names = [result for result in exec_results if result is not None]
    executor.shutdown(wait=True)
    return node_instance_names


def _restart_cluster(request: Request, logger: Logger):
    start = time()
    instance_client = InstancesClient()

    email = request.session.get("X-User-Email")
    authorization = request.session.get("Authorization")
    auth_headers = {"Authorization": authorization, "X-User-Email": email}

    futures = []
    executor = ThreadPoolExecutor(max_workers=128)

    # delete all nodes
    node_filter = FieldFilter("status", "in", ["READY", "BOOTING", "RUNNING"])
    for node_snapshot in DB.collection("nodes").where(filter=node_filter).stream():
        node = Node.from_snapshot(DB, logger, node_snapshot, auth_headers, instance_client)
        futures.append(executor.submit(node.delete))

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

    # log telemetry
    try:
        msg = f"Booting {config['Nodes'][0]['quantity']} {config['Nodes'][0]['machine_type']} nodes"
        json = {"project_id": PROJECT_ID, "message": msg}
        requests.post(f"{BURLA_BACKEND_URL}/v1/telemetry/log/INFO", json=json, timeout=1)
    except Exception:
        pass

    def _add_node_logged(**node_start_kwargs):
        return Node.start(**node_start_kwargs).instance_name

    for node_spec in config["Nodes"]:
        for _ in range(node_spec["quantity"]):
            if IN_LOCAL_DEV_MODE:  # avoid trying to open same port on multiple local containers
                node_service_port += 1
            node_start_kwargs = dict(
                db=DB,
                logger=logger,
                machine_type=node_spec["machine_type"],
                gcp_region=node_spec["gcp_region"],
                containers=[c["image"] for c in node_spec["containers"]],
                auth_headers=auth_headers,
                service_port=node_service_port,
                sync_gcs_bucket_name=config["gcs_bucket_name"],
                as_local_container=IN_LOCAL_DEV_MODE,  # start in a container if IN_LOCAL_DEV_MODE
                inactivity_shutdown_time_sec=node_spec.get("inactivity_shutdown_time_sec"),
                disk_size=node_spec.get("disk_size_gb"),
            )
            future = executor.submit(_add_node_logged, **node_start_kwargs)
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

    email = request.session.get("X-User-Email")
    authorization = request.session.get("Authorization")
    auth_headers = {"Authorization": authorization, "X-User-Email": email}

    try:
        json = {"project_id": PROJECT_ID, "message": "Cluster turned off."}
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
        # Check for empty collection before starting snapshot listener
        display_filter = FieldFilter("display_in_dashboard", "==", True)
        query = DB.collection("nodes").where(filter=display_filter)
        if len([doc for doc in query.stream()]) == 0:
            yield f"data: {json.dumps({'type': 'empty'})}\n\n"

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

        display_filter = FieldFilter("display_in_dashboard", "==", True)
        node_watch = DB.collection("nodes").where(filter=display_filter).on_snapshot(on_snapshot)
        try:
            # set 5s reconnect on drop and send an initial comment to open the stream
            yield "retry: 5000\n\n"
            yield ": init\n\n"
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=2)
                    yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            node_watch.unsubscribe()

    return StreamingResponse(
        node_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache, no-transform"},
    )


@router.delete("/v1/cluster/{node_id}")
def delete_node(
    node_id: str,
    request: Request,
    hide_if_failed: bool = True,
    add_background_task=Depends(get_add_background_task_function),
    logger: Logger = Depends(get_logger),
):
    email = request.session.get("X-User-Email")
    authorization = request.session.get("Authorization")
    auth_headers = {"Authorization": authorization, "X-User-Email": email}
    node_doc = DB.collection("nodes").document(node_id).get()

    node = Node.from_snapshot(DB, logger, node_doc, auth_headers)
    add_background_task(node.delete, hide_if_failed=hide_if_failed)


@router.get("/v1/cluster/{node_id}/logs")
async def node_log_stream(node_id: str, request: Request):
    queue = asyncio.Queue()
    current_loop = asyncio.get_running_loop()
    tz = pytz.timezone(request.cookies.get("timezone", "UTC"))
    ts_to_str = lambda ts: f"[{datetime.fromtimestamp(ts, tz).strftime('%I:%M %p').lstrip('0')}]"

    last_date_str = None
    first_log_processed = False

    def on_snapshot(query_snapshot, changes, read_time):
        nonlocal last_date_str, first_log_processed
        sorted_changes = sorted(changes, key=lambda change: change.document.to_dict().get("ts"))
        for change in sorted_changes:
            log_doc_dict = change.document.to_dict()
            timestamp = log_doc_dict.get("ts")
            current_date_str = datetime.fromtimestamp(timestamp, tz).strftime("%B %d, %Y (%Z)")
            if not first_log_processed or current_date_str != last_date_str:
                padding_size = (120 - 2 - len(current_date_str)) // 2
                msg = f"{'-' * padding_size} {current_date_str} {'-' * padding_size}"
                current_loop.call_soon_threadsafe(queue.put_nowait, {"message": msg})
                last_date_str = current_date_str
                first_log_processed = True

            msg_clean = ""
            timestamp_str = ts_to_str(timestamp)
            msg_raw = log_doc_dict.get("msg").rstrip()
            line_len = 120 - len(timestamp_str)
            wrapper = textwrap.TextWrapper(line_len, break_long_words=True, break_on_hyphens=True)
            formatted_lines = []
            for original_line in msg_raw.splitlines():
                wrapped_segments = wrapper.wrap(original_line)
                for segment in wrapped_segments:
                    if not formatted_lines:
                        formatted_lines.append(f"{timestamp_str} {segment}")
                    else:
                        formatted_lines.append(f" {' ' * len(timestamp_str)}{segment}")
            msg_clean = "\n".join(formatted_lines)

            current_loop.call_soon_threadsafe(queue.put_nowait, {"message": msg_clean})

    logs_ref = DB.collection("nodes").document(node_id).collection("logs")
    watch = logs_ref.on_snapshot(on_snapshot)

    async def log_generator():
        try:
            # set 5s reconnect on drop and send initial comment
            yield "retry: 5000\n\n"
            yield ": init\n\n"
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=2)
                    yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            watch.unsubscribe()

    return StreamingResponse(
        log_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache, no-transform"},
    )
