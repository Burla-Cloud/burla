import json
import asyncio
import docker
from time import time
from typing import Callable

import slack_sdk
from fastapi import APIRouter, Depends
from google.cloud.firestore_v1 import FieldFilter
from google.cloud.compute_v1 import InstancesClient
from starlette.responses import StreamingResponse
from concurrent.futures import ThreadPoolExecutor

from main_service import (
    DB,
    IN_PROD,
    IN_LOCAL_DEV_MODE,
    LOCAL_DEV_CONFIG,
    get_logger,
    get_add_background_task_function,
)
from main_service.cluster import reconcile
from main_service.node import Container, Node
from main_service.helpers import Logger, get_secret

router = APIRouter()


def _cluster_shutdown_futures(
    executor: ThreadPoolExecutor, instance_client: InstancesClient, logger: Logger
):
    futures = []

    # remove all containers that are NOT the MAIN_SERVICE
    if IN_LOCAL_DEV_MODE:
        docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
        for container in docker_client.containers():
            is_not_main_service = not container["Names"][0].startswith("/main_service")
            if is_not_main_service:
                futures.append(
                    executor.submit(docker_client.remove_container, container["Id"], force=True)
                )

    # delete all nodes
    node_filter = FieldFilter("status", "in", ["READY", "BOOTING", "RUNNING"])
    for node_snapshot in DB.collection("nodes").where(filter=node_filter).stream():
        node = Node.from_snapshot(DB, logger, node_snapshot, instance_client)
        futures.append(executor.submit(node.delete))

    return futures


def _cluster_startup_futures(executor: ThreadPoolExecutor, logger: Logger):
    futures = []

    # use separate cluster config if IN_LOCAL_DEV_MODE:
    config = DB.collection("cluster_config").document("cluster_config").get().to_dict()
    config = LOCAL_DEV_CONFIG if IN_LOCAL_DEV_MODE else config
    node_service_port = 8080  # <- must default to 8080 because only 8080 is open in GCP firewall

    # schedule adding nodes
    for node_spec in config["Nodes"]:
        for _ in range(node_spec["quantity"]):

            if IN_LOCAL_DEV_MODE:  # avoid trying to open same port on multiple local containers
                node_service_port += 1

            machine_type = node_spec["machine_type"]
            containers = [Container.from_dict(c) for c in node_spec["containers"]]
            inactivity_time = node_spec.get("inactivity_shutdown_time_sec")
            node_args = dict(
                db=DB,
                logger=logger,
                machine_type=machine_type,
                containers=containers,
                service_port=node_service_port,
                as_local_container=IN_LOCAL_DEV_MODE,  # <- start in a container if IN_LOCAL_DEV_MODE
                inactivity_shutdown_time_sec=inactivity_time,
                verbose=True,
            )
            future = executor.submit(Node.start, **node_args)
            futures.append(future)

    return futures


@router.post("/v1/cluster/restart")
def restart_cluster(
    add_background_task: Callable = Depends(get_add_background_task_function),
    logger: Logger = Depends(get_logger),
):
    start = time()
    instance_client = InstancesClient()
    futures = []
    executor = ThreadPoolExecutor(max_workers=32)

    if IN_PROD:
        client = slack_sdk.WebClient(token=get_secret("slackbot-token"))
        msg = "Someone started/restarted the prod cluster."
        client.chat_postMessage(channel="user-activity", text=msg)

    # schedule shotdown of all CURRENT nodes and containers
    shutdown_futures = _cluster_shutdown_futures(executor, instance_client, logger)
    futures.extend(shutdown_futures)

    # check if stop requested, if so don't schedule any new nodes
    cluster_status = DB.collection("cluster_status").document("current").get().to_dict()
    cluster_stop_requested = cluster_status and cluster_status.get("stop_requested", False)

    if cluster_stop_requested:
        # schuedule startup of any NEW nodes / containers
        startup_futures = _cluster_startup_futures(executor, logger)
        futures.extend(startup_futures)

    # wait until all scheduled operations complete:
    [future.result() for future in futures]
    executor.shutdown(wait=True)

    # check again if stop was requested
    cluster_status = DB.collection("cluster_status").document("current").get().to_dict()
    cluster_stop_requested = cluster_status and cluster_status.get("stop_requested", False)

    # if so, schedule shutdown again to ensure any nodes that still managed to start are shutdown.
    if cluster_stop_requested:
        logger.log("Stop requested during reboot: aborting startup and shutting down!")
        add_background_task(shutdown_cluster, logger)
        # reset shutdown requested indicator:
        DB.collection("cluster_status").document("current").set({"stop_requested": False})
    else:
        logger.log("Done restarting, reconciling ...")
        add_background_task(reconcile, DB, logger, add_background_task)

    duration = time() - start
    logger.log(f"Exiting restart after {duration//60}m {duration%60}s")


@router.post("/v1/cluster/shutdown")
async def shutdown_cluster(logger: Logger = Depends(get_logger)):
    start = time()
    instance_client = InstancesClient()
    DB.collection("cluster_status").document("current").set({"stop_requested": True})

    if IN_PROD:
        client = slack_sdk.WebClient(token=get_secret("slackbot-token"))
        client.chat_postMessage(channel="user-activity", text="Someone shut the prod cluster off.")

    futures = []
    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = _cluster_shutdown_futures(executor, instance_client, logger)
        [future.result() for future in futures]

    # reset to False after shutdown finished.
    DB.collection("cluster_status").document("current").set({"stop_requested": False})

    duration = time() - start
    logger.log(f"Shut down after {duration//60}m {duration%60}s")


@router.get("/v1/cluster")
async def cluster_info():
    queue = asyncio.Queue()
    current_loop = asyncio.get_running_loop()

    # Callback to handle Firestore changes
    def on_snapshot(query_snapshot, changes, read_time):
        for change in changes:
            doc = change.document
            doc_data = doc.to_dict() or {}
            instance_name = doc_data.get("instance_name")
            if change.type.name == "REMOVED":
                event_data = {"nodeId": instance_name, "deleted": True}
            else:  # For ADDED or MODIFIED events
                event_data = {"nodeId": instance_name, "status": doc_data.get("status")}
            # Use call_soon_threadsafe because this callback runs in a separate thread.
            current_loop.call_soon_threadsafe(queue.put_nowait, event_data)
            print(f"Firestore on_snapshot event: {event_data}")

    # Define your query with the same filter from before.
    status_filter = FieldFilter("status", "not-in", ["DELETED", "FAILED"])
    query = DB.collection("nodes").where(filter=status_filter)
    # Subscribe to changes. The returned unsubscribe function can be used to close the listener.
    unsubscribe = query.on_snapshot(on_snapshot)

    try:
        while True:
            # Wait for events from the Firestore snapshot listener.
            event = await queue.get()
            yield f"data: {json.dumps(event)}\n\n"
    finally:
        unsubscribe()  # Unsubscribe when the client disconnects.
