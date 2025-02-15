import json
import asyncio
import docker
from time import time
from typing import Callable

import slack_sdk
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from google.cloud.firestore_v1 import FieldFilter
from google.cloud.compute_v1 import InstancesClient
from starlette.responses import StreamingResponse
from concurrent.futures import ThreadPoolExecutor, wait

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

    config = DB.collection("cluster_config").document("cluster_config").get().to_dict()
    config = LOCAL_DEV_CONFIG if IN_LOCAL_DEV_MODE else config
    node_service_port = 8080  # <- must default to 8080 because only 8080 is open in GCP firewall

    for node_spec in config["Nodes"]:
        for _ in range(node_spec["quantity"]):

            if IN_LOCAL_DEV_MODE:  # ports all need to be different if running locally
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
async def restart_cluster(
    add_background_task: Callable = Depends(get_add_background_task_function),
    logger: Logger = Depends(get_logger),
):
    start = time()
    instance_client = InstancesClient()

    if IN_PROD:
        client = slack_sdk.WebClient(token=get_secret("slackbot-token"))
        client.chat_postMessage(channel="user-activity", text="Prod cluster started/restarted.")

    with ThreadPoolExecutor(max_workers=32) as executor:
        shutdown_futures = _cluster_shutdown_futures(executor, instance_client, logger)
        startup_futures = _cluster_startup_futures(executor, logger)
        [future.result() for future in shutdown_futures + startup_futures]

    duration = time() - start
    logger.log(f"Done restarting after {duration//60}m {duration%60}s")
    add_background_task(reconcile, DB, logger, add_background_task)


@router.post("/v1/cluster/shutdown")
async def shutdown_cluster(logger: Logger = Depends(get_logger)):
    start = time()
    instance_client = InstancesClient()

    if IN_PROD:
        client = slack_sdk.WebClient(token=get_secret("slackbot-token"))
        client.chat_postMessage(channel="user-activity", text="Someone shut the prod cluster off.")

    futures = []
    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = _cluster_shutdown_futures(executor, instance_client, logger)
        [future.result() for future in futures]

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
                    event_data = {"nodeId": instance_name, "status": doc_data.get("status")}

                current_loop.call_soon_threadsafe(queue.put_nowait, event_data)
                logger.log(f"Firestore event detected: {event_data}")

        status_filter = FieldFilter("status", "not-in", ["DELETED", "FAILED"])
        query = DB.collection("nodes").where(filter=status_filter)
        unsubscribe = query.on_snapshot(on_snapshot)

        try:
            while True:
                event = await queue.get()
                yield f"data: {json.dumps(event)}\n\n"
        finally:
            unsubscribe()

    return StreamingResponse(node_stream(), media_type="text/event-stream")
