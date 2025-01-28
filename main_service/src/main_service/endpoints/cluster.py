import json
import asyncio
import docker
from time import time
from typing import Callable

import slack_sdk
from fastapi import APIRouter, Depends, HTTPException
from google.cloud.firestore_v1 import FieldFilter
from google.cloud.compute_v1 import InstancesClient, ZonesClient, ListInstancesRequest
from starlette.responses import StreamingResponse
from concurrent.futures import ThreadPoolExecutor, as_completed

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


@router.post("/v1/cluster/restart")
def restart_cluster(
    add_background_task: Callable = Depends(get_add_background_task_function),
    logger: Logger = Depends(get_logger),
):
    start = time()
    instance_client = InstancesClient()

    if IN_PROD:
        client = slack_sdk.WebClient(token=get_secret("slackbot-token"))
        client.chat_postMessage(channel="user-activity", text="Someone started the prod cluster.")

    futures = []
    executor = ThreadPoolExecutor(max_workers=32)

    # delete all nodes
    node_filter = FieldFilter("status", "in", ["READY", "BOOTING", "RUNNING"])
    for node_snapshot in DB.collection("nodes").where(filter=node_filter).stream():
        node = Node.from_snapshot(DB, logger, node_snapshot, instance_client)
        futures.append(executor.submit(node.delete))

    # add nodes according to cluster_config doc
    def _add_node_logged(machine_type, containers, node_service_port, inactivity_time):
        node = Node.start(
            db=DB,
            logger=logger,
            machine_type=machine_type,
            containers=containers,
            service_port=node_service_port,
            as_local_container=IN_LOCAL_DEV_MODE,  # <- start in a container if IN_LOCAL_DEV_MODE
            inactivity_shutdown_time_sec=inactivity_time,
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
    config = DB.collection("cluster_config").document("cluster_config").get().to_dict()
    config = LOCAL_DEV_CONFIG if IN_LOCAL_DEV_MODE else config
    node_service_port = 8080  # <- must default to 8080 because only 8080 is open in GCP firewall

    for node_spec in config["Nodes"]:
        for _ in range(node_spec["quantity"]):

            if IN_LOCAL_DEV_MODE:  # avoid trying to open same port on multiple local containers
                node_service_port += 1
            machine_type = node_spec["machine_type"]
            containers = [Container.from_dict(c) for c in node_spec["containers"]]
            inactivity_time = node_spec.get("inactivity_shutdown_time_sec")

            node_args = (machine_type, containers, node_service_port, inactivity_time)
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

    logger.log("Done restarting, reconciling ...")
    add_background_task(reconcile, DB, logger, add_background_task)

    duration = time() - start
    logger.log(f"Restarted after {duration//60}m {duration%60}s")


@router.post("/v1/cluster/shutdown")
async def shutdown_cluster(logger: Logger = Depends(get_logger)):

    start = time()
    instance_client = InstancesClient()

    if IN_PROD:
        client = slack_sdk.WebClient(token=get_secret("slackbot-token"))
        client.chat_postMessage(channel="user-activity", text="Someone shut the prod cluster off.")

    futures = []
    executor = ThreadPoolExecutor(max_workers=32)

    # delete all nodes
    node_filter = FieldFilter("status", "in", ["READY", "BOOTING", "RUNNING"])
    for node_snapshot in DB.collection("nodes").where(filter=node_filter).stream():
        node = Node.from_snapshot(DB, logger, node_snapshot, instance_client)
        futures.append(executor.submit(node.delete))

    # remove any existing node/worker service containers if in IN_LOCAL_DEV_MODE
    if IN_LOCAL_DEV_MODE:
        docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
        for container in docker_client.containers():
            is_node_service_container = container["Names"][0].startswith("/node")
            is_worker_service_container = "worker" in container["Names"][0]
            if is_node_service_container or is_worker_service_container:
                docker_client.remove_container(container["Id"], force=True)

    # wait until all operations done
    [future.result() for future in futures]
    executor.shutdown(wait=True)

    duration = time() - start
    logger.log(f"Shut down after {duration//60}m {duration%60}s")


@router.get("/v1/cluster")
async def cluster_info():
    node_name_to_status = {}

    async def node_stream():
        while True:
            status_filter = FieldFilter("status", "not-in", ["DELETED", "FAILED"])
            node_docs = list(DB.collection("nodes").where(filter=status_filter).stream())
            nodes = [doc.to_dict() for doc in node_docs]
            node_names = [node["instance_name"] for node in nodes]
            names_of_deleted_nodes = set(node_name_to_status.keys()) - set(node_names)

            # brodcast deleted nodes:
            for node_name in names_of_deleted_nodes:
                event_data = dict(nodeId=node_name, deleted=True)
                yield f"data: {json.dumps(event_data)}\n\n"
                del node_name_to_status[node_name]
                print(f"deleted node: {event_data}")

            # brodcast status updates:
            for node in nodes:
                instance_name = node["instance_name"]
                current_status = node["status"]
                previous_status = node_name_to_status.get(instance_name)

                if current_status != previous_status:
                    node_name_to_status[instance_name] = current_status
                    event_data = dict(nodeId=instance_name, status=current_status)
                    yield f"data: {json.dumps(event_data)}\n\n"
                    print(f"updated status: {event_data}")

            await asyncio.sleep(1)

    return StreamingResponse(node_stream(), media_type="text/event-stream")
