from time import time
import requests
from typing import Optional
import concurrent.futures

import docker
from fastapi import APIRouter, Depends, Response
from google.cloud import firestore

from node_service import (
    PROJECT_ID,
    SELF,
    REINIT_SELF,
    INSTANCE_N_CPUS,
    INSTANCE_NAME,
    IN_LOCAL_DEV_MODE,
    BURLA_BACKEND_URL,
    CLUSTER_ID_TOKEN,
    get_logger,
    Container,
)
from node_service.helpers import Logger
from node_service.worker import Worker

router = APIRouter()


@router.post("/reboot")
def reboot_containers_endpoint(
    new_container_config: Optional[list[Container]] = None, logger: Logger = Depends(get_logger)
):
    if SELF["BOOTING"]:
        return Response("Node already BOOTING, unable to satisfy request.", status_code=409)
    return reboot_containers(new_container_config, logger)


def reboot_containers(
    new_container_config: Optional[list[Container]] = None,
    logger: Logger = Depends(get_logger),
):
    """
    Rebooting will reboot the containers that are currently/ were previously running.
    If new containers are passed with the reboot request, those containers will be booted instead.
    """
    logger.log(f"Rebooting Node: {INSTANCE_NAME}")
    db = firestore.Client(project=PROJECT_ID, database="burla")
    node_doc = db.collection("nodes").document(INSTANCE_NAME)
    node_doc.update(
        {
            "status": "BOOTING",
            "current_job": None,
            "parallelism": None,
            "target_parallelism": None,
            "started_booting_at": time(),
            "all_inputs_received": False,
        }
    )

    try:
        # reset state of the node service, except current_container_config, and the job_watcher.
        current_container_config = SELF["current_container_config"]
        REINIT_SELF(SELF)
        SELF["current_container_config"] = current_container_config
        if new_container_config:
            SELF["current_container_config"] = new_container_config

        # get list of authorized users/tokens from backend service
        headers = {"Authorization": f"Bearer {CLUSTER_ID_TOKEN}"}
        url = f"{BURLA_BACKEND_URL}/v1/projects/{PROJECT_ID}/users"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        SELF["authorized_users"] = response.json()["authorized_users"]

        futures = []
        executor = concurrent.futures.ThreadPoolExecutor()
        docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
        if IN_LOCAL_DEV_MODE:
            # Remove all "old" worker containers.
            # Mark all existing workers as "old".
            all_containers = docker_client.containers(all=True)
            worker_containers = [c for c in all_containers if "worker" in c["Names"][0]]
            for container in worker_containers:
                is_old = container["Names"][0][1:].startswith("OLD")
                belongs_to_current_node = f"node_{INSTANCE_NAME[11:]}" in container["Names"][0]

                if is_old and belongs_to_current_node:
                    kwargs = dict(container=container["Id"], force=True)
                    futures.append(executor.submit(docker_client.remove_container, **kwargs))

                elif belongs_to_current_node:
                    args = (container["Id"], f"OLD--{container['Names'][0][1:]}")
                    futures.append(executor.submit(docker_client.rename, *args))
                    kwargs = dict(container=container["Id"], timeout=0)
                    futures.append(executor.submit(docker_client.stop, **kwargs))
        else:
            # remove all worker containers
            for container in docker_client.containers():
                if "worker" in container["Names"][0]:
                    kwargs = dict(container=container["Id"], force=True)
                    futures.append(executor.submit(docker_client.remove_container, **kwargs))

        # Wait until all workers have been removed/marked old before starting new ones.
        try:
            [future.result() for future in futures]
        except docker.errors.APIError as e:
            if "already in progress" not in str(e):
                raise e

        # start new workers.
        futures = []
        for spec in SELF["current_container_config"]:
            for i in range(INSTANCE_N_CPUS):
                # have just one worker send logs to gcl, too many will break gcl
                send_logs_to_gcl = False  # (i == 0) and (not IN_LOCAL_DEV_MODE)
                args = (spec.python_version, spec.image, docker_client)
                futures.append(executor.submit(Worker, *args, send_logs_to_gcl=send_logs_to_gcl))

        executor.shutdown(wait=True)
        SELF["workers"] = [future.result() for future in futures]
        SELF["BOOTING"] = False
        node_doc.update({"status": "READY"})

    except Exception as parent_exception:
        SELF["FAILED"] = True
        try:
            node_doc.delete()
        except Exception as e:
            raise e from parent_exception
        raise parent_exception

    logger.log(f"Done booting {len(SELF['workers'])} workers, {INSTANCE_NAME} is READY!")
