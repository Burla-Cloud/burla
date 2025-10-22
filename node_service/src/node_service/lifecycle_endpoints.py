from time import time, sleep
import requests
from typing import Optional, Callable
import concurrent.futures
import traceback
import threading
import subprocess

import aiohttp
import docker
from docker.errors import APIError
from pydantic import BaseModel
from fastapi import APIRouter, Depends, Response
from google.cloud import firestore
from google.cloud.compute_v1 import InstancesClient
from google.auth.transport.requests import Request
from google.cloud.firestore import AsyncClient

from node_service import (
    PROJECT_ID,
    SELF,
    REINIT_SELF,
    CREDENTIALS,
    INSTANCE_N_CPUS,
    INSTANCE_NAME,
    IN_LOCAL_DEV_MODE,
    BURLA_BACKEND_URL,
    CLUSTER_ID_TOKEN,
    NUM_GPUS,
    ENV_IS_READY_PATH,
    get_logger,
    get_add_background_task_function,
    __version__,
)
from node_service.helpers import Logger
from node_service.worker import Worker

router = APIRouter()


class Container(BaseModel):
    image: str
    python_version: str


@router.post("/shutdown")
async def shutdown_node(logger: Logger = Depends(get_logger)):
    """
    We dont need to delete the node here because the only way to call this is to run the shutdown
    script (by deleting the node)
    """
    SELF["SHUTTING_DOWN"] = True
    SELF["job_watcher_stop_event"].set()

    try:
        url = "http://metadata.google.internal/computeMetadata/v1/instance/preempted"
        async with aiohttp.ClientSession(headers={"Metadata-Flavor": "Google"}) as session:
            async with session.get(url, timeout=1) as response:
                response.raise_for_status()
                preempted = (await response.text()).strip() == "TRUE"
    except Exception as e:
        logger.log(f"Error checking if node {INSTANCE_NAME} was preempted: {e}", severity="WARNING")
        preempted = False

    if preempted:
        logger.log(f"Node {INSTANCE_NAME} was preempted!")
    else:
        logger.log(f"Received shutdown request for node {INSTANCE_NAME}.")

    async_db = AsyncClient(project=PROJECT_ID, database="burla")
    doc_ref = async_db.collection("nodes").document(INSTANCE_NAME)
    snapshot = await doc_ref.get()
    if snapshot.exists:
        node_dict = snapshot.to_dict()
        if node_dict.get("status") != "FAILED" and node_dict.get("idle_for_too_long"):
            await doc_ref.update({"status": "DELETED", "display_in_dashboard": True})
        elif node_dict.get("status") != "FAILED":
            await doc_ref.update({"status": "DELETED", "display_in_dashboard": False})


@router.post("/reboot")
def reboot_containers_endpoint(
    new_container_config: Optional[list[Container]] = None,
    logger: Logger = Depends(get_logger),
    add_background_task: Callable = Depends(get_add_background_task_function),
):
    if SELF["BOOTING"]:
        return Response("Node already BOOTING, unable to satisfy request.", status_code=409)
    return reboot_containers(new_container_config, logger, add_background_task)


def _call_docker_threadsafe(method, *args, **kwargs):
    client = docker.APIClient(base_url="unix://var/run/docker.sock")
    try:
        getattr(client, method)(*args, **kwargs)
    finally:
        client.close()


def _LOCAL_DEV_ONLY_pull_image_if_missing(
    image: str, logger: Logger, docker_client: docker.APIClient
):
    """
    Cannot pull using cli in local dev mode because this is already running in a docker container
    and im too lazy to setup docker-in-docker that works with the CLI.
    It dosent use this in prod because it's unreliable, `docker_client.pull` often fails silently.
    """
    try:
        docker_client.inspect_image(image)
    except docker.errors.ImageNotFound:
        logger.log(f"Pulling image {image} ...")
        try:
            docker_client.pull(image)
        except APIError as e:
            if "Unauthenticated request" in str(e):
                print("Image is not public, trying again with credentials ...")
                CREDENTIALS.refresh(Request())
                auth_config = {"username": "oauth2accesstoken", "password": CREDENTIALS.token}
                docker_client.pull(image, auth_config=auth_config)
            else:
                raise
        # ODDLY, if docker_client.pull fails to pull the image, it will NOT throw any error >:(
        # check here that the image was actually pulled and exists on disk,
        try:
            docker_client.inspect_image(image)
        except docker.errors.ImageNotFound:
            msg = f"Image {image} not found after pulling!\nDid vm run out of disk space?"
            raise Exception(msg)


def _pull_image_if_missing(image: str, logger: Logger):
    # Use CLI instead of python api because that api just generally horrible and broken.
    # I already tried using it correctly, it wasnt worth it.

    def _run_command(command, raise_error=True):
        result = subprocess.run(command, shell=True, capture_output=True)
        if result.returncode != 0 and raise_error:
            print("")
            raise Exception(command, result.stderr)
        else:
            return result

    attempt = 0
    while True:
        attempt += 1
        logger.log(f"Pulling image {image} ...")
        result = _run_command(f"docker pull {image}", raise_error=False)
        text_output = result.stderr.decode() + result.stdout.decode()
        no_transient_error = not (result.returncode != 0 and "unexpected EOF" in text_output)

        if no_transient_error or attempt > 5:
            break
        else:
            logger.log(f"`Unexpected EOF` error detected, retrying... (attempt {attempt})")
            sleep(3)

    docker_pull_failed = result.returncode != 0
    docker_pull_stderr = result.stderr.decode()
    not_hosted_in_google_artifact_registry = "docker.pkg.dev" not in image

    if docker_pull_failed and not_hosted_in_google_artifact_registry:
        raise Exception(f"CMD `docker pull {image}` failed with error:\n{docker_pull_stderr}\n")

    # if failed and image is in GAR, try again using service account credentials
    if docker_pull_failed:
        svc_email = getattr(CREDENTIALS, "service_account_email", "<no svc account email found>")
        msg = f"Failed to pull image: {image}\n"
        msg += "Trying again using the service account credentials attached to this VM:\n"
        logger.log(f"{msg}\n{svc_email}")

        if image.startswith("https://"):
            host = f'https://{image.split("/")[2]}'
        else:
            host = f'https://{image.split("/")[0]}'

        CREDENTIALS.refresh(Request())
        login_cmd = f"docker login {host} -u oauth2accesstoken --password {CREDENTIALS.token}"
        result = _run_command(login_cmd, raise_error=False)
        if result.returncode != 0:
            msg = f"CMD `docker pull {image}` failed with error:\n{docker_pull_stderr}\n"
            msg += f"Following attempt to login to {host} using service account: "
            msg += f"`{svc_email}` also failed with error:\n{result.stderr}\n"
            raise Exception(msg)

        _run_command(f"docker pull {image}")

    # sanity check, not positive this is necessary with cli, but was with python api.
    result = _run_command(f"docker inspect {image}", raise_error=False)
    if result.returncode != 0:
        msg = f"CMD: `docker pull {image}` succeeded, but subsequent `docker inspect ...` failed!\n"
        msg += f"`docker inspect` stderr:\n{result.stderr}\n"
        raise Exception(msg)
    logger.log(f"Image {image} pulled successfully.\nWaiting for containers to start ...")


# Removing large GPU containers can take several minutes. The node should not block on the full
# deletion – it only needs the process to be gone. A quick `kill` is enough for that. We then
# queue the slower `remove_container` call as a FastAPI background task when available. When the
# reboot function is executed outside of a request context (e.g. during lifespan startup), it
# falls back to running the removal in a daemon thread so behaviour remains unchanged.
def _remove_container_task(container_id: str, logger: Logger):
    client = docker.APIClient(base_url="unix://var/run/docker.sock", timeout=600)
    try:
        client.remove_container(container=container_id, force=True)
    except Exception as e:
        logger.log(f"Failed to remove container {container_id}: {e}", severity="WARNING")
    finally:
        client.close()


def _schedule_container_removal(
    container_id: str, logger: Logger, add_background_task: Optional[Callable] = None
):
    if add_background_task is not None:
        add_background_task(_remove_container_task, container_id, logger)
    else:
        threading.Thread(
            target=_remove_container_task, args=(container_id, logger), daemon=True
        ).start()


def reboot_containers(
    new_container_config: Optional[list[Container]] = None,
    logger: Logger = Depends(get_logger),
    add_background_task: Optional[Callable] = None,
):
    """
    Rebooting will reboot the containers that are currently/ were previously running.
    If new containers are passed with the reboot request, those containers will be booted instead.
    """
    # immediately stop watcher thread, this IS set in REINIT_SELF below
    # but watcher breaks sometimes if it's not set right away.
    SELF["job_watcher_stop_event"].set()

    # important to delete or workers wont install packages
    ENV_IS_READY_PATH.unlink(missing_ok=True)

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
    msg = f"Booting {INSTANCE_N_CPUS if NUM_GPUS == 0 else NUM_GPUS} workers ..."
    node_doc.collection("logs").document().set({"msg": msg, "ts": time()})

    try:
        # reset state of the node service, except current_container_config, and the job_watcher.
        current_container_config = SELF["current_container_config"]
        REINIT_SELF(SELF)
        SELF["current_container_config"] = current_container_config
        if new_container_config:
            SELF["current_container_config"] = new_container_config

        # get list of authorized users/tokens from backend service
        headers = {"Authorization": f"Bearer {CLUSTER_ID_TOKEN}"}
        url = f"{BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}/users"
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
                    try:
                        docker_client.kill(container["Id"])
                    except Exception:
                        pass  # container might already be stopped
                    _schedule_container_removal(container["Id"], logger, add_background_task)

                elif belongs_to_current_node:
                    args = (container["Id"], f"OLD--{container['Names'][0][1:]}")
                    futures.append(executor.submit(_call_docker_threadsafe, "rename", *args))
                    kwargs = dict(container=container["Id"], timeout=0)
                    futures.append(executor.submit(_call_docker_threadsafe, "stop", **kwargs))
        else:
            # remove all worker containers
            for container in docker_client.containers():
                if "worker" in container["Names"][0]:
                    try:
                        docker_client.kill(container["Id"])
                    except Exception:
                        pass
                    _schedule_container_removal(container["Id"], logger, add_background_task)

        # Wait until all workers have been removed/marked old before starting new ones.
        try:
            [future.result() for future in futures]
        except docker.errors.APIError as e:
            if "already in progress" not in str(e):
                raise e

        # start new workers.
        futures = []
        for spec in SELF["current_container_config"]:
            if IN_LOCAL_DEV_MODE:
                _LOCAL_DEV_ONLY_pull_image_if_missing(spec.image, logger, docker_client)
            else:
                _pull_image_if_missing(spec.image, logger)
            num_workers = INSTANCE_N_CPUS if NUM_GPUS == 0 else NUM_GPUS
            for i in range(num_workers):
                # have just one worker install the worker svc, then share through docker volume
                # (too many will ddoss github / be slow)
                install_worker = i == 0
                args = (spec.python_version, spec.image)
                futures.append(executor.submit(Worker, *args, elected_installer=install_worker))

        docker_client.close()
        executor.shutdown(wait=True)
        SELF["workers"] = [future.result() for future in futures]
        SELF["BOOTING"] = False
        node_doc.update({"status": "READY"})

    except Exception as parent_exception:
        SELF["FAILED"] = True
        try:
            node_doc.update({"status": "FAILED"})
            msg = f"Error from Node-Service: {traceback.format_exc()}"
            node_doc.collection("logs").document().set({"msg": msg, "ts": time()})

            if not IN_LOCAL_DEV_MODE:
                instance_client = InstancesClient()
                silly = instance_client.aggregated_list(project=PROJECT_ID)
                vms_per_zone = [getattr(vms_in_zone, "instances", []) for _, vms_in_zone in silly]
                vms = [vm for vms_in_zone in vms_per_zone for vm in vms_in_zone]
                vm = next((vm for vm in vms if vm.name == INSTANCE_NAME), None)
                if vm:
                    zone = vm.zone.split("/")[-1]
                    instance_client.delete(project=PROJECT_ID, zone=zone, instance=INSTANCE_NAME)
        except Exception as e:
            raise e from parent_exception
        raise parent_exception

    logger.log(f"Done booting {len(SELF['workers'])} workers, {INSTANCE_NAME} is READY!")
