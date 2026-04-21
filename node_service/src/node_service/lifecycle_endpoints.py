from time import time
import requests
import asyncio
from typing import Optional, Callable
import traceback

import aiodocker
from fastapi import APIRouter, Depends, Response
from google.cloud import firestore
from google.cloud.compute_v1 import InstancesClient
from google.auth.transport.requests import Request
from google.cloud.firestore import AsyncClient

from node_service import (
    ASYNC_DB,
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
    GCL_CLIENT,
    get_logger,
    get_add_background_task_function,
    __version__,
)
from node_service.helpers import Logger
from node_service.worker_client import WorkerClient

router = APIRouter()


@router.post("/shutdown")
async def shutdown_node(logger: Logger = Depends(get_logger)):
    """
    We dont need to delete the node here because the only way to call this is to run the shutdown
    script (by deleting the node)
    """
    SELF["job_watcher_stop_event"].set()
    SELF["current_parallelism"] = 0
    await logger.log(f"Received shutdown request for node {INSTANCE_NAME}.")

    async_db = AsyncClient(project=PROJECT_ID, database="burla")
    doc_ref = async_db.collection("nodes").document(INSTANCE_NAME)
    snapshot = await doc_ref.get()
    if snapshot.exists and snapshot.to_dict().get("status") != "FAILED":
        await doc_ref.update({"status": "DELETED", "ended_at": time()})


@router.post("/reboot")
async def reboot_containers_endpoint(
    new_container_config: Optional[list[str]] = None,
    logger: Logger = Depends(get_logger),
    add_background_task: Callable = Depends(get_add_background_task_function),
):
    if SELF["BOOTING"]:
        return Response("Node already BOOTING, unable to satisfy request.", status_code=409)
    return await reboot_containers(new_container_config, logger, add_background_task)


def image_size_GB(image: str):
    name, tag = image.rsplit(":", 1) if ":" in image else (image, "latest")
    name = name if "/" in name else f"library/{name}"
    params = {"service": "registry.docker.io", "scope": f"repository:{name}:pull"}
    token = requests.get("https://auth.docker.io/token", params=params).json()["token"]
    auth = {"Authorization": f"Bearer {token}"}
    headers = {**auth, "Accept": "application/vnd.docker.distribution.manifest.list.v2+json"}
    url = f"https://registry-1.docker.io/v2/{name}/manifests/{tag}"
    manifest = requests.get(url, headers=headers).json()
    if "manifests" in manifest:
        is_linux = lambda m: m["platform"]["os"] == "linux"
        is_amd64 = lambda m: m["platform"]["architecture"] == "amd64"
        m = next(m for m in manifest["manifests"] if is_linux(m) and is_amd64(m))
        headers = {**auth, "Accept": "application/vnd.docker.distribution.manifest.v2+json"}
        url = f"https://registry-1.docker.io/v2/{name}/manifests/{m['digest']}"
        manifest = requests.get(url, headers=headers).json()
    size = sum(l["size"] for l in manifest["layers"])
    return round(size / 1_000_000_000, 2)


async def _LOCAL_DEV_ONLY_pull_image_if_missing(
    image: str, logger: Logger, docker: aiodocker.Docker
):
    """
    Cannot pull using cli in local dev mode because this is already running in a docker container
    and im too lazy to setup docker-in-docker that works with the CLI.
    It dosent use this in prod because it's unreliable, `docker_client.pull` often fails silently.
    """
    try:
        await docker.images.inspect(image)
    except aiodocker.DockerError as e:
        if e.status != 404:
            raise

        try:
            await logger.log(f"Pulling image {image} ({image_size_GB(image)} GB) ...")
        except Exception:
            await logger.log(f"Pulling image {image} ...")

        try:
            await docker.images.pull(image)
        except aiodocker.DockerError as e:
            if "Unauthenticated request" in str(e):
                print("Image is not public, trying again with credentials ...")
                CREDENTIALS.refresh(Request())
                auth_config = {"username": "oauth2accesstoken", "password": CREDENTIALS.token}
                await docker.images.pull(image, auth=auth_config)
            else:
                raise
        # ODDLY, if docker_client.pull fails to pull the image, it will NOT throw any error >:(
        # check here that the image was actually pulled and exists on disk,
        try:
            await docker.images.inspect(image)
        except aiodocker.DockerError as e:
            if e.status == 404:
                raise Exception(
                    f"Image {image} not found after pulling!\nDid vm run out of disk space?"
                )
            raise


async def _pull_image_if_missing(image: str, logger: Logger, docker: aiodocker.Docker):
    # Use CLI instead of python api because that api just generally horrible and broken.
    # I already tried using it correctly, it wasnt worth it.

    if IN_LOCAL_DEV_MODE:
        return await _LOCAL_DEV_ONLY_pull_image_if_missing(image, logger, docker)

    async def _run_command(command, raise_error=True):
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0 and raise_error:
            print("")
            raise Exception(command, stderr)
        return process.returncode, stdout, stderr

    attempt = 0
    while True:
        attempt += 1

        try:
            await logger.log(f"Pulling image {image} ({image_size_GB(image)} GB) ...")
        except Exception:
            await logger.log(f"Pulling image {image} ...")

        returncode, stdout, stderr = await _run_command(f"docker pull {image}", raise_error=False)
        text_output = stderr.decode() + stdout.decode()
        no_transient_error = not (returncode != 0 and "unexpected EOF" in text_output)

        if no_transient_error or attempt > 5:
            break
        else:
            await logger.log(f"`Unexpected EOF` error detected, retrying... (attempt {attempt})")
            await asyncio.sleep(3)

    docker_pull_failed = returncode != 0
    docker_pull_stderr = stderr.decode()
    not_hosted_in_google_artifact_registry = "docker.pkg.dev" not in image

    if docker_pull_failed and not_hosted_in_google_artifact_registry:
        raise Exception(f"CMD `docker pull {image}` failed with error:\n{docker_pull_stderr}\n")

    # if failed and image is in GAR, try again using service account credentials
    if docker_pull_failed:
        svc_email = getattr(CREDENTIALS, "service_account_email", "<no svc account email found>")
        msg = f"Failed to pull image: {image}\n"
        msg += "Trying again using the service account credentials attached to this VM:\n"
        await logger.log(f"{msg}\n{svc_email}")

        if image.startswith("https://"):
            host = f'https://{image.split("/")[2]}'
        else:
            host = f'https://{image.split("/")[0]}'

        CREDENTIALS.refresh(Request())
        login_cmd = f"docker login {host} -u oauth2accesstoken --password {CREDENTIALS.token}"
        returncode, stdout, stderr = await _run_command(login_cmd, raise_error=False)
        if returncode != 0:
            msg = f"CMD `docker pull {image}` failed with error:\n{docker_pull_stderr}\n"
            msg += f"Following attempt to login to {host} using service account: "
            msg += f"`{svc_email}` also failed with error:\n{stderr}\n"
            raise Exception(msg)

        await _run_command(f"docker pull {image}")

    # sanity check, not positive this is necessary with cli, but was with python api.
    returncode, stdout, stderr = await _run_command(f"docker inspect {image}", raise_error=False)
    if returncode != 0:
        msg = f"CMD: `docker pull {image}` succeeded, but subsequent `docker inspect ...` failed!\n"
        msg += f"`docker inspect` stderr:\n{stderr}\n"
        raise Exception(msg)


# Removing large GPU containers can take several minutes. The node should not block on the full
# deletion – it only needs the process to be gone. A quick `kill` is enough for that. We then
# queue the slower `remove_container` call as a background task.
async def _remove_container(container_id: str, logger: Logger):
    docker = aiodocker.Docker()
    try:
        container = docker.containers.container(container_id)
        await container.delete(force=True)
    except Exception as e:
        node_doc = ASYNC_DB.collection("nodes").document(INSTANCE_NAME)
        status = (await node_doc.get()).to_dict().get("status")
        if status not in ("DELETED", "FAILED"):
            msg = f"Failed to remove container {container_id}: {e}"
            await logger.log(msg, severity="WARNING")
    finally:
        await docker.close()


def _schedule_container_removal(
    container_id: str, logger: Logger, add_background_task: Optional[Callable] = None
):
    if add_background_task is not None:
        add_background_task(_remove_container, container_id, logger)
    else:
        asyncio.create_task(_remove_container(container_id, logger))


RESERVATION_ASSIGNMENT_TIMEOUT_SEC = 60


async def _watch_reservation(job_id: str):
    """
    Wait until this node is assigned to `job_id`, or until the reservation is no longer valid.
    A reservation is no longer valid if the job is not RUNNING, or if the assignment never
    arrives within `RESERVATION_ASSIGNMENT_TIMEOUT_SEC`. In either case, clear the reservation
    so another job can use this node.
    """
    sync_db = firestore.Client(project=PROJECT_ID, database="burla")
    loop = asyncio.get_running_loop()
    reservation_ended = asyncio.Event()

    def _on_job_snapshot(doc_snapshot, changes, read_time):
        for change in changes:
            data = change.document.to_dict()
            if data and data.get("status") != "RUNNING":
                loop.call_soon_threadsafe(reservation_ended.set)
                return

    watch = sync_db.collection("jobs").document(job_id).on_snapshot(_on_job_snapshot)
    try:
        await asyncio.wait_for(
            reservation_ended.wait(),
            timeout=RESERVATION_ASSIGNMENT_TIMEOUT_SEC,
        )
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass
    finally:
        watch.unsubscribe()

    if SELF["reserved_for_job"] == job_id:
        SELF["reserved_for_job"] = None
        node_doc = ASYNC_DB.collection("nodes").document(INSTANCE_NAME)
        await node_doc.update({"reserved_for_job": None})


async def reboot_containers(
    new_container_config: Optional[list[str]] = None,
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

    try:
        db = firestore.Client(project=PROJECT_ID, database="burla")
        node_doc = db.collection("nodes").document(INSTANCE_NAME)
        current_status = node_doc.get().to_dict().get("status")
        if current_status in ("DELETED", "FAILED"):
            return

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

        # reset state of the node service, except current_container_config, and the job_watcher.
        current_container_config = SELF["current_container_config"]
        reserved_for_job = SELF["reserved_for_job"]
        REINIT_SELF(SELF)
        SELF["BOOTING"] = True
        SELF["current_container_config"] = current_container_config
        SELF["reserved_for_job"] = reserved_for_job
        if new_container_config:
            SELF["current_container_config"] = new_container_config

        # get list of authorized users/tokens from backend service
        headers = {"Authorization": f"Bearer {CLUSTER_ID_TOKEN}"}
        url = f"{BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}/users"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        SELF["authorized_users"] = response.json()["authorized_users"]

        docker = aiodocker.Docker()
        try:
            if IN_LOCAL_DEV_MODE:
                # Remove all "old" worker containers.
                # Mark all existing workers as "old".
                all_containers = await docker.containers.list(all=True)
                worker_containers = [
                    c for c in all_containers if "worker" in c._container["Names"][0]
                ]
                tasks = []
                for container in worker_containers:
                    name = container._container["Names"][0][1:]
                    is_old = name.startswith("OLD")
                    belongs_to_current_node = f"node_{INSTANCE_NAME[11:]}" in name

                    if is_old and belongs_to_current_node:
                        try:
                            await container.kill()
                        except Exception:
                            pass
                        _schedule_container_removal(container.id, logger, add_background_task)

                    elif belongs_to_current_node:
                        tasks.append(container.rename(f"OLD--{name}"))
                        tasks.append(container.stop(t=0))

                try:
                    await asyncio.gather(*tasks)
                except aiodocker.DockerError as e:
                    if "already in progress" not in str(e):
                        raise
            else:
                # remove all worker containers
                all_containers = await docker.containers.list()
                for container in all_containers:
                    if "worker" in container._container["Names"][0]:
                        try:
                            await container.kill()
                        except Exception:
                            pass
                        _schedule_container_removal(container.id, logger, add_background_task)

            # start new workers.
            workers = []
            for image in SELF["current_container_config"]:
                await _pull_image_if_missing(image, logger, docker)
                num_workers = INSTANCE_N_CPUS if NUM_GPUS == 0 else NUM_GPUS

                msg = f"Image {image} pulled successfully.\nWaiting for {num_workers} workers to start ..."
                await logger.log(msg)

                for _ in range(num_workers):
                    workers.append(WorkerClient(image))
        finally:
            await docker.close()

        SELF["workers"] = workers
        # boot only one first so it downloads uv / sets up env
        # then others use that env instead of setting up themself.
        await workers[0].boot()
        await asyncio.gather(*[worker.boot() for worker in workers[1:]])
        SELF["BOOTING"] = False

        # main_service writes the host field after creating the VM/container.
        # Wait for that before marking READY so clients never see READY with host=None.
        while node_doc.get().to_dict().get("host") is None:
            await asyncio.sleep(1)

        current_status = node_doc.get().to_dict().get("status")
        if current_status in ("DELETED", "FAILED"):
            return

        node_doc.update({"status": "READY"})

        if SELF["reserved_for_job"]:
            SELF["watch_reservation_task"] = asyncio.create_task(
                _watch_reservation(SELF["reserved_for_job"])
            )

    except Exception as parent_exception:
        SELF["FAILED"] = True
        try:
            # using `logger` here makes this appear in node logs in dashboard, this makes it too
            # hard for users to find their container error (by putting a big traceback below),
            # which is why we log directly to gcl instead of using the `logger` instance
            # it's possible this hides important `reboot_containers` errors from users,
            # im gonna wait until that's an issue ti fix
            msg = f"Error from Node-Service:\n{traceback.format_exc()}"
            GCL_CLIENT.log_struct(dict(message=msg), severity="ERROR")

            node_doc.update({"status": "FAILED"})
            msg = f"Error from Node-Service: {str(parent_exception)}"
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

    await logger.log(f"Done booting {len(SELF['workers'])} workers, {INSTANCE_NAME} is READY!")
