from time import time
import requests
import asyncio
from typing import Optional, Callable
import traceback

import aiodocker
from fastapi import APIRouter, Depends, Response

from node_service import (
    PROJECT_ID,
    SELF,
    REINIT_SELF,
    INSTANCE_N_CPUS,
    INSTANCE_NAME,
    IN_LOCAL_DEV_MODE,
    BURLA_BACKEND_URL,
    CLUSTER_ID_TOKEN,
    NUM_GPUS,
    get_logger,
    get_add_background_task_function,
    head_client,
)
from node_service.helpers import Logger
from node_service.worker_client import WorkerClient, verify_worker_cgroup_isolation

router = APIRouter()


@router.post("/shutdown")
async def shutdown_node(logger: Logger = Depends(get_logger)):
    """
    We dont need to delete the node here because this is only called by the in-VM
    shutdown hooks, i.e. the VM is already being deleted.
    """
    SELF["job_watcher_stop_event"].set()
    SELF["current_parallelism"] = 0
    SELF["SHUTTING_DOWN"] = True
    await logger.log(f"Received shutdown request for node {INSTANCE_NAME}.")

    # FAILED nodes keep their status so they remain visible for debugging.
    if not SELF["FAILED"]:
        SELF["reported_status"] = "DELETED"
        await head_client.push_state(status="DELETED", ended_at=time())


@router.post("/reboot")
async def reboot_containers_endpoint(
    new_container_config: Optional[list[str]] = None,
    logger: Logger = Depends(get_logger),
    add_background_task: Callable = Depends(get_add_background_task_function),
):
    if SELF["BOOTING"]:
        return Response(
            "Node already BOOTING, unable to satisfy request.", status_code=409
        )
    return await reboot_containers(new_container_config, logger, add_background_task)


def image_size_GB(image: str):
    name, tag = image.rsplit(":", 1) if ":" in image else (image, "latest")
    name = name if "/" in name else f"library/{name}"
    params = {"service": "registry.docker.io", "scope": f"repository:{name}:pull"}
    token = requests.get("https://auth.docker.io/token", params=params).json()["token"]
    auth = {"Authorization": f"Bearer {token}"}
    headers = {
        **auth,
        "Accept": "application/vnd.docker.distribution.manifest.list.v2+json",
    }
    url = f"https://registry-1.docker.io/v2/{name}/manifests/{tag}"
    manifest = requests.get(url, headers=headers).json()
    if "manifests" in manifest:
        is_linux = lambda m: m["platform"]["os"] == "linux"
        is_amd64 = lambda m: m["platform"]["architecture"] == "amd64"
        m = next(m for m in manifest["manifests"] if is_linux(m) and is_amd64(m))
        headers = {
            **auth,
            "Accept": "application/vnd.docker.distribution.manifest.v2+json",
        }
        url = f"https://registry-1.docker.io/v2/{name}/manifests/{m['digest']}"
        manifest = requests.get(url, headers=headers).json()
    size = sum(l["size"] for l in manifest["layers"])
    return round(size / 1_000_000_000, 2)


def _gcp_artifact_registry_auth() -> dict:
    """Private-image pulls from Google Artifact Registry. Only reachable on
    GCP (local-dev containers mount gcloud creds; GCE VMs use ADC)."""
    import google.auth
    from google.auth.transport.requests import Request

    credentials, _ = google.auth.default()
    credentials.refresh(Request())
    return {"username": "oauth2accesstoken", "password": credentials.token}


async def _pull_image_if_missing(image: str, logger: Logger, docker: aiodocker.Docker):
    # Use CLI instead of python api because that api just generally horrible and broken.
    # I already tried using it correctly, it wasnt worth it.

    async def _run_command(*args, input_bytes=None, raise_error=True):
        process = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.PIPE if input_bytes is not None else None,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate(input_bytes)
        if process.returncode != 0 and raise_error:
            raise Exception(args, stderr)
        return process.returncode, stdout, stderr

    attempt = 0
    while True:
        attempt += 1

        try:
            await logger.log(f"Pulling image {image} ({image_size_GB(image)} GB) ...")
        except Exception:
            await logger.log(f"Pulling image {image} ...")

        returncode, stdout, stderr = await _run_command(
            "docker", "pull", image, raise_error=False
        )
        text_output = stderr.decode() + stdout.decode()
        no_transient_error = not (returncode != 0 and "unexpected EOF" in text_output)

        if no_transient_error or attempt > 5:
            break
        else:
            await logger.log(
                f"`Unexpected EOF` error detected, retrying... (attempt {attempt})"
            )
            await asyncio.sleep(3)

    docker_pull_failed = returncode != 0
    docker_pull_stderr = stderr.decode()
    not_hosted_in_google_artifact_registry = "docker.pkg.dev" not in image

    if docker_pull_failed and not_hosted_in_google_artifact_registry:
        raise Exception(
            f"CMD `docker pull {image}` failed with error:\n{docker_pull_stderr}\n"
        )

    # if failed and image is in GAR, try again using service account credentials
    if docker_pull_failed:
        msg = f"Failed to pull image: {image}\n"
        msg += "Trying again using the service account credentials attached to this VM."
        await logger.log(msg)

        if image.startswith("https://"):
            host = f'https://{image.split("/")[2]}'
        else:
            host = f'https://{image.split("/")[0]}'

        auth_config = _gcp_artifact_registry_auth()
        returncode, stdout, stderr = await _run_command(
            "docker",
            "login",
            host,
            "-u",
            "oauth2accesstoken",
            "--password-stdin",
            input_bytes=auth_config["password"].encode(),
            raise_error=False,
        )
        if returncode != 0:
            msg = (
                f"CMD `docker pull {image}` failed with error:\n{docker_pull_stderr}\n"
            )
            msg += (
                f"Following attempt to login to {host} using the VM's service account "
            )
            msg += f"also failed with error:\n{stderr}\n"
            raise Exception(msg)

        await _run_command("docker", "pull", image)

    # sanity check, not positive this is necessary with cli, but was with python api.
    returncode, stdout, stderr = await _run_command(
        "docker", "inspect", image, raise_error=False
    )
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
        if not (SELF["SHUTTING_DOWN"] or SELF["FAILED"]):
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
RESERVATION_POLL_INTERVAL_SEC = 2


async def watch_reservation(job_id: str):
    """
    Wait until this node is assigned to `job_id`, or until the reservation is no longer valid.
    A reservation is no longer valid if the job is not RUNNING, or if the assignment never
    arrives within `RESERVATION_ASSIGNMENT_TIMEOUT_SEC`. In either case, clear the reservation
    so another job can use this node.
    """
    started_at = time()
    try:
        while time() - started_at < RESERVATION_ASSIGNMENT_TIMEOUT_SEC:
            await asyncio.sleep(RESERVATION_POLL_INTERVAL_SEC)
            try:
                job = await head_client.get_job(job_id)
            except Exception:
                # A transient head outage must not kill this task: an uncleared
                # reservation pins the inactivity watchdog and immortalizes
                # the VM (observed with grow nodes booted mid-shutdown).
                continue
            if job is None or job.get("status") != "RUNNING":
                break
    except asyncio.CancelledError:
        return

    if SELF["reserved_for_job"] == job_id:
        SELF["reserved_for_job"] = None
        await head_client.push_state(reserved_for_job=None)


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
    # Set before the direct push so the 1s push loop can't race in a stale
    # READY/RUNNING between the push below and REINIT_SELF.
    SELF["reported_status"] = "BOOTING"

    try:
        # The head refuses to downgrade a terminal DELETED/FAILED status; a
        # terminal status in the response means this node was deleted or
        # failed externally and should not keep booting.
        view = await head_client.push_state(
            status="BOOTING",
            current_job=None,
            started_booting_at=time(),
        )
        if view.get("status") in ("DELETED", "FAILED"):
            return

        # reset state of the node service, except current_container_config, and the job_watcher.
        current_container_config = SELF["current_container_config"]
        reserved_for_job = SELF["reserved_for_job"]
        REINIT_SELF(SELF)
        SELF["BOOTING"] = True
        SELF["reported_status"] = "BOOTING"
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
            # remove all worker containers
            all_containers = await docker.containers.list()
            for container in all_containers:
                if "worker" in container._container["Names"][0]:
                    try:
                        await container.kill()
                    except Exception:
                        pass
                    _schedule_container_removal(
                        container.id, logger, add_background_task
                    )

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
        await verify_worker_cgroup_isolation(workers, logger)
        SELF["BOOTING"] = False

        # main_service learns the host when it creates the VM/container and
        # hands it down in state-push responses. Wait for it before marking
        # READY so clients never see READY with host=None.
        while SELF["host"] is None:
            view = await head_client.push_state(status="BOOTING")
            SELF["host"] = view.get("host")
            if view.get("status") in ("DELETED", "FAILED"):
                return
            if SELF["host"] is None:
                await asyncio.sleep(1)

        SELF["reported_status"] = "READY"
        await head_client.push_state(status="READY")

        if SELF["reserved_for_job"]:
            SELF["watch_reservation_task"] = asyncio.create_task(
                watch_reservation(SELF["reserved_for_job"])
            )

    except Exception as parent_exception:
        SELF["FAILED"] = True
        SELF["reported_status"] = "FAILED"
        try:
            # Full tracebacks stay out of the dashboard's node-log view (it
            # makes users' container errors too hard to find), so print the
            # traceback here and send only the short message to the head.
            print(f"Error from Node-Service:\n{traceback.format_exc()}")

            await head_client.push_state(status="FAILED")
            msg = f"Error from Node-Service: {str(parent_exception)}"
            await head_client.post_node_logs([{"msg": msg, "ts": time()}])

            if not IN_LOCAL_DEV_MODE:
                await head_client.request_self_delete()
        except Exception as e:
            raise e from parent_exception
        raise parent_exception

    await logger.log(
        f"Done booting {len(SELF['workers'])} workers, {INSTANCE_NAME} is READY!"
    )
