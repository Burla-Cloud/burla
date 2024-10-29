import sys
import pickle
import requests
from time import time, sleep
from typing import List
from threading import Thread
from typing import Optional, Callable
import traceback
import asyncio
import aiohttp

import docker
from docker.errors import APIError, NotFound
from fastapi import APIRouter, Path, Depends, Response
from google.cloud import firestore

from node_service import (
    PROJECT_ID,
    SELF,
    INSTANCE_N_CPUS,
    INSTANCE_NAME,
    get_request_json,
    get_logger,
    get_request_files,
    get_add_background_task_function,
    Container,
)
from node_service.helpers import Logger, format_traceback
from node_service.worker import Worker
from node_service import ACCESS_TOKEN

router = APIRouter()


def watch_job(job_id: str):
    """Runs in an independent thread, restarts node when all workers are done or if any failed."""
    logger = Logger()

    start = time()

    try:
        while True:
            sleep(2)
            workers_status = [worker.status() for worker in SELF["workers"]]
            any_failed = any([status == "FAILED" for status in workers_status])
            all_done = all([status == "DONE" for status in workers_status])
            if all_done or any_failed:
                break

        if not SELF["BOOTING"]:
            reboot_containers(logger=logger)
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
        traceback_str = format_traceback(tb_details)
        logger.log(str(e), "ERROR", traceback=traceback_str)


@router.get("/jobs/{job_id}")
def get_job_status(job_id: str = Path(...)):
    if not job_id == SELF["current_job"]:
        return Response("job not found", status_code=404)

    workers_status = [worker.status() for worker in SELF["workers"]]
    any_failed = any([status == "FAILED" for status in workers_status])
    all_done = all([status == "DONE" for status in workers_status])
    return {"all_workers_done": all_done, "any_workers_failed": any_failed}


@router.post("/jobs/{job_id}")
def execute(
    job_id: str = Path(...),
    request_json: dict = Depends(get_request_json),
    request_files: Optional[dict] = Depends(get_request_files),
    logger: Logger = Depends(get_logger),
    add_background_task: Callable = Depends(get_add_background_task_function),
):
    if SELF["RUNNING"]:
        return Response(f"Node in state `RUNNING`, unable to satisfy request", status_code=409)
    elif request_json["parallelism"] == 0:
        return Response(f"parallelism must be greater than 0", status_code=400)

    SELF["current_job"] = job_id
    SELF["RUNNING"] = True
    function_pkl = (request_files or {}).get("function_pkl")
    db = firestore.Client(project=PROJECT_ID)
    node_doc = db.collection("nodes").document(INSTANCE_NAME)
    node_doc.update({"status": "RUNNING", "current_job": job_id})

    job_watcher_thread = Thread(target=watch_job, args=(job_id,))
    job_watcher_thread.start()
    SELF["job_watcher_thread"] = job_watcher_thread

    job_ref = db.collection("jobs").document(job_id)
    job = job_ref.get().to_dict()

    # determine which workers to call and which to remove
    workers_to_remove = []
    workers_to_keep = []
    future_parallelism = 0
    user_python_version = job["user_python_version"]
    for worker in SELF["workers"]:
        correct_python_version = worker.python_version == user_python_version
        need_more_parallelism = future_parallelism < request_json["parallelism"]

        if correct_python_version and need_more_parallelism:
            workers_to_keep.append(worker)
            future_parallelism += 1
        else:
            workers_to_remove.append(worker)

    if not workers_to_keep:
        msg = "No compatible containers.\n"
        msg += f"User is running python version {user_python_version}, "
        cluster_python_versions = list(set([e.python_version for e in SELF["workers"]]))
        cluster_python_versions_msg = ", ".join(cluster_python_versions[:-1])
        cluster_python_versions_msg += f", and {cluster_python_versions[-1]}"
        msg += f"containers in the cluster are running: {cluster_python_versions_msg}.\n"
        msg += "To fix this you can either:\n"
        msg += f" - update the cluster to run containers with python v{user_python_version}\n"
        msg += f" - update your local python version to be one of {cluster_python_versions}"
        return Response(msg, status_code=409)

    # call workers concurrently
    async def assign_worker(session, url, starting_index):
        request_json = {
            "inputs_id": job["inputs_id"],
            "n_inputs": job["n_inputs"],
            "starting_index": starting_index,
            "planned_future_job_parallelism": job["planned_future_job_parallelism"],
            "sa_access_token": ACCESS_TOKEN,
        }
        data = {"function_pkl": function_pkl, "request_json": pickle.dumps(request_json)}
        async with session.post(url, data=data) as response:
            response.raise_for_status()
        return starting_index

    async def assign_workers(workers):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for index, worker in enumerate(workers):
                url = f"{worker.host}/jobs/{job_id}"
                worker_starting_index = request_json["starting_index"] + index
                tasks.append(assign_worker(session, url, worker_starting_index))
                worker.id = worker_starting_index
            return await asyncio.gather(*tasks)

    assigned_starting_indicies = asyncio.run(assign_workers(workers_to_keep))

    if not len(assigned_starting_indicies) == len(workers_to_keep):
        desired_starting_indicies = list(range(starting_index, len(workers_to_keep)))
        unassigned_indicies = set(desired_starting_indicies) - set(assigned_starting_indicies)
        raise Exception(f"failed to assign workers to inputs at indicies: {unassigned_indicies}")

    SELF["workers"] = workers_to_keep
    # remove_workers = lambda workers_to_remove: [w.remove() for w in workers_to_remove]
    # add_background_task(remove_workers, workers_to_remove)

    starting_index = request_json["starting_index"]
    ending_index = starting_index + len(workers_to_keep)
    add_background_task(logger.log, f"Assigned inputs: {starting_index} - {ending_index}")


@router.post("/reboot")
def reboot_containers(
    new_container_config: Optional[List[Container]] = None,
    logger: Logger = Depends(get_logger),
):
    """
    Rebooting will reboot the containers that are currently/ were previously running.
    If new containers are passed with the reboot request, those containers will be booted instead.
    """
    started_booting_at = time()

    # TODO: seems to have like a 1/5 chance (only after running a job) of throwing a:
    # `Unable to reboot, not all containers started!`
    # Error, prececed by many `PORT ALREADY IN USE, TRYING AGAIN.`'s
    # This only happens with a high number of containers.

    if SELF["BOOTING"]:
        return Response("Node already BOOTING, unable to satisfy request.", status_code=409)

    try:
        SELF["RUNNING"] = False
        SELF["BOOTING"] = True
        SELF["workers"] = []
        if new_container_config:
            SELF["current_container_config"] = new_container_config

        docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
        node_doc = firestore.Client(project=PROJECT_ID).collection("nodes").document(INSTANCE_NAME)
        node_doc.update(
            {
                "status": "BOOTING",
                "current_job": None,
                "parallelism": None,
                "target_parallelism": None,
                "started_booting_at": started_booting_at,
            }
        )

        # remove all containers (except those named `main_service`)
        for container in docker_client.containers():
            if container["Names"][0] != "/main_service":
                try:
                    docker_client.remove_container(container["Id"], force=True)
                except (APIError, NotFound, requests.exceptions.HTTPError) as e:
                    # re-raise any errors that aren't an "already-in-progress" error
                    if not (("409" in str(e)) or ("404" in str(e))):
                        raise e

        def create_worker(*a, **kw):
            # Log error inside thread because sometimes it isn't sent to the main thread, idk why.
            try:
                worker = Worker(*a, **kw)
                SELF["workers"].append(worker)
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
                traceback_str = format_traceback(tb_details)
                logger.log(str(e), "ERROR", traceback=traceback_str)

        # start one container per container-config per cpu
        # max num containers is 1024 due to some network/port related limit
        threads = []
        for container_spec in SELF["current_container_config"]:
            for _ in range(INSTANCE_N_CPUS):
                args = (
                    container_spec.python_version,
                    container_spec.python_executable,
                    container_spec.image,
                    docker_client,
                )
                thread = Thread(target=create_worker, args=args)
                threads.append(thread)
                thread.start()

        for thread in threads:
            thread.join()

        # Sometimes on larger machines, some containers don't start, or get stuck in "CREATED" state
        # This has not been diagnosed, this check is performed to ensure all containers started.
        containers = [c for c in docker_client.containers() if c["Names"][0] != "/main_service"]
        containers_status = [c["State"] for c in containers]
        num_running_containers = sum([status == "running" for status in containers_status])
        expected_n_containers = len(SELF["current_container_config"]) * INSTANCE_N_CPUS
        some_containers_missing = num_running_containers != expected_n_containers

        if some_containers_missing:
            SELF["FAILED"] = True
            node_doc.update({"status": "FAILED"})
            raise Exception("Unable to reboot, not all containers started!")
        else:
            SELF["BOOTING"] = False
            SELF["current_job"] = None
            node_doc.update({"status": "READY"})

        SELF["job_watcher_thread"] = None

    except Exception as e:
        SELF["FAILED"] = True
        raise e
