import sys
import pickle
from time import time, sleep
from typing import List
from threading import Thread, Event
from typing import Optional, Callable
import traceback
import asyncio
import aiohttp

import docker
from fastapi import APIRouter, Path, Depends, Response
from google.cloud import firestore

from node_service import (
    PROJECT_ID,
    SELF,
    INSTANCE_N_CPUS,
    INSTANCE_NAME,
    JOB_HEALTHCHECK_FREQUENCY_SEC,
    get_request_json,
    get_logger,
    get_request_files,
    get_add_background_task_function,
    Container,
)
from node_service.helpers import Logger, format_traceback, ignore_400_409_404
from node_service.worker import Worker
from node_service import ACCESS_TOKEN, IN_LOCAL_DEV_MODE

router = APIRouter()


def watch_job(job_id: str):
    """Runs in an independent thread, restarts node when all workers are done or if any failed."""
    logger = Logger()
    UDF_error_thrown = Event()

    def on_snapshot(collection_snapshot, changes, read_time):
        for change in changes:
            if change.type.name == "ADDED":
                doc = change.document
                if doc.get("is_error") is True:
                    UDF_error_thrown.set()

    # Watch for UDF errors from other nodes:
    # restart if a udf error from any other node is detected.
    job_doc = firestore.Client(project=PROJECT_ID).collection("jobs").document(job_id)
    UDF_error_watcher = job_doc.collection("results").on_snapshot(on_snapshot)

    try:
        while True:

            sleep(2)
            SELF["time_until_client_disconnect_shutdown"] -= 2
            client_disconnected = SELF["time_until_client_disconnect_shutdown"] < 0

            workers_status = [worker.status() for worker in SELF["workers"]]
            any_failed = any([status == "FAILED" for status in workers_status])
            all_done = all([status == "DONE" for status in workers_status])
            logger.log(f"Got workers status: all_done={all_done}, any_failed={any_failed}")

            if all_done or any_failed or UDF_error_thrown.is_set() or client_disconnected:
                break

        if not SELF["BOOTING"]:
            logger.log("Rebooting node.")
            reboot_containers(logger=logger)
        else:
            logger.log("NOT rebooting because node is ALREADY rebooting!")

    except Exception as e:
        UDF_error_watcher.unsubscribe()
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
        traceback_str = format_traceback(tb_details)
        logger.log(str(e), "ERROR", traceback=traceback_str)


@router.get("/jobs/{job_id}")
def get_job_status(job_id: str = Path(...)):
    if not job_id == SELF["current_job"]:
        return Response("job not found", status_code=404)

    # reset because healtheck received
    # no real reason I picked 10 here other than that 5 barely worked
    # fixing this properly dosent matter because we should move to grpc soonish
    SELF["time_until_client_disconnect_shutdown"] = JOB_HEALTHCHECK_FREQUENCY_SEC + 10

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
    remove_workers = lambda workers_to_remove: [w.remove() for w in workers_to_remove]
    add_background_task(remove_workers, workers_to_remove)

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

        threads = []
        if IN_LOCAL_DEV_MODE:
            # allow us to read old worker logs before workers are deleteted.
            # workers are marked as "old", then deleted in subsequent restart.
            # `just_marked_old`/`worker_just_created` exist to prevent race condition where
            # one node_service makes workers and the other instantly marks them old/deletes them.
            workers_marked_old = []
            old_workers_removed = []

            # REGARDING `ignore_400_409_404`:
            # We assume that when these errors occur it is always because the operation has already
            # been completed/is-happening by another thread.

            for container in docker_client.containers(all=True):
                name = container["Names"][0][1:]
                is_worker = "worker" in name
                is_old = name.startswith("OLD")
                worker_belongs_to_self = f"node_{INSTANCE_NAME[11:]}" in name

                if is_old and worker_belongs_to_self:
                    remove_container = ignore_400_409_404(docker_client.remove_container)
                    kwargs = {"container": container["Id"], "force": True}
                    threads.append(Thread(target=remove_container, kwargs=kwargs))
                    old_workers_removed.append(name)

                elif is_worker and worker_belongs_to_self:
                    rename_container = ignore_400_409_404(docker_client.rename)
                    stop_container = ignore_400_409_404(docker_client.stop)
                    rename_args = (container["Id"], f"OLD--{name}")
                    stop_kwargs = {"container": container["Id"], "timeout": 0}
                    threads.append(Thread(target=rename_container, args=rename_args))
                    threads.append(Thread(target=stop_container, kwargs=stop_kwargs))
                    workers_marked_old.append(name)

            logger.log(f'Marked {len(workers_marked_old)} workers as "OLD": {workers_marked_old}')
            logger.log(f'Removed {len(old_workers_removed)} "OLD" workers: {old_workers_removed}')
        else:
            # remove all worker containers
            workers_removed = []
            for container in docker_client.containers():
                if "worker" in container["Names"][0]:
                    remove_container = ignore_400_409_404(docker_client.remove_container)
                    kwargs = {"container": container["Id"], "force": True}
                    threads.append(Thread(target=remove_container, kwargs=kwargs))
                    workers_removed.append(container["Names"][0])

            logger.log(f"Removed {len(workers_removed)} workers: {workers_removed}")

        [thread.start() for thread in threads]
        [thread.join() for thread in threads]

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

        [thread.join() for thread in threads]
        worker_names = [w.container_name for w in SELF["workers"]]
        logger.log(f'Started {len(SELF["workers"])} new workers: {worker_names}')

        # Sometimes on larger machines, some containers don't start, or get stuck in "CREATED" state
        # This has not been diagnosed, this check is performed to ensure all containers started.
        _containers = docker_client.containers()
        is_worker_container = lambda c: c["Names"][0][1:13] not in ["main_service", "node_service"]
        worker_containers_status = [c["State"] for c in _containers if is_worker_container(c)]
        num_running_containers = sum([status == "running" for status in worker_containers_status])
        expected_n_containers = len(SELF["current_container_config"]) * INSTANCE_N_CPUS
        some_containers_missing = num_running_containers < expected_n_containers

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

    logger.log(f"Done Rebooting.\n{INSTANCE_NAME} is READY!")
