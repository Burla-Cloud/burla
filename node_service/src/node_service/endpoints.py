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
    get_request_json,
    get_logger,
    get_request_files,
    get_add_background_task_function,
    Container,
)
from node_service.helpers import Logger, format_traceback, ignore_400_409_404
from node_service.worker import Worker
from node_service import IN_LOCAL_DEV_MODE

router = APIRouter()


def restart_on_client_disconnect():
    logger = Logger()
    try:
        while True:
            sleep(3)
            seconds_since_last_healthcheck = time() - SELF["last_healthcheck_timestamp"]
            logger.log(f"checking for restart: {seconds_since_last_healthcheck}")
            client_disconnected = seconds_since_last_healthcheck > 20

            print(f"seconds_since_last_healthcheck: {seconds_since_last_healthcheck}")
            print(f"client_disconnected: {client_disconnected}")

            if client_disconnected and not SELF["BOOTING"]:
                msg = "No healthcheck received from client in the last "
                msg += f"{seconds_since_last_healthcheck}s, REBOOTING NODE!"
                logger.log(msg)

                print(msg)
                # print(1 / 0)

                reboot_containers(logger=logger)
                break
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
        traceback_str = format_traceback(tb_details)
        logger.log(str(e), "ERROR", traceback=traceback_str)


@router.post("/jobs/{job_id}/inputs")
async def upload_inputs(
    job_id: str = Path(...),
    request_files: Optional[dict] = Depends(get_request_files),
    logger: Logger = Depends(get_logger),
):
    SELF["last_healthcheck_timestamp"] = time()
    if not job_id == SELF["current_job"]:
        return Response("job not found", status_code=404)

    inputs_pkl_with_idx = pickle.loads(request_files["inputs_pkl_with_idx"])
    logger.log(f"Received {len(inputs_pkl_with_idx)} inputs for job {job_id}")

    # separate into batches to be sent to each worker
    input_batches = []
    batch_size = len(inputs_pkl_with_idx) // len(SELF["workers"])
    extra = len(inputs_pkl_with_idx) % len(SELF["workers"])
    start = 0
    for i, worker in enumerate(SELF["workers"]):
        end = start + batch_size + (1 if i < extra else 0)
        input_batches.append(inputs_pkl_with_idx[start:end])
        start = end

    # concurrently send to each worker
    async with aiohttp.ClientSession() as session:
        tasks = []
        for worker, batch in zip(SELF["workers"], input_batches):
            logger.log(f"Sending {len(batch)} inputs to {worker.url}")
            data = aiohttp.FormData()
            data.add_field("inputs_pkl_with_idx", pickle.dumps(batch))
            tasks.append(session.post(f"{worker.url}/jobs/{job_id}/inputs", data=data))
        await asyncio.gather(*tasks)

    SELF["last_healthcheck_timestamp"] = time()


@router.get("/jobs/{job_id}")
def healthcheck(job_id: str = Path(...)):
    if not job_id == SELF["current_job"]:
        return Response("job not found", status_code=404)

    print("RECEIVED HEALTHCHECK")
    SELF["last_healthcheck_timestamp"] = time()


@router.post("/jobs/{job_id}")
def execute(
    job_id: str = Path(...),
    request_json: dict = Depends(get_request_json),
    request_files: Optional[dict] = Depends(get_request_files),
    add_background_task: Callable = Depends(get_add_background_task_function),
):
    SELF["last_healthcheck_timestamp"] = time()

    if SELF["RUNNING"]:
        return Response(f"Node in state `RUNNING`, unable to satisfy request", status_code=409)
    elif request_json["parallelism"] == 0:
        return Response(f"parallelism must be greater than 0", status_code=400)

    SELF["current_job"] = job_id
    SELF["RUNNING"] = True
    function_pkl = (request_files or {}).get("function_pkl")
    db = firestore.Client(project=PROJECT_ID, database="burla")
    node_doc = db.collection("nodes").document(INSTANCE_NAME)
    node_doc.update({"status": "RUNNING", "current_job": job_id})

    job_watcher_thread = Thread(target=restart_on_client_disconnect)
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
    async def assign_worker(session, url):
        async with session.post(url, data={"function_pkl": function_pkl}) as response:
            response.raise_for_status()

    async def assign_workers(workers):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for worker in workers:
                url = f"{worker.url}/jobs/{job_id}"
                tasks.append(assign_worker(session, url))
            await asyncio.gather(*tasks)

    asyncio.run(assign_workers(workers_to_keep))

    SELF["workers"] = workers_to_keep
    remove_workers = lambda workers_to_remove: [w.remove() for w in workers_to_remove]
    add_background_task(remove_workers, workers_to_remove)
    SELF["last_healthcheck_timestamp"] = time()


@router.post("/background_reboot")
def background_reboot(
    logger: Logger = Depends(get_logger),
    add_background_task: Callable = Depends(get_add_background_task_function),
):
    add_background_task(reboot_containers, logger=logger)


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
        node_doc = (
            firestore.Client(project=PROJECT_ID, database="burla")
            .collection("nodes")
            .document(INSTANCE_NAME)
        )
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

            # logger.log(f'Marked {len(workers_marked_old)} workers as "OLD": {workers_marked_old}')
            # logger.log(f'Removed {len(old_workers_removed)} "OLD" workers: {old_workers_removed}')
        else:
            # remove all worker containers
            workers_removed = []
            for container in docker_client.containers():
                if "worker" in container["Names"][0]:
                    remove_container = ignore_400_409_404(docker_client.remove_container)
                    kwargs = {"container": container["Id"], "force": True}
                    threads.append(Thread(target=remove_container, kwargs=kwargs))
                    workers_removed.append(container["Names"][0])

            # logger.log(f"Removed {len(workers_removed)} workers: {workers_removed}")

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
        # logger.log(f'Started {len(SELF["workers"])} new workers: {worker_names}')

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
            SELF["last_healthcheck_timestamp"] = time()
            node_doc.update({"status": "READY"})

        SELF["job_watcher_thread"] = None

    except Exception as e:
        SELF["FAILED"] = True
        raise e

    logger.log(f"Done Rebooting.\n{INSTANCE_NAME} is READY!")
