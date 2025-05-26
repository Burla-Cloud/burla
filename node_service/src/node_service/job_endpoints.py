import pickle
from queue import Empty
from typing import Optional, Callable

import asyncio
import aiohttp
from fastapi import APIRouter, Path, Depends, Response
from google.cloud.firestore import AsyncClient

from node_service import (
    PROJECT_ID,
    SELF,
    INSTANCE_NAME,
    get_request_json,
    get_logger,
    get_request_files,
    get_add_background_task_function,
)
from node_service.helpers import Logger
from node_service.job_watcher import send_inputs_to_workers, job_watcher_logged

router = APIRouter()


@router.get("/jobs/{job_id}/inputs")
async def get_inputs(job_id: str = Path(...), logger: Logger = Depends(get_logger)):
    if job_id != SELF["current_job"]:
        return Response("job not found", status_code=404)
    elif SELF["SHUTTING_DOWN"]:
        return Response("Node is shutting down, can't give inputs.", status_code=409)

    min_reply_size_bytes = 1_000_000 * 0.5
    min_reply_size_per_worker = min_reply_size_bytes / len(SELF["workers"])

    async def _get_inputs_from_worker(session, worker):
        try:
            url = f"{worker.url}/jobs/{job_id}/inputs?min_reply_size={min_reply_size_per_worker}"
            async with session.get(url, timeout=1) as response:
                response.raise_for_status()
                if response.status == 200:
                    return pickle.loads(await response.read())
                elif response.status == 204:
                    return []
        except Exception as e:
            msg = f"Failed to get inputs from worker {worker.container_name} for job {job_id}: {e}"
            logger.log(msg, severity="WARNING")
            return []

    async with aiohttp.ClientSession() as session:
        tasks = [_get_inputs_from_worker(session, w) for w in SELF["workers"]]
        inputs = [input for inputs in await asyncio.gather(*tasks) for input in inputs]

    if not inputs:
        return Response(status_code=204)
    else:
        logger.log(f"Sending {len(inputs)} inputs to another node!")
        data = pickle.dumps(inputs)
        await asyncio.sleep(0)
        headers = {"Content-Disposition": 'attachment; filename="inputs.pkl"'}
        return Response(content=data, media_type="application/octet-stream", headers=headers)


@router.post("/jobs/{job_id}/inputs/done")
async def input_upload_done(job_id: str = Path(...)):
    if job_id != SELF["current_job"]:
        return Response("job not found", status_code=404)
    SELF["all_inputs_uploaded"] = True


@router.post("/jobs/{job_id}/inputs")
async def upload_inputs(
    job_id: str = Path(...),
    request_files: Optional[dict] = Depends(get_request_files),
):
    if job_id != SELF["current_job"]:
        return Response("job not found", status_code=404)
    elif SELF["SHUTTING_DOWN"]:
        return Response("Node is shutting down, inputs not accepted.", status_code=409)

    # needs to be here so this is reset when transferring from another dying node
    SELF["current_input_batch_forwarded"] = False
    SELF["all_inputs_uploaded"] = False

    inputs_pkl_with_idx = pickle.loads(request_files["inputs_pkl_with_idx"])
    await asyncio.sleep(0)
    async with aiohttp.ClientSession() as session:
        await send_inputs_to_workers(session, inputs_pkl_with_idx)

    SELF["current_input_batch_forwarded"] = True


@router.get("/jobs/{job_id}/results")
async def get_results(job_id: str = Path(...)):
    if job_id != SELF["current_job"]:
        return Response("job not found", status_code=404)

    results = []
    total_bytes = 0
    while (not SELF["results_queue"].empty()) and (total_bytes < (1_000_000 * 0.5)):
        try:
            result = SELF["results_queue"].get_nowait()
            results.append(result)
            total_bytes += len(result[2])
        except Empty:
            break

    await asyncio.sleep(0)
    response_json = {
        "results": results,
        "current_parallelism": SELF["current_parallelism"],
        "is_empty": SELF["results_queue"].empty(),
    }
    data = pickle.dumps(response_json)
    await asyncio.sleep(0)
    headers = {"Content-Disposition": 'attachment; filename="results.pkl"'}
    return Response(content=data, media_type="application/octet-stream", headers=headers)


@router.post("/shutdown")
async def shutdown_node(logger: Logger = Depends(get_logger)):
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
    await async_db.collection("nodes").document(INSTANCE_NAME).delete()


@router.post("/jobs/{job_id}")
async def execute(
    job_id: str = Path(...),
    request_json: dict = Depends(get_request_json),
    request_files: Optional[dict] = Depends(get_request_files),
    logger: Logger = Depends(get_logger),
    add_background_task: Callable = Depends(get_add_background_task_function),
):
    if SELF["RUNNING"] or SELF["BOOTING"]:
        return Response("Node currently running or booting, request refused.", status_code=409)

    SELF["current_job"] = job_id
    SELF["RUNNING"] = True

    # determine which workers to call and which to remove
    workers_to_remove = []
    workers_to_keep = []
    future_parallelism = 0
    is_background_job = request_json["is_background_job"]
    user_python_version = request_json["user_python_version"]
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
        versions = list(set([e.python_version for e in SELF["workers"]]))
        msg += f"containers in the cluster are running: {', '.join(versions)}.\n"
        msg += "To fix this you can either:\n"
        msg += f" - update the cluster to run containers with python{user_python_version}\n"
        msg += f" - update your local python version to be one of {versions}"
        return Response(msg, status_code=409)

    async def assign_worker(session, worker):
        data = aiohttp.FormData()
        data.add_field("function_pkl", request_files["function_pkl"])
        async with session.post(f"{worker.url}/jobs/{job_id}", data=data) as response:
            if response.status == 200:
                return worker
            else:
                msg = f"Worker {worker.container_name} returned error: {response.status}"
                logger.log(msg, severity="WARNING")
                return None

    async with aiohttp.ClientSession() as session:
        tasks = [assign_worker(session, worker) for worker in workers_to_keep]
        successfully_assigned_workers = [w for w in await asyncio.gather(*tasks) if w is not None]

    if len(successfully_assigned_workers) == 0:
        raise Exception("Failed to assign job to any workers")

    logger.log(f"Successfully assigned to {len(successfully_assigned_workers)} workers.")

    SELF["workers"] = workers_to_keep
    remove_workers = lambda workers_to_remove: [w.remove() for w in workers_to_remove]
    add_background_task(remove_workers, workers_to_remove)

    SELF["job_watcher_stop_event"].clear()  # is initalized as set by default
    job_watcher_coroutine = job_watcher_logged(request_json["n_inputs"], is_background_job)
    SELF["job_watcher_task"] = asyncio.create_task(job_watcher_coroutine)
