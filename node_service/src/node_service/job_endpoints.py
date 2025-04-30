import pickle
from time import time, sleep
from queue import Empty
from threading import Thread
from typing import Optional, Callable
import asyncio
import aiohttp
from io import BytesIO
from fastapi import APIRouter, Path, Depends, Response, Request
from fastapi.responses import StreamingResponse
from google.cloud import firestore
from google.cloud.firestore import FieldFilter, And

from node_service import (
    PROJECT_ID,
    SELF,
    INSTANCE_NAME,
    get_request_json,
    get_logger,
    get_request_files,
    get_add_background_task_function,
)
from node_service.job_watcher import send_inputs_to_workers, job_watcher_logged
from node_service.helpers import Logger
from node_service.worker import Worker

router = APIRouter()


@router.get("/jobs/{job_id}/inputs")
async def get_half_inputs(
    job_id: str = Path(...),
    logger: Logger = Depends(get_logger),
):
    if job_id != SELF["current_job"]:
        return Response("job not found", status_code=404)
    elif SELF["SHUTTING_DOWN"]:
        return Response("Node is shutting down, can't give inputs.", status_code=409)

    async def _get_half_inputs_from_worker(session, worker, job_id):
        try:
            async with session.get(f"{worker.url}/jobs/{job_id}/inputs", timeout=1) as response:
                response.raise_for_status()
                if response.status == 200:
                    return pickle.loads(await response.read())
                elif response.status == 204:
                    return []
        except Exception as e:
            msg = f"Failed to get inputs from worker {worker.container_name} for job {job_id}: {e}"
            logger.log(msg, severity="WARNING")
            return []

    inputs = []
    async with aiohttp.ClientSession() as session:
        tasks = [_get_half_inputs_from_worker(session, w, job_id) for w in SELF["workers"]]
        for half_of_worker_inputs in await asyncio.gather(*tasks):
            inputs.extend(half_of_worker_inputs)

    if not inputs:
        return Response(status_code=204)
    else:
        logger.log(f"Sending {len(inputs)} inputs to another node!")
        data = BytesIO(pickle.dumps((inputs)))
        data.seek(0)  # ensure file pointer is at the beginning of the file.
        headers = {"Content-Disposition": 'attachment; filename="inputs.pkl"'}
        return StreamingResponse(data, media_type="application/octet-stream", headers=headers)


@router.post("/jobs/{job_id}/inputs/done")
def input_upload_done(job_id: str = Path(...)):
    if job_id != SELF["current_job"]:
        return Response("job not found", status_code=404)
    SELF["all_inputs_uploaded"] = True


@router.post("/jobs/{job_id}/inputs")
async def upload_inputs(
    job_id: str = Path(...),
    request_files: Optional[dict] = Depends(get_request_files),
    logger: Logger = Depends(get_logger),
):
    if job_id != SELF["current_job"]:
        return Response("job not found", status_code=404)
    elif SELF["SHUTTING_DOWN"]:
        return Response("Node is shutting down, inputs not accepted.", status_code=409)

    # needs to be here so this is reset when transferring from another dying node
    SELF["current_input_batch_forwarded"] = False
    SELF["all_inputs_uploaded"] = False

    inputs_pkl_with_idx = pickle.loads(request_files["inputs_pkl_with_idx"])
    await send_inputs_to_workers(inputs_pkl_with_idx)

    SELF["current_input_batch_forwarded"] = True
    # logger.log(f"Received {len(inputs_pkl_with_idx)} inputs.")


@router.get("/jobs/{job_id}/results")
def get_results(job_id: str = Path(...), logger: Logger = Depends(get_logger)):
    if job_id != SELF["current_job"]:
        return Response("job not found", status_code=404)

    start = time()
    results = []
    total_bytes = 0
    while (not SELF["results_queue"].empty()) and (total_bytes < (1_000_000 * 0.5)):
        try:
            result = SELF["results_queue"].get_nowait()
            results.append(result)
            total_bytes += len(result[2])
        except Empty:
            break

    # logger.log(f"returning {len(results)} results after {time() - start:.2f}s")
    response_json = {
        "results": results,
        "current_parallelism": SELF["current_parallelism"],
        "is_empty": SELF["results_queue"].empty(),
    }
    data = pickle.dumps(response_json)
    headers = {"Content-Disposition": 'attachment; filename="results.pkl"'}
    return Response(content=data, media_type="application/octet-stream", headers=headers)


@router.post("/shutdown")
async def shutdown_node(request: Request, logger: Logger = Depends(get_logger)):
    # Only allow shutdown requests from localhost (inside the shutdown script defined in main_svc)
    # if request.client.host != "127.0.0.1":
    #     return Response("Shutdown endpoint can only be called from localhost", status_code=403)

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

    try:
        db = firestore.Client(project=PROJECT_ID, database="burla")
        db.collection("nodes").document(INSTANCE_NAME).delete()
    except Exception as e:
        logger.log(f"Error deleting node {INSTANCE_NAME} from firestore: {e}", severity="WARNING")

    # # before transferring inputs, wait for curent batch to finish uploading:
    # # It's really important the client and node-service are on the same page.
    # # which is why we don't just stop it and take the inputs that are there.
    # if SELF["current_input_batch_forwarded"] == False:
    #     start_time = time()
    #     while not SELF["current_input_batch_forwarded"]:
    #         if time() - start_time > 10:
    #             raise Exception("Timeout waiting for input batch to be forwarded (>10 seconds)")
    #         sleep(0.1)

    # if SELF["current_job"]:
    #     # send remaining inputs to another node
    #     status_filter = FieldFilter("status", "==", "RUNNING")
    #     job_filter = FieldFilter("current_job", "==", SELF["current_job"])
    #     query = db.collection("nodes").where(filter=And([status_filter, job_filter]))

    #     async def transfer_inputs(worker: Worker, node_url: str, session: aiohttp.ClientSession):
    #         worker_url = f"{worker.url}/jobs/{SELF['current_job']}/transfer_inputs"
    #         json = {"target_node_url": node_url}
    #         async with session.post(worker_url, json=json) as response:
    #             response.raise_for_status()

    #     async with aiohttp.ClientSession() as session:
    #         success = False
    #         for node in query.stream():
    #             try:
    #                 host = node.get("host")
    #                 tasks = [transfer_inputs(w, host, session) for w in SELF["workers"]]
    #                 await asyncio.gather(*tasks)
    #                 done_url = f"{host}/jobs/{SELF['current_job']}/inputs/done"
    #                 async with session.post(done_url) as response:
    #                     response.raise_for_status()
    #                 success = True
    #                 break
    #             except Exception as e:
    #                 msg = f"Failed to transfer inputs to node {host}: {e}"
    #                 logger.log(msg, severity="WARNING")
    #         if not success:
    #             raise e
    #     logger.log(f"Successfully transferred remaining inputs to node {node.get('instance_name')}")


@router.post("/jobs/{job_id}")
def execute(
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
        cluster_python_versions = list(set([e.python_version for e in SELF["workers"]]))
        cluster_python_versions_msg = ", ".join(cluster_python_versions[:-1])
        cluster_python_versions_msg += f", and {cluster_python_versions[-1]}"
        msg += f"containers in the cluster are running: {cluster_python_versions_msg}.\n"
        msg += "To fix this you can either:\n"
        msg += f" - update the cluster to run containers with python v{user_python_version}\n"
        msg += f" - update your local python version to be one of {cluster_python_versions}"
        return Response(msg, status_code=409)

    # call workers concurrently
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

    async def assign_workers(workers):
        async with aiohttp.ClientSession() as session:
            tasks = [assign_worker(session, worker) for worker in workers]
            results = await asyncio.gather(*tasks)
            return [worker for worker in results if worker is not None]

    successfully_assigned_workers = asyncio.run(assign_workers(workers_to_keep))
    if len(successfully_assigned_workers) == 0:
        raise Exception("Failed to assign job to any workers")

    logger.log(f"Successfully assigned to {len(successfully_assigned_workers)} workers.")

    SELF["workers"] = workers_to_keep
    remove_workers = lambda workers_to_remove: [w.remove() for w in workers_to_remove]
    add_background_task(remove_workers, workers_to_remove)

    SELF["job_watcher_stop_event"].clear()  # is initalized as set by default
    args = (request_json["n_inputs"], is_background_job)
    SELF["job_watcher_thread"] = Thread(target=job_watcher_logged, args=args)
    SELF["job_watcher_thread"].start()
