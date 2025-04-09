import sys
import pickle
from time import time, sleep
from threading import Thread, Event
from typing import Optional, Callable
import traceback
import asyncio
import aiohttp
from io import BytesIO
from fastapi import APIRouter, Path, Depends, Response, Request
from fastapi.responses import StreamingResponse
from google.cloud import firestore
from google.cloud.firestore import FieldFilter
import requests

from node_service import (
    PROJECT_ID,
    SELF,
    INSTANCE_NAME,
    get_request_json,
    get_logger,
    get_request_files,
    get_add_background_task_function,
)
from node_service.reboot_endpoints import reboot_containers
from node_service.helpers import Logger, format_traceback
from node_service.worker import Worker

router = APIRouter()


async def _result_check_single_worker(session, worker, logger):
    async with session.get(f"{worker.url}/jobs/{SELF['current_job']}/results") as response:
        if response.status != 200:
            return worker, response.status

        response_pkl = b"".join([c async for c in response.content.iter_chunked(8192)])
        response = pickle.loads(response_pkl)
        # msg = f"Received {len(response['results'])} results from {worker.container_name} "
        # logger.log(msg + f"({len(response_pkl)} bytes)")

        for result in response["results"]:
            SELF["results_queue"].put(result)

        worker.is_idle = response["is_idle"]
        return worker, response.status


async def _result_check_all_workers(logger):
    async with aiohttp.ClientSession() as session:
        tasks = [_result_check_single_worker(session, w, logger) for w in SELF["workers"]]
        return await asyncio.gather(*tasks)


def _job_watcher(is_background_job: bool, logger: Logger):
    db = firestore.Client(project=PROJECT_ID, database="burla")
    job_doc = db.collection("jobs").document(SELF["current_job"])
    LAST_CLIENT_PING_TIMESTAMP = time()

    def _on_job_snapshot(doc_snapshot, changes, read_time):
        nonlocal LAST_CLIENT_PING_TIMESTAMP
        LAST_CLIENT_PING_TIMESTAMP = time()

    if not is_background_job:
        # Client intentionally updates the job doc every 2sec to signal that it's still listening.
        job_watch = job_doc.on_snapshot(_on_job_snapshot)

    all_workers_idle = False
    while not SELF["job_watcher_stop_event"].is_set():
        sleep(0.4)

        # enqueue results from workers
        workers_info = asyncio.run(_result_check_all_workers(logger))
        SELF["current_parallelism"] = sum([not w.is_idle for w in SELF["workers"]])
        failed_workers = [f"{w.container_name}: {rs}" for w, rs in workers_info if rs != 200]
        if failed_workers:
            # TODO: if one worker dies, don't kill the entire job
            logger.log(f"REBOOTING, result-check failed for workers: {', '.join(failed_workers)}")
            break

        # has this node finished all it's assigned inputs ?
        successfully_retreived_more_inputs = False
        all_workers_idle_twice = all_workers_idle and SELF["current_parallelism"] == 0
        all_workers_idle = SELF["current_parallelism"] == 0
        node_finished_all_inputs = SELF["workers_have_all_inputs"] and all_workers_idle_twice
        if node_finished_all_inputs:
            # try to grab more inputs from other nodes
            logger.log("Finished all assigned inputs, asking other nodes for more inputs ...")
            status_filter = FieldFilter("status", "==", "RUNNING")
            job_filter = FieldFilter("current_job", "==", SELF["current_job"])
            for node in db.collection("nodes").where(filter=status_filter & job_filter).stream():
                if node.id == INSTANCE_NAME:
                    continue
                response = requests.get(f"{node['host']}/jobs/{SELF['current_job']}/inputs")
                response.raise_for_status()
                if response.status_code == 204:
                    continue
                new_inputs = pickle.loads(response.content)
                for input_pkl in new_inputs:
                    SELF["inputs_queue"].put(input_pkl)
                logger.log(f"Queue empty, got {len(new_inputs)} more inputs from {node.id}")
                successfully_retreived_more_inputs = True

        # has the entire job ended ?
        if node_finished_all_inputs and not successfully_retreived_more_inputs:
            # Mark current node as done
            # must use separate node doc because OG node doc is cleared on reboot.
            job_nodes = job_doc.collection("assigned_nodes")
            job_nodes.document(INSTANCE_NAME).set({"is_done": True})
            logger.log(f"Node {INSTANCE_NAME} is DONE executing job {SELF['current_job']}")
            # Check if all nodes are done, mark entire job as DONE if so
            filter = FieldFilter("is_done", "==", False)
            all_nodes_done = not list(job_nodes.where(filter=filter).limit(1).stream())
            if all_nodes_done:
                logger.log(f"All nodes done, marking job {SELF['current_job']} as DONE")
                job_doc.update({"status": "COMPLETED"})
            break

        # client still listening? (if this is NOT a background job)
        seconds_since_last_ping = time() - LAST_CLIENT_PING_TIMESTAMP
        client_disconnected = seconds_since_last_ping > 2.5
        if not is_background_job and client_disconnected:
            logger.log(f"No client ping in the last {seconds_since_last_ping}s, REBOOTING")
            break

    if not is_background_job:
        job_watch.unsubscribe()
    reboot_containers(logger=logger)


def job_watcher_logged(is_background_job: bool):
    logger = Logger()
    try:
        _job_watcher(is_background_job, logger)
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
        traceback_str = format_traceback(tb_details)
        logger.log(str(e), "ERROR", traceback=traceback_str)


@router.post("/jobs/{job_id}/inputs/done")
def notify_all_inputs_forwarded(job_id: str = Path(...)):
    # Tells node that no more inputs will be arriving.
    # This is important because it allows the node to figure out when it can restart.
    SELF["workers_have_all_inputs"] = True


@router.get("/jobs/{job_id}/inputs")
async def get_half_inputs(job_id: str = Path(...), logger: Logger = Depends(get_logger)):

    async def _get_half_inputs_from_worker(session, worker, job_id):
        async with session.get(f"{worker.url}/jobs/{job_id}/inputs") as response:
            if response.status == 204:
                return []
            else:
                response.raise_for_status()
                return pickle.loads(await response.read())

    inputs = []
    async with aiohttp.ClientSession() as session:
        tasks = [_get_half_inputs_from_worker(session, w, job_id) for w in SELF["workers"]]
        for half_of_worker_inputs in await asyncio.gather(*tasks):
            inputs.extend(half_of_worker_inputs)

    if not inputs:
        return Response(status_code=204)
    else:
        data = BytesIO(pickle.dumps(inputs))
        data.seek(0)  # ensure file pointer is at the beginning of the file.
        headers = {"Content-Disposition": 'attachment; filename="inputs.pkl"'}
        return StreamingResponse(data, media_type="application/octet-stream", headers=headers)


@router.post("/jobs/{job_id}/inputs")
async def upload_inputs(
    job_id: str = Path(...),
    request_files: Optional[dict] = Depends(get_request_files),
    logger: Logger = Depends(get_logger),
):
    if SELF["SHUTTING_DOWN"]:
        return Response("Node is shutting down, inputs not accepted.", status_code=409)

    # needs to be here so this is reset when transferring from another dying node
    SELF["workers_have_all_inputs"] = False
    SELF["current_input_batch_forwarded"] = False

    # separate into batches to send to each worker
    input_batches = []
    inputs_pkl_with_idx = pickle.loads(request_files["inputs_pkl_with_idx"])
    batch_size = len(inputs_pkl_with_idx) // len(SELF["workers"])
    extra = len(inputs_pkl_with_idx) % len(SELF["workers"])
    start = 0
    for i in range(len(SELF["workers"])):
        end = start + batch_size + (1 if i < extra else 0)
        batch = inputs_pkl_with_idx[start:end]
        if batch:
            input_batches.append(batch)
        start = end
    assert sum(len(batch) for batch in input_batches) == len(inputs_pkl_with_idx)

    # send batches to workers
    async def _upload_to_single_worker(session, url, batch):
        data = aiohttp.FormData()
        data.add_field("inputs_pkl_with_idx", pickle.dumps(batch))
        async with session.post(url, data=data) as response:
            response.raise_for_status()

    async with aiohttp.ClientSession() as session:
        tasks = []
        for batch in input_batches:
            # update index so input distribution is even
            if SELF["index_of_last_worker_given_inputs"] == len(SELF["workers"]) - 1:
                SELF["index_of_last_worker_given_inputs"] = 0
                current_worker_index = 0
            else:
                SELF["index_of_last_worker_given_inputs"] += 1
                current_worker_index = SELF["index_of_last_worker_given_inputs"]
            # send batch to worker
            current_worker = SELF["workers"][current_worker_index]
            url = f"{current_worker.url}/jobs/{job_id}/inputs"
            tasks.append(_upload_to_single_worker(session, url, batch))
        await asyncio.gather(*tasks)

    SELF["current_input_batch_forwarded"] = True


@router.get("/jobs/{job_id}/results")
def get_results(job_id: str = Path(...), logger: Logger = Depends(get_logger)):
    results = []
    while not SELF["results_queue"].empty():
        results.append(SELF["results_queue"].get())

    response = {"results": results, "current_parallelism": SELF["current_parallelism"]}
    data = BytesIO(pickle.dumps(response))
    data.seek(0)  # ensure file pointer is at the beginning of the file.
    headers = {"Content-Disposition": 'attachment; filename="results.pkl"'}
    return StreamingResponse(data, media_type="application/octet-stream", headers=headers)


@router.post("/shutdown")
async def shutdown_node(request: Request, logger: Logger = Depends(get_logger)):
    # Only allow shutdown requests from localhost (inside the shutdown script defined in main_svc)
    if request.client.host != "127.0.0.1":
        return Response("Shutdown endpoint can only be called from localhost", status_code=403)

    SELF["SHUTTING_DOWN"] = True
    logger.log(f"Received shutdown request for node {INSTANCE_NAME}.")

    url = "http://metadata.google.internal/computeMetadata/v1/instance/preempted"
    async with aiohttp.ClientSession(headers={"Metadata-Flavor": "Google"}) as session:
        async with session.get(url, timeout=2) as response:
            response.raise_for_status()
            preempted = await response.text().strip() == "TRUE"

    db = firestore.Client(project=PROJECT_ID, database="burla")
    node_doc = db.collection("nodes").document(INSTANCE_NAME)
    node_doc.update({"status": "DELETED", "preempted": preempted})

    # before transferring inputs, wait for curent batch to finish uploading:
    # It's really important the client and node-service are on the same page.
    # which is why we don't just stop it and take the inputs that are there.
    if SELF["current_input_batch_forwarded"] == False:
        start_time = time()
        while not SELF["current_input_batch_forwarded"]:
            if time() - start_time > 10:
                raise Exception("Timeout waiting for input batch to be forwarded (>10 seconds)")
            sleep(0.1)

    if SELF["current_job"]:
        # send remaining inputs to another node
        status_filter = FieldFilter("status", "==", "RUNNING")
        job_filter = FieldFilter("current_job", "==", SELF["current_job"])
        query = db.collection("nodes").where(filter=status_filter & job_filter)

        async def transfer_inputs(worker: Worker, node_url: str, session: aiohttp.ClientSession):
            worker_url = f"{worker.url}/jobs/{SELF['current_job']}/transfer_inputs"
            json = {"target_node_url": node_url}
            async with session.post(worker_url, json=json) as response:
                response.raise_for_status()

        async with aiohttp.ClientSession() as session:
            success = False
            for node in query.stream():
                try:
                    tasks = [transfer_inputs(w, node["host"], session) for w in SELF["workers"]]
                    await asyncio.gather(*tasks)
                    success = True
                    break
                except Exception as e:
                    msg = f"Failed to transfer inputs to node {node['host']}: {e}"
                    logger.log(msg, severity="WARNING")
            if not success:
                raise e
        logger.log(f"Successfully transferred remaining inputs to node {node['instance_name']}")


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
    function_pkl = (request_files or {}).get("function_pkl")
    db = firestore.Client(project=PROJECT_ID, database="burla")
    node_doc = db.collection("nodes").document(INSTANCE_NAME)
    node_doc.update({"status": "RUNNING", "current_job": job_id})

    # permanently associate this node to the job document (`current_job` is cleared later)
    job_doc = db.collection("jobs").document(job_id)
    job_node_doc = job_doc.collection("assigned_nodes").document(INSTANCE_NAME)
    job_node_doc.set({"is_done": False})

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
    async def assign_worker(session, url):
        async with session.post(url, data={"function_pkl": function_pkl}) as response:
            if response.status == 200:
                return url
            else:
                logger.log(f"Worker {url} returned error: {response.status}", severity="WARNING")
                return None

    async def assign_workers(workers):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for worker in workers:
                url = f"{worker.url}/jobs/{job_id}"
                tasks.append(assign_worker(session, url))
            results = await asyncio.gather(*tasks)
            return [url for url in results if url]

    successful_worker_urls = asyncio.run(assign_workers(workers_to_keep))
    logger.log(f"Successfully assigned to {len(successful_worker_urls)} workers.")

    SELF["workers"] = workers_to_keep
    remove_workers = lambda workers_to_remove: [w.remove() for w in workers_to_remove]
    add_background_task(remove_workers, workers_to_remove)

    SELF["job_watcher_stop_event"].clear()  # is initalized as set by default
    job_watcher_thread = Thread(target=job_watcher_logged, args=(is_background_job,), daemon=True)
    job_watcher_thread.start()
