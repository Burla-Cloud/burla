import sys
import pickle
import traceback
import asyncio
import aiohttp
import requests
from time import time, sleep

from google.cloud import firestore
from google.cloud.firestore import FieldFilter, And
from google.cloud.firestore_v1 import aggregation
from google.cloud.firestore_v1.field_path import FieldPath

from node_service import PROJECT_ID, SELF, INSTANCE_NAME
from node_service.reboot_endpoints import reboot_containers
from node_service.helpers import Logger, format_traceback


async def _result_check_single_worker(session, worker, logger):
    async with session.get(f"{worker.url}/jobs/{SELF['current_job']}/results") as http_response:
        if http_response.status != 200:
            return worker, http_response.status

        response_pkl = b"".join([c async for c in http_response.content.iter_chunked(8192)])
        response = pickle.loads(response_pkl)
        # msg = f"Received {len(response['results'])} results from {worker.container_name} "
        # logger.log(msg + f"({len(response_pkl)} bytes)")

        for result in response["results"]:
            SELF["results_queue"].put(result)
            SELF["num_results_received"] += 1

        worker.is_idle = response["is_idle"]
        return worker, http_response.status


async def _result_check_all_workers(logger):
    async with aiohttp.ClientSession() as session:
        tasks = [_result_check_single_worker(session, w, logger) for w in SELF["workers"]]
        return await asyncio.gather(*tasks)


def _get_neighboring_node(db, job_id):
    am_only_node_working_on_job = False
    status_filter = FieldFilter("status", "==", "RUNNING")
    job_filter = FieldFilter("current_job", "==", SELF["current_job"])
    base_query = db.collection("nodes").where(filter=And([status_filter, job_filter]))
    base_query = base_query.order_by(FieldPath.document_id())
    query = base_query.start_after({FieldPath.document_id(): INSTANCE_NAME}).limit(1)
    neighboring_node = next(query.stream(), None)
    if not neighboring_node:
        # means this ^ was either the only or last node, in this case get 0th node.
        neighboring_node = next(base_query.limit(1).stream())
        am_only_node_working_on_job = neighboring_node.id == INSTANCE_NAME
    if not am_only_node_working_on_job:
        return neighboring_node


def _job_watcher(n_inputs: int, is_background_job: bool, logger: Logger):
    db = firestore.Client(project=PROJECT_ID, database="burla")
    job_doc = db.collection("jobs").document(SELF["current_job"])
    node_doc = db.collection("nodes").document(INSTANCE_NAME)
    node_doc.update({"status": "RUNNING", "current_job": SELF["current_job"]})
    node_docs_collection = job_doc.collection("assigned_nodes")
    node_doc = node_docs_collection.document(INSTANCE_NAME)
    node_doc.set({"current_num_results": 0})

    LAST_CLIENT_PING_TIMESTAMP = time()
    neighboring_node = None
    neighbor_had_no_inputs_at = None
    seconds_neighbor_had_no_inputs = 0

    def _on_job_snapshot(doc_snapshot, changes, read_time):
        nonlocal LAST_CLIENT_PING_TIMESTAMP
        LAST_CLIENT_PING_TIMESTAMP = time()

    if not is_background_job:
        # Client intentionally updates the job doc every 2sec to signal that it's still listening.
        job_watch = job_doc.on_snapshot(_on_job_snapshot)

    all_workers_idle = False
    start = time()
    while not SELF["job_watcher_stop_event"].is_set():
        elapsed_seconds = time() - start
        if elapsed_seconds > 5:
            sleep(0.2)
        elif elapsed_seconds > 30:
            sleep(1)

        node_doc.update({"current_num_results": SELF["num_results_received"]})

        if SELF["SHUTTING_DOWN"]:
            break

        # enqueue results from workers
        workers_info = asyncio.run(_result_check_all_workers(logger))
        SELF["current_parallelism"] = sum([not w.is_idle for w in SELF["workers"]])
        failed_workers = [f"{w.container_name}: {rs}" for w, rs in workers_info if rs != 200]
        if failed_workers:
            # TODO: if one worker dies, don't kill the entire job
            logger.log(f"REBOOTING, result-check failed for workers: {', '.join(failed_workers)}")
            break
        logger.log(f"checked workers, current_parallelism={SELF['current_parallelism']}")

        # has this node finished all it's inputs ?
        all_workers_idle_twice = all_workers_idle and SELF["current_parallelism"] == 0
        all_workers_idle = SELF["current_parallelism"] == 0
        finished_all_assigned_inputs = all_workers_idle_twice and SELF["all_inputs_uploaded"]

        # if finished_all_assigned_inputs:
        #     logger.log("Finished all inputs.")
        #     neighboring_node = _get_neighboring_node(db, SELF["current_job"])
        #     neighboring_node_host = neighboring_node.get("host") if neighboring_node else None

        #     if neighboring_node and not SELF["SHUTTING_DOWN"]:
        #         url = f"{neighboring_node_host}/jobs/{SELF['current_job']}/inputs"
        #         response = requests.get(url, timeout=1)  # <- must be close to SHUTTING_DOWN check
        #         logger.log("Asked neighboring node for more inputs ...")  # <- must log after get ^
        #         neighbor_has_no_inputs = response.status_code in [204, 404]

        #         if neighbor_has_no_inputs:
        #             neighbor_had_no_inputs_at = neighbor_had_no_inputs_at or time()
        #             seconds_neighbor_had_no_inputs = time() - neighbor_had_no_inputs_at
        #             logger.log(f"{neighboring_node.id} doesn't have any extra inputs to give.")
        #         else:
        #             try:
        #                 response.raise_for_status()
        #                 neighbor_had_no_inputs_at = None
        #                 seconds_neighbor_had_no_inputs = 0
        #                 new_inputs = pickle.loads(response.content)
        #                 asyncio.run(send_inputs_to_workers(new_inputs))
        #                 logger.log(f"Got {len(new_inputs)} more inputs from {neighboring_node.id}")
        #             except Exception as e:
        #                 logger.log(f"Error getting inputs from {neighboring_node.id}: {e}", "ERROR")
        #     else:
        #         logger.log("No neighbors to ask for more inputs ... I am the only node.")

        #  job ended ?
        job_is_done = False
        we_have_all_inputs = SELF["all_inputs_uploaded"]
        client_has_all_results = SELF["results_queue"].empty() or is_background_job
        node_is_done = we_have_all_inputs and all_workers_idle_twice and client_has_all_results

        # neighbor_is_done = (not neighboring_node) or (seconds_neighbor_had_no_inputs > 2)
        if node_is_done:  # and neighbor_is_done:
            total_results = node_docs_collection.sum("current_num_results").get()[0][0].value
            client_has_all_results = job_doc.get(["client_has_all_results"]) or is_background_job
            job_is_done = total_results == n_inputs and client_has_all_results

        if job_is_done:
            logger.log("Job is done, updating job status and rebooting ...")
            try:
                job_doc.update({"status": "COMPLETED"})
            except Exception:
                # ignore because this can get hit by like 100's of nodes at once
                # one of them will succeed and the others will throw errors we can ignore.
                pass
            break

        # client still listening? (if this is NOT a background job)
        seconds_since_last_ping = time() - LAST_CLIENT_PING_TIMESTAMP
        client_disconnected = seconds_since_last_ping > 4
        if not is_background_job and client_disconnected:
            job_doc.update({"status": "FAILED"})
            logger.log(f"No client ping in the last {seconds_since_last_ping}s, REBOOTING")
            break

    if not is_background_job:
        job_watch.unsubscribe()

    if not SELF["SHUTTING_DOWN"]:
        reboot_containers(logger=logger)

    # I can't seeem to get this thread to exit gracefully. FastAPI always prints "Background thread
    # did not exit" in the console because of this thread.
    # I know it is not coming from background tasks or shutdown_if_idle_for_too_long because I've
    # reproduced it consistently with both disabled.


def job_watcher_logged(*a, **kw):
    logger = Logger()
    try:
        _job_watcher(*a, **kw, logger=logger)
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
        traceback_str = format_traceback(tb_details)
        logger.log(str(e), "ERROR", traceback=traceback_str)


async def send_inputs_to_workers(inputs_pkl_with_idx: list):
    # separate into batches to send to each worker
    input_batches = []
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
            url = f"{current_worker.url}/jobs/{SELF['current_job']}/inputs"
            tasks.append(_upload_to_single_worker(session, url, batch))
        await asyncio.gather(*tasks)
