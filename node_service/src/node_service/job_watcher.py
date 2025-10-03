import sys
import pickle
import traceback
import asyncio
import aiohttp
from time import time

from google.cloud import firestore
from google.cloud.firestore import FieldFilter, And, ArrayUnion
from google.cloud.firestore_v1.field_path import FieldPath
from google.cloud.firestore_v1.async_client import AsyncClient

from node_service import PROJECT_ID, SELF, INSTANCE_NAME, REINIT_SELF, ENV_IS_READY_PATH
from node_service.helpers import Logger, format_traceback
from node_service.lifecycle_endpoints import reboot_containers


CLIENT_DC_TIMEOUT_SEC = 5


async def get_neighboring_node(async_db):
    am_only_node_working_on_job = False
    status_filter = FieldFilter("status", "==", "RUNNING")
    job_filter = FieldFilter("current_job", "==", SELF["current_job"])
    base_query = async_db.collection("nodes").where(filter=And([status_filter, job_filter]))
    base_query = base_query.order_by(FieldPath.document_id())
    query = base_query.start_after({FieldPath.document_id(): INSTANCE_NAME}).limit(1)
    neighboring_node = await anext(query.stream(), None)
    if not neighboring_node:
        # means this ^ was either the only or last node, in this case get 0th node.
        neighboring_node = await anext(base_query.limit(1).stream())
        am_only_node_working_on_job = neighboring_node.id == INSTANCE_NAME
    if not am_only_node_working_on_job:
        return neighboring_node


async def get_inputs_from_neighbor(neighboring_node, session, logger, auth_headers):
    neighboring_node_host = neighboring_node.get("host") if neighboring_node else None

    if (not neighboring_node) or SELF["SHUTTING_DOWN"]:
        logger.log("No neighbors to ask for more inputs ... I am the only node.")
        return

    try:
        url = f"{neighboring_node_host}/jobs/{SELF['current_job']}/inputs"
        # must be close to SHUTTING_DOWN check \/
        async with session.get(url, timeout=2, headers=auth_headers) as response:
            logger.log("Asked neighboring node for more inputs ...")  # must log after get ^
            if response.status in [204, 404]:
                logger.log(f"{neighboring_node.id} doesn't have any extra inputs to give.")
                return
            elif response.status == 200:
                return pickle.loads(await response.read())
            else:
                msg = f"Error getting inputs from {neighboring_node.id}: {response.status}"
                logger.log(msg, "ERROR")
    except asyncio.TimeoutError:
        pass


async def result_check_all_workers(session: aiohttp.ClientSession, logger: Logger):
    async def _result_check_single_worker(worker):
        url = f"{worker.url}/jobs/{SELF['current_job']}/results"
        async with session.get(url) as http_response:
            if http_response.status != 200:
                return worker, http_response.status

            response_content = await http_response.content.read()
            response = pickle.loads(response_content)
            for result in response["results"]:
                SELF["results_queue"].put(result, len(result[2]))
                SELF["num_results_received"] += 1

            if response.get("udf_start_latency"):
                SELF["udf_start_latency"] = response["udf_start_latency"]
            if response.get("packages_to_install"):
                SELF["packages_to_install"] = response["packages_to_install"]
            if response.get("all_packages_installed"):
                SELF["all_packages_installed"] = response["all_packages_installed"]

            worker.is_idle = response["is_idle"]
            worker.is_empty = response["is_empty"]
            worker.currently_installing_package = response["currently_installing_package"]
            return worker, http_response.status

    return await asyncio.gather(*[_result_check_single_worker(w) for w in SELF["workers"]])


async def _job_watcher(
    n_inputs: int,
    is_background_job: bool,
    logger: Logger,
    auth_headers: dict,
    async_db: AsyncClient,
    session: aiohttp.ClientSession,
):
    sync_db = firestore.Client(project=PROJECT_ID, database="burla")
    job_doc = async_db.collection("jobs").document(SELF["current_job"])
    node_doc = async_db.collection("nodes").document(INSTANCE_NAME)
    node_docs_collection = job_doc.collection("assigned_nodes")
    node_doc = node_docs_collection.document(INSTANCE_NAME)
    await node_doc.set({"current_num_results": 0})

    JOB_FAILED = False
    JOB_CANCELED = False
    LAST_CLIENT_PING_TIMESTAMP = time()
    neighboring_node = None
    neighbor_had_no_inputs_at = None
    seconds_neighbor_had_no_inputs = 0

    def _on_job_snapshot(doc_snapshot, changes, read_time):
        nonlocal LAST_CLIENT_PING_TIMESTAMP, JOB_FAILED, JOB_CANCELED
        LAST_CLIENT_PING_TIMESTAMP = time()
        for change in changes:
            if change.document.to_dict()["status"] == "FAILED":
                JOB_FAILED = True
                break
            elif change.document.to_dict()["status"] == "CANCELED":
                JOB_CANCELED = True
                break

    # Client intentionally updates the job doc every 2sec to signal that it's still listening.
    sync_job_doc = sync_db.collection("jobs").document(SELF["current_job"])
    job_watch = sync_job_doc.on_snapshot(_on_job_snapshot)

    all_workers_idle = False
    all_workers_empty = False
    while not SELF["job_watcher_stop_event"].is_set():
        await node_doc.update({"current_num_results": SELF["num_results_received"]})
        if all_workers_empty:
            await asyncio.sleep(0.2)

        # avoid race condition:
        if SELF["job_watcher_stop_event"].is_set():
            break

        # enqueue results from workers
        threshold = SELF["return_queue_ram_threshold_gb"]
        result_queue_not_too_big = SELF["results_queue"].size_gb < threshold

        if result_queue_not_too_big:
            workers_info = await result_check_all_workers(session, logger)
            SELF["current_parallelism"] = sum(not w.is_idle for w in SELF["workers"])
            SELF["currently_installing_package"] = SELF["workers"][0].currently_installing_package
            all_workers_empty = all(w.is_empty for w in SELF["workers"])
            failed = [f"{w.container_name}:{status}" for w, status in workers_info if status != 200]

            for worker, status in workers_info:
                if status == 500:
                    logs = worker.logs() if worker.exists() else "Unable to retrieve container logs"
                    error_title = f"Worker {worker.container_name} returned status 500!"
                    msg = f"{error_title} Logs from container:\n{logs.strip()}"
                    firestore_client = firestore.Client(project=PROJECT_ID, database="burla")
                    node_ref = firestore_client.collection("nodes").document(INSTANCE_NAME)
                    node_ref.collection("logs").document().set({"msg": msg, "ts": time()})

            if failed:
                logger.log(f"workers failed: {', '.join(failed)}", severity="ERROR")
                reboot_containers(logger=logger)
                break
        else:
            msg = f"Result queue is too big ({SELF['results_queue'].size_gb:.2f}GB)"
            logger.log(f"{msg}, skipping result check...")

        # attempt to send pending inputs to workers:
        if SELF["pending_inputs"]:
            SELF["pending_inputs"] = await send_inputs_to_workers(session, SELF["pending_inputs"])

        # has this node finished all it's inputs ?
        all_workers_idle_twice = all_workers_idle and SELF["current_parallelism"] == 0
        all_workers_idle = SELF["current_parallelism"] == 0
        no_pending_inputs = not SELF["pending_inputs"]
        finished_all_assigned_inputs = (
            all_workers_idle_twice and SELF["all_inputs_uploaded"] and no_pending_inputs
        )
        if finished_all_assigned_inputs:
            logger.log("Finished all inputs.")
            neighboring_node = await get_neighboring_node(async_db)
            new_inputs = await get_inputs_from_neighbor(
                neighboring_node, session, logger, auth_headers
            )
            if new_inputs:
                logger.log(f"Got {len(new_inputs)} more inputs from {neighboring_node.id}")
                neighbor_had_no_inputs_at = None
                seconds_neighbor_had_no_inputs = 0
                rejected_inputs = await send_inputs_to_workers(session, new_inputs)
                # rejected = no space to store
                if rejected_inputs:
                    # This is theoretically impossible because all nodes have the same
                    # IO queue memory limits, and this node's input queues must first be empty
                    # in order to attempt getting more inputs from another node.
                    # Therefore this node should always be able to fit 100% of another node's inputs
                    msg = "Recieved inputs from neighbor that I do not have space to store!"
                    raise Exception(msg)
            else:
                neighbor_had_no_inputs_at = neighbor_had_no_inputs_at or time()
                seconds_neighbor_had_no_inputs = time() - neighbor_had_no_inputs_at

        #  job ended ?
        job_is_done = False
        node_is_done = SELF["all_inputs_uploaded"] and all_workers_idle_twice
        node_is_done = node_is_done and SELF["results_queue"].empty()
        neighbor_is_done = (not neighboring_node) or (seconds_neighbor_had_no_inputs > 2)

        if node_is_done and neighbor_is_done:
            query_result = await node_docs_collection.sum("current_num_results").get()
            total_results = query_result[0][0].value
            job_snapshot = await job_doc.get()
            client_has_all_results = job_snapshot.to_dict()["client_has_all_results"]
            client_has_all_results = client_has_all_results or is_background_job
            job_is_done = total_results == n_inputs and client_has_all_results

        if job_is_done or JOB_FAILED or JOB_CANCELED:
            logger.log("Job has failed!" if JOB_FAILED else "Job is done!")
            # check again in case `job_is_done` then failed or canceled
            job_snapshot = await job_doc.get()
            JOB_FAILED = job_snapshot.to_dict()["status"] == "FAILED"
            JOB_CANCELED = job_snapshot.to_dict()["status"] == "CANCELED"
            if not (JOB_FAILED or JOB_CANCELED):
                try:
                    doc = {"status": "COMPLETED"}
                    if SELF["udf_start_latency"]:
                        doc["udf_start_latency"] = SELF["udf_start_latency"]
                    if SELF["packages_to_install"]:
                        doc["packages_to_install"] = SELF["packages_to_install"]
                    await job_doc.update(doc)
                except Exception:
                    # ignore because this can get hit by like 100's of nodes at once
                    # one of them will succeed and the others will throw errors we can ignore.
                    pass
            else:
                doc = {}
                if SELF["udf_start_latency"]:
                    doc["udf_start_latency"] = SELF["udf_start_latency"]
                if SELF["packages_to_install"]:
                    doc["packages_to_install"] = SELF["packages_to_install"]
                if doc:
                    await job_doc.update(doc)
            await restart_workers(session, logger, async_db)
            break

        # client still listening? (if this is NOT a background job)
        seconds_since_last_ping = time() - LAST_CLIENT_PING_TIMESTAMP
        client_disconnected = seconds_since_last_ping > CLIENT_DC_TIMEOUT_SEC
        if not is_background_job and client_disconnected:
            # check again (synchronously) because sometimes the ping watcher thread is starved.
            sync_job_doc = sync_db.collection("jobs").document(SELF["current_job"])
            last_ping_timestamp = sync_job_doc.get().to_dict()["last_ping_from_client"]
            client_disconnected = time() - last_ping_timestamp > CLIENT_DC_TIMEOUT_SEC
            if client_disconnected:
                msg = f"No client ping in the last {CLIENT_DC_TIMEOUT_SEC}s, "
                msg += "setting job status to FAILED"
                logger.log(msg)
                try:
                    msg = f"job watcher ({INSTANCE_NAME}) hasn't had client ping in the last "
                    msg += f"{CLIENT_DC_TIMEOUT_SEC}s"
                    await job_doc.update({"status": "FAILED", "fail_reason": ArrayUnion([msg])})
                except Exception:
                    # ignore because this can get hit by like 100's of nodes at once
                    # one of them will succeed and the others will throw errors we can ignore.
                    pass
                await restart_workers(session, logger, async_db)
                break

    if not is_background_job:
        job_watch.unsubscribe()


async def job_watcher_logged(n_inputs: int, is_background_job: bool, auth_headers: dict):
    logger = Logger()  # new logger has no request attached like the one in execute job did.

    async with aiohttp.ClientSession() as session:
        try:
            async_db = AsyncClient(project=PROJECT_ID, database="burla")
            await _job_watcher(n_inputs, is_background_job, logger, auth_headers, async_db, session)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
            traceback_str = format_traceback(tb_details)
            logger.log(str(e), "ERROR", traceback=traceback_str)


async def reinit_node(assigned_workers: list, async_db: AsyncClient):
    # important to delete or workers wont install packages
    ENV_IS_READY_PATH.unlink(missing_ok=True)

    # reset per-job fields on preserved workers to avoid leaking prior job state
    for w in SELF["workers"]:
        w.packages_to_install = None
        w.is_idle = False
        w.is_empty = False

    current_container_config = SELF["current_container_config"]
    current_workers = assigned_workers + SELF["idle_workers"]
    authorized_users = SELF["authorized_users"]
    REINIT_SELF(SELF)
    SELF["current_container_config"] = current_container_config
    SELF["workers"] = current_workers
    SELF["authorized_users"] = authorized_users
    node_doc = async_db.collection("nodes").document(INSTANCE_NAME)
    await node_doc.update({"status": "READY"})


async def restart_workers(session: aiohttp.ClientSession, logger: Logger, async_db: AsyncClient):
    async def _restart_single_worker(worker):
        try:
            async with session.get(f"{worker.url}/restart", timeout=1):
                pass
        except Exception:
            # worker service kills itself in /restart and is restarted by container script
            # -> why we don't check for a 200 response and expect a disconnect error.
            pass

        async def _wait_til_worker_ready(attempt=0):
            try:
                async with session.get(f"{worker.url}/", timeout=1) as response:
                    status = response.status
            except Exception:
                status = None

            if status == 200:
                return worker
            elif attempt > 20:
                worker.log_debug_info()
                raise Exception(f"Worker {worker.container_name} not ready after 10s")
            else:
                await asyncio.sleep(0.5)
                return await _wait_til_worker_ready(attempt + 1)

        return await _wait_til_worker_ready()

    try:
        tasks = [_restart_single_worker(w) for w in SELF["workers"]]
        restarted_workers = await asyncio.gather(*tasks)
    except Exception as e:
        logger.log(f"Error restarting workers: {e}", severity="ERROR")
        logger.log("Some workers failed to restart, rebooting containers ...")
        reboot_containers(logger=logger)
    else:
        await reinit_node(restarted_workers, async_db)


async def send_inputs_to_workers(session: aiohttp.ClientSession, inputs_pkl_with_idx: list):
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

    # send batches to workers
    async def _upload_to_single_worker(session, url, batch):
        data = aiohttp.FormData()
        data.add_field("inputs_pkl_with_idx", pickle.dumps(batch))
        async with session.post(url, data=data) as response:
            if response.status == 409:
                return batch
            elif response.status != 200:
                response.raise_for_status()
            return []

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

    # input batch rejected if worker has no memory available to store it.
    rejected_batches = await asyncio.gather(*tasks)
    rejected_inputs_pkl_with_idx = [input for batch in rejected_batches for input in batch]
    return rejected_inputs_pkl_with_idx
