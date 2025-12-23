import sys
import pickle
import traceback
import asyncio
import aiohttp
from time import time, sleep

from google.cloud import firestore
from google.cloud.firestore import ArrayUnion
from google.cloud.firestore_v1.async_client import AsyncClient

from node_service import PROJECT_ID, SELF, INSTANCE_NAME, REINIT_SELF, ENV_IS_READY_PATH
from node_service.helpers import Logger, format_traceback
from node_service.lifecycle_endpoints import (
    reboot_containers,
    get_neighboring_nodes,
    load_results_from_worker,
)

FIRST_PING_TIMEOUT = 12
CLIENT_DC_TIMEOUT_SEC = 5


async def get_inputs_from_neighbor(neighboring_node, session, logger, auth_headers):
    instance_name = neighboring_node["instance_name"]
    try:
        url = f"{neighboring_node['host']}/jobs/{SELF['current_job']}/inputs"
        # must be close to SHUTTING_DOWN check \/
        async with session.get(url, timeout=2, headers=auth_headers) as response:
            logger.log("Asked neighboring node for more inputs ...")  # must log after get ^
            if response.status in [204, 404]:
                logger.log(f"{instance_name} doesn't have any extra inputs to give.")
                return
            elif response.status == 200:
                return pickle.loads(await response.read())
            else:
                msg = f"Error getting inputs from {instance_name}: {response.status}"
                logger.log(msg, "ERROR")
    except asyncio.TimeoutError:
        pass


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
    JOB_FAILED_TWO = False
    JOB_CANCELED = False
    LAST_CLIENT_PING_TIMESTAMP = None
    LAST_LAST_CLIENT_PING_TIMESTAMP = None
    watcher_start_time = time()
    neighboring_nodes = []
    neighbor_had_no_inputs_at = None
    seconds_neighbor_had_no_inputs = 0

    def _on_job_snapshot(doc_snapshot, changes, read_time):
        nonlocal LAST_CLIENT_PING_TIMESTAMP, JOB_FAILED, JOB_CANCELED
        for change in changes:
            job_dict = change.document.to_dict()
            LAST_CLIENT_PING_TIMESTAMP = job_dict.get("last_ping_from_client")
            if job_dict["status"] == "FAILED":
                JOB_FAILED = True
                break
            elif job_dict["status"] == "CANCELED":
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
            try:
                tasks = [load_results_from_worker(w, session) for w in SELF["workers"]]
                await asyncio.gather(*tasks)
            except Exception:
                logger.log(f"Some workers failed. Rebooting containers...", severity="ERROR")
                reboot_containers(logger=logger)
                break

            SELF["current_parallelism"] = sum(not w.is_idle for w in SELF["workers"])
            SELF["currently_installing_package"] = SELF["workers"][0].currently_installing_package
            all_workers_empty = all(w.is_empty for w in SELF["workers"])

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
            # logger.log("Finished all inputs.")
            neighboring_nodes = await get_neighboring_nodes(async_db)
            new_inputs = []
            if neighboring_nodes and not SELF["SHUTTING_DOWN"]:
                args = (neighboring_nodes[0].to_dict(), session, logger, auth_headers)
                new_inputs = await get_inputs_from_neighbor(*args)
            if new_inputs:
                logger.log(f"Got {len(new_inputs)} more inputs from {neighboring_nodes[0].id}")
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

        # job ended ?
        job_is_done = False
        node_is_done = SELF["all_inputs_uploaded"] and all_workers_idle_twice
        node_is_done = node_is_done and (SELF["results_queue"].empty() or is_background_job)
        neighbor_is_done = (not neighboring_nodes) or (seconds_neighbor_had_no_inputs > 2)

        if node_is_done and neighbor_is_done:
            query_result = await node_docs_collection.sum("current_num_results").get()
            total_results = query_result[0][0].value
            job_snapshot = await job_doc.get()
            all_inputs_processed = total_results == n_inputs
            client_has_all_results = job_snapshot.to_dict()["client_has_all_results"]
            # used to make sure we don't wait for disconnected client to grab results:
            not_waiting_for_client = client_disconnected and is_background_job
            job_is_done = all_inputs_processed and (
                client_has_all_results or not_waiting_for_client
            )

        if LAST_CLIENT_PING_TIMESTAMP and not LAST_LAST_CLIENT_PING_TIMESTAMP:
            seconds_since_watcher_start = time() - watcher_start_time
            logger.log(f"First ping recieved! Watcher started {seconds_since_watcher_start}s ago.")
            LAST_LAST_CLIENT_PING_TIMESTAMP = LAST_CLIENT_PING_TIMESTAMP

        if LAST_CLIENT_PING_TIMESTAMP and LAST_LAST_CLIENT_PING_TIMESTAMP:
            if LAST_CLIENT_PING_TIMESTAMP != LAST_LAST_CLIENT_PING_TIMESTAMP:
                ping_diff = LAST_CLIENT_PING_TIMESTAMP - LAST_LAST_CLIENT_PING_TIMESTAMP
                logger.log(f"Ping recieved at {time()}. Time between pings: {ping_diff}s")
                LAST_LAST_CLIENT_PING_TIMESTAMP = LAST_CLIENT_PING_TIMESTAMP

        # `not_waiting_for_client` used to make sure client has time to grab errors when failed.
        client_disconnected = False
        client_never_connected = None
        if LAST_CLIENT_PING_TIMESTAMP:
            seconds_since_last_ping = time() - LAST_CLIENT_PING_TIMESTAMP
            if seconds_since_last_ping > CLIENT_DC_TIMEOUT_SEC:
                # double check synchronously, sometimes the thread just didnt get enough attention:
                LAST_CLIENT_PING_TIMESTAMP = sync_job_doc.get().to_dict()["last_ping_from_client"]
                seconds_since_last_ping = time() - LAST_CLIENT_PING_TIMESTAMP
                client_disconnected = seconds_since_last_ping > CLIENT_DC_TIMEOUT_SEC
        else:
            seconds_since_watcher_start = time() - watcher_start_time
            client_never_connected = seconds_since_watcher_start > FIRST_PING_TIMEOUT

        if client_disconnected:
            msg = f"Client disconnected! Last ping recieved {seconds_since_last_ping}s ago."
            logger.log(msg)
        elif client_never_connected:
            msg = f"No ping from client after {FIRST_PING_TIMEOUT}s!"
            logger.log(msg)

        results_queue_empty = SELF["results_queue"].empty()
        not_waiting_for_client = results_queue_empty or client_disconnected

        if JOB_FAILED and not JOB_FAILED_TWO:
            # give worker a sec to put error result in result queue
            # then loop again to clear worker results again
            sleep(1)
            JOB_FAILED_TWO = True

        elif (job_is_done or JOB_FAILED_TWO or JOB_CANCELED) and not_waiting_for_client:
            if JOB_FAILED:
                logger.log(f"Job has failed! (id={SELF['current_job']})")
            else:
                logger.log(f"Job is done! (id={SELF['current_job']})")
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

        can_fail_from_client_dc = is_background_job and not SELF["all_inputs_uploaded"]
        can_fail_from_client_dc = can_fail_from_client_dc or not is_background_job

        # client still listening? (if this is NOT a background job)
        if (client_disconnected or client_never_connected) and can_fail_from_client_dc:
            try:
                msg = "client disconnected" if client_disconnected else "client never connected"
                msg = f"{msg}! ({INSTANCE_NAME})"
                await job_doc.update({"status": "FAILED", "fail_reason": ArrayUnion([msg])})
            except Exception:
                # ignore because this can get hit by like 100's of nodes at once
                # one of them will succeed and the others will throw errors we can ignore.
                pass
            await restart_workers(session, logger, async_db)
            break

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
            try:
                job_doc = async_db.collection("jobs").document(SELF["current_job"])
                await job_doc.update({"status": "FAILED", "fail_reason": ArrayUnion([str(e)])})
            except Exception:
                # ignore because this can get hit by like 100's of nodes at once
                # one of them will succeed and the others will throw errors we can ignore.
                pass
            await restart_workers(session, logger, async_db)


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
        # checks that PID's change to prevent scenario where:
        # /restart called
        # / called and returns 200, but service hasn't actually restarted yet
        # thinks it did restart and continues.
        async with session.get(f"{worker.url}/pid", timeout=1) as response:
            response_json = await response.json()
            PID_BEFORE_RESTART = response_json["pid"]

        try:
            async with session.get(f"{worker.url}/restart", timeout=1):
                pass
        except Exception:
            # worker service kills itself in /restart and is restarted by container script
            # -> why we don't check for a 200 response and expect a disconnect error.
            pass

        async def _wait_til_worker_ready(attempt=0):
            try:
                async with session.get(f"{worker.url}/pid", timeout=1) as response:
                    response_json = await response.json()
                    PID_AFTER_RESTART = response_json["pid"]
            except Exception:
                PID_AFTER_RESTART = PID_BEFORE_RESTART

            if PID_AFTER_RESTART != PID_BEFORE_RESTART:
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
