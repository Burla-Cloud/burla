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

EMPTY_NEIGHBOR_TIMEOUT_SEC = 2 * 60
BYTES_PER_GB = 1024**3
CLIENT_CONTACT_TIMEOUT_SEC = 5


async def get_inputs_from_neighbor(
    neighboring_node, target_reply_size, session, logger, auth_headers
):
    instance_name = neighboring_node["instance_name"]
    try:
        url = f"{neighboring_node['host']}/jobs/{SELF['current_job']}/inputs"
        url += f"?target_reply_size={target_reply_size}"
        # must be close to SHUTTING_DOWN check \/
        async with session.get(url, timeout=2, headers=auth_headers) as response:
            # logger.log("Asked neighboring node for more inputs ...")  # must log after get ^
            if response.status in [204, 404]:
                # logger.log(f"{instance_name} doesn't have any extra inputs to give.")
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
    job_started_at: float,
    logger: Logger,
    auth_headers: dict,
    async_db: AsyncClient,
    session: aiohttp.ClientSession,
):
    sync_db = firestore.Client(project=PROJECT_ID, database="burla")
    job_doc = async_db.collection("jobs").document(SELF["current_job"])
    node_docs_collection = job_doc.collection("assigned_nodes")
    node_doc = node_docs_collection.document(INSTANCE_NAME)
    await node_doc.set({"current_num_results": 0, "client_contact_last_1s": True})

    JOB_FAILED = False
    JOB_CANCELED = False
    neighboring_nodes = []
    neighbor_had_no_inputs_at = None
    seconds_neighbor_had_no_inputs = 0

    def _on_job_snapshot(doc_snapshot, changes, read_time):
        global JOB_FAILED, JOB_CANCELED
        for change in changes:
            job_dict = change.document.to_dict()
            if job_dict["all_inputs_uploaded"] == True:
                SELF["all_inputs_uploaded"] = True
            if job_dict["status"] == "FAILED":
                sleep(2)  # give worker a sec to put error result in result queue
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
    neighbor_had_no_inputs_at = None
    last_results_update_time = time()
    last_reported_result_count = 0
    last_reported_client_contact_last_1s = True
    while not SELF["job_watcher_stop_event"].is_set():
        if all_workers_empty:
            if (time() - job_started_at) < 7:
                await asyncio.sleep(0.02)
            else:
                await asyncio.sleep(0.2)

        # avoid race condition:
        if SELF["job_watcher_stop_event"].is_set():
            break

        # enqueue results from workers (if there is space in mem)
        threshold = SELF["return_queue_ram_threshold_gb"]
        result_queue_full = SELF["results_queue"].size_gb > threshold
        if not result_queue_full:
            try:
                tasks = [load_results_from_worker(w, session) for w in SELF["workers"]]
                await asyncio.gather(*tasks)
            except Exception as e:
                msg = f"Failed to collect results from workers, rebooting...\n"
                logger.log(msg + f"{e}\n{traceback.format_exc()}", severity="ERROR")
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

        input_queue_empty = not SELF["pending_inputs"]
        current_num_results = SELF["num_results_received"]
        results_changed = current_num_results != last_reported_result_count
        seconds_since_results_update = time() - last_results_update_time
        if input_queue_empty and results_changed:
            await node_doc.update({"current_num_results": current_num_results})
            last_results_update_time = time()
            last_reported_result_count = current_num_results
        elif (not input_queue_empty) and seconds_since_results_update > 2:
            await node_doc.update({"current_num_results": current_num_results})
            last_results_update_time = time()
            last_reported_result_count = current_num_results

        # is client connected?
        client_disconnected = False
        sec_since_last_request = time() - SELF["last_request_timestamp"]
        client_contact_last_1s = sec_since_last_request < CLIENT_CONTACT_TIMEOUT_SEC
        client_contact_last_1s = client_contact_last_1s or SELF["active_client_request_count"] > 0
        if client_contact_last_1s != last_reported_client_contact_last_1s:
            await node_doc.update({"client_contact_last_1s": client_contact_last_1s})
            last_reported_client_contact_last_1s = client_contact_last_1s
        if not client_contact_last_1s:
            node_dicts = [d.to_dict() for d in await node_docs_collection.get()]
            client_disconnected = not any([d["client_contact_last_1s"] for d in node_dicts])
        must_be_connected = not is_background_job or not SELF["all_inputs_uploaded"]
        if client_disconnected and must_be_connected:
            JOB_FAILED = True
            await job_doc.update({"status": "FAILED", "fail_reason": ArrayUnion(["Client DC"])})
            logger.log(f"Client disconnected!")

        # is this node done working ?
        # TODO: Should get more inputs from neighbor whenever more than a couple workers dont
        # have inputs instead of only when none have inputs
        all_local_work_complete = False
        all_workers_idle = SELF["current_parallelism"] == 0
        all_inputs_sent_to_workers = SELF["all_inputs_uploaded"] and (not SELF["pending_inputs"])
        all_inputs_processed = all_workers_idle and all_inputs_sent_to_workers
        node_results_queue_empty = SELF["results_queue"].empty()
        workers_results_queues_empty = all(w.is_empty for w in SELF["workers"])
        no_buffered_results = node_results_queue_empty and workers_results_queues_empty
        if all_inputs_processed:
            if time() - job_started_at > 10:  # allows short jobs to finish faster.
                new_inputs = []
                neighboring_nodes = await get_neighboring_nodes(async_db)
                if neighboring_nodes and not SELF["SHUTTING_DOWN"]:
                    worker_input_queue_size_limit_gb = SELF["return_queue_ram_threshold_gb"] / 4
                    worker_input_queue_size_limit_bytes = (
                        worker_input_queue_size_limit_gb * BYTES_PER_GB
                    )
                    total_free_input_space = worker_input_queue_size_limit_bytes * len(
                        SELF["workers"]
                    )
                    args = (
                        neighboring_nodes[0].to_dict(),
                        int(total_free_input_space),
                        session,
                        logger,
                        auth_headers,
                    )
                    new_inputs = await get_inputs_from_neighbor(*args)
                if new_inputs:
                    logger.log(f"Got {len(new_inputs)} more inputs from {neighboring_nodes[0].id}")
                    neighbor_had_no_inputs_at = None
                    seconds_neighbor_had_no_inputs = 0
                    rejected_inputs = await send_inputs_to_workers(session, new_inputs)
                    if rejected_inputs:
                        SELF["pending_inputs"] = rejected_inputs + SELF["pending_inputs"]
                        msg = f"Queued {len(rejected_inputs)} borrowed inputs for retry."
                        logger.log(msg, severity="WARNING")
                else:
                    neighbor_had_no_inputs_at = neighbor_had_no_inputs_at or time()
                    seconds_neighbor_had_no_inputs = time() - neighbor_had_no_inputs_at
                    if seconds_neighbor_had_no_inputs > EMPTY_NEIGHBOR_TIMEOUT_SEC:
                        if no_buffered_results:
                            msg = f"Neighbor had no extra inputs for {EMPTY_NEIGHBOR_TIMEOUT_SEC//60}"
                            logger.log(f"{msg} minutes, done working on job!")
                            await restart_workers(session, logger, async_db)
                            break
                        logger.log(
                            "Neighbor timeout reached but waiting for buffered results to drain."
                        )
            not_waiting_on_client = SELF["results_queue"].empty() or client_disconnected
            all_local_work_complete = all_inputs_processed and no_buffered_results and not_waiting_on_client

        # job over ?
        job_completed = False
        if all_local_work_complete and client_disconnected:
            node_docs = await node_docs_collection.get()
            job_completed = n_inputs == sum([d.to_dict()["current_num_results"] for d in node_docs])
        elif all_local_work_complete:
            job_completed = (await job_doc.get()).to_dict()["client_has_all_results"]
        if job_completed or JOB_FAILED or JOB_CANCELED:
            status = sync_job_doc.get().to_dict()["status"]
            status = status if status in ["FAILED", "CANCELED"] else "COMPLETED"
            logger.log(f"Job is {status}! (id={SELF['current_job']})")
            try:
                sync_job_doc.update(
                    {
                        "udf_start_latency": SELF.get("udf_start_latency"),
                        "status": status,
                    }
                )
            except Exception:
                # ignore because this can get hit by like 100's of nodes at once
                # one of them will succeed and the others will throw errors we can ignore.
                pass

            await restart_workers(session, logger, async_db)
            break

    job_watch.unsubscribe()


async def job_watcher_logged(
    n_inputs: int, is_background_job: bool, job_started_at: float, auth_headers: dict
):
    logger = Logger()  # new logger has no request attached like the one in execute job did.

    async with aiohttp.ClientSession() as session:
        try:
            async_db = AsyncClient(project=PROJECT_ID, database="burla")
            await _job_watcher(
                n_inputs, is_background_job, job_started_at, logger, auth_headers, async_db, session
            )
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
    restart_wait_seconds = 20
    restart_poll_interval_seconds = 0.5
    max_restart_attempts = int(restart_wait_seconds / restart_poll_interval_seconds)

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
            elif attempt > max_restart_attempts:
                worker.log_debug_info()
                raise Exception(
                    f"Worker {worker.container_name} not ready after {restart_wait_seconds}s"
                )
            else:
                await asyncio.sleep(restart_poll_interval_seconds)
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
