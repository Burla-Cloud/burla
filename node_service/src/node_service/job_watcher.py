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
from node_service.lifecycle_endpoints import reboot_containers, get_neighboring_nodes

EMPTY_NEIGHBOR_TIMEOUT_SEC = 60
CLIENT_CONTACT_TIMEOUT_SEC = 5
JOB_DOC_CONTACT_TIMEOUT_SEC = 4


async def get_inputs_from_neighbor(neighboring_node, session, logger, auth_headers):
    instance_name = neighboring_node["instance_name"]
    try:
        url = f"{neighboring_node['host']}/jobs/{SELF['current_job']}/inputs"
        async with session.get(url, timeout=2, headers=auth_headers) as response:
            if response.status in [204, 404]:
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
    sync_job_doc = sync_db.collection("jobs").document(SELF["current_job"])
    last_job_doc_update_time = sync_job_doc.get().update_time.timestamp()
    node_docs_collection = job_doc.collection("assigned_nodes")
    node_doc = node_docs_collection.document(INSTANCE_NAME)
    await node_doc.set({"current_num_results": 0, "client_contact_last_1s": True})

    JOB_FAILED = False
    JOB_CANCELED = False
    neighboring_nodes = []
    neighbor_had_no_inputs_at = None
    seconds_neighbor_had_no_inputs = 0

    def _on_job_snapshot(doc_snapshot, changes, read_time):
        nonlocal JOB_FAILED, JOB_CANCELED, last_job_doc_update_time
        for change in changes:
            last_job_doc_update_time = change.document.update_time.timestamp()
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

    # Client intentionally updates the job doc every few seconds to signal that it's still listening.
    job_watch = sync_job_doc.on_snapshot(_on_job_snapshot)

    last_results_update_time = time()
    last_reported_result_count = 0
    last_reported_client_contact_last_1s = True
    try:
        while not SELF["job_watcher_stop_event"].is_set():
            all_workers_idle = all(worker.is_idle for worker in SELF["workers"])
            input_queue_empty = SELF["inputs_queue"].empty()
            if input_queue_empty and all_workers_idle:
                if (time() - job_started_at) < 7:
                    await asyncio.sleep(0.02)
                else:
                    await asyncio.sleep(0.2)
            else:
                await asyncio.sleep(0.02)

            if SELF["job_watcher_stop_event"].is_set():
                break

            SELF["current_parallelism"] = sum(not worker.is_idle for worker in SELF["workers"])

            input_queue_empty = SELF["inputs_queue"].empty()
            all_workers_idle = SELF["current_parallelism"] == 0

            current_num_results = SELF["num_results_received"]
            results_changed = current_num_results != last_reported_result_count
            seconds_since_results_update = time() - last_results_update_time
            if input_queue_empty and results_changed:
                await node_doc.update({"current_num_results": current_num_results})
                last_results_update_time = time()
                last_reported_result_count = current_num_results
            elif (
                not input_queue_empty or not all_workers_idle
            ) and seconds_since_results_update > 2:
                await node_doc.update({"current_num_results": current_num_results})
                last_results_update_time = time()
                last_reported_result_count = current_num_results

            client_disconnected = False
            sec_since_last_activity = time() - SELF["last_client_activity_timestamp"]
            client_contact_last_1s = sec_since_last_activity < CLIENT_CONTACT_TIMEOUT_SEC
            active_request = (
                SELF["active_client_request_count"] > 0 and sec_since_last_activity < 15
            )
            client_contact_last_1s = client_contact_last_1s or active_request
            if client_contact_last_1s != last_reported_client_contact_last_1s:
                await node_doc.update({"client_contact_last_1s": client_contact_last_1s})
                last_reported_client_contact_last_1s = client_contact_last_1s
            if not client_contact_last_1s:
                seconds_since_job_doc_update = time() - last_job_doc_update_time
                if seconds_since_job_doc_update > JOB_DOC_CONTACT_TIMEOUT_SEC:
                    node_dicts = [d.to_dict() for d in await node_docs_collection.get()]
                    client_disconnected = not any(d["client_contact_last_1s"] for d in node_dicts)
            must_be_connected = not is_background_job or not SELF["all_inputs_uploaded"]
            if client_disconnected and must_be_connected:
                JOB_FAILED = True
                await job_doc.update({"status": "FAILED", "fail_reason": ArrayUnion(["Client DC"])})
                logger.log("Client disconnected!")

            all_inputs_processed = (
                SELF["all_inputs_uploaded"] and input_queue_empty and all_workers_idle
            )
            no_buffered_results = SELF["results_queue"].empty()

            # TODO: Should get more inputs from neighbor whenever more than a couple workers dont
            # have inputs instead of only when none have inputs
            if all_inputs_processed and time() - job_started_at > 10:
                new_inputs = []
                neighboring_nodes = await get_neighboring_nodes(async_db)
                if neighboring_nodes and not SELF["SHUTTING_DOWN"]:
                    new_inputs = await get_inputs_from_neighbor(
                        neighboring_nodes[0].to_dict(), session, logger, auth_headers
                    )
                if new_inputs:
                    logger.log(f"Got {len(new_inputs)} more inputs from {neighboring_nodes[0].id}")
                    neighbor_had_no_inputs_at = None
                    seconds_neighbor_had_no_inputs = 0
                    for input_pkl_with_idx in new_inputs:
                        await SELF["inputs_queue"].put(
                            input_pkl_with_idx, len(input_pkl_with_idx[1])
                        )
                else:
                    neighbor_had_no_inputs_at = neighbor_had_no_inputs_at or time()
                    seconds_neighbor_had_no_inputs = time() - neighbor_had_no_inputs_at
                    if seconds_neighbor_had_no_inputs > EMPTY_NEIGHBOR_TIMEOUT_SEC:
                        if no_buffered_results:
                            msg = (
                                f"Neighbor had no extra inputs for {EMPTY_NEIGHBOR_TIMEOUT_SEC//60}"
                            )
                            logger.log(f"{msg} minutes, done working on job!")
                            await reset_workers(logger, async_db)
                            break
                        logger.log(
                            "Neighbor timeout reached but waiting for buffered results to drain."
                        )

            not_waiting_on_client = no_buffered_results or client_disconnected
            all_local_work_complete = (
                all_inputs_processed and no_buffered_results and not_waiting_on_client
            )

            job_completed = False
            if all_local_work_complete and client_disconnected:
                node_docs = await node_docs_collection.get()
                result_count = sum(doc.to_dict()["current_num_results"] for doc in node_docs)
                job_completed = n_inputs == result_count
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
                    pass

                await reset_workers(logger, async_db)
                break
    finally:
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
                pass
            await reset_workers(logger, async_db)


async def reinit_node(assigned_workers: list, async_db: AsyncClient):
    # important to delete or workers wont install packages
    ENV_IS_READY_PATH.unlink(missing_ok=True)

    current_workers = assigned_workers + SELF["idle_workers"]
    for w in current_workers:
        w.is_idle = True

    current_container_config = SELF["current_container_config"]
    authorized_users = SELF["authorized_users"]
    REINIT_SELF(SELF)
    SELF["current_container_config"] = current_container_config
    SELF["workers"] = current_workers
    SELF["authorized_users"] = authorized_users
    node_doc = async_db.collection("nodes").document(INSTANCE_NAME)
    await node_doc.update({"status": "READY", "current_job": None})


async def reset_workers(logger: Logger, async_db: AsyncClient):
    try:
        await asyncio.gather(*(worker.reset() for worker in SELF["workers"]))
    except Exception as e:
        logger.log(f"Error resetting workers: {e}", severity="ERROR")
        logger.log("Some workers failed to reset, rebooting containers ...")
        await reboot_containers(logger=logger)
        return
    await reinit_node(SELF["workers"], async_db)
