import sys
import traceback
import asyncio
import aiohttp
from time import time

from google.cloud import firestore
from google.cloud.firestore import ArrayUnion
from google.cloud.firestore_v1.async_client import AsyncClient
from google.cloud.firestore import FieldFilter, And
from google.cloud.firestore_v1.field_path import FieldPath

from node_service import PROJECT_ID, SELF, INSTANCE_NAME, REINIT_SELF
from node_service.helpers import Logger, format_traceback
from node_service.lifecycle_endpoints import reboot_containers

EMPTY_NEIGHBOR_TIMEOUT_SEC = 60
CLIENT_CONTACT_TIMEOUT_SEC = 5
JOB_DOC_CONTACT_TIMEOUT_SEC = 4

NEIGHBOR_CACHE_TTL = 5
SEC_NEIGHBOR_HAD_NO_INPUTS = 0


async def get_neighbor(async_db):
    status_filter = FieldFilter("status", "==", "RUNNING")
    job_filter = FieldFilter("current_job", "==", SELF["current_job"])
    query = async_db.collection("nodes").where(filter=And([status_filter, job_filter]))
    query = query.order_by(FieldPath.document_id())
    nodes = [node async for node in query.stream()]
    self_index = [i for i, n in enumerate(nodes) if n.id == INSTANCE_NAME]
    if self_index and len(nodes) > 1:
        neighbors = nodes[self_index[0] + 1 :] + nodes[: self_index[0]]
        return neighbors[0].id, neighbors[0].to_dict()["host"]
    return None, None


async def _input_steal_loop(async_db, session, logger, auth_headers, self_host, job_started_at):
    global SEC_NEIGHBOR_HAD_NO_INPUTS

    last_neighbor_refresh = 0
    no_neighbor_since = None
    neighbor_had_no_inputs_at = None

    while not SELF["job_watcher_stop_event"].is_set():
        await asyncio.sleep(0.2)

        # should steal?
        remaining_inputs = SELF["inputs_queue"].qsize()
        job_past_startup = time() - job_started_at > 10
        should_steal = SELF["all_inputs_uploaded"] and job_past_startup
        if not should_steal:
            await asyncio.sleep(1)
            continue

        try:
            # get neighbor
            if time() - last_neighbor_refresh > NEIGHBOR_CACHE_TTL:
                neighbor_id, neighbor_host = await get_neighbor(async_db)
                last_neighbor_refresh = time()
                if not neighbor_id:
                    no_neighbor_since = no_neighbor_since or time()
                    if time() - no_neighbor_since > 600:
                        await logger.log("No neighbors found for 10 minutes, giving up.")
                        return
                    await asyncio.sleep(20)
                    continue
                no_neighbor_since = None

            # get inputs from neighbor
            num_inputs_received = 0
            params = {"requester_queue_size": remaining_inputs, "requester_host": self_host}
            url = f"{neighbor_host}/jobs/{SELF['current_job']}/input_transfer"
            async with session.get(url, params=params, headers=auth_headers) as response:
                if response.status == 404:
                    last_neighbor_refresh = time() - NEIGHBOR_CACHE_TTL - 1
                    continue
                elif response.status == 200:
                    num_inputs_received = int(await response.text())
                else:
                    msg = f"Error getting inputs from neighbor: {response.status}"
                    await logger.log(msg, "ERROR")
        except Exception as e:
            await logger.log(f"Error in steal loop: {e}", "ERROR")
            await asyncio.sleep(5)
            continue

        if num_inputs_received > 0:
            neighbor_had_no_inputs_at = None
            SEC_NEIGHBOR_HAD_NO_INPUTS = 0
            await logger.log(f"Got {num_inputs_received} more inputs from {neighbor_id}")
        else:
            neighbor_had_no_inputs_at = neighbor_had_no_inputs_at or time()
            SEC_NEIGHBOR_HAD_NO_INPUTS = time() - neighbor_had_no_inputs_at
            await asyncio.sleep(1)


async def _job_watcher(
    n_inputs: int,
    is_background_job: bool,
    job_started_at: float,
    logger: Logger,
    auth_headers: dict,
    async_db: AsyncClient,
    session: aiohttp.ClientSession,
    exit_stack: list,
):
    sync_db = firestore.Client(project=PROJECT_ID, database="burla")
    job_doc = async_db.collection("jobs").document(SELF["current_job"])
    sync_job_doc = sync_db.collection("jobs").document(SELF["current_job"])
    last_job_doc_update_time = sync_job_doc.get().update_time.timestamp()
    node_docs_collection = job_doc.collection("assigned_nodes")
    node_doc = node_docs_collection.document(INSTANCE_NAME)
    self_node_doc = async_db.collection("nodes").document(INSTANCE_NAME)
    self_host = (await self_node_doc.get()).to_dict()["host"]
    await node_doc.set({"current_num_results": 0, "client_contact_last_1s": True})

    JOB_FAILED = False
    JOB_CANCELED = False

    def _on_job_snapshot(doc_snapshot, changes, read_time):
        nonlocal JOB_FAILED, JOB_CANCELED, last_job_doc_update_time
        for change in changes:
            last_job_doc_update_time = change.document.update_time.timestamp()
            job_dict = change.document.to_dict()
            if job_dict["all_inputs_uploaded"] == True:
                SELF["all_inputs_uploaded"] = True
            if job_dict["status"] == "FAILED":
                JOB_FAILED = True
                break
            elif job_dict["status"] == "CANCELED":
                JOB_CANCELED = True
                break

    # Client intentionally updates the job doc every few seconds to signal it's still connected.
    job_watch = sync_job_doc.on_snapshot(_on_job_snapshot)
    exit_stack.append(job_watch.unsubscribe)

    steal_auth_headers = {
        "Authorization": auth_headers.get("Authorization", ""),
        "X-User-Email": auth_headers.get("X-User-Email", ""),
    }
    steal_task = asyncio.create_task(
        _input_steal_loop(async_db, session, logger, steal_auth_headers, self_host, job_started_at)
    )

    last_results_update_time = time()
    last_reported_result_count = 0
    last_reported_client_contact_last_1s = True
    while not SELF["job_watcher_stop_event"].is_set():

        SELF["current_parallelism"] = sum(not worker.is_idle for worker in SELF["workers"])
        remaining_inputs = SELF["inputs_queue"].qsize()
        input_queue_empty = remaining_inputs == 0
        all_workers_idle = SELF["current_parallelism"] == 0
        slow_poll = input_queue_empty and all_workers_idle and (time() - job_started_at) >= 7
        await asyncio.sleep(0.2 if slow_poll else 0.02)

        # Update num results in db?
        current_num_results = SELF["num_results_received"]
        results_changed = current_num_results != last_reported_result_count
        seconds_since_results_update = time() - last_results_update_time
        workers_busy = not input_queue_empty or not all_workers_idle
        stale_update = workers_busy and seconds_since_results_update > 2
        should_update_results = (input_queue_empty and results_changed) or stale_update
        if should_update_results:
            await node_doc.update({"current_num_results": current_num_results})
            last_results_update_time = time()
            last_reported_result_count = current_num_results

        # Client still listening?
        client_disconnected = False
        sec_since_last_activity = time() - SELF["last_client_activity_timestamp"]
        client_contact_last_1s = sec_since_last_activity < CLIENT_CONTACT_TIMEOUT_SEC
        active_request = SELF["active_client_request_count"] > 0 and sec_since_last_activity < 15
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
            await logger.log("Client disconnected!")

        # Neighbor had no inputs for too long?
        if SEC_NEIGHBOR_HAD_NO_INPUTS and SEC_NEIGHBOR_HAD_NO_INPUTS > EMPTY_NEIGHBOR_TIMEOUT_SEC:
            if SELF["results_queue"].empty() and all_workers_idle:
                msg = f"Neighbor had no extra inputs for {EMPTY_NEIGHBOR_TIMEOUT_SEC}s"
                await logger.log(msg + ", done working on job!")
                await reset_workers(logger, async_db)
                break

        # Job over?
        job_completed = False
        all_uploaded = SELF["all_inputs_uploaded"]
        all_inputs_processed = all_uploaded and input_queue_empty and all_workers_idle
        all_local_work_complete = all_inputs_processed and SELF["results_queue"].empty()
        if all_local_work_complete and client_disconnected:
            node_docs = await node_docs_collection.get()
            result_count = sum(doc.to_dict()["current_num_results"] for doc in node_docs)
            job_completed = n_inputs == result_count
        elif all_local_work_complete:
            job_completed = (await job_doc.get()).to_dict()["client_has_all_results"]
        if job_completed or JOB_FAILED or JOB_CANCELED:
            status = sync_job_doc.get().to_dict()["status"]
            status = status if status in ["FAILED", "CANCELED"] else "COMPLETED"
            await logger.log(f"Job is {status}! (id={SELF['current_job']})")
            try:
                sync_job_doc.update({"status": status})
            except Exception:
                pass
            await reset_workers(logger, async_db)
            break

    steal_task.cancel()


async def job_watcher_logged(
    n_inputs: int, is_background_job: bool, job_started_at: float, auth_headers: dict
):
    logger = Logger()  # new logger has no request attached like the one in execute job did.

    exit_stack = []
    async with aiohttp.ClientSession() as session:
        try:
            async_db = AsyncClient(project=PROJECT_ID, database="burla")
            await _job_watcher(
                n_inputs,
                is_background_job,
                job_started_at,
                logger,
                auth_headers,
                async_db,
                session,
                exit_stack,
            )
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
            traceback_str = format_traceback(tb_details)
            await logger.log(str(e), "ERROR", traceback=traceback_str)
            try:
                job_doc = async_db.collection("jobs").document(SELF["current_job"])
                await job_doc.update({"status": "FAILED", "fail_reason": ArrayUnion([str(e)])})
            except Exception:
                pass
            await reset_workers(logger, async_db)
        finally:
            for cleanup in exit_stack:
                cleanup()


async def reinit_node(assigned_workers: list, async_db: AsyncClient):
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
        await logger.log(f"Error resetting workers: {e}", severity="ERROR")
        await logger.log("Some workers failed to reset, rebooting containers ...")
        await reboot_containers(logger=logger)
        return
    await reinit_node(SELF["workers"], async_db)
