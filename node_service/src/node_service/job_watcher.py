import sys
import pickle
import traceback
import asyncio
import aiohttp
from time import time
from uuid import uuid4

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
ACK_RETRY_TIMEOUT_SEC = 600
ACK_RETRY_DELAY_SEC = 15

SEC_NEIGHBOR_HAD_NO_INPUTS = 0


async def get_neighbor(async_db, node_ids_expected):
    status_filter = FieldFilter("status", "==", "RUNNING")
    job_filter = FieldFilter("current_job", "==", SELF["current_job"])
    query = async_db.collection("nodes").where(filter=And([status_filter, job_filter]))
    query = query.order_by(FieldPath.document_id())
    nodes = [node async for node in query.stream()]
    self_index = [i for i, n in enumerate(nodes) if n.id == INSTANCE_NAME]

    running_node_ids = {n.id for n in nodes}
    missing_node_ids = [nid for nid in node_ids_expected if nid not in running_node_ids]
    still_booting = False
    if missing_node_ids:
        booting_filter = FieldFilter("status", "==", "BOOTING")
        query = async_db.collection("nodes").where(filter=booting_filter).stream()
        booting_nodes = {n.id async for n in query}
        still_booting = any(nid in booting_nodes for nid in missing_node_ids)

    neighbor_id, neighbor_host = None, None
    if self_index and len(nodes) > 1:
        neighbors = nodes[self_index[0] + 1 :] + nodes[: self_index[0]]
        neighbor_id = neighbors[0].id
        neighbor_host = neighbors[0].to_dict()["host"]
    return neighbor_id, neighbor_host, still_booting


async def _input_steal_loop(async_db, session, logger, job_started_at, node_ids_expected):
    global SEC_NEIGHBOR_HAD_NO_INPUTS

    should_steal = lambda: SELF["all_inputs_uploaded"] and (time() - job_started_at > 10)
    neighbor_id, neighbor_host, nodes_might_join = await get_neighbor(async_db, node_ids_expected)
    neighbor_had_no_inputs_at = None

    while not SELF["job_watcher_stop_event"].is_set():
        await asyncio.sleep(1)

        if not should_steal():
            await asyncio.sleep(1)
            continue

        if nodes_might_join and (time() - job_started_at > 60):
            _get_neighbor = get_neighbor(async_db, node_ids_expected)
            neighbor_id, neighbor_host, nodes_might_join = await _get_neighbor
            if not (neighbor_id or nodes_might_join):
                return
            if not neighbor_id:
                continue

        transfer_id = uuid4().hex
        remaining_inputs = SELF["inputs_queue"].qsize()
        get_url = f"{neighbor_host}/jobs/{SELF['current_job']}/get_inputs"
        get_params = {"transfer_id": transfer_id, "requester_queue_size": remaining_inputs}

        items = None
        try:
            async with session.get(
                get_url, params=get_params, headers=SELF["auth_headers"]
            ) as response:
                if response.status == 404:
                    nodes_might_join = True
                    continue
                if response.status == 200:
                    items = pickle.loads(await response.read())
        except Exception as error:
            await logger.log(f"GET inputs from {neighbor_id} failed: {error}", "WARNING")

        if items:
            for input_index, input_pkl in items:
                SELF["inputs_queue"].put_nowait((input_index, input_pkl), len(input_pkl))

        received = bool(items)

        ack_url = f"{neighbor_host}/jobs/{SELF['current_job']}/ack_transfer"
        ack_params = {"transfer_id": transfer_id, "received": "true" if received else "false"}
        ack_started = time()
        ack_ok = False
        while time() - ack_started < ACK_RETRY_TIMEOUT_SEC:
            if SELF["job_watcher_stop_event"].is_set():
                return
            try:
                async with session.post(
                    ack_url, params=ack_params, headers=SELF["auth_headers"]
                ) as response:
                    response.raise_for_status()
                ack_ok = True
                break
            except Exception:
                await asyncio.sleep(ACK_RETRY_DELAY_SEC)

        if not ack_ok:
            reason = (
                f"Could not ACK transfer {transfer_id} to {neighbor_id} after "
                f"{ACK_RETRY_TIMEOUT_SEC}s. Failing job to preserve exactly-once semantics."
            )
            await logger.log(reason, "ERROR")
            job_doc = async_db.collection("jobs").document(SELF["current_job"])
            try:
                await job_doc.update({"status": "FAILED", "fail_reason": ArrayUnion([reason])})
            except Exception:
                pass
            return

        if received:
            neighbor_had_no_inputs_at = None
            SEC_NEIGHBOR_HAD_NO_INPUTS = 0
            await logger.log(f"Got {len(items)} more inputs from {neighbor_id}")
        else:
            neighbor_had_no_inputs_at = neighbor_had_no_inputs_at or time()
            SEC_NEIGHBOR_HAD_NO_INPUTS = time() - neighbor_had_no_inputs_at
            await asyncio.sleep(1)


async def _job_watcher(
    n_inputs: int,
    is_background_job: bool,
    job_started_at: float,
    node_ids_expected: list,
    logger: Logger,
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

    steal_task = asyncio.create_task(
        _input_steal_loop(async_db, session, logger, job_started_at, node_ids_expected)
    )

    last_results_update_time = time()
    last_reported_result_count = 0
    last_reported_client_contact_last_1s = True
    while not SELF["job_watcher_stop_event"].is_set():

        SELF["current_parallelism"] = sum(not worker.is_idle for worker in SELF["workers"])
        pending_transfer_count = sum(len(batch) for batch in SELF["pending_transfers"].values())
        remaining_inputs = SELF["inputs_queue"].qsize() + pending_transfer_count
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
            await logger.log(f"[TIMING] watcher loop detected terminal status: t={time():.3f}")
            status = sync_job_doc.get().to_dict()["status"]
            status = status if status in ["FAILED", "CANCELED"] else "COMPLETED"
            await logger.log(f"Job is {status}! (id={SELF['current_job']})")
            try:
                sync_job_doc.update({"status": status})
            except Exception:
                pass
            await logger.log(f"[TIMING] watcher calling reset_workers: t={time():.3f}")
            await reset_workers(logger, async_db)
            await logger.log(f"[TIMING] watcher reset_workers returned: t={time():.3f}")
            break

    steal_task.cancel()


async def job_watcher_logged(
    n_inputs: int, is_background_job: bool, job_started_at: float, node_ids_expected: list
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
                node_ids_expected,
                logger,
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
    await node_doc.update({"status": "READY", "current_job": None, "reserved_for_job": None})


async def reset_workers(logger: Logger, async_db: AsyncClient):
    gather_start = time()
    try:
        await asyncio.gather(*(worker.reset(logger=logger) for worker in SELF["workers"]))
    except Exception as e:
        await logger.log(f"[TIMING] reset_workers gather failed after {time()-gather_start:.3f}s")
        await logger.log(f"Error resetting workers: {e}", severity="ERROR")
        await logger.log("Some workers failed to reset, rebooting containers ...")
        await reboot_containers(logger=logger)
        return
    await logger.log(f"[TIMING] reset_workers gather done in {time()-gather_start:.3f}s")
    await reinit_node(SELF["workers"], async_db)
