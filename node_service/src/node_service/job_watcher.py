import sys
import pickle
import traceback
import asyncio
import aiohttp
import os
import ssl
from time import time
from uuid import uuid4

from node_service import (
    SELF,
    INSTANCE_NAME,
    NODE_AUTH_CREDENTIALS_PATH,
    REINIT_SELF,
    head_client,
)
from node_service.helpers import Logger, format_traceback
from node_service.lifecycle_endpoints import reboot_containers

EMPTY_NEIGHBOR_TIMEOUT_SEC = 120
CLIENT_CONTACT_TIMEOUT_SEC = 5
ACK_RETRY_TIMEOUT_SEC = 600
ACK_RETRY_DELAY_SEC = 15
WORKER_CLEANUP_TIMEOUT_SEC = 120
# The set of nodes on a job changes rarely, so re-asking the head is cheap to do
# seldom. Matches the client's wait before it hands a booting node the job.
PEER_RECHECK_INTERVAL_SEC = 30
# How long a node can make no progress before it logs its input accounting.
STALL_REPORT_INTERVAL_SEC = 10

SEC_NEIGHBOR_HAD_NO_INPUTS = 0


def _lifecycle_canceled(job_view: dict) -> bool:
    return (
        job_view.get("cluster_shutdown")
        or job_view.get("cluster_restarted")
        or job_view.get("dashboard_canceled")
        or job_view.get("status") == "CANCELED"
    )


async def get_neighbor(node_ids_expected):
    """Pick the next RUNNING node after this one in the (name-sorted) ring of
    nodes assigned to this job. Peer list comes from the head."""
    response = await head_client.get_peers(SELF["current_job"])
    peers = response["peers"]
    self_index = [i for i, p in enumerate(peers) if p["instance_name"] == INSTANCE_NAME]

    running_node_ids = {p["instance_name"] for p in peers}
    missing_node_ids = [nid for nid in node_ids_expected if nid not in running_node_ids]
    still_booting = bool(missing_node_ids) and any(
        nid in response["booting_node_ids"] for nid in missing_node_ids
    )

    neighbor_id, neighbor_host = None, None
    if self_index and len(peers) > 1:
        neighbors = peers[self_index[0] + 1 :] + peers[: self_index[0]]
        neighbor_id = neighbors[0]["instance_name"]
        neighbor_host = neighbors[0]["host"]
    return neighbor_id, neighbor_host, still_booting


async def _input_steal_loop(session, logger, job_started_at, node_ids_expected):
    global SEC_NEIGHBOR_HAD_NO_INPUTS

    should_steal = lambda: SELF["all_inputs_uploaded"] and (
        time() - job_started_at > 10
    )
    neighbor_id, neighbor_host, nodes_might_join = await get_neighbor(node_ids_expected)
    if not (neighbor_id or nodes_might_join):
        return
    neighbor_had_no_inputs_at = None
    last_peer_check = time()

    while not SELF["job_watcher_stop_event"].is_set():
        await asyncio.sleep(1)

        if not should_steal():
            await asyncio.sleep(1)
            continue

        if nodes_might_join and (time() - last_peer_check > PEER_RECHECK_INTERVAL_SEC):
            last_peer_check = time()
            neighbor_id, neighbor_host, nodes_might_join = await get_neighbor(
                node_ids_expected
            )
            if not (neighbor_id or nodes_might_join):
                return

        if not neighbor_id:
            continue

        transfer_id = uuid4().hex
        remaining_inputs = SELF["inputs_queue"].qsize()
        get_url = f"{neighbor_host}/jobs/{SELF['current_job']}/get_inputs"
        get_params = {
            "transfer_id": transfer_id,
            "requester_queue_size": remaining_inputs,
        }

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
            error_name = type(error).__name__
            await logger.log(
                f"GET inputs from {neighbor_id} failed: {error_name}: {error}",
                "WARNING",
            )

        if items:
            for input_index, input_pkl in items:
                SELF["inputs_queue"].put_nowait(
                    (input_index, input_pkl), len(input_pkl)
                )

        received = bool(items)

        ack_url = f"{neighbor_host}/jobs/{SELF['current_job']}/ack_transfer"
        ack_params = {
            "transfer_id": transfer_id,
            "received": "true" if received else "false",
        }
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
            try:
                await head_client.update_job(
                    SELF["current_job"], {"status": "FAILED"}, append_fail_reason=reason
                )
            except Exception:
                pass
            return

        if received:
            neighbor_had_no_inputs_at = None
            SEC_NEIGHBOR_HAD_NO_INPUTS = 0
            # await logger.log(f"Got {len(items)} more inputs from {neighbor_id}")
        else:
            neighbor_had_no_inputs_at = neighbor_had_no_inputs_at or time()
            SEC_NEIGHBOR_HAD_NO_INPUTS = time() - neighbor_had_no_inputs_at
            await asyncio.sleep(1)


async def _push_progress() -> dict:
    """Push this node's job progress and return the fresh job view."""
    while True:
        try:
            view = await head_client.push_state(include_job_progress=True)
            head_client.apply_job_signals(view.get("job"))
            return view.get("job") or {"exists": False}
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
            await asyncio.sleep(1)


async def _job_watcher(
    n_inputs: int,
    is_background_job: bool,
    job_started_at: float,
    node_ids_expected: list,
    logger: Logger,
    session: aiohttp.ClientSession,
):
    # Module-global: reset per-job so prior-job state doesn't leak in.
    global SEC_NEIGHBOR_HAD_NO_INPUTS
    SEC_NEIGHBOR_HAD_NO_INPUTS = 0

    # First push registers this node's progress with the head (the
    # `assigned_nodes` entry) and returns the job's current signal set.
    # The job was created synchronously inside `POST /v1/jobs/{id}/start`,
    # before the client could possibly have contacted this node.
    job_view = await _push_progress()
    if not job_view.get("exists"):
        raise RuntimeError(f"Job {SELF['current_job']} does not exist on the head.")

    steal_task = asyncio.create_task(
        _input_steal_loop(session, logger, job_started_at, node_ids_expected)
    )

    JOB_FAILED = False
    JOB_CANCELED = False
    last_results_update_time = time()
    last_reported_result_count = 0
    last_loop_at = time()
    last_progress_at = time()
    last_progress_result_count = 0
    while not SELF["job_watcher_stop_event"].is_set():

        SELF["current_parallelism"] = sum(
            not worker.is_idle and not worker.retired for worker in SELF["workers"]
        )
        pending_transfer_count = sum(
            len(batch) for batch in SELF["pending_transfers"].values()
        )
        remaining_inputs = SELF["inputs_queue"].qsize() + pending_transfer_count
        input_queue_empty = remaining_inputs == 0
        all_workers_idle = SELF["current_parallelism"] == 0
        slow_poll = (
            input_queue_empty and all_workers_idle and (time() - job_started_at) >= 7
        )
        await asyncio.sleep(0.2 if slow_poll else 0.02)
        pending_results_empty = SELF["pending_result_batch"] is None

        # Signals delivered by the 1s state-push loop (or a direct push below).
        job_view = SELF.get("job_view") or job_view
        if job_view.get("status") == "FAILED":
            JOB_FAILED = True
        elif job_view.get("status") == "CANCELED":
            JOB_CANCELED = True

        # A workload heavy enough to starve this process makes the loop skip
        # whole seconds. The client cannot be blamed for a window this node
        # slept through, so give it a fresh one instead of reading the gap as
        # a disconnect.
        loop_gap = time() - last_loop_at
        if loop_gap > CLIENT_CONTACT_TIMEOUT_SEC:
            SELF["last_client_activity_timestamp"] = time()
            await logger.log(
                f"Job watcher was starved for {loop_gap:.0f}s, "
                "not counting that against the client.",
                severity="WARNING",
            )
        last_loop_at = time()

        # Client still listening? (the direct /client-heartbeat is the signal;
        # the head aggregates every node's flag for the quorum check below)
        sec_since_last_activity = time() - SELF["last_client_activity_timestamp"]
        client_contact_last_1s = sec_since_last_activity < CLIENT_CONTACT_TIMEOUT_SEC
        active_request = (
            SELF["active_client_request_count"] > 0 and sec_since_last_activity < 15
        )
        client_contact_last_1s = client_contact_last_1s or active_request
        contact_flag_changed = client_contact_last_1s != SELF["client_contact_last_1s"]
        SELF["client_contact_last_1s"] = client_contact_last_1s

        # Push progress immediately on meaningful changes; the 1s loop covers
        # the steady state.
        current_num_results = SELF["num_results_received"]
        results_changed = current_num_results != last_reported_result_count
        seconds_since_results_update = time() - last_results_update_time
        workers_busy = not input_queue_empty or not all_workers_idle
        stale_update = workers_busy and seconds_since_results_update > 2
        should_push = (
            (input_queue_empty and results_changed)
            or stale_update
            or contact_flag_changed
        )
        if should_push:
            job_view = await _push_progress()
            last_results_update_time = time()
            last_reported_result_count = current_num_results

        # A job that stops advancing is only diagnosable if you can see where
        # its inputs went: this node's queue, a parked transfer, or a worker.
        if current_num_results != last_progress_result_count:
            last_progress_result_count = current_num_results
            last_progress_at = time()
        elif time() - last_progress_at > STALL_REPORT_INTERVAL_SEC:
            last_progress_at = time()
            await logger.log(
                f"No new results for {STALL_REPORT_INTERVAL_SEC}s: "
                f"queued_inputs={SELF['inputs_queue'].qsize()} "
                f"inputs_in_transfer={pending_transfer_count} "
                f"transfers={list(SELF['pending_transfers'])} "
                f"busy_workers={SELF['current_parallelism']} "
                f"results_produced={current_num_results} "
                f"queued_results={SELF['results_queue'].qsize()} "
                f"queued_result_bytes={SELF['results_queue'].size_bytes} "
                f"unacked_result_batch={not pending_results_empty} "
                f"workers={[(w.is_idle, w.retired) for w in SELF['workers']]} "
                f"all_inputs_uploaded={SELF['all_inputs_uploaded']}",
                severity="WARNING",
            )

        client_disconnected = False
        if not client_contact_last_1s and SELF["client_heartbeat_received"]:
            client_disconnected = not job_view.get("any_node_client_contact")
        must_be_connected = not is_background_job or not SELF["all_inputs_uploaded"]
        if (
            client_disconnected
            and must_be_connected
            and not (JOB_FAILED or JOB_CANCELED)
        ):
            if _lifecycle_canceled(job_view):
                JOB_CANCELED = True
            else:
                JOB_FAILED = True
                await head_client.update_job(
                    SELF["current_job"],
                    {"status": "FAILED"},
                    append_fail_reason="Client DC",
                )
                await logger.log("Client disconnected!")

        # Neighbor had no inputs for too long?
        if (
            SEC_NEIGHBOR_HAD_NO_INPUTS
            and SEC_NEIGHBOR_HAD_NO_INPUTS > EMPTY_NEIGHBOR_TIMEOUT_SEC
        ):
            if (
                SELF["results_queue"].empty()
                and pending_results_empty
                and all_workers_idle
            ):
                steal_task.cancel()
                msg = f"Neighbor had no extra inputs for {EMPTY_NEIGHBOR_TIMEOUT_SEC}s"
                await logger.log(msg + ", done working on job!")
                await reset_workers(logger)
                break

        # Job over?
        job_completed = False
        all_uploaded = SELF["all_inputs_uploaded"]
        all_inputs_processed = all_uploaded and input_queue_empty and all_workers_idle
        if all_inputs_processed and client_disconnected and pending_results_empty:
            job_view = await _push_progress()
            job_completed = n_inputs == job_view.get("total_num_results")
        elif all_inputs_processed:
            job_view = await _push_progress()
            job_completed = job_view.get("client_has_all_results")
        if job_completed or JOB_FAILED or JOB_CANCELED:
            steal_task.cancel()
            if JOB_FAILED:
                status = "FAILED"
            elif JOB_CANCELED:
                status = "CANCELED"
            else:
                # job_view is fresh here (the completion branches above just
                # pushed); another node may have failed/canceled first.
                status = job_view.get("status")
                status = status if status in ["FAILED", "CANCELED"] else "COMPLETED"
            await logger.log(f"Job is {status}! (id={SELF['current_job']})")
            try:
                await head_client.update_job(SELF["current_job"], {"status": status})
            except Exception:
                pass
            await reset_workers(logger)
            break

    steal_task.cancel()


async def job_watcher_logged(
    n_inputs: int,
    is_background_job: bool,
    job_started_at: float,
    node_ids_expected: list,
):
    logger = (
        Logger()
    )  # new logger has no request attached like the one in execute job did.

    ca_path = os.environ.get("CLUSTER_CA_PATH")
    ssl_context = ssl.create_default_context(cafile=ca_path) if ca_path else None
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    async with aiohttp.ClientSession(connector=connector) as session:
        try:
            await _job_watcher(
                n_inputs,
                is_background_job,
                job_started_at,
                node_ids_expected,
                logger,
                session,
            )
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
            traceback_str = format_traceback(tb_details)
            await logger.log(str(e), "ERROR", traceback=traceback_str)
            try:
                await head_client.update_job(
                    SELF["current_job"], {"status": "FAILED"}, append_fail_reason=str(e)
                )
            except Exception:
                pass
            await reset_workers(logger)


async def reinit_node(assigned_workers: list):
    current_workers = assigned_workers + SELF["idle_workers"]
    for w in current_workers:
        w.is_idle = True

    current_container_config = SELF["current_container_config"]
    authorized_users = SELF["authorized_users"]
    REINIT_SELF(SELF)
    SELF["current_container_config"] = current_container_config
    SELF["workers"] = current_workers
    SELF["authorized_users"] = authorized_users
    SELF["reported_status"] = "READY"
    await head_client.push_state(
        status="READY", current_job=None, reserved_for_job=None
    )


async def reset_workers(logger: Logger):
    # Stops idle or reassigned workers from holding creds for a finished job.
    NODE_AUTH_CREDENTIALS_PATH.unlink(missing_ok=True)
    for task_key in ("dynamic_ram_monitor_task", "cpu_pressure_monitor_task"):
        monitor_task = SELF[task_key]
        if monitor_task is not None:
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass
            SELF[task_key] = None
    if SELF["reboot_containers_after_job"]:
        await logger.log(
            "Rebooting worker containers to restore dynamic worker capacity ..."
        )
        try:
            await asyncio.wait_for(
                reboot_containers(logger=logger),
                timeout=WORKER_CLEANUP_TIMEOUT_SEC,
            )
        except Exception as e:
            SELF["reported_status"] = "FAILED"
            await head_client.push_state(status="FAILED")
            await logger.log(
                f"Timed out rebooting worker containers: {e}", severity="ERROR"
            )
        return
    try:
        await asyncio.wait_for(
            asyncio.gather(*(worker.reset() for worker in SELF["workers"])),
            timeout=WORKER_CLEANUP_TIMEOUT_SEC,
        )
    except Exception as e:
        # dont throw errors if node deleting
        if SELF["SHUTTING_DOWN"] or SELF["FAILED"]:
            return

        await logger.log(f"Error resetting workers: {e}", severity="ERROR")
        await logger.log("Some workers failed to reset, rebooting containers ...")
        try:
            await asyncio.wait_for(
                reboot_containers(logger=logger),
                timeout=WORKER_CLEANUP_TIMEOUT_SEC,
            )
        except Exception as reboot_error:
            SELF["reported_status"] = "FAILED"
            await head_client.push_state(status="FAILED")
            await logger.log(
                f"Timed out rebooting worker containers after reset failure: {reboot_error}",
                severity="ERROR",
            )
        return
    await reinit_node(SELF["workers"])
