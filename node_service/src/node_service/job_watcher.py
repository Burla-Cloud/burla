import sys
import pickle
import traceback
import asyncio
import aiohttp
import os
import ssl
from time import time
from uuid import uuid4

import psutil

from node_service import (
    SELF,
    INSTANCE_NAME,
    INSTANCE_N_CPUS,
    IN_LOCAL_DEV_MODE,
    NODE_AUTH_CREDENTIALS_PATH,
    NUM_GPUS,
    REINIT_SELF,
    head_client,
)
from node_service.helpers import Logger, debug_log, format_traceback
from node_service.lifecycle_endpoints import reboot_containers
from node_service.worker_client import (
    CPU_PRESSURE_FILE,
    READD_MAX_CPU_STALL_FRACTION,
    READD_MAX_WORKER_MEMORY_USED_FRACTION,
    READD_PRESSURE_COOLDOWN_SECONDS,
    WorkerStallTracker,
    _workers_memory_limit_bytes,
)

EMPTY_NEIGHBOR_TIMEOUT_SEC = 120
CLIENT_CONTACT_TIMEOUT_SEC = 5
ACK_RETRY_TIMEOUT_SEC = 600
ACK_RETRY_DELAY_SEC = 15
WORKER_CLEANUP_TIMEOUT_SEC = 120
# How long a worker deficit must persist before this node asks the head to
# boot replacement machines for it. Long enough for transient pressure to
# clear and for un-retiring to win when the machine recovers on its own.
REPLACEMENT_DEFICIT_WINDOW_SEC = 60
REPLACEMENT_RETRY_SEC = 60

# Pairwise slot trading (packing): how often a hungry node may ask its ring
# neighbor for slots, and how far beyond one-worker-per-CPU it may grow.
TRADE_INTERVAL_SEC = 15
OVERSUBSCRIBE_MAX_WORKERS_PER_CPU = 2
# The set of nodes on a job changes rarely, so re-asking the head is cheap to do
# seldom. Matches the client's wait before it hands a booting node the job.
PEER_RECHECK_INTERVAL_SEC = 30
# How long a node can make no progress before it logs its input accounting.
STALL_REPORT_INTERVAL_SEC = 10
# Cadence of the slot_state debug event: a continuous record of this node's
# slot ledger, so post-mortems can read a timeseries instead of replaying
# every change event.
SLOT_STATE_LOG_INTERVAL_SEC = 60

SEC_NEIGHBOR_HAD_NO_INPUTS = 0


def _lifecycle_canceled(job_view: dict) -> bool:
    return (
        job_view.get("cluster_shutdown")
        or job_view.get("cluster_restarted")
        or job_view.get("dashboard_canceled")
        or job_view.get("status") == "CANCELED"
    )


async def get_neighbor():
    """Pick the next RUNNING node after this one in the (name-sorted) ring of
    nodes assigned to this job. Peer list comes from the head."""
    response = await head_client.get_peers(SELF["current_job"])
    peers = response["peers"]
    self_index = [i for i, p in enumerate(peers) if p["instance_name"] == INSTANCE_NAME]

    neighbor_id, neighbor_host = None, None
    if self_index and len(peers) > 1:
        neighbors = peers[self_index[0] + 1 :] + peers[: self_index[0]]
        neighbor_id = neighbors[0]["instance_name"]
        neighbor_host = neighbors[0]["host"]
    return neighbor_id, neighbor_host


async def _input_steal_loop(session, logger, job_started_at):
    global SEC_NEIGHBOR_HAD_NO_INPUTS

    # A node traded down to zero slots must stop pulling work in: it has no
    # workers left to run it, and holding inputs would keep it on the job.
    should_steal = lambda: (
        SELF["all_inputs_uploaded"]
        and (time() - job_started_at > 10)
        and SELF["target_parallelism"] > 0
    )
    # Replacement nodes can join the ring at any point in the job, so the
    # peer list is re-checked on an interval for the whole job instead of
    # only while initially-expected nodes are still booting. last_peer_check
    # starts at 0 so the first active tick fetches the initial neighbor.
    neighbor_id, neighbor_host = None, None
    neighbor_had_no_inputs_at = None
    last_peer_check = 0.0

    while not SELF["job_watcher_stop_event"].is_set():
        await asyncio.sleep(1)

        if not should_steal():
            await asyncio.sleep(1)
            continue

        if time() - last_peer_check > PEER_RECHECK_INTERVAL_SEC:
            last_peer_check = time()
            try:
                neighbor_id, neighbor_host = await get_neighbor()
            except Exception:
                # Head briefly unreachable: keep the current neighbor and let
                # the next interval retry, instead of silently killing
                # stealing for the rest of the job (this task's exceptions
                # are never observed).
                pass

        if not neighbor_id:
            continue

        transfer_id = uuid4().hex
        remaining_inputs = SELF["inputs_queue"].qsize()
        # Idle unthrottled workers = genuinely free capacity right now. The
        # neighbor uses this to decide whether revoking its parked (throttled)
        # workers' inputs for us is worth the kill: a pressured node reports
        # 0, so two pressured nodes can never swap parked work back and forth.
        idle_worker_count = sum(
            worker.is_idle and not worker.retired and not worker.throttled
            for worker in SELF["workers"]
        )
        get_url = f"{neighbor_host}/jobs/{SELF['current_job']}/get_inputs"
        get_params = {
            "transfer_id": transfer_id,
            "requester_queue_size": remaining_inputs,
            "requester_idle_workers": idle_worker_count,
        }

        items = None
        try:
            async with session.get(
                get_url, params=get_params, headers=SELF["auth_headers"]
            ) as response:
                if response.status == 404:
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


async def _slot_trade_loop(session, logger):
    """Acquire slots from the ring neighbor when this node could productively
    run more workers than it owns: it is at its slot count, unsaturated, and
    has more queued inputs than workers (the blog's "add workers to a machine
    to increase utilization ... remove workers elsewhere to stay below the
    job's maximum allowed parallelism"). The neighbor gives up capacity it is
    using worse (see `trade_slots` in job_endpoints.py); the re-add loop then
    boots workers here toward the raised slot count, oversubscribing beyond
    one per CPU. Only meaningful for fully dynamic jobs: with a fixed
    func_cpu/func_ram, packing extra workers in would break the per-call
    resource guarantee. GPU nodes never oversubscribe (one worker per GPU).
    """
    fully_dynamic = SELF["dynamic_func_cpu"] and SELF["dynamic_func_ram"]
    if not fully_dynamic or NUM_GPUS:
        return
    max_workers = OVERSUBSCRIBE_MAX_WORKERS_PER_CPU * INSTANCE_N_CPUS
    can_check_cpu = CPU_PRESSURE_FILE.exists()
    stall_tracker = WorkerStallTracker()

    while not SELF["job_watcher_stop_event"].is_set():
        await asyncio.sleep(1)
        if time() - SELF["last_slot_trade_attempt_at"] < TRADE_INTERVAL_SEC:
            continue

        stall_fraction = 0.0
        if can_check_cpu:
            unthrottled_workers = [
                w for w in SELF["workers"] if not w.retired and not w.throttled
            ]
            stall_fraction = stall_tracker.max_stall_fraction(unthrottled_workers)

        if SELF["target_parallelism"] <= 0:
            return  # traded out; this node is on its way off the job
        if not SELF["all_inputs_uploaded"]:
            continue
        # A node that just shed workers under pressure has no business
        # acquiring more capacity (mirrors the re-add cooldown, and stops
        # a freshly-degraded node from instantly reclaiming the slot it
        # donated).
        recently_pressured = (
            time() - SELF["last_pressure_retirement_at"]
            < READD_PRESSURE_COOLDOWN_SECONDS
        )
        if recently_pressured:
            continue
        alive_workers = [w for w in SELF["workers"] if not w.retired]
        # A deficit is the re-add / replacement paths' problem, not trading's.
        if len(alive_workers) != SELF["target_parallelism"]:
            continue
        if len(alive_workers) >= max_workers:
            continue
        queued_inputs = SELF["inputs_queue"].qsize()
        if queued_inputs <= len(alive_workers):
            continue
        if stall_fraction > READD_MAX_CPU_STALL_FRACTION:
            continue
        if not IN_LOCAL_DEV_MODE and alive_workers:
            memory_limit_bytes = _workers_memory_limit_bytes(alive_workers[0])
            used_bytes = 0
            for worker in alive_workers:
                try:
                    used_bytes += worker.memory_rss_bytes()
                except psutil.NoSuchProcess:
                    used_bytes = None  # worker mid-relaunch; skip this tick
                    break
            if used_bytes is None:
                continue
            if used_bytes / memory_limit_bytes > READD_MAX_WORKER_MEMORY_USED_FRACTION:
                continue

        try:
            neighbor_id, neighbor_host = await get_neighbor()
        except Exception:
            continue
        if not neighbor_id:
            continue

        want = min(
            queued_inputs - len(alive_workers),
            max_workers - len(alive_workers),
        )
        # The id lives from the first send attempt until a response is seen,
        # so a retry after a lost response replays the same trade instead of
        # taking the neighbor's slots twice.
        if SELF["slot_trade_id"] is None:
            SELF["slot_trade_id"] = uuid4().hex
        SELF["last_slot_trade_attempt_at"] = time()
        url = f"{neighbor_host}/jobs/{SELF['current_job']}/trade_slots"
        params = {
            "requesting_node": INSTANCE_NAME,
            "slots_requested": want,
            "trade_id": SELF["slot_trade_id"],
        }
        try:
            async with session.post(
                url, params=params, headers=SELF["auth_headers"]
            ) as response:
                if response.status == 404:
                    # Neighbor is no longer on this job; nothing was granted.
                    SELF["slot_trade_id"] = None
                    continue
                response.raise_for_status()
                granted = int((await response.json()).get("slots_granted") or 0)
        except Exception as error:
            await debug_log(
                "trade_failed",
                neighbor=neighbor_id,
                requested=want,
                error=f"{type(error).__name__}: {error}",
            )
            continue

        SELF["slot_trade_id"] = None
        if granted:
            SELF["target_parallelism"] += granted
        await debug_log(
            "trade_result",
            neighbor=neighbor_id,
            requested=want,
            granted=granted,
            target_now=SELF["target_parallelism"],
        )


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
        _input_steal_loop(session, logger, job_started_at)
    )
    trade_task = asyncio.create_task(_slot_trade_loop(session, logger))

    JOB_FAILED = False
    JOB_CANCELED = False
    last_results_update_time = time()
    last_reported_result_count = 0
    last_loop_at = time()
    last_progress_at = time()
    last_progress_result_count = 0
    last_slot_state_logged_at = 0.0
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

        # --- replacement requester ------------------------------------
        # Pressure retirement permanently shrinks this node's worker pool
        # (until un-retiring recovers it). A deficit that persists while
        # queued work exists means this machine cannot run the slots it owes
        # the job, so hand them to a fresh machine. All policy lives here;
        # the head only executes the boot. Slots are conserved: on success
        # this node's target shrinks by exactly what was booted. A connected
        # client is required because only it can assign the job (it holds
        # the pickled function).
        alive_workers = sum(not worker.retired for worker in SELF["workers"])
        deficit = SELF["target_parallelism"] - alive_workers
        unfinished_inputs = remaining_inputs + SELF["current_parallelism"]
        wants_replacement = (
            deficit > 0
            and job_view.get("grow")
            and not SELF["replacement_refused"]
            and unfinished_inputs > alive_workers
            and client_contact_last_1s
        )
        if not wants_replacement:
            SELF["replacement_deficit_since"] = None
        else:
            if SELF["replacement_deficit_since"] is None:
                SELF["replacement_deficit_since"] = time()
            deficit_sustained = (
                time() - SELF["replacement_deficit_since"]
                > REPLACEMENT_DEFICIT_WINDOW_SEC
            )
            retry_ok = (
                time() - SELF["last_replacement_request_at"] > REPLACEMENT_RETRY_SEC
            )
            if deficit_sustained and retry_ok:
                SELF["last_replacement_request_at"] = time()
                # The id lives from the first send attempt until a response
                # is seen, so a retry after a lost response replays the same
                # request instead of booting a second set of machines.
                if SELF["replacement_request_id"] is None:
                    SELF["replacement_request_id"] = uuid4().hex
                request_id = SELF["replacement_request_id"]
                try:
                    response = await head_client.request_replacement_nodes(
                        SELF["current_job"], deficit, request_id
                    )
                    SELF["replacement_request_id"] = None
                    SELF["replacement_deficit_since"] = None
                    slots_booted = int(response.get("slots_booted") or 0)
                    SELF["target_parallelism"] -= slots_booted
                    booted = [n["instance_name"] for n in response.get("booted", [])]
                    await logger.log(
                        f"Handed {slots_booted} slots lost to pressure "
                        f"retirement to replacement node(s) {booted}."
                    )
                    await debug_log(
                        "replacement_booted",
                        request_id=request_id,
                        deficit=deficit,
                        slots_booted=slots_booted,
                        booted=booted,
                        target_now=SELF["target_parallelism"],
                    )
                except aiohttp.ClientResponseError as e:
                    if e.status == 409:
                        # Grow budget exhausted (or job no longer grow=True):
                        # permanent for this job, stop asking.
                        SELF["replacement_refused"] = True
                        SELF["replacement_request_id"] = None
                    await debug_log(
                        "replacement_request_failed",
                        request_id=request_id,
                        deficit=deficit,
                        status=e.status,
                        refused_permanently=SELF["replacement_refused"],
                    )
                except Exception as e:
                    await debug_log(
                        "replacement_request_failed",
                        request_id=request_id,
                        deficit=deficit,
                        error=f"{type(e).__name__}: {e}",
                    )

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

        # Continuous slot-ledger record: target/alive/queued as a timeseries,
        # so a failed run's forensics are a query instead of replaying every
        # change event.
        if time() - last_slot_state_logged_at > SLOT_STATE_LOG_INTERVAL_SEC:
            last_slot_state_logged_at = time()
            await debug_log(
                "slot_state",
                target=SELF["target_parallelism"],
                alive_workers=sum(not w.retired for w in SELF["workers"]),
                busy_workers=SELF["current_parallelism"],
                throttled_workers=sum(
                    w.throttled and not w.retired for w in SELF["workers"]
                ),
                queued_inputs=remaining_inputs,
                results=SELF["num_results_received"],
            )

        # A job that stops advancing is only diagnosable if you can see where
        # its inputs went: this node's queue, a parked transfer, or a worker.
        if current_num_results != last_progress_result_count:
            last_progress_result_count = current_num_results
            last_progress_at = time()
        elif time() - last_progress_at > STALL_REPORT_INTERVAL_SEC:
            last_progress_at = time()
            await debug_log(
                "stall",
                queued_inputs=SELF["inputs_queue"].qsize(),
                inputs_in_transfer=pending_transfer_count,
                transfers=list(SELF["pending_transfers"]),
                busy_workers=SELF["current_parallelism"],
                results_produced=current_num_results,
                queued_results=SELF["results_queue"].qsize(),
                queued_result_bytes=SELF["results_queue"].size_bytes,
                unacked_result_batch=not pending_results_empty,
                workers=[(w.is_idle, w.retired, w.throttled) for w in SELF["workers"]],
                all_inputs_uploaded=SELF["all_inputs_uploaded"],
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

        # Traded down to zero slots and drained? Finish this node's part of
        # the job immediately instead of waiting out the empty-neighbor
        # timeout: its slots (and any requeued inputs) live elsewhere now, so
        # the machine is pure idle cost (this is what "packing into fewer
        # machines" frees).
        traded_out = (
            SELF["target_parallelism"] <= 0
            and input_queue_empty
            and all_workers_idle
            and SELF["results_queue"].empty()
            and pending_results_empty
        )
        if traded_out:
            steal_task.cancel()
            trade_task.cancel()
            await logger.log("All slots traded away and drained, done working on job!")
            await reset_workers(logger)
            break

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
                trade_task.cancel()
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
            trade_task.cancel()
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
    trade_task.cancel()


async def job_watcher_logged(
    n_inputs: int,
    is_background_job: bool,
    job_started_at: float,
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
    for task_key in (
        "dynamic_ram_monitor_task",
        "cpu_pressure_monitor_task",
        "worker_readd_task",
    ):
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
