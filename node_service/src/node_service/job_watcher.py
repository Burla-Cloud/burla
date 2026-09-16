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
    CPU_UTILIZATION_ADD_MAX,
    CPU_UTILIZATION_RECOVER_MAX,
    DYNAMIC_RAM_MAX_WORKER_MEMORY_USED_FRACTION,
    GATE_EWMA_TAU_SECONDS,
    READD_MAX_CPU_STALL_FRACTION,
    READD_MAX_IO_STALL_FRACTION,
    READD_MAX_MEMORY_STALL_FRACTION,
    READD_MAX_NETWORK_UTILIZATION_FRACTION,
    READD_MAX_WORKER_MEMORY_USED_FRACTION,
    SLOT_TRADE_PRESSURE_COOLDOWN_SECONDS,
    AddGateSampler,
    Ewma,
    SliceCpuSampler,
    WorkerStallTracker,
    _workers_memory_limit_bytes,
    _workers_memory_used_bytes,
    parks_in_last,
)

EMPTY_NEIGHBOR_TIMEOUT_SEC = 120
CLIENT_CONTACT_TIMEOUT_SEC = 5
ACK_RETRY_TIMEOUT_SEC = 600
ACK_RETRY_DELAY_SEC = 15
WORKER_CLEANUP_TIMEOUT_SEC = 120
# How long a worker deficit must persist, with the recovery loop's add-gates
# red the entire time, before this node asks the head to boot replacement
# machines for it. Red gates the whole window prove the machine genuinely
# cannot host its slots; any green reading restarts the clock, because green
# gates mean the recovery loop can absorb the deficit itself. (A 60s
# deficit-only window once declared parked capacity "permanently lost" that
# hundreds of later unthrottles recovered.)
REPLACEMENT_DEFICIT_WINDOW_SEC = 180
REPLACEMENT_RETRY_SEC = 60

# Slot acquisition: how often a hungry node may ask its ring neighbor for
# slots (and mint new ones when the neighbor has nothing to give), and how
# many slots it may mint per attempt. Minting is capped per attempt so the
# re-add loop (one boot per second, every add-gate re-checked) absorbs the
# deficit well inside REPLACEMENT_DEFICIT_WINDOW_SEC; an unbounded mint would
# read as a sustained deficit and boot replacement machines for slots this
# node just invented.
TRADE_INTERVAL_SEC = 15
MINT_MAX_SLOTS_PER_ATTEMPT = 32
# Mint refractory and sizing. Minting once manufactured its own demand on a
# two-phase workload: a download lull read as spare capacity, and the minted
# slots then guaranteed the next pressure spike. Every mint requires green
# smoothed gates while every worker is busy for one full EWMA time constant
# (with the EWMAs aged past ramp lag: three NEXRAD reruns showed a fresh
# node's first minute of full load reads download-lean, so ramp always looks
# green), plus a park-free last minute, and the batch halves per park in the
# last five minutes. Sizing is proportional to measured idle headroom: a
# machine at least half idle is demonstrably IO-bound and may take its full
# headroom in one batch, while a busy machine takes half its remaining
# headroom (in cores) per step. Half-the-gap steps converge geometrically
# without overshoot (fixed one-slot creep proved far too slow to reach
# saturation); the walk ends when queueing (stall past the re-add bar)
# breaks the streak, which lands at "every core busy, stall still under
# half the park trigger" - the oversubscription target - instead of an
# arbitrary utilization line.
#
# Cadence: the full streak is required before the first mint (EWMA ramp
# anchor) and again after any park, but between consecutive mints on a
# park-free node the wait drops to MINT_STEP_INTERVAL_SEC. One full time
# constant between every step meant a 12-minute IO-bound job spent its
# whole life ramping and peaked at ~50% CPU (observed on pd12m); parks
# remain the fast brake, and any park in the halving window restores the
# slow cadence.
MINT_PARK_REFRACTORY_SEC = 60
MINT_GREEN_STREAK_SEC = GATE_EWMA_TAU_SECONDS
MINT_STEP_INTERVAL_SEC = 15
MINT_BULK_MAX_UTILIZATION = 0.5
MINT_BATCH_HALVING_LOOKBACK_SEC = 300
# There is deliberately no local ceiling on minted parallelism: slots are
# granted by the head's mint controller (main_service/mint_controller.py),
# which only keeps funding growth while job-wide goodput measurably rises
# with it. A workload stalled on something no local gate can see (a
# rate-limited server, zram-hidden memory thrash) stops earning grants
# instead of walking to an arbitrary per-core cap.
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


async def _neighbor_finished_scoped_job(neighbor_id: str) -> bool:
    try:
        node = await head_client.get_node_including_deleted(neighbor_id)
    except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
        return False
    if node is None:
        return False
    reason = node.get("terminal_reason") or {}
    return (
        node.get("status") == "DELETED"
        and node.get("job_scope_id") == SELF["current_job"]
        and reason.get("code") == "job_scope_finished"
    )


async def _claim_drained_job_part() -> bool:
    # Serialize this decision with get_inputs so a peer cannot park a batch
    # after this node has decided it is safe to leave the job. A requester
    # must also finish its own GET/ACK transaction: otherwise its task can be
    # canceled after the donor removes a batch but before the ACK settles it.
    async with SELF["input_transfer_lock"]:
        drained = (
            SELF["inputs_queue"].empty()
            and not SELF["pending_transfers"]
            and SELF["active_input_steal_id"] is None
            and all(w.is_idle or w.retired for w in SELF["workers"])
            and SELF["results_queue"].empty()
            and SELF["pending_result_batch"] is None
        )
        if drained:
            SELF["job_watcher_stop_event"].set()
        return drained


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
        SELF["active_input_steal_id"] = transfer_id
        remaining_inputs = SELF["inputs_queue"].qsize()
        # Idle unthrottled workers = genuinely free capacity right now. The
        # neighbor uses this to decide whether revoking in-flight inputs
        # (parked attempts, or running ones while it is saturated) for us is
        # worth the kill. Idle workers refuse the queue while anything is
        # parked locally, so a node with parked workers reports 0: its "idle"
        # workers could not actually run a revoked input, and two pressured
        # nodes must never swap parked work back and forth via kills. A node
        # whose cores are already busy reports 0 for the same reason: revoked
        # work would just queue here instead of there.
        idle_worker_count = 0
        has_free_capacity = (
            not any(w.throttled and not w.retired for w in SELF["workers"])
            and SELF["cpu_utilization"] <= CPU_UTILIZATION_RECOVER_MAX
        )
        if has_free_capacity:
            idle_worker_count = sum(
                worker.is_idle and not worker.retired for worker in SELF["workers"]
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
                    SELF["active_input_steal_id"] = None
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
            # should_steal() passed before the GET, but a trade can zero the
            # target mid-flight; inputs enqueued after that have no workers
            # left to run them and stall the job forever (observed). The
            # retire lock serializes this against trade_slots; a node traded
            # to zero bounces the batch so the neighbor requeues it.
            async with SELF["dynamic_retire_lock"]:
                if SELF["target_parallelism"] > 0:
                    for input_index, input_pkl in items:
                        SELF["inputs_queue"].put_nowait(
                            (input_index, input_pkl), len(input_pkl)
                        )
                else:
                    items = None

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
            except Exception as error:
                await debug_log(
                    "transfer_ack_failed",
                    transfer_id=transfer_id,
                    neighbor=neighbor_id,
                    received=received,
                    error=f"{type(error).__name__}: {error}",
                )
                if await _neighbor_finished_scoped_job(neighbor_id):
                    await debug_log(
                        "transfer_ack_resolved_by_peer_completion",
                        transfer_id=transfer_id,
                        neighbor=neighbor_id,
                        received=received,
                    )
                    ack_ok = True
                    break
                await asyncio.sleep(ACK_RETRY_DELAY_SEC)

        SELF["active_input_steal_id"] = None
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
    """Raise this node's slot count when it could productively run more
    workers than it owns: it is at its slot count, unsaturated, and has more
    queued inputs than workers. Slots come from the ring neighbor first (it
    gives up capacity it is using worse - see `trade_slots` in
    job_endpoints.py - and a neighbor drained to zero frees its machine);
    any remainder is requested from the head's mint controller when the
    job's parallelism is otherwise unconstrained (see mint_slots_allowed in
    job_endpoints.py), which only grants while job-wide goodput measurably
    rises with added workers. The re-add loop then boots workers toward the
    raised count one per second with every add-gate re-checked, so
    saturation (CPU stall, disk IO stall, NIC utilization, worker RSS) caps
    how far a node oversubscribes, and the mint refractory (see
    MINT_PARK_REFRACTORY_SEC) keeps minting from re-creating the saturation
    it just measured its way out of. Only meaningful for fully dynamic jobs:
    with a fixed func_cpu/func_ram, packing extra workers in would break the
    per-call resource guarantee. GPU nodes never oversubscribe (one worker
    per GPU).
    """
    fully_dynamic = SELF["dynamic_func_cpu"] and SELF["dynamic_func_ram"]
    if not fully_dynamic or NUM_GPUS:
        return
    can_check_cpu = CPU_PRESSURE_FILE.exists()
    stall_tracker = WorkerStallTracker()
    gate_sampler = AddGateSampler()
    cpu_sampler = SliceCpuSampler()
    stall_ewma = Ewma(GATE_EWMA_TAU_SECONDS)
    io_ewma = Ewma(GATE_EWMA_TAU_SECONDS)
    network_ewma = Ewma(GATE_EWMA_TAU_SECONDS)
    memory_ewma = Ewma(GATE_EWMA_TAU_SECONDS)
    utilization_ewma = Ewma(GATE_EWMA_TAU_SECONDS)
    mint_gates_green_since = None
    fully_busy_since = None
    last_mint_at = None

    while not SELF["job_watcher_stop_event"].is_set():
        await asyncio.sleep(1)

        # Sample every tick, not only when a trade is due: the smoothed gates
        # and the mint loop's green-streak clock are meaningless unless they
        # see every second.
        alive_workers = [w for w in SELF["workers"] if not w.retired]
        raw_stall = 0.0
        if can_check_cpu:
            unthrottled_workers = [w for w in alive_workers if not w.throttled]
            raw_stall = stall_tracker.max_stall_fraction(unthrottled_workers)
        stall_fraction = stall_ewma.update(raw_stall)
        raw_io_stall, raw_network_utilization, raw_memory_stall = (
            gate_sampler.sample()
        )
        io_stall = io_ewma.update(raw_io_stall)
        network_utilization = network_ewma.update(raw_network_utilization)
        memory_stall = memory_ewma.update(raw_memory_stall)
        utilization = utilization_ewma.update(cpu_sampler.sample(alive_workers))

        mint_gates_green = (
            stall_fraction <= READD_MAX_CPU_STALL_FRACTION
            and io_stall <= READD_MAX_IO_STALL_FRACTION
            and network_utilization <= READD_MAX_NETWORK_UTILIZATION_FRACTION
            and memory_stall <= READD_MAX_MEMORY_STALL_FRACTION
            and utilization <= CPU_UTILIZATION_ADD_MAX
        )
        # The green streak only counts while the EWMAs have averaged over a
        # full time constant of full-parallelism load (see
        # MINT_GREEN_STREAK_SEC); anything measured before that is ramp lag.
        # A backlogged node counts as fully busy even when the 1 Hz busy
        # sample dips below the worker count: with sub-second tasks those
        # dips are sampling artifacts and resetting on them starved an
        # IO-bound job of every mint (observed). Nothing-executing still
        # resets, so install / assignment phases never start the clock.
        fully_busy = (
            bool(alive_workers)
            and SELF["current_parallelism"] > 0
            and (
                SELF["current_parallelism"] >= len(alive_workers)
                or SELF["inputs_queue"].qsize() > len(alive_workers)
            )
        )
        if not fully_busy:
            fully_busy_since = None
        elif fully_busy_since is None:
            fully_busy_since = time()
        ewmas_warm = (
            fully_busy_since is not None
            and time() - fully_busy_since >= GATE_EWMA_TAU_SECONDS
        )
        if not mint_gates_green or not ewmas_warm:
            mint_gates_green_since = None
        elif mint_gates_green_since is None:
            mint_gates_green_since = time()

        if time() - SELF["last_slot_trade_attempt_at"] < TRADE_INTERVAL_SEC:
            continue

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
            < SLOT_TRADE_PRESSURE_COOLDOWN_SECONDS
        )
        if recently_pressured:
            continue
        # A deficit is the re-add / replacement paths' problem, not trading's.
        if len(alive_workers) != SELF["target_parallelism"]:
            continue
        queued_inputs = SELF["inputs_queue"].qsize()
        if queued_inputs <= len(alive_workers):
            continue
        if stall_fraction > READD_MAX_CPU_STALL_FRACTION:
            continue
        if io_stall > READD_MAX_IO_STALL_FRACTION:
            continue
        if network_utilization > READD_MAX_NETWORK_UTILIZATION_FRACTION:
            continue
        if memory_stall > READD_MAX_MEMORY_STALL_FRACTION:
            continue
        if not IN_LOCAL_DEV_MODE and alive_workers:
            try:
                memory_limit_bytes = _workers_memory_limit_bytes(alive_workers[0])
                used_bytes = _workers_memory_used_bytes(alive_workers[0], alive_workers)
            except OSError:
                continue  # that worker's process just died; skip this tick
            if used_bytes / memory_limit_bytes > READD_MAX_WORKER_MEMORY_USED_FRACTION:
                continue

        try:
            neighbor_id, neighbor_host = await get_neighbor()
        except Exception:
            continue

        want = queued_inputs - len(alive_workers)
        SELF["last_slot_trade_attempt_at"] = time()
        granted = 0
        if neighbor_id:
            # The id lives from the first send attempt until a response is
            # seen, so a retry after a lost response replays the same trade
            # instead of taking the neighbor's slots twice.
            if SELF["slot_trade_id"] is None:
                SELF["slot_trade_id"] = uuid4().hex
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
                        # Neighbor is no longer on this job; nothing granted.
                        SELF["slot_trade_id"] = None
                        continue
                    response.raise_for_status()
                    granted = int(
                        (await response.json()).get("slots_granted") or 0
                    )
            except Exception as error:
                # The grant may have landed on the neighbor with the response
                # lost; the replay on the next attempt settles it. Minting now
                # on top of a grant that later replays would double-add, so
                # skip this tick entirely.
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
            now = time()
            await debug_log(
                "trade_result",
                neighbor=neighbor_id,
                requested=want,
                granted=granted,
                target_now=SELF["target_parallelism"],
                # Mint-gate forensics: which condition is holding minting back.
                stall=round(stall_fraction, 4),
                io=round(io_stall, 4),
                net=round(network_utilization, 4),
                mem=round(memory_stall, 4),
                util=round(utilization, 4),
                busy=SELF["current_parallelism"],
                busy_age=(
                    round(now - fully_busy_since, 1) if fully_busy_since else None
                ),
                green_age=(
                    round(now - mint_gates_green_since, 1)
                    if mint_gates_green_since
                    else None
                ),
            )

        # The neighbor covered what it could; ask the head to mint the rest.
        # Every node on a saturated-but-idle job hits this path together,
        # which is exactly the case trading can never help with (nobody has
        # spares) and the reason minting exists.
        if not SELF["mint_slots_allowed"]:
            continue
        # Mint refractory (see MINT_PARK_REFRACTORY_SEC): one instantaneously
        # green sample is not spare capacity, and neither is a green quarter
        # minute during startup.
        if parks_in_last(MINT_PARK_REFRACTORY_SEC) > 0:
            continue
        recent_parks = parks_in_last(MINT_BATCH_HALVING_LOOKBACK_SEC)
        # Cadence (see MINT_STEP_INTERVAL_SEC): full streak before the first
        # mint and after any park, fast steps between mints on a clean node.
        slow_cadence = last_mint_at is None or recent_parks > 0
        required_wait = MINT_GREEN_STREAK_SEC if slow_cadence else MINT_STEP_INTERVAL_SEC
        green_age = (
            time() - mint_gates_green_since if mint_gates_green_since else 0.0
        )
        if green_age < required_wait:
            continue
        if last_mint_at is not None and time() - last_mint_at < required_wait:
            continue
        mint_cap = max(1, MINT_MAX_SLOTS_PER_ATTEMPT >> recent_parks)
        # Proportional sizing (see the comment on MINT_BULK_MAX_UTILIZATION):
        # an IO-bound machine may take its full measured headroom, a busy one
        # takes half the remaining gap per step so it converges on saturation
        # without overshooting it.
        headroom_slots = int(
            (CPU_UTILIZATION_ADD_MAX - utilization) * (os.cpu_count() or 1)
        )
        if utilization <= MINT_BULK_MAX_UTILIZATION:
            batch_limit = headroom_slots
        else:
            batch_limit = max(1, headroom_slots // 2)
        asked = min(want - granted, mint_cap, batch_limit)
        if asked <= 0:
            continue
        try:
            minted = await head_client.request_mint(SELF["current_job"], asked)
        except Exception as error:
            await debug_log(
                "mint_request_failed",
                asked=asked,
                error=f"{type(error).__name__}: {error}",
            )
            continue
        if minted <= 0:
            continue
        SELF["target_parallelism"] += minted
        last_mint_at = time()
        await debug_log(
            "slots_minted",
            backlog=queued_inputs,
            traded=granted,
            asked=asked,
            minted=minted,
            mint_cap=mint_cap,
            parks_last_5min=recent_parks,
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
            # The window only counts time with the recovery loop's add-gates
            # red (see REPLACEMENT_DEFICIT_WINDOW_SEC); a green reading means
            # this machine can still absorb the deficit itself.
            deficit_red_since = SELF["replacement_deficit_since"]
            last_green_at = SELF["readd_gates_last_green_at"]
            if last_green_at is not None and last_green_at > deficit_red_since:
                deficit_red_since = last_green_at
            deficit_sustained = (
                time() - deficit_red_since > REPLACEMENT_DEFICIT_WINDOW_SEC
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
                # Memory sheds are what shrink the pool; after them the alive
                # count is this machine type's measured capacity for the job.
                # A fresh node starts its slots together, though, so they
                # peak together: cap the target at what fits when every
                # worker is at a typical attempt's peak (its recovery loop
                # probes up to the mixed-phase steady state from there).
                # Observed without the cap: ~150 attempts 3-8 minutes old
                # killed within six minutes of each replacement wave.
                slots_per_node = None
                if SELF["last_memory_shed_at"]:
                    slots_per_node = alive_workers
                    typical_peak = SELF["typical_attempt_peak_rss_bytes"]
                    live_workers = [
                        worker
                        for worker in SELF["workers"]
                        if not worker.retired and worker.worker_host_pid is not None
                    ]
                    if typical_peak and live_workers:
                        try:
                            limit_bytes = _workers_memory_limit_bytes(live_workers[0])
                        except OSError:
                            limit_bytes = None  # that worker died mid-read
                        if limit_bytes:
                            fits_at_peak = int(
                                limit_bytes * DYNAMIC_RAM_MAX_WORKER_MEMORY_USED_FRACTION
                                // typical_peak
                            )
                            slots_per_node = max(1, min(alive_workers, fits_at_peak))
                try:
                    response = await head_client.request_replacement_nodes(
                        SELF["current_job"], deficit, request_id, slots_per_node
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

        # Mint-epoch rollback (see head_client.push_state): with a backlog,
        # every worker is busy and sheds itself between inputs (see the check
        # at the top of _process_inputs). Workers parked on an empty queue
        # never reach that check, so they are retired here instead - but only
        # while the queue is empty under the retire lock (matching
        # trade_slots), because cancelling a get() that was just handed an
        # input loses that input (observed: a rollback ended a job 1499/1500).
        if SELF["shed_slots_pending"] > 0:
            shed_now = []
            async with SELF["dynamic_retire_lock"]:
                if SELF["inputs_queue"].qsize() == 0:
                    for worker in SELF["workers"]:
                        if SELF["shed_slots_pending"] <= 0:
                            break
                        if worker.retired or not worker.is_idle:
                            continue
                        if worker.current_input is not None:
                            continue
                        SELF["shed_slots_pending"] -= 1
                        worker.retired = True
                        SELF["reboot_containers_after_job"] = True
                        # The task is parked on inputs_queue.get(); left
                        # alive it would swallow (and lose) the next input
                        # to arrive.
                        if worker.process_inputs_task is not None:
                            worker.process_inputs_task.cancel()
                        shed_now.append(worker)
            for worker in shed_now:
                await worker.retire_for_pressure()
            if shed_now:
                await debug_log(
                    "workers_shed",
                    retired_idle=len(shed_now),
                    still_pending=SELF["shed_slots_pending"],
                    target_now=SELF["target_parallelism"],
                )

        # Safety net for stranded inputs: with zero alive workers and zero
        # target, nothing ever drains the queue (the re-add loop sees no
        # deficit and peers that finished their share never steal again), so
        # any input that slipped in after a trade-to-zero deadlocks the job.
        # Reclaiming target lets the re-add loop boot workers to finish it.
        stranded = (
            SELF["inputs_queue"].qsize() > 0
            and SELF["target_parallelism"] == 0
            and not any(not w.retired for w in SELF["workers"])
        )
        if stranded:
            SELF["target_parallelism"] = min(
                SELF["inputs_queue"].qsize(), os.cpu_count()
            )
            await debug_log(
                "stranded_inputs_reclaimed",
                queued_inputs=SELF["inputs_queue"].qsize(),
                target_now=SELF["target_parallelism"],
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
                active_input_steal=SELF["active_input_steal_id"],
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
        if SELF["target_parallelism"] <= 0 and await _claim_drained_job_part():
            steal_task.cancel()
            trade_task.cancel()
            await logger.log("All slots traded away and drained, done working on job!")
            await reset_workers(logger)
            break

        # Neighbor had no inputs for too long?
        if (
            SEC_NEIGHBOR_HAD_NO_INPUTS
            and SEC_NEIGHBOR_HAD_NO_INPUTS > EMPTY_NEIGHBOR_TIMEOUT_SEC
            and await _claim_drained_job_part()
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
        # _cancel_worker_input_tasks marks every worker retired to mute
        # teardown-kill logging; workers only reach here (the no-reboot reuse
        # path) when none genuinely died, so un-retire them or the next job
        # starts with zero alive workers and strands its inputs.
        w.retired = False

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


async def _cancel_worker_input_tasks(workers: list):
    # Cancel before any container kill so teardown does not get logged as
    # "Worker container stopped unexpectedly" on in-flight calls.
    tasks = []
    for worker in workers:
        task = worker.process_inputs_task
        if task is None:
            continue
        worker.retired = True
        task.cancel()
        tasks.append(task)
        worker.process_inputs_task = None
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def reset_workers(logger: Logger):
    # Stops idle or reassigned workers from holding creds for a finished job.
    NODE_AUTH_CREDENTIALS_PATH.unlink(missing_ok=True)
    for task_key in (
        "dynamic_ram_monitor_task",
        "dynamic_cpu_task",
        "worker_readd_task",
    ):
        monitor_task = SELF[task_key]
        if monitor_task is not None:
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                # A monitor that crashed mid-job (observed: FileNotFoundError
                # reading /proc/<pid>/cgroup of a worker the kernel OOM-killed)
                # re-raises here; it must never block teardown or the node
                # stays "running" forever and leaks its VM.
                await logger.log(
                    f"{task_key} crashed during the job: {e}", severity="ERROR"
                )
            SELF[task_key] = None
    workers = list(SELF["workers"]) + list(SELF["idle_workers"])
    await _cancel_worker_input_tasks(workers)
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
