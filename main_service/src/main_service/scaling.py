"""
Scaling policy and machine planning.

Machine planning (`plan_grow_nodes`) translates "N more slots" into concrete
machines to boot; it is shared by job-start growth, reconciler grow waves,
and mid-job replacement boots.

Scaling policy lives in the demand reconciler below. Nodes report load
(queued inputs, busy/alive workers, draining) on every state push; the
reconciler turns that global view plus rolling completion throughput and
observed boot durations into staged, forecast-gated scale-out:

- max_parallelism is a ceiling, not a provisioning target. Capacity is added
  in bounded waves, each justified by the demand that will remain once the
  new machines finish booting.
- A wave boots only while current capacity is saturated and the forecasted
  queue would keep the new machines at least WAVE_MIN_BUSY_FRACTION busy for
  WAVE_PAYBACK_SEC after their observed boot time. Pending (still-booting)
  capacity counts against demand before anything new boots.
- Pressure-replacement boots run through the same forecast and never start
  while another node for the job is already booting.
"""

import asyncio
import math
from collections import deque
from statistics import median
from time import time
from typing import Optional
from uuid import uuid4

from main_service import CLOUD_PROVIDER, IN_LOCAL_DEV_MODE, cluster_state, history
from main_service.providers.catalog import (
    gpu_machine_type,
    is_packable_cpu_machine,
    machine_type_cpu_count,
    pack_cpu_machines,
    parallelism_capacity,
)


def required_cpus_per_call(func_cpu: int | str, func_ram: int | str) -> int:
    func_cpu_for_scheduling = 1 if func_cpu == "dynamic" else int(func_cpu)
    func_ram_for_scheduling = 4 if func_ram == "dynamic" else int(func_ram)
    required_cpus_for_ram = (func_ram_for_scheduling + 3) // 4
    return max(func_cpu_for_scheduling, required_cpus_for_ram)


def plan_grow_nodes(
    missing_slots: int,
    func_cpu: int | str,
    func_ram: int | str,
    func_gpu: Optional[str],
    config: dict,
    max_additional_cpus: Optional[int] = None,
) -> list[dict]:
    """Plan machines covering `missing_slots` more parallelism.

    Returns one `{"instance_name", "machine_type", "target_parallelism"}` per
    node to boot; empty when nothing can be booted. `max_additional_cpus`
    (None = uncapped) bounds CPU jobs only; GPU jobs are sized purely by
    slots, matching job-start growth.
    """
    gpu_mt = gpu_machine_type(func_gpu, CLOUD_PROVIDER)

    if gpu_mt:
        gpu_slots_per_node = parallelism_capacity(gpu_mt, func_cpu, func_ram)
        n_nodes = math.ceil(missing_slots / gpu_slots_per_node)
        node_machine_types = [gpu_mt] * n_nodes
        parallelism_to_add = missing_slots
    else:
        cpus_per_call = required_cpus_per_call(func_cpu, func_ram)
        num_cpus_to_add = missing_slots * cpus_per_call
        if max_additional_cpus is not None:
            num_cpus_to_add = min(num_cpus_to_add, max(0, max_additional_cpus))
        parallelism_to_add = num_cpus_to_add // cpus_per_call
        if parallelism_to_add <= 0:
            return []

        node_spec = config["Nodes"][0]
        configured_machine_type = node_spec["machine_type"]

        # For CPU clusters, ignore the configured size and pack the required
        # CPUs into as many of the family's largest size as fit, with the
        # remainder covered by the smallest size that fits. GPU clusters keep
        # using the configured machine type so GPU jobs still land on GPU
        # hardware. Local dev stays homogeneous because node containers
        # hard-code 2 workers regardless of the advertised machine_type
        # (see INSTANCE_N_CPUS).
        pack_by_size = not IN_LOCAL_DEV_MODE and is_packable_cpu_machine(
            configured_machine_type
        )

        if pack_by_size:
            node_machine_types = pack_cpu_machines(num_cpus_to_add, CLOUD_PROVIDER)
        else:
            cpu_per_node = machine_type_cpu_count(configured_machine_type)
            n_nodes_to_add = math.ceil(num_cpus_to_add / cpu_per_node)
            node_machine_types = [configured_machine_type] * n_nodes_to_add

    # A machine_type whose capacity is 0 for this func_cpu/func_ram would boot
    # a node that can't run a single call, and the client would then send
    # parallelism=0 to it, producing a misleading 409 from the node.
    node_machine_types = [
        mt
        for mt in node_machine_types
        if parallelism_capacity(mt, func_cpu, func_ram) > 0
    ]
    if not node_machine_types:
        return []

    planned = []
    remaining_parallelism = parallelism_to_add
    for machine_type in node_machine_types:
        node_parallelism = min(
            remaining_parallelism,
            parallelism_capacity(machine_type, func_cpu, func_ram),
        )
        planned.append(
            {
                "instance_name": f"burla-node-{uuid4().hex[:8]}",
                "machine_type": machine_type,
                "target_parallelism": node_parallelism,
            }
        )
        remaining_parallelism -= node_parallelism
    return planned


def planned_cpu_count(planned: list[dict]) -> int:
    return sum(machine_type_cpu_count(p["machine_type"]) for p in planned)


# ------------------------------------------------------------------ reconciler

RECONCILE_INTERVAL_SEC = 3
THROUGHPUT_WINDOW_SEC = 30
# A new machine must be expected to run at least this busy for this long
# after it becomes ready, or the boot is not worth its cost.
WAVE_PAYBACK_SEC = 30.0
WAVE_MIN_BUSY_FRACTION = 0.5
# Current capacity counts as saturated when this fraction of alive workers
# is busy; waves never boot into unsaturated capacity.
SATURATION_BUSY_FRACTION = 0.9
# Wave bounds. The first wave (planned synchronously at job start) is small;
# each reconciler wave may then at most double the job's current slot count,
# capped in CPUs. Local-dev waves are one 2-CPU node container so staging is
# observable with LOCAL_DEV_MAX_GROW_CPUS=4.
GROW_CPUS_FIRST_WAVE = 2 if IN_LOCAL_DEV_MODE else 64
GROW_CPUS_PER_WAVE_MAX = 2 if IN_LOCAL_DEV_MODE else 256
# Used until enough boots have been observed to estimate from data.
DEFAULT_BOOT_SEC = 20.0 if IN_LOCAL_DEV_MODE else 150.0

# job_id -> reconciler state. wave_in_flight covers the window between
# deciding a wave and its nodes registering as BOOTING, during which a
# concurrent replacement request must not boot alongside it.
_JOB_SCALING: dict[str, dict] = {}


def _job_scaling_state(job_id: str) -> dict:
    return _JOB_SCALING.setdefault(
        job_id,
        {"samples": deque(maxlen=64), "last_deferral": None, "wave_in_flight": False},
    )


def _record_throughput_sample(job_id: str, total_results: int) -> float:
    """Record the job's completion count and return results/sec over the
    rolling window."""
    samples = _job_scaling_state(job_id)["samples"]
    now = time()
    samples.append((now, total_results))
    window_floor = now - THROUGHPUT_WINDOW_SEC
    oldest = next((s for s in samples if s[0] >= window_floor), samples[0])
    elapsed = now - oldest[0]
    if elapsed <= 0:
        return 0.0
    return max(0, total_results - oldest[1]) / elapsed


def boot_sec_estimate() -> float:
    durations = cluster_state.recent_boot_durations()
    return median(durations) if durations else DEFAULT_BOOT_SEC


def _forecast(snapshot: dict, throughput: float, new_slots: int) -> dict:
    """Will `new_slots` more slots still be worth having once they finish
    booting? Existing capacity keeps consuming the queue during the boot;
    pending (already-booting) slots must be fed before new ones count."""
    demand = sum(load["queued_inputs"] for load in snapshot["node_loads"])
    busy = sum(load["busy_workers"] for load in snapshot["node_loads"])
    pending_slots = sum(
        node["target_parallelism"] for node in snapshot["pending_nodes"]
    )
    boot_sec = boot_sec_estimate()
    queue_at_ready = demand - throughput * boot_sec
    if throughput > 0:
        # tasks each slot must find to stay WAVE_MIN_BUSY_FRACTION busy for
        # WAVE_PAYBACK_SEC, given the observed average task duration.
        average_task_sec = max(busy, 1) / throughput
        required_per_slot = WAVE_MIN_BUSY_FRACTION * (
            WAVE_PAYBACK_SEC / average_task_sec
        )
    else:
        # No completions yet: tasks are long (or the job just started), so
        # one queued input occupies a slot well past the payback window.
        required_per_slot = WAVE_MIN_BUSY_FRACTION
    required = (pending_slots + new_slots) * required_per_slot
    return {
        "justified": queue_at_ready >= max(required, 1.0),
        "demand": demand,
        "busy": busy,
        "pending_slots": pending_slots,
        "throughput_per_sec": round(throughput, 3),
        "boot_sec": round(boot_sec, 1),
        "queue_at_ready": round(queue_at_ready, 1),
        "required_inputs_at_ready": round(required, 1),
    }


def replacement_deferral(
    job_id: str, snapshot: Optional[dict], missing_slots: int
) -> Optional[str]:
    """Reason to defer a pressure-replacement boot right now, or None to let
    it proceed. Replacements are strictly serial with any other boot for the
    job, then face the same payback forecast as a grow wave."""
    if snapshot is None:
        return "job_not_running_grow"
    if snapshot["pending_nodes"] or _job_scaling_state(job_id)["wave_in_flight"]:
        return "node_already_booting"
    throughput = _record_throughput_sample(job_id, snapshot["total_num_results"])
    forecast = _forecast(snapshot, throughput, missing_slots)
    if not forecast["justified"]:
        return "forecast_unjustified"
    return None


def _plan_wave(
    snapshot: dict, throughput: float
) -> tuple[list[dict], Optional[str], dict]:
    """One reconciler evaluation: (nodes to boot, deferral reason, forecast).

    Gate order mirrors the policy: a client must be connected (only it can
    assign the job to new nodes), the parallelism ceiling and CPU budget must
    have room, current capacity must be saturated, and the payback forecast
    must justify the wave.
    """
    if not snapshot["any_node_client_contact"]:
        return [], "client_disconnected", {}

    node_loads = snapshot["node_loads"]
    steady_loads = [load for load in node_loads if not load["draining"]]
    demand = sum(load["queued_inputs"] for load in node_loads)
    busy = sum(load["busy_workers"] for load in steady_loads)
    alive = sum(load["alive_workers"] for load in steady_loads)

    unfinished = snapshot["n_inputs"] - snapshot["total_num_results"]
    parallelism_ceiling = min(unfinished, snapshot["max_parallelism"])
    targets_sum = sum(load["target_parallelism"] for load in node_loads) + sum(
        node["target_parallelism"] for node in snapshot["pending_nodes"]
    )
    slots_budget = parallelism_ceiling - targets_sum
    if slots_budget <= 0:
        return [], "at_parallelism_ceiling", {}

    grow_cpus_remaining = snapshot["grow_cpus_remaining"]
    if grow_cpus_remaining is not None and grow_cpus_remaining <= 0:
        return [], "grow_cpu_budget_exhausted", {}

    if not node_loads:
        # Nothing is running yet (first wave still booting, or nothing ever
        # became ready): there is no evidence to justify more capacity.
        return [], "no_running_capacity", {}
    saturated = alive == 0 or busy >= SATURATION_BUSY_FRACTION * alive
    if not (saturated and demand > alive):
        return [], "capacity_not_saturated", {}

    # Each wave may at most double the job's current slot count (exponential
    # ramp), bounded by the ceiling and the per-wave CPU cap.
    wave_slots = min(slots_budget, max(targets_sum, 1))
    cpus_per_call = required_cpus_per_call(snapshot["func_cpu"], snapshot["func_ram"])
    if not gpu_machine_type(snapshot["func_gpu"], CLOUD_PROVIDER):
        wave_slots = min(wave_slots, max(GROW_CPUS_PER_WAVE_MAX // cpus_per_call, 1))

    forecast = _forecast(snapshot, throughput, wave_slots)
    if not forecast["justified"]:
        return [], "forecast_unjustified", forecast

    max_additional_cpus = GROW_CPUS_PER_WAVE_MAX
    if grow_cpus_remaining is not None:
        max_additional_cpus = min(grow_cpus_remaining, GROW_CPUS_PER_WAVE_MAX)

    # Lazy import: cluster_lifecycle imports from the main_service package,
    # which is mid-initialization when this module is first imported.
    from main_service.endpoints.cluster_lifecycle import (
        _get_cluster_config,
        config_with_job_overrides,
    )

    config = config_with_job_overrides(
        _get_cluster_config(), snapshot["region"], snapshot["disk_gb"]
    )
    planned = plan_grow_nodes(
        missing_slots=wave_slots,
        func_cpu=snapshot["func_cpu"],
        func_ram=snapshot["func_ram"],
        func_gpu=snapshot["func_gpu"],
        config=config,
        max_additional_cpus=max_additional_cpus,
    )
    if not planned:
        return [], "no_bootable_machines", forecast
    snapshot["_config"] = config
    return planned, None, forecast


async def _reconcile_job(job_id: str, logger):
    from main_service.endpoints.cluster_lifecycle import (
        GROW_INACTIVITY_SHUTDOWN_TIME_SEC,
        _start_nodes,
    )

    snapshot = cluster_state.job_scaling_snapshot(job_id)
    if snapshot is None:
        return
    state = _job_scaling_state(job_id)
    # Sampled every tick, gated or not, so the throughput window is
    # continuous for both grow waves and replacement-boot forecasts.
    throughput = _record_throughput_sample(job_id, snapshot["total_num_results"])

    planned, deferral, forecast = _plan_wave(snapshot, throughput)

    if deferral is not None:
        # Log once per reason change, not every tick.
        if deferral != state["last_deferral"]:
            state["last_deferral"] = deferral
            await asyncio.to_thread(
                history.add_debug_logs,
                "head",
                [
                    {
                        "job_id": job_id,
                        "event": "grow_wave_deferred",
                        "fields": {"reason": deferral, **forecast},
                    }
                ],
            )
        return
    state["last_deferral"] = None

    image = snapshot["image"]
    containers_override = [{"image": image}] if image else None
    wave = {
        "at": time(),
        "booted": planned,
        "slots": sum(p["target_parallelism"] for p in planned),
    }
    # Record the wave before the (slow, blocking) boot so a concurrent
    # replacement request already sees the spent budget; wave_in_flight
    # covers the gap until the boot registers the BOOTING nodes that make
    # the wave visible as pending capacity.
    state["wave_in_flight"] = True
    cluster_state.record_grow_wave(job_id, wave, cpus_booted=planned_cpu_count(planned))
    names = [p["instance_name"] for p in planned]
    logger.log(
        f"Booting grow wave of {len(planned)} node(s) {names} covering "
        f"{wave['slots']} slots for job {job_id}."
    )
    await asyncio.to_thread(
        history.add_debug_logs,
        "head",
        [
            {
                "job_id": job_id,
                "event": "grow_wave_planned",
                "fields": {"booted": planned, **forecast},
            }
        ],
    )
    try:
        # Blocks until every VM create call returns, which is what makes the
        # new nodes visible as pending before this job is evaluated again.
        await asyncio.to_thread(
            _start_nodes,
            logger,
            snapshot["_config"],
            len(planned),
            names,
            job_id,
            [p["machine_type"] for p in planned],
            containers_override,
            GROW_INACTIVITY_SHUTDOWN_TIME_SEC,
            [p["target_parallelism"] for p in planned],
        )
    finally:
        state["wave_in_flight"] = False


async def demand_reconciler_loop(logger):
    """Head-owned scale-out: evaluate every RUNNING grow job on an interval
    and boot the next justified wave. Replaces per-node judgement calls with
    one global view."""
    while True:
        await asyncio.sleep(RECONCILE_INTERVAL_SEC)
        try:
            active = cluster_state.grow_job_ids()
            for job_id in list(_JOB_SCALING.keys() - set(active)):
                del _JOB_SCALING[job_id]
            for job_id in active:
                await _reconcile_job(job_id, logger)
        except Exception as error:
            print(f"demand reconciler tick failed: {error}")
