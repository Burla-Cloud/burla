import asyncio
import errno
import math
import os
import pickle
import signal
import socket
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import aiodocker
import psutil

from node_service import (
    SELF,
    BURLA_CLUSTER_NAME,
    IN_LOCAL_DEV_MODE,
    INSTANCE_N_CPUS,
    Logger,
    __version__,
    head_client,
)
from node_service.helpers import debug_log
from node_service.resource_metrics import record_call_event

# Sized so node_service's buffering fits inside its own memory reservation
# (NODE_SERVICE_RESERVED_MEMORY_GB, 4GB on real VMs): workers own the rest of
# the machine, so a bigger budget here lets node_service + workers outgrow
# physical RAM and thrash the host. The fraction keeps small machines sane.
RESULTS_QUEUE_RAM_LIMIT_BYTES = min(
    2 * 1024**3, int(psutil.virtual_memory().total * 0.25)
)

WORKER_INTERNAL_PORT = 8080
LOG_FLUSH_INTERVAL_SECONDS = 1
MAX_LOG_DOCUMENT_SIZE_BYTES = 100_000
TRUNCATED_LOG_SUFFIX = "<too-long--remaining-msg-truncated-due-to-length>"
LOG_START_MARKER_PREFIX = "__burla_input_start__:"
LOG_END_MARKER_PREFIX = "__burla_input_end__:"
OOM_KILL_MARKER_PREFIX = "__burla_oom_kill__:"

# The first worker on a fresh VM downloads uv from GitHub and installs burla + its deps into
# /worker_service_python_env before opening its socket. Under network slowness, or in local-dev
# where that env dir lives behind Docker Desktop and an extra layer of NAT, this legitimately
# takes minutes.
WORKER_BOOT_TIMEOUT_SECONDS = 180
# Park when the workers slice is genuinely close to its cap. "Genuinely"
# means unreclaimable bytes (anon + shmem), never summed worker-process RSS
# and never memory.current: page cache reads as fullness while costing
# nothing to reclaim, and process RSS misses page cache, /dev/shm, and child
# processes entirely (observed on pd12m: slices filled via cache+shm while
# RSS sums read comfortable, so parking never fired and nodes thrashed to a
# standstill instead).
DYNAMIC_RAM_MAX_WORKER_MEMORY_USED_FRACTION = 0.95
DYNAMIC_RAM_TARGET_WORKER_MEMORY_USED_FRACTION = 0.90
DYNAMIC_RAM_MONITOR_INTERVAL_SECONDS = 0.25
DYNAMIC_RAM_STARTUP_MONITOR_INTERVAL_SECONDS = 0.05
DYNAMIC_RAM_STARTUP_MONITOR_SECONDS = 30

# PSI-availability probe only; sensing reads each worker container's own
# cpu.pressure instead (see WorkerStallTracker). The root file exists iff the
# kernel has PSI, and its cgroup-root path (rather than /proc/pressure/cpu)
# keeps the probe scoped to the fake VM inside a local-dev DinD node.
CPU_PRESSURE_FILE = Path("/sys/fs/cgroup/cpu.pressure")
# Dynamic CPU never parks a worker for CPU: a saturated machine does the same
# total work however it is split, so pausing only reorders who finishes
# (cpu.weight below does that without freezing anyone) and every park/unpark
# cycle is churn (observed: 17k parks and 400 restarts on one job for no wall
# time gained). Saturation is instead the signal that in-flight calls gain
# from moving: with no idle core every runnable thread is queueing, so a peer
# with idle capacity may take this node's least-progressed running calls
# (revoke_inputs_for_idle_peer). Below saturation nobody waits for a core and
# moving a call only forfeits its progress. The gate reads the latest
# one-second sample so a node stops donating (or pulling) the second a
# transfer changes its load; a 30s average kept a donor draining onto one
# neighbor long after it had stopped being saturated.
#
# Queueing (PSI stall), not utilization, is the brake on adding capacity: a
# two-phase task mix idles cores between downloads, so a node at 100%
# utilization with stall under the re-add bar is perfectly packed, not
# overloaded. The add-side ceiling exists only to stop growth when PSI has a
# blind spot, so it sits above the saturation line, not below it: capping
# adds at 0.90 measurably stranded whole fleets at ~92% CPU because slot
# growth stopped three points shy of saturation.
CPU_PRESSURE_MONITOR_INTERVAL_SECONDS = 1
CPU_UTILIZATION_SATURATED = 0.95
CPU_UTILIZATION_ADD_MAX = 0.97
# Fast unpark, and pulling peers' in-flight work, need clearly idle cores,
# below the saturation line, so a node never both pulls and donates. Per-worker
# PSI is ignored here: a multiprocess call pins its own cgroup's stall at any
# node utilization, which used to hold recovery shut while cores sat empty.
# This path never mints workers.
CPU_UTILIZATION_RECOVER_MAX = 0.90
CPU_PRESSURE_RAW_RECOVER_SECONDS = 3

# Progress-weighted CPU shares. Under contention the kernel splits the cores
# among the worker cgroups by cpu.weight, so with equal weights one long
# multi-threaded call competing with dozens of short ones is held to about
# one core and can run ten times longer than it would on an idle machine.
# Growing a call's weight by one point per CPU-second it has already burned
# lets the calls with the most invested finish first, which is what bounds
# the job's tail; idle cores still go to whoever is runnable, so weights
# cost nothing when the machine is not saturated. The kernel default is 100
# and the maximum 10000.
CPU_WEIGHT_DEFAULT = 100
CPU_WEIGHT_MAX = 10_000

# Damping: on a download-then-parse workload every task alternates near-zero
# and pinned CPU, so raw 1s samples flip the gates each phase change. Gates
# therefore run on a ~30s EWMA. Recovery's raw path unparks only after a few
# seconds of clearly idle cores, so a parse-gap blip cannot immediately undo
# a park. Near saturation, recovery still waits for the stall-gated smoothed
# add path. The RAM monitor's park/kill triggers stay raw because memory
# emergencies cannot wait.
GATE_EWMA_TAU_SECONDS = 30
# Longest lookback any damper uses (the mint halving in job_watcher.py).
PARK_EVENT_RETENTION_SECONDS = 300

# Throttled workers are parked, not progressing: the quota keeps TCP,
# heartbeats, and library timers alive while leaving the machine to the
# unthrottled workers, and keeps the attempt cheap to revoke to a peer node.
# 1000us per 100ms period is the kernel-minimum quota: 1% of one core.
THROTTLED_CPU_QUOTA_USEC = 1_000
CPU_QUOTA_PERIOD_USEC = 100_000

# Worker recovery: the inverse of the pressure monitors. Thresholds sit well
# below the throttle/retire thresholds (hysteresis) so a node doesn't
# oscillate between parking and recovering the same worker.
READD_MONITOR_INTERVAL_SECONDS = 1
# Parallel boots per re-add tick (see the batch comment in the loop).
READD_BOOT_BATCH_MAX = 8
READD_MAX_CPU_STALL_FRACTION = 0.05
# A worker is added only if a typical attempt's peak still fits under the
# park trigger: instantaneous usage is a poor gate for workloads whose
# per-worker memory swings between phases (Sentinel-2 downloads at ~2GiB,
# then inflates to 15-60GiB), where it either strands RAM (a 0.75 gate
# averaged ~55% fleet RAM) or adds during a lull only to kill a minute later.
# The typical peak is a smoothed average of finished attempts' peaks; until
# one has finished, the fraction below gates instead.
READD_MAX_WORKER_MEMORY_USED_FRACTION = 0.85
ATTEMPT_PEAK_EWMA_ALPHA = 0.3
# Slot transfers keep a cooldown so pressured nodes do not immediately trade
# capacity back and forth. Worker recovery itself has no time-based cooldown.
SLOT_TRADE_PRESSURE_COOLDOWN_SECONDS = 30

# Capacity recovery is also blocked while the disk or the NIC is already the
# bottleneck: IO-bound work looks like low CPU + low RAM, which would
# otherwise invite over-packing. Neither has a park/kill-side monitor, so an
# overload caused by adding or unparking a worker would never self-correct;
# these gates are the only control.
IO_PRESSURE_FILE = Path("/sys/fs/cgroup/io.pressure")
READD_MAX_IO_STALL_FRACTION = 0.05
READD_MAX_NETWORK_UTILIZATION_FRACTION = 0.6
# Memory stall gates adds/mints the same way: zram swap absorbs an
# over-packed slice without CPU stall or high measured utilization, so
# thrash reads as headroom to every other gate (observed: minting ran a
# 64-core node to ~490 workers and the job's tail thrashed to a standstill).
MEMORY_PRESSURE_FILE = Path("/sys/fs/cgroup/memory.pressure")
READD_MAX_MEMORY_STALL_FRACTION = 0.05

# Memory parking: under memory pressure workers are parked (CPU throttle +
# resident memory reclaimed into swap via their cgroup's memory.reclaim)
# instead of killed; the kill path stays as the backstop below. Chunks are
# small enough that a slow disk still shows progress every couple of seconds
# (observed ~50MiB/s on EBS-backed swap), since the thrash kill timer treats
# a reclaim that stops progressing as not recovering.
MEMORY_RECLAIM_CHUNK_BYTES = 64 * 1024**2
# Soft ceiling on the workers slice, set for dynamic-RAM jobs: an allocator
# crossing it is stalled by the kernel (direct reclaim + forced sleeps inside
# the allocation path), so a fast-growing worker cannot outrun the monitor's
# ticks to OOM while parked workers are being reclaimed. It MUST sit between
# the park trigger (DYNAMIC_RAM_MAX_WORKER_MEMORY_USED_FRACTION) and
# memory.max: placed below the trigger, the kernel clamps usage under the
# trigger and the monitor never parks anyone, stalling the job forever.
MEMORY_HIGH_WORKER_MEMORY_FRACTION = 0.985
# Reactive park trigger, paired with the byte gauge above: the slice is
# measurably paying time to memory (some-stall) even when the byte gauge
# reads fine, e.g. thrash driven by charges the gauge can't attribute. avg10
# is already a kernel-smoothed window; reclaim-in-flight is excluded exactly
# like the thrash-kill timer below, since our own reclaim stalls the slice.
MEMORY_PSI_SOME_PARK_FRACTION = 0.05
# avg10 keeps reading high for seconds after a shed, and a kill (unlike a
# park) starts no reclaim to gate the next tick, so PSI-only shedding would
# otherwise kill one worker per tick until the average caught up (observed:
# 5 -> 2 workers in one second). Byte-triggered sheds need no cooldown; the
# gauge reflects a kill on the next tick.
STALL_SHED_COOLDOWN_SECONDS = 5
# A shed frees exactly the current excess, and lowest-RSS-first frees the
# least per worker, so when every survivor is still growing the freed bytes
# are gone by the next tick and the slice sits pinned at memory.high with the
# growers stalled (observed: 64 -> 5 workers took eight minutes, and a stalled
# download timed out after 120s). Survivors take 3-15s to refill after a shed,
# so sheds within this window count as one storm and double the worker count
# each time, capped at a quarter of the running workers per shed so a
# transient spike cannot gut the node; the re-add dwell fills overshoot back.
SHED_ESCALATION_WINDOW_SECONDS = 30
SHED_ESCALATION_MAX_FRACTION = 0.25
# After a memory shed the survivors' heaps are still growing, so the re-add
# gauge reads low and would boot a full batch that gets killed a minute later
# (observed: 2 -> 16 workers in three seconds, then 16 -> 10). Within this
# window of a shed, re-add one worker per dwell so demand shows before the
# next add.
MEMORY_READD_DWELL_SECONDS = 10
# Backstop kill triggers: swap nearly full, or memory PSI showing the slice
# stalled on memory (thrash) while no reclaim is actually freeing memory.
SWAP_NEARLY_FULL_FRACTION = 0.90
MEMORY_PSI_FULL_KILL_FRACTION = 0.15
MEMORY_PSI_FULL_KILL_SECONDS = 10
# A reclaim that has not freed memory for this long no longer excuses the
# stall: an in-flight reclaim used to mask the thrash timer unconditionally,
# and under sustained pressure one is nearly always in flight, so a node could
# thrash to death with its own alarm switched off.
RECLAIM_PROGRESS_GRACE_SECONDS = 2
# Kill vs park: a shed worker is killed (its input requeues) when redoing the
# attempt costs less than moving its heap to swap and back, or when it is
# mid-download (parking freezes its sockets, so the attempt fails on resume
# anyway). The round trip is priced with the node's measured swap rate: a
# 256MiB out-and-back at boot, refined by every real reclaim afterward.
SWAP_CALIBRATION_BYTES = 256 * 1024**2
SWAP_CALIBRATION_CGROUP = "burla-swap-calibration"
SWAP_RATE_EWMA_ALPHA = 0.3
SWAP_RATE_MIN_SAMPLE_BYTES = 64 * 1024**2
NETWORK_BOUND_WINDOW_SECONDS = 3
NETWORK_BOUND_MIN_RX_BYTES = 1024**2
# A swapped worker may resume only when its swapped pages fit back into RAM
# without recreating the pressure that parked it; below the re-add threshold
# would never resume anything the moment two parked workers exist.
RESUME_MEMORY_HEADROOM_FRACTION = 0.85
# Swap-parked workers that cannot fit back into RAM while idle workers wait
# on them would deadlock a single-node job; after this grace period one is
# killed so its input requeues to the waiting idle capacity.
PARKED_UNRESUMABLE_KILL_SECONDS = 30


class WorkerOutOfMemoryError(RuntimeError):
    pass


class WorkerProcessTerminatedError(RuntimeError):
    pass


class WorkerFunctionError(Exception):
    def __init__(self, error_info_pkl: bytes, traceback_str: str):
        self.error_info_pkl = error_info_pkl
        self.traceback_str = traceback_str
        super().__init__(traceback_str)


def oom_kill_marker_count(logs: str):
    return sum(
        line.strip().startswith(OOM_KILL_MARKER_PREFIX) for line in logs.splitlines()
    )


def _is_worker_internal_log_message(message: str) -> bool:
    stripped = message.strip()
    return (
        stripped == "Killed"
        or stripped.startswith(OOM_KILL_MARKER_PREFIX)
        or stripped in {"3.11", "3.12", "3.13", "3.14"}
        or stripped.startswith("Using CPython ")
        or stripped.startswith("× No solution found when resolving dependencies:")
        or stripped.startswith("╰─▶ Because there is no version of burla==")
        or stripped.startswith("burla==")
        or stripped.startswith("Checked 1 package in ")
    )


def _active_dynamic_workers():
    return [worker for worker in SELF["workers"] if not worker.retired]


class Ewma:
    """Exponential moving average with a wall-time constant, so irregular tick
    spacing (asyncio loops stall under load) doesn't change how much history
    the smoothed value carries."""

    def __init__(self, tau_seconds: float):
        self.tau_seconds = tau_seconds
        self.value = 0.0
        self._last_update_at = None

    def update(self, sample: float) -> float:
        now = time.perf_counter()
        if self._last_update_at is None:
            self.value = sample
        else:
            elapsed = now - self._last_update_at
            weight = 1 - math.exp(-elapsed / self.tau_seconds)
            self.value += (sample - self.value) * weight
        self._last_update_at = now
        return self.value


def record_park_event():
    now = time.time()
    SELF["park_event_times"] = [
        parked_at
        for parked_at in SELF["park_event_times"]
        if now - parked_at < PARK_EVENT_RETENTION_SECONDS
    ]
    SELF["park_event_times"].append(now)


def parks_in_last(seconds: float) -> int:
    cutoff = time.time() - seconds
    return sum(parked_at >= cutoff for parked_at in SELF["park_event_times"])


async def _relocate_worker_process_or_retire(worker: "WorkerClient"):
    # The cached pid goes stale whenever worker_server.py exits and the
    # container's shell loop relaunches it (OOM kill, crash). That is a
    # restart, not a death: re-locate the process and only retire the worker
    # when its container is actually gone.
    stale_pid = worker.worker_host_pid
    try:
        worker.worker_host_pid = await worker._get_worker_host_pid()
    except Exception:
        container_info = await worker.container.show()
        if container_info["State"]["Running"]:
            return  # worker_server.py is mid-relaunch, check next poll
        worker.retired = True
        worker.is_idle = True
        SELF["reboot_containers_after_job"] = True
        SELF["last_pressure_retirement_at"] = time.time()
        await Logger().log(
            f"Retired {worker.container_name}: process {stale_pid} is "
            "gone and its container is not running.",
            severity="WARNING",
        )


WORKERS_CGROUP_SLICE = "burla-workers.slice"
NODE_SERVICE_CGROUP_SLICE = "burla-node-service.slice"


def _workers_cgroup_slice_dir(worker):
    # systemd nests slices by dash-splitting their names (burla-workers.slice
    # lives at /sys/fs/cgroup/burla.slice/burla-workers.slice), so resolve the
    # slice's real directory from a worker's own cgroup path instead of
    # guessing it.
    worker_cgroup = Path(f"/proc/{worker.worker_host_pid}/cgroup").read_text()
    for line in worker_cgroup.splitlines():
        cgroup_path = line.split(":", 2)[2]
        if WORKERS_CGROUP_SLICE in cgroup_path:
            segments = cgroup_path.strip("/").split("/")
            slice_depth = segments.index(WORKERS_CGROUP_SLICE) + 1
            return Path("/sys/fs/cgroup", *segments[:slice_depth])
    return None


def _workers_memory_limit_bytes(worker) -> int:
    """How much memory the kernel actually lets the workers use.

    The VM startup script caps burla-workers.slice at MemTotal minus
    node_service's reservation (see NODE_SERVICE_RESERVED_MEMORY_GB in
    main_service/node.py), so on nodes below ~40GiB that cap sits under any
    trigger keyed to MemTotal: the cgroup OOM-kills a worker before shedding
    could ever fire.
    """
    slice_dir = _workers_cgroup_slice_dir(worker)
    memory_max = None
    if slice_dir is not None and (slice_dir / "memory.max").exists():
        memory_max = (slice_dir / "memory.max").read_text().strip()
    if memory_max is None or memory_max == "max":
        # Isolation isn't active; verify_worker_cgroup_isolation already logged
        # that as an ERROR, and physical RAM is the only real ceiling left.
        return psutil.virtual_memory().total
    return int(memory_max)


async def verify_worker_cgroup_isolation(workers: list, logger: Logger):
    """The VM startup script (see main_service/node.py) puts node_service and
    the workers in systemd slices so user load can never starve node_service.
    Whether that actually takes effect depends on the image's cgroup version,
    docker's cgroup driver, and systemd, any of which can silently ignore it,
    so every node proves the isolation at boot instead of trusting it.
    """
    if IN_LOCAL_DEV_MODE:
        # Fake VMs build the same two slices by hand (see
        # local_dev_entrypoint.sh) but deliberately leave the workers' memory
        # cap off: their "machine" is the docker VM every cluster shares, so
        # there is no per-node memory total to carve up.
        return

    problems = []
    node_cgroup = Path("/proc/self/cgroup").read_text()
    if NODE_SERVICE_CGROUP_SLICE not in node_cgroup:
        problems.append(
            f"node_service runs outside {NODE_SERVICE_CGROUP_SLICE} "
            f"(cgroup: {node_cgroup.strip()!r})"
        )
    slice_dir = None
    for worker in workers:
        worker_cgroup = Path(f"/proc/{worker.worker_host_pid}/cgroup").read_text()
        if WORKERS_CGROUP_SLICE not in worker_cgroup:
            problems.append(
                f"{worker.container_name} runs outside {WORKERS_CGROUP_SLICE} "
                f"(cgroup: {worker_cgroup.strip()!r})"
            )
        elif slice_dir is None:
            slice_dir = _workers_cgroup_slice_dir(worker)

    memory_max = cpu_weight = None
    if slice_dir is not None and not (slice_dir / "memory.max").exists():
        # Enough detail to diagnose from the log alone, since this only ever
        # fires on a real VM nobody can shell into.
        slice_contents = (
            sorted(p.name for p in slice_dir.iterdir())[:12]
            if slice_dir.is_dir()
            else "<no such directory>"
        )
        problems.append(
            f"{slice_dir} has no memory.max file (slice dir: {slice_contents})"
        )
    elif slice_dir is not None:
        memory_max = (slice_dir / "memory.max").read_text().strip()
        cpu_weight = (slice_dir / "cpu.weight").read_text().strip()
        if memory_max == "max":
            problems.append(
                f"{WORKERS_CGROUP_SLICE} has no memory cap (memory.max=max)"
            )
        # 80 is what the startup script writes; anything else means the config
        # was not applied (100 is the kernel default).
        if cpu_weight != "80":
            problems.append(
                f"{WORKERS_CGROUP_SLICE} cpu.weight is {cpu_weight}, expected 80"
            )

    if problems:
        message = (
            "WORKER CGROUP ISOLATION IS NOT ACTIVE on this node: "
            + "; ".join(problems)
            + ". An intense workload can starve node_service here, making a "
            "healthy node look dead."
        )
        await logger.log(message, severity="ERROR")
    else:
        # Swap total tells whether memory parking can work on this node (the
        # RAM monitor falls back to killing when there is none).
        swap_total_bytes = psutil.swap_memory().total
        await logger.log(
            f"Worker cgroup isolation verified: {len(workers)} workers in "
            f"{WORKERS_CGROUP_SLICE} (memory.max={memory_max}, "
            f"cpu.weight={cpu_weight}), node_service in {NODE_SERVICE_CGROUP_SLICE}. "
            f"Node swap: {swap_total_bytes // 1024**2}MiB."
        )


async def dynamic_ram_monitor_loop():
    started_at = time.perf_counter()
    worker_memory_limit_bytes = None
    memory_high_bytes = None
    memory_high_active = False
    slice_dir = None
    psi_thrashing_since = None
    unresumable_since = None
    shed_times = []
    try:
        while SELF["dynamic_func_ram"]:
            startup_window = (
                time.perf_counter() - started_at < DYNAMIC_RAM_STARTUP_MONITOR_SECONDS
            )
            interval = (
                DYNAMIC_RAM_STARTUP_MONITOR_INTERVAL_SECONDS
                if startup_window
                else DYNAMIC_RAM_MONITOR_INTERVAL_SECONDS
            )
            await asyncio.sleep(interval)
            active_workers = _active_dynamic_workers()
            if not active_workers:
                return
            if worker_memory_limit_bytes is None:
                try:
                    worker_memory_limit_bytes = _workers_memory_limit_bytes(
                        active_workers[0]
                    )
                    slice_dir = _workers_cgroup_slice_dir(active_workers[0])
                except OSError:
                    # That worker's process just died (e.g. kernel OOM);
                    # /proc/<pid>/cgroup vanished mid-read. Retry next tick.
                    worker_memory_limit_bytes = None
                    continue
                memory_high_bytes = int(
                    worker_memory_limit_bytes * MEMORY_HIGH_WORKER_MEMORY_FRACTION
                )

            worker_memory = []
            for worker in active_workers:
                try:
                    rss_bytes = worker.memory_rss_bytes()
                    worker_memory.append((rss_bytes, worker))
                    worker.attempt_peak_rss_bytes = max(
                        worker.attempt_peak_rss_bytes, rss_bytes
                    )
                    worker.sample_activity()
                except psutil.NoSuchProcess:
                    await _relocate_worker_process_or_retire(worker)
            if not worker_memory:
                continue

            # Kernel-level stall net (see MEMORY_HIGH_GAP_*), active only
            # while parking can still help: with one worker left the job is
            # in its terminal straight-to-OOM regime and the stall would just
            # delay the OOM error the user needs to see. Cleared in the
            # finally so fixed-RAM jobs are never affected.
            if slice_dir is not None:
                want_memory_high = len(worker_memory) > 1
                if want_memory_high and not memory_high_active:
                    (slice_dir / "memory.high").write_text(str(memory_high_bytes))
                    memory_high_active = True
                elif not want_memory_high and memory_high_active:
                    (slice_dir / "memory.high").write_text("max")
                    memory_high_active = False

            reclaim_in_flight = any(
                worker.reclaim_in_progress for worker in active_workers
            )
            reclaim_recovering = any(
                worker.reclaim_in_progress
                and time.time() - worker.last_reclaim_progress_at
                < RECLAIM_PROGRESS_GRACE_SECONDS
                for worker in active_workers
            )
            # Per-worker RSS (worker_memory) stays the victim-ranking metric;
            # the aggregate gauge is the slice's unreclaimable usage, which
            # sees the cache/shm/child charges process RSS cannot.
            if slice_dir is not None:
                used_memory_bytes = _read_memory_stat_unreclaimable(slice_dir)
            else:
                used_memory_bytes = sum(rss_bytes for rss_bytes, _ in worker_memory)

            # Deadlock backstop, checked every tick (a stuck node can sit well
            # below the pressure trigger): swap-parked workers nobody can
            # resume, while idle workers wait on them, would hang a
            # single-node job forever. After a grace period kill one (largest
            # RSS, mirroring the kill path's parked-first order) so its input
            # requeues to the waiting idle capacity.
            swap_parked_memory = [
                (rss_bytes, worker)
                for rss_bytes, worker in worker_memory
                if worker.swap_parked and worker.current_input is not None
            ]
            idle_workers_waiting = any(
                worker.is_idle and not worker.throttled for worker in active_workers
            )
            deadlocked = (
                bool(swap_parked_memory)
                and idle_workers_waiting
                and not reclaim_in_flight
                and not _any_parked_worker_resumable(
                    active_workers,
                    used_memory_bytes,
                    worker_memory_limit_bytes,
                )
            )
            if not deadlocked:
                unresumable_since = None
            elif unresumable_since is None:
                unresumable_since = time.time()
            elif time.time() - unresumable_since > PARKED_UNRESUMABLE_KILL_SECONDS:
                unresumable_since = None
                swap_parked_memory.sort(key=lambda item: item[0], reverse=True)
                await retire_workers_for_pressure(
                    swap_parked_memory[:1],
                    reason="memory pressure (parked worker cannot resume)",
                )
                continue

            used_memory_fraction = used_memory_bytes / worker_memory_limit_bytes
            over_byte_trigger = (
                used_memory_fraction >= DYNAMIC_RAM_MAX_WORKER_MEMORY_USED_FRACTION
            )
            # Reactive twin of the byte gauge: pd12m proved a slice can pay
            # heavy reclaim cost while every byte counter reads comfortable.
            # When PSI alone triggers, bytes_to_free lands at 0 and the park
            # selection below naturally sheds exactly one worker per tick.
            memory_psi_some = (
                _read_memory_psi_some_avg10(slice_dir)
                if slice_dir is not None
                else 0.0
            )
            stall_pressured = (
                memory_psi_some >= MEMORY_PSI_SOME_PARK_FRACTION
                and not reclaim_in_flight
                and time.time() - SELF["last_pressure_retirement_at"]
                >= STALL_SHED_COOLDOWN_SECONDS
            )
            if not over_byte_trigger and not stall_pressured:
                psi_thrashing_since = None
                continue

            if len(worker_memory) <= 1:
                continue

            target_used_bytes = int(
                worker_memory_limit_bytes
                * DYNAMIC_RAM_TARGET_WORKER_MEMORY_USED_FRACTION
            )
            bytes_to_free = max(0, used_memory_bytes - target_used_bytes)

            # Backstops: parking only helps if swap can absorb the parked
            # memory and reclaim actually recovers RAM. When either fails,
            # fall back to killing (swap thrash must not replace OOM as the
            # failure mode).
            swap = psutil.swap_memory()
            kill_reason = None
            if swap.total == 0:
                kill_reason = "memory pressure (no swap on this node)"
            elif swap.percent >= SWAP_NEARLY_FULL_FRACTION * 100:
                kill_reason = "memory pressure (swap nearly full)"
            elif slice_dir is not None:
                psi_full_avg10 = _read_memory_psi_full_avg10(slice_dir)
                # A reclaim that is freeing memory legitimately stalls the
                # slice; one that has stopped making progress does not.
                psi_thrashing = (
                    psi_full_avg10 > MEMORY_PSI_FULL_KILL_FRACTION
                    and not reclaim_recovering
                )
                if not psi_thrashing:
                    psi_thrashing_since = None
                elif psi_thrashing_since is None:
                    psi_thrashing_since = time.time()
                elif time.time() - psi_thrashing_since > MEMORY_PSI_FULL_KILL_SECONDS:
                    psi_thrashing_since = None
                    kill_reason = "memory pressure (reclaim not recovering)"

            if kill_reason is not None:
                # Parked (throttled) workers die first, largest RSS first:
                # their attempts were parked precisely because they are cheap
                # to abandon, so they are the obvious source of bytes, and
                # largest-first clears the target with the fewest kills.
                # Running workers die only if the parked ones weren't enough,
                # smallest first (small kills give large tasks room while
                # losing the least in-flight progress).
                throttled_worker_memory = [
                    (rss_bytes, worker)
                    for rss_bytes, worker in worker_memory
                    if worker.throttled and worker.current_input is not None
                ]
                throttled_worker_memory.sort(key=lambda item: item[0], reverse=True)
                running_worker_memory = [
                    (rss_bytes, worker)
                    for rss_bytes, worker in worker_memory
                    if not worker.throttled
                    and not worker.is_idle
                    and worker.current_input is not None
                ]
                running_worker_memory.sort(key=lambda item: item[0])
                candidate_worker_memory = (
                    throttled_worker_memory + running_worker_memory
                )
                if not candidate_worker_memory:
                    continue

                selected_worker_memory = []
                selected_rss_bytes = 0
                for rss_bytes, worker in candidate_worker_memory:
                    if len(worker_memory) - len(selected_worker_memory) <= 1:
                        break
                    selected_worker_memory.append((rss_bytes, worker))
                    selected_rss_bytes += rss_bytes
                    if selected_rss_bytes >= bytes_to_free:
                        break

                await retire_workers_for_pressure(
                    selected_worker_memory, reason=kill_reason
                )
                continue

            # Shed path. Each shed worker is either killed (its input requeues)
            # or parked, whichever is cheaper (see _kill_instead_of_park).
            # Kills free RAM instantly, so they never wait; parks run one
            # reclaim batch at a time, since parked RSS only leaves RAM once
            # reclaim lands and parking more meanwhile would just over-shed
            # (memory.high stalls any grower during the gap).
            running_worker_memory = [
                (rss_bytes, worker)
                for rss_bytes, worker in worker_memory
                if not worker.throttled
                and not worker.is_idle
                and worker.current_input is not None
            ]
            # Never shed the node's only running worker: idle workers refuse
            # the queue while anything is parked, so parking the sole runner
            # would leave the node running nothing at all until recovery.
            if len(running_worker_memory) <= 1:
                continue
            # Lowest RSS first: cheapest to move to swap and back, and the
            # biggest allocators (the likely pressure source) keep their
            # momentum while memory.high meters them. (Youngest-first was
            # tried and wasted 25% more worker time on the Sentinel-2 cohort.)
            running_worker_memory.sort(key=lambda item: item[0])

            now = time.time()
            shed_times = [
                shed_at
                for shed_at in shed_times
                if now - shed_at < SHED_ESCALATION_WINDOW_SECONDS
            ]
            min_shed_count = min(
                2 ** len(shed_times),
                max(1, int(len(running_worker_memory) * SHED_ESCALATION_MAX_FRACTION)),
            )
            kill_selected = []
            park_selected = []
            decisions = []
            selected_rss_bytes = 0
            for rss_bytes, worker in running_worker_memory[:-1]:
                kill, decision = _kill_instead_of_park(worker, rss_bytes)
                if kill:
                    kill_selected.append((rss_bytes, worker))
                elif reclaim_in_flight:
                    continue
                else:
                    park_selected.append((rss_bytes, worker))
                decisions.append(decision)
                selected_rss_bytes += rss_bytes
                if (
                    selected_rss_bytes >= bytes_to_free
                    and len(decisions) >= min_shed_count
                ):
                    break
            if decisions:
                shed_times.append(now)
            for decision in decisions:
                await debug_log(
                    "shed_decision",
                    swap_bytes_per_sec=SELF["swap_bytes_per_sec"],
                    **decision,
                )

            shed_reason = (
                "memory pressure"
                if over_byte_trigger
                else "memory pressure (stall)"
            )
            if kill_selected:
                await retire_workers_for_pressure(
                    kill_selected,
                    reason=f"{shed_reason}; redoing these attempts is cheaper than swapping them",
                )
            if park_selected:
                await park_workers_for_memory(park_selected, reason=shed_reason)
    finally:
        if slice_dir is not None:
            try:
                (slice_dir / "memory.high").write_text("max")
            except OSError:
                pass  # slice teardown at node shutdown


def _worker_cgroup_dir(worker) -> Path:
    # cgroup v2: single `0::/path` line. The deepest cgroup containing
    # worker_server.py is the worker's container cgroup (quota target and
    # PSI scope for everything in that container, UDF children included).
    cgroup_path = (
        Path(f"/proc/{worker.worker_host_pid}/cgroup")
        .read_text()
        .splitlines()[0]
        .split(":", 2)[2]
    )
    return Path("/sys/fs/cgroup", cgroup_path.strip("/"))


def _read_memory_stat_anon(cgroup_dir: Path) -> int:
    # `anon` counts anonymous pages still resident in RAM; pages moved to
    # swap leave it (and appear in memory.swap.current), which makes it the
    # honest measure of how much memory a reclaim actually got out of RAM.
    for line in (cgroup_dir / "memory.stat").read_text().splitlines():
        if line.startswith("anon "):
            return int(line.split()[1])
    return 0


def _read_memory_psi_full_avg10(cgroup_dir: Path) -> float:
    # `full` line: fraction of time ALL non-idle tasks in the cgroup were
    # stalled on memory at once, i.e. thrash rather than mere contention.
    for line in (cgroup_dir / "memory.pressure").read_text().splitlines():
        if line.startswith("full"):
            return float(line.split("avg10=")[1].split()[0])
    return 0.0


def _read_memory_psi_some_avg10(cgroup_dir: Path) -> float:
    # `some` line: fraction of time at least one task in the cgroup was
    # stalled on memory: reclaim cost being paid right now, whatever the
    # byte counters claim.
    for line in (cgroup_dir / "memory.pressure").read_text().splitlines():
        if line.startswith("some"):
            return float(line.split("avg10=")[1].split()[0])
    return 0.0


def _read_memory_stat_unreclaimable(slice_dir: Path) -> int:
    """anon + shmem + zswap: the bytes the kernel cannot free without swapping
    to disk or killing. zswap's compressed pool is charged to the slice, so
    parked heaps still occupy budget until they are written back. File cache
    is excluded on purpose: a warm cache reads as a full slice while costing
    nothing to reclaim, which is exactly the false signal that must not park
    workers."""
    unreclaimable_bytes = 0
    for line in (slice_dir / "memory.stat").read_text().splitlines():
        if line.startswith(("anon ", "shmem ", "zswap ")):
            unreclaimable_bytes += int(line.split()[1])
    return unreclaimable_bytes


def _kill_instead_of_park(worker, rss_bytes: int) -> tuple[bool, dict]:
    """Kill when redoing the attempt costs less than the heap's swap round
    trip, or when the attempt is waiting on a download (parking would freeze
    its sockets and it would fail on resume anyway). Without a swap rate
    (no swap, or calibration failed) everything parks, as before. Returns the
    decision and its inputs for the debug trail."""
    swap_bytes_per_sec = SELF["swap_bytes_per_sec"]
    redo_seconds = worker.attempt_elapsed_seconds()
    park_seconds = (
        None if swap_bytes_per_sec is None else 2 * rss_bytes / swap_bytes_per_sec
    )
    kill = park_seconds is not None and (
        worker.network_bound or redo_seconds < park_seconds
    )
    return kill, {
        "input_index": worker.current_input[0],
        "decision": "kill" if kill else "park",
        "rss_bytes": int(rss_bytes),
        "redo_seconds": round(redo_seconds, 1),
        "park_seconds": None if park_seconds is None else round(park_seconds, 1),
        "network_bound": worker.network_bound,
    }


def _observe_swap_rate(bytes_per_sec: float):
    current = SELF["swap_bytes_per_sec"]
    if current is None:
        SELF["swap_bytes_per_sec"] = bytes_per_sec
    else:
        SELF["swap_bytes_per_sec"] = current + SWAP_RATE_EWMA_ALPHA * (
            bytes_per_sec - current
        )


def _observe_attempt_peak(peak_rss_bytes: int):
    current = SELF["typical_attempt_peak_rss_bytes"]
    if current is None:
        SELF["typical_attempt_peak_rss_bytes"] = peak_rss_bytes
    else:
        SELF["typical_attempt_peak_rss_bytes"] = current + ATTEMPT_PEAK_EWMA_ALPHA * (
            peak_rss_bytes - current
        )


def _another_worker_fits(used_bytes: int, limit_bytes: int, running_workers) -> bool:
    """Whether the slice stays under the park trigger if every running worker
    still climbing toward a typical attempt's peak gets there and one more
    worker joins them. Gating on instantaneous usage alone admitted workers
    during download lulls and killed them when the stacks inflated (observed:
    a node sawing 12 -> 32 -> 12 every few minutes). Until an attempt has
    finished, the largest peak seen so far among running attempts stands in
    for the typical peak; with no evidence at all the plain fraction gates."""
    typical_peak = SELF["typical_attempt_peak_rss_bytes"]
    if typical_peak is None:
        typical_peak = max(
            (worker.attempt_peak_rss_bytes for worker in running_workers), default=0
        )
    if not typical_peak:
        return used_bytes / limit_bytes <= READD_MAX_WORKER_MEMORY_USED_FRACTION
    growth_bytes = sum(
        max(0, typical_peak - worker.attempt_peak_rss_bytes)
        for worker in running_workers
    )
    projected_bytes = used_bytes + growth_bytes + typical_peak
    return projected_bytes / limit_bytes <= DYNAMIC_RAM_MAX_WORKER_MEMORY_USED_FRACTION


# Runs as a separate process so its pages can be moved into the workers slice
# (node_service's own slice forbids swap). Each stdin line is a go-ahead from
# the parent: allocate, then re-touch after the parent swapped it out.
_SWAP_CALIBRATION_CHILD = """
import sys, time
size = int(sys.argv[1])
sys.stdin.readline()
buffer = bytearray(size)
for offset in range(0, size, 4096):
    buffer[offset] = 1
print("allocated", flush=True)
sys.stdin.readline()
started = time.perf_counter()
touched = 0
for offset in range(0, size, 4096):
    touched += buffer[offset]
print(time.perf_counter() - started, flush=True)
sys.stdin.readline()
"""


def _calibrate_swap_rate_sync() -> dict:
    # systemd nests dashed slice names: burla-workers.slice lives under burla.slice.
    slice_dir = Path("/sys/fs/cgroup", "burla.slice", WORKERS_CGROUP_SLICE)
    if "memory" not in (slice_dir / "cgroup.subtree_control").read_text().split():
        (slice_dir / "cgroup.subtree_control").write_text("+memory")
    scratch_dir = slice_dir / SWAP_CALIBRATION_CGROUP
    scratch_dir.mkdir(exist_ok=True)
    child = subprocess.Popen(
        [sys.executable, "-c", _SWAP_CALIBRATION_CHILD, str(SWAP_CALIBRATION_BYTES)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        (scratch_dir / "cgroup.procs").write_text(str(child.pid))
        child.stdin.write("\n")
        child.stdin.flush()
        child.stdout.readline()
        started = time.perf_counter()
        remaining_bytes = int((scratch_dir / "memory.current").read_text())
        while remaining_bytes > 0:
            chunk_bytes = min(MEMORY_RECLAIM_CHUNK_BYTES, remaining_bytes)
            try:
                (scratch_dir / "memory.reclaim").write_text(str(chunk_bytes))
            except OSError as error:
                if error.errno != errno.EAGAIN:
                    raise
            remaining_bytes -= chunk_bytes
        out_seconds = time.perf_counter() - started
        swapped_bytes = int((scratch_dir / "memory.swap.current").read_text())
        if swapped_bytes < SWAP_RATE_MIN_SAMPLE_BYTES:
            raise RuntimeError(f"reclaim moved only {swapped_bytes} bytes to swap")
        child.stdin.write("\n")
        child.stdin.flush()
        in_seconds = float(child.stdout.readline())
        child.stdin.write("\n")
        child.stdin.flush()
        child.wait(timeout=60)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()
        scratch_dir.rmdir()
    return {
        "swapped_bytes": swapped_bytes,
        "out_bytes_per_sec": swapped_bytes / out_seconds,
        "in_bytes_per_sec": swapped_bytes / in_seconds,
    }


async def calibrate_swap_rate(logger: Logger):
    """Measure this node's swap round-trip speed before any job runs, so the
    first pressure event can already price park against kill."""
    if IN_LOCAL_DEV_MODE or psutil.swap_memory().total == 0:
        return
    try:
        metrics = await asyncio.to_thread(_calibrate_swap_rate_sync)
    except Exception as error:
        await logger.log(
            f"Swap calibration failed ({error}); memory pressure will park "
            "workers without weighing a kill.",
            severity="WARNING",
        )
        return
    SELF["swap_bytes_per_sec"] = min(
        metrics["out_bytes_per_sec"], metrics["in_bytes_per_sec"]
    )
    await logger.log(
        f"Swap calibrated with {metrics['swapped_bytes'] // 1024**2}MiB: "
        f"out {metrics['out_bytes_per_sec'] / 1024**2:.0f}MiB/s, "
        f"in {metrics['in_bytes_per_sec'] / 1024**2:.0f}MiB/s."
    )
    await debug_log("swap_calibrated", **metrics)


def _workers_rss_sum_bytes(workers) -> int:
    rss_sum_bytes = 0
    for worker in workers:
        try:
            rss_sum_bytes += worker.memory_rss_bytes()
        except psutil.NoSuchProcess:
            continue  # worker_server.py mid-relaunch; its RSS is ~0 anyway
    return rss_sum_bytes


def _workers_memory_used_bytes(reference_worker, workers) -> int:
    """Unreclaimable slice usage when isolation is active; summed process RSS
    when it is not (isolation-off is already an ERROR at boot, and process
    RSS is the only accounting left there)."""
    slice_dir = _workers_cgroup_slice_dir(reference_worker)
    if slice_dir is None:
        return _workers_rss_sum_bytes(workers)
    return _read_memory_stat_unreclaimable(slice_dir)


def _swap_parked_worker_resumable(worker, used_bytes, limit_bytes) -> bool:
    """A swapped worker may resume only when faulting its swapped pages back
    into RAM would leave unreclaimable usage under the headroom threshold."""
    try:
        swap_file = _worker_cgroup_dir(worker) / "memory.swap.current"
        swap_current_bytes = int(swap_file.read_text())
    except OSError:
        return True  # process/container mid-teardown; resuming is harmless
    projected = (used_bytes + swap_current_bytes) / limit_bytes
    return projected < RESUME_MEMORY_HEADROOM_FRACTION


def _any_parked_worker_resumable(active_workers, used_bytes, limit_bytes) -> bool:
    for worker in active_workers:
        if not worker.throttled:
            continue
        if not worker.swap_parked:
            return True  # CPU-parked: resuming needs no RAM headroom
        if _swap_parked_worker_resumable(worker, used_bytes, limit_bytes):
            return True
    return False


class WorkerStallTracker:
    """CPU stall as the max PSI `some` fraction across the given workers, each
    read from its own container cgroup's cpu.pressure.

    The monitors used to read the root cpu.pressure, but a quota-throttled
    runnable task counts as stalled in every ancestor file up to the root, so
    the first throttle would pin the shared signal above threshold, throttle
    everything down to the last worker, and hold recovery shut forever.
    Per-container files keep the signal scoped to exactly the workers still
    competing for CPU (callers pass only unthrottled workers).
    """

    def __init__(self):
        self._last_sample = {}  # container_id -> (stall_usec, read_at)

    def max_stall_fraction(self, workers) -> float:
        max_fraction = 0.0
        for worker in workers:
            read_at = time.perf_counter()
            try:
                pressure_file = _worker_cgroup_dir(worker) / "cpu.pressure"
                # `some` line, `total` field: cumulative microseconds during
                # which at least one runnable task sat waiting for a core.
                some_line = pressure_file.read_text().splitlines()[0]
            except OSError:
                continue  # worker process or container mid-teardown/relaunch
            stall_usec = int(some_line.rsplit("total=", 1)[1])
            last_sample = self._last_sample.get(worker.container_id)
            self._last_sample[worker.container_id] = (stall_usec, read_at)
            if last_sample is None:
                continue  # first sample only opens this worker's interval
            last_stall_usec, last_read_at = last_sample
            elapsed_usec = (read_at - last_read_at) * 1_000_000
            fraction = (stall_usec - last_stall_usec) / elapsed_usec
            max_fraction = max(max_fraction, fraction)
        return max_fraction


def _read_stall_usec(pressure_file: Path) -> int:
    # `some` line, `total` field: cumulative microseconds during which at
    # least one task sat waiting for the resource.
    some_line = pressure_file.read_text().splitlines()[0]
    return int(some_line.rsplit("total=", 1)[1])


def _read_cpu_usage_usec(cgroup_dir: Path) -> int:
    for line in (cgroup_dir / "cpu.stat").read_text().splitlines():
        if line.startswith("usage_usec "):
            return int(line.split()[1])


def _primary_nic() -> tuple[str | None, float | None]:
    """Default-route interface name and its link capacity in bytes/sec.
    Capacity is None when the driver reports no real speed (virtio and veth
    report -1 or refuse the read), which disables the network add-gate."""
    nic_name = None
    for line in Path("/proc/net/route").read_text().splitlines()[1:]:
        fields = line.split()
        if fields[1] == "00000000":  # destination 0.0.0.0 = default route
            nic_name = fields[0]
            break
    if nic_name is None:
        return None, None
    try:
        speed_mbps = int(Path(f"/sys/class/net/{nic_name}/speed").read_text())
    except OSError:
        return nic_name, None
    if speed_mbps <= 0:
        return nic_name, None
    return nic_name, speed_mbps * 1_000_000 / 8


def _read_nic_bytes(nic_name: str) -> int:
    counters = psutil.net_io_counters(pernic=True, nowrap=True)[nic_name]
    return counters.bytes_recv + counters.bytes_sent


class AddGateSampler:
    """One instance per recovery/trade loop: tracks the counters behind the
    disk-IO, network, and memory "don't add workers" signals and returns each
    signal's level over the window since the previous sample() call. Unlike
    CPU stall (per-worker, see WorkerStallTracker) these are node-wide:
    workers carry no io or network quotas, so the root io.pressure /
    memory.pressure files and the primary NIC measure exactly the load the
    job puts on the machine. Unmeasurable signals read 0.0 (no PSI file, no
    default route, or no reported link speed)."""

    def __init__(self):
        self._can_check_io = IO_PRESSURE_FILE.exists()
        self._can_check_memory = MEMORY_PRESSURE_FILE.exists()
        self._nic_name, self._nic_capacity_bytes_per_sec = _primary_nic()
        self._last_io_stall_usec = (
            _read_stall_usec(IO_PRESSURE_FILE) if self._can_check_io else 0
        )
        self._last_memory_stall_usec = (
            _read_stall_usec(MEMORY_PRESSURE_FILE) if self._can_check_memory else 0
        )
        self._last_nic_bytes = (
            _read_nic_bytes(self._nic_name) if self._nic_capacity_bytes_per_sec else 0
        )
        self._last_read_at = time.perf_counter()

    def sample(self) -> tuple[float, float, float]:
        read_at = time.perf_counter()
        elapsed_sec = read_at - self._last_read_at
        io_stall = network_utilization = memory_stall = 0.0
        if self._can_check_io:
            stall_usec = _read_stall_usec(IO_PRESSURE_FILE)
            io_stall = (stall_usec - self._last_io_stall_usec) / (
                elapsed_sec * 1_000_000
            )
            self._last_io_stall_usec = stall_usec
        if self._can_check_memory:
            stall_usec = _read_stall_usec(MEMORY_PRESSURE_FILE)
            memory_stall = (stall_usec - self._last_memory_stall_usec) / (
                elapsed_sec * 1_000_000
            )
            self._last_memory_stall_usec = stall_usec
        if self._nic_capacity_bytes_per_sec:
            nic_bytes = _read_nic_bytes(self._nic_name)
            bytes_per_sec = (nic_bytes - self._last_nic_bytes) / elapsed_sec
            network_utilization = bytes_per_sec / self._nic_capacity_bytes_per_sec
            self._last_nic_bytes = nic_bytes
        self._last_read_at = read_at
        return io_stall, network_utilization, memory_stall


class SliceCpuSampler:
    """Aggregate CPU utilization (0..1) of the workers slice over the window
    since the previous sample() call: cpu.stat usage_usec delta divided by
    elapsed core-time. The slice directory is resolved lazily from a live
    worker and cached. When the slice cannot be read (isolation inactive,
    worker mid-relaunch, local-dev quirks) it falls back to whole-VM
    utilization via psutil, which is the same quantity plus node_service."""

    def __init__(self):
        self._slice_dir = None
        self._last_usage_usec = None
        self._last_read_at = None
        psutil.cpu_percent()  # open the fallback's measurement interval

    def _usage_usec(self, workers):
        if self._slice_dir is None:
            for worker in workers:
                try:
                    self._slice_dir = _workers_cgroup_slice_dir(worker)
                except OSError:
                    continue  # worker process mid-relaunch; try the next one
                if self._slice_dir is not None:
                    break
        if self._slice_dir is None:
            return None
        return _read_cpu_usage_usec(self._slice_dir)

    def sample(self, workers) -> float:
        read_at = time.perf_counter()
        try:
            usage_usec = self._usage_usec(workers)
        except OSError:
            usage_usec = None  # slice mid-teardown; fall back this tick
        if usage_usec is None:
            return psutil.cpu_percent() / 100
        last_usage_usec, last_read_at = self._last_usage_usec, self._last_read_at
        self._last_usage_usec, self._last_read_at = usage_usec, read_at
        if last_usage_usec is None:
            return psutil.cpu_percent() / 100  # first sample only opens the interval
        elapsed_usec = (read_at - last_read_at) * 1_000_000
        return (usage_usec - last_usage_usec) / (elapsed_usec * (os.cpu_count() or 1))


async def dynamic_cpu_loop():
    cpu_sampler = SliceCpuSampler()
    while SELF["dynamic_func_cpu"]:
        await asyncio.sleep(CPU_PRESSURE_MONITOR_INTERVAL_SECONDS)
        active_workers = _active_dynamic_workers()
        if not active_workers:
            return
        unthrottled_workers = [
            worker for worker in active_workers if not worker.throttled
        ]

        for worker in unthrottled_workers:
            if not psutil.pid_exists(worker.worker_host_pid):
                await _relocate_worker_process_or_retire(worker)

        # Rewritten every tick: systemd resets a scope's cpu.weight to the
        # default whenever it re-applies the unit's CPU settings, which every
        # throttle/unthrottle quota change does.
        for worker in unthrottled_workers:
            if not worker.is_idle and worker.current_input is not None:
                cpu_seconds = worker.attempt_cpu_seconds()
                worker.set_cpu_weight(
                    min(CPU_WEIGHT_MAX, CPU_WEIGHT_DEFAULT + int(cpu_seconds))
                )

        SELF["cpu_utilization"] = cpu_sampler.sample(active_workers)


async def _boot_readded_worker(template=None):
    """Boot one fresh worker container toward this node's slot count and hand
    it the retained function. Retirement deletes the worker's container, so
    recovering capacity means booting a new container, not reviving the old
    one. A replaced retired worker leaves SELF["workers"] so the list's
    length keeps meaning "intended capacity"; with no retired worker to
    replace, the deficit came from slots acquired in a trade, and the new
    worker oversubscribes the machine (CPU nodes only - the trade loop never
    runs on GPU nodes). The caller assigns templates so parallel boots in one
    batch never claim the same retired worker."""
    if template is not None:
        image, gpu_index = template.image, template.gpu_index
    else:
        image, gpu_index = SELF["workers"][0].image, None
    worker = WorkerClient(image, gpu_index=gpu_index)
    try:
        await worker.boot()
        await worker.load_function(SELF["function_pkl"])
    except Exception as e:
        if worker.container_id is not None:
            asyncio.create_task(worker._remove_retired_container(worker.container_id))
        await debug_log("worker_readd_failed", error=f"{type(e).__name__}: {e}")
        return

    if template is not None:
        SELF["workers"].remove(template)
    SELF["workers"].append(worker)
    new_parallelism = len(_active_dynamic_workers())
    reason = "pressure subsided" if template is not None else "acquired slots"
    await Logger().log(
        f"Node parallelism increased from {new_parallelism - 1} to "
        f"{new_parallelism}: {reason}, added a worker.",
        job_id=SELF["current_job"],
        old_parallelism=new_parallelism - 1,
        new_parallelism=new_parallelism,
    )


def _parked_workers_exist() -> bool:
    return any(worker.throttled and not worker.retired for worker in SELF["workers"])


async def _unthrottle_one_parked_worker(reason: str, via: str):
    async with SELF["dynamic_retire_lock"]:
        # Re-filter under the lock: a revocation or RAM kill may have
        # consumed the parked worker since the caller checked.
        active_workers = _active_dynamic_workers()
        throttled_workers = [worker for worker in active_workers if worker.throttled]
        if not throttled_workers:
            return
        # Most progress first: the attempt closest to done frees its slot
        # (and its RAM) soonest, while the least-progressed attempts stay
        # parked, which are exactly the ones a peer can steal most cheaply.
        # Swap-parked candidates additionally need RAM headroom to fault
        # their pages back without recreating the pressure that parked them.
        throttled_workers.sort(key=lambda w: w.attempt_cpu_seconds(), reverse=True)
        worker = None
        limit_bytes = None
        used_bytes = None
        for candidate in throttled_workers:
            if candidate.swap_parked:
                try:
                    if limit_bytes is None:
                        limit_bytes = _workers_memory_limit_bytes(candidate)
                        used_bytes = _workers_memory_used_bytes(
                            candidate, active_workers
                        )
                except OSError:
                    continue  # candidate's process just died; skip it
                if not _swap_parked_worker_resumable(
                    candidate, used_bytes, limit_bytes
                ):
                    continue
            worker = candidate
            break
        if worker is None:
            return
        throttled_for = time.time() - worker.throttled_at
        was_swap_parked = worker.swap_parked
        swap_used_bytes = None
        if was_swap_parked:
            try:
                swap_file = _worker_cgroup_dir(worker) / "memory.swap.current"
                swap_used_bytes = int(swap_file.read_text())
            except OSError:
                pass  # worker process mid-relaunch; metric only
        await worker.unthrottle()
        unthrottled_count = len(active_workers) - len(throttled_workers) + 1
        await Logger().log(
            f"Node parallelism increased from {unthrottled_count - 1} to "
            f"{unthrottled_count}: {reason}, restored full CPU to a parked "
            "worker.",
            job_id=SELF["current_job"],
            old_parallelism=unthrottled_count - 1,
            new_parallelism=unthrottled_count,
        )
        await debug_log(
            "worker_unthrottled",
            via=via,
            input_index=worker.current_input[0] if worker.current_input else None,
            throttled_for_sec=round(throttled_for, 1),
            n_still_throttled=len(throttled_workers) - 1,
            was_swap_parked=was_swap_parked,
            swap_used_bytes=swap_used_bytes,
        )


async def dynamic_worker_readd_loop():
    """Inverse of the memory parks and peer revocations: recover capacity one
    worker per one-second tick, unthrottling parked workers first, then
    booting replacements for retired ones. Unparking has a raw fast path when the
    node itself has idle cores; minting and the near-saturation climb still
    require the stall-gated smoothed add path. The idle handoff in
    _process_inputs remains immediate because it swaps a finishing worker's
    capacity to a parked attempt without raising parallelism."""
    can_check_cpu = CPU_PRESSURE_FILE.exists()
    stall_tracker = WorkerStallTracker()
    stall_tracker.max_stall_fraction(_active_dynamic_workers())  # open intervals
    cpu_sampler = SliceCpuSampler()
    gate_sampler = AddGateSampler()
    stall_ewma = Ewma(GATE_EWMA_TAU_SECONDS)
    utilization_ewma = Ewma(GATE_EWMA_TAU_SECONDS)
    io_ewma = Ewma(GATE_EWMA_TAU_SECONDS)
    network_ewma = Ewma(GATE_EWMA_TAU_SECONDS)
    memory_ewma = Ewma(GATE_EWMA_TAU_SECONDS)
    last_memory_readd_at = 0.0
    raw_idle_streak = 0
    while SELF["dynamic_func_ram"] or SELF["dynamic_func_cpu"]:
        await asyncio.sleep(READD_MONITOR_INTERVAL_SECONDS)

        active_workers = _active_dynamic_workers()
        raw_stall = 0.0
        if can_check_cpu:
            unthrottled_workers = [
                worker for worker in active_workers if not worker.throttled
            ]
            raw_stall = stall_tracker.max_stall_fraction(unthrottled_workers)
        stall_fraction = stall_ewma.update(raw_stall)
        raw_utilization = cpu_sampler.sample(active_workers)
        utilization = utilization_ewma.update(raw_utilization)
        raw_io_stall, raw_network_utilization, raw_memory_stall = (
            gate_sampler.sample()
        )
        io_stall = io_ewma.update(raw_io_stall)
        network_utilization = network_ewma.update(raw_network_utilization)
        memory_stall = memory_ewma.update(raw_memory_stall)

        # The io/network/memory gates also cover the unthrottle below: unlike
        # CPU, nothing re-parks a worker if resuming it swamps the disk, NIC,
        # or RAM, so prevention is the only control.
        resource_gates_green = (
            io_stall <= READD_MAX_IO_STALL_FRACTION
            and network_utilization <= READD_MAX_NETWORK_UTILIZATION_FRACTION
            and memory_stall <= READD_MAX_MEMORY_STALL_FRACTION
        )
        gates_green = (
            stall_fraction <= READD_MAX_CPU_STALL_FRACTION
            and utilization <= CPU_UTILIZATION_ADD_MAX
            and resource_gates_green
        )
        raw_idle_streak = (
            raw_idle_streak + 1
            if raw_utilization <= CPU_UTILIZATION_RECOVER_MAX
            else 0
        )
        fast_recover = (
            raw_idle_streak >= CPU_PRESSURE_RAW_RECOVER_SECONDS
            and resource_gates_green
        )
        now = time.time()

        throttled_workers = [worker for worker in active_workers if worker.throttled]
        deficit = SELF["target_parallelism"] - len(active_workers)
        if throttled_workers and (gates_green or fast_recover):
            SELF["readd_gates_last_green_at"] = now
            await _unthrottle_one_parked_worker(
                reason="cores are idle",
                via="recovery_loop",
            )
            continue
        if not gates_green:
            continue

        # A dynamic-RAM node with room and queued work keeps probing past its
        # target: after a memory shed its target is whatever it had left, and
        # a workload whose per-worker memory swings between phases would
        # otherwise sit at that count with half its RAM idle (observed: 32
        # nodes averaging ~55% RAM with peaks near 99%).
        memory_probe = (
            SELF["dynamic_func_ram"]
            and not IN_LOCAL_DEV_MODE
            and len(active_workers) < INSTANCE_N_CPUS
        )
        if deficit <= 0 and not memory_probe:
            continue
        if SELF["inputs_queue"].qsize() == 0:
            continue  # no queued work for another worker to pull
        # RAM headroom check mirrors the RAM monitor's gauge (unreclaimable
        # slice usage; the fake-VM caveat is that local-dev slices carry no
        # memory cap, so the check only means something on real VMs).
        if SELF["dynamic_func_ram"] and not IN_LOCAL_DEV_MODE and active_workers:
            try:
                memory_limit_bytes = _workers_memory_limit_bytes(active_workers[0])
                used_bytes = _workers_memory_used_bytes(
                    active_workers[0], active_workers
                )
            except OSError:
                continue  # that worker's process just died; skip this tick
            running_workers = [
                worker
                for worker in active_workers
                if worker.current_input is not None and not worker.throttled
            ]
            if not _another_worker_fits(used_bytes, memory_limit_bytes, running_workers):
                continue

        # One boot per tick fills a 32-slot mint in minutes and the trade
        # loop won't mint again until the deficit is filled, so serial boots
        # capped the whole ramp (observed: pd12m plateaued at ~50% CPU while
        # every mint gate was green). Boot a bounded batch in parallel; the
        # gates and RAM check re-run before the next batch. After a memory
        # shed, or past the target, one worker per dwell (see
        # MEMORY_READD_DWELL_SECONDS) so demand shows before the next add.
        batch_size = min(deficit, READD_BOOT_BATCH_MAX)
        memory_regime = now - SELF["last_memory_shed_at"] < PARK_EVENT_RETENTION_SECONDS
        if deficit <= 0 or memory_regime:
            last_memory_event_at = max(SELF["last_memory_shed_at"], last_memory_readd_at)
            if now - last_memory_event_at < MEMORY_READD_DWELL_SECONDS:
                continue
            batch_size = 1
            last_memory_readd_at = now
            if deficit <= 0:
                SELF["target_parallelism"] += 1  # minted by measured RAM headroom
        else:
            # The replacement requester (job_watcher.py) treats "gates never
            # green since the deficit began" as proof this machine cannot
            # host its slots. A memory-bound node probing one worker per
            # dwell is not absorbing a deficit of dozens, so only full-batch
            # adds count as green; memory sheds revoke it.
            SELF["readd_gates_last_green_at"] = now
        retired_workers = [worker for worker in SELF["workers"] if worker.retired]
        templates = retired_workers[:batch_size]
        templates += [None] * (batch_size - len(templates))
        await asyncio.gather(
            *(_boot_readded_worker(template) for template in templates)
        )


class JobLogWriter:
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.lock = asyncio.Lock()
        self.stop_event = asyncio.Event()
        self.pending_flush_event = asyncio.Event()
        self.log_buffers = {}
        self.pending_documents = []
        self.active_input_index = None
        self.partial_container_output = ""
        self.input_end_events = {}
        self.flush_task = asyncio.create_task(self._flush_loop())

    def _end_event(self, input_index: int) -> asyncio.Event:
        event = self.input_end_events.get(input_index)
        if event is None:
            event = asyncio.Event()
            self.input_end_events[input_index] = event
        return event

    def _get_log_buffer(self, input_index: int):
        if input_index not in self.log_buffers:
            self.log_buffers[input_index] = {"logs": [], "size_bytes": 0}
        return self.log_buffers[input_index]

    def _truncate_message(self, message: str):
        message_size = len(message.encode("utf-8")) + 180
        if message_size <= MAX_LOG_DOCUMENT_SIZE_BYTES:
            return message
        max_bytes = MAX_LOG_DOCUMENT_SIZE_BYTES - len(
            TRUNCATED_LOG_SUFFIX.encode("utf-8")
        )
        truncated_bytes = message.encode("utf-8")[:max_bytes]
        truncated_message = truncated_bytes.decode("utf-8", errors="ignore")
        return truncated_message + TRUNCATED_LOG_SUFFIX

    def _queue_document_locked(self, input_index: int, is_error: bool = False):
        log_buffer = self.log_buffers.get(input_index)
        if not log_buffer or not log_buffer["logs"]:
            return
        document = {
            "logs": log_buffer["logs"],
            "timestamp": time.time(),
            "input_index": input_index,
        }
        if is_error:
            document["is_error"] = True
        self.pending_documents.append(document)
        # Client-visible immediately: logs for an input must be fetchable
        # before its result is (the client stops polling once it has every
        # result). The flush loop only handles the head's persistent copy.
        if not is_error:
            SELF["pending_logs"].append(document)
        self.log_buffers[input_index] = {"logs": [], "size_bytes": 0}

    def _queue_all_buffers_locked(self):
        for input_index in list(self.log_buffers):
            self._queue_document_locked(input_index)

    def _write_locked(self, input_index: int, message: str, timestamp: datetime):
        if not message.strip():
            return
        message = self._truncate_message(message)
        message_size = len(message.encode("utf-8")) + 180
        log_buffer = self._get_log_buffer(input_index)
        if log_buffer["size_bytes"] and (
            log_buffer["size_bytes"] + message_size > MAX_LOG_DOCUMENT_SIZE_BYTES
        ):
            self._queue_document_locked(input_index)
            log_buffer = self._get_log_buffer(input_index)
        log_buffer["logs"].append({"timestamp": timestamp, "message": message})
        log_buffer["size_bytes"] += message_size
        if log_buffer["size_bytes"] >= MAX_LOG_DOCUMENT_SIZE_BYTES:
            self._queue_document_locked(input_index)
            self.pending_flush_event.set()

    def _parse_container_log_line(self, container_log_line: str):
        timestamp_string, _, message = container_log_line.partition(" ")
        timestamp = datetime.fromisoformat(
            timestamp_string.replace("Z", "+00:00")
        ).timestamp()
        return timestamp, message

    def _capture_container_log_line_locked(self, container_log_line: str):
        timestamp, message = self._parse_container_log_line(container_log_line)
        stripped_message = message.strip()
        if stripped_message.startswith(LOG_START_MARKER_PREFIX):
            self.active_input_index = int(
                stripped_message.removeprefix(LOG_START_MARKER_PREFIX)
            )
            return
        if stripped_message.startswith(LOG_END_MARKER_PREFIX):
            input_index = int(stripped_message.removeprefix(LOG_END_MARKER_PREFIX))
            self._queue_document_locked(input_index)
            self.active_input_index = None
            self._end_event(input_index).set()
            self.pending_flush_event.set()
            return
        if _is_worker_internal_log_message(stripped_message):
            return
        if self.active_input_index is None:
            return
        self._write_locked(self.active_input_index, message, timestamp)

    async def capture_container_output(self, container_output_chunk: str):
        async with self.lock:
            complete_output = self.partial_container_output + container_output_chunk
            output_lines = complete_output.splitlines(keepends=True)
            if output_lines and not output_lines[-1].endswith(("\n", "\r")):
                self.partial_container_output = output_lines.pop()
            else:
                self.partial_container_output = ""
            for output_line in output_lines:
                self._capture_container_log_line_locked(output_line)

    async def write_error(self, input_index: int, traceback_str: str):
        async with self.lock:
            self.pending_documents.append(
                {
                    "logs": [{"timestamp": time.time(), "message": traceback_str}],
                    "timestamp": time.time(),
                    "input_index": input_index,
                    "is_error": True,
                }
            )
            self.pending_flush_event.set()

    async def write_warning(self, input_index: int, message: str):
        async with self.lock:
            document = {
                "logs": [{"timestamp": time.time(), "message": message}],
                "timestamp": time.time(),
                "input_index": input_index,
                "severity": "WARNING",
            }
            self.pending_documents.append(document)
            SELF["pending_logs"].append(document)
            self.pending_flush_event.set()

    async def finish_input(self, input_index: int):
        # UDF prints ride the container log stream, which can trail the TCP
        # result by a few ms; wait for the end-of-input marker so this
        # input's logs are client-visible before its result is released.
        # (2s cap: a crashed container never prints the marker.)
        try:
            await asyncio.wait_for(self._end_event(input_index).wait(), timeout=2)
        except asyncio.TimeoutError:
            pass
        self.input_end_events.pop(input_index, None)
        async with self.lock:
            self._queue_document_locked(input_index)
            self.pending_flush_event.set()

    async def _flush_pending_documents(self):
        async with self.lock:
            self._queue_all_buffers_locked()
            if not self.pending_documents:
                return
            documents = self.pending_documents
            self.pending_documents = []

        try:
            await head_client.post_job_logs(self.job_id, documents)
        except Exception as e:
            # The client still gets these logs live via /results (pending_logs
            # above); only the dashboard's persistent copy is lost.
            print(f"failed to forward {len(documents)} job log docs to head: {e}")

    async def _flush_loop(self):
        while True:
            try:
                await asyncio.wait_for(
                    self.pending_flush_event.wait(), timeout=LOG_FLUSH_INTERVAL_SECONDS
                )
            except asyncio.TimeoutError:
                pass
            self.pending_flush_event.clear()
            await self._flush_pending_documents()
            if self.stop_event.is_set():
                break

    async def stop(self):
        self.stop_event.set()
        self.pending_flush_event.set()
        await self.flush_task


def _worker_oom_error():
    return WorkerOutOfMemoryError(
        "\n\nWorker container was killed by the Linux OOM killer.\n"
        "This usually means the submitted function used more memory than the container had available.\n"
        'Increase `func_ram`, use `func_ram="dynamic"`, or reduce memory usage inside the function.\n'
    )


def _worker_process_oom_error():
    return WorkerOutOfMemoryError(
        "\n\nWorker process was killed by the Linux OOM killer while the container stayed healthy.\n"
        "This usually means this function call used more memory than was available at the current node parallelism.\n"
        'Increase `func_ram`, use `func_ram="dynamic"`, or reduce memory usage inside the function.\n'
    )


def _dynamic_terminal_oom_error():
    return WorkerOutOfMemoryError(
        '\n\nWorker ran out of memory while `func_ram="dynamic"` was already down to one active worker on this node.\n'
        "Burla cannot give this input more memory on the current machine. Reduce memory usage inside the function or use a larger node.\n"
    )


def _worker_boot_timeout_error(logs: str):
    message = (
        f"\n\nWorker boot timed out after {WORKER_BOOT_TIMEOUT_SECONDS} seconds.\n"
    )
    message += "The worker container never became ready to accept connections.\n"
    message += "\nBuffered worker logs:\n"
    message += "---------------------\n"
    message += f"{logs}\n"
    return RuntimeError(message)


async def retire_workers_for_pressure(
    selected_workers: list[tuple[float, "WorkerClient"]],
    reason: str,
):
    if not selected_workers:
        return
    async with SELF["dynamic_retire_lock"]:
        active_workers = [worker for worker in SELF["workers"] if not worker.retired]
        max_retire_count = max(0, len(active_workers) - 1)
        selected_workers = [
            (metric, worker)
            for metric, worker in selected_workers
            if not worker.retired and worker.current_input is not None
        ][:max_retire_count]
        if not selected_workers:
            return

        current_inputs = [
            (worker, worker.current_input) for _, worker in selected_workers
        ]
        old_parallelism = len(active_workers)
        new_parallelism = old_parallelism - len(current_inputs)
        input_indexes = []
        throttled_indexes = []
        for worker, current_input in current_inputs:
            input_index, input_pkl = current_input
            input_indexes.append(input_index)
            if worker.throttled:
                throttled_indexes.append(input_index)
            worker.retired = True
            worker.is_idle = True
            await SELF["inputs_queue"].put((input_index, input_pkl), len(input_pkl))

        SELF["reboot_containers_after_job"] = True
        SELF["last_pressure_retirement_at"] = time.time()
        SELF["last_memory_shed_at"] = time.time()
        SELF["readd_gates_last_green_at"] = None
        record_park_event()
        msg = (
            f"Node parallelism decreased from {old_parallelism} to {new_parallelism} "
            f"due to {reason}."
        )
        if throttled_indexes:
            msg += (
                f" Killed {len(throttled_indexes)} parked (throttled) worker(s) "
                "first to free memory; their inputs were requeued."
            )
        await Logger().log(
            msg,
            severity="WARNING",
            job_id=SELF["current_job"],
            input_indexes=input_indexes,
            old_parallelism=old_parallelism,
            new_parallelism=new_parallelism,
        )
        if throttled_indexes:
            await debug_log(
                "throttled_workers_killed",
                reason=reason,
                input_indexes=throttled_indexes,
            )

        for worker, _ in current_inputs:
            worker.current_input = None

        await asyncio.gather(
            *(worker.retire_for_pressure() for worker, _ in current_inputs)
        )


async def park_workers_for_memory(
    selected_workers: list[tuple[float, "WorkerClient"]],
    reason: str,
):
    """Park the selected workers (CPU throttle + swap access) and start a
    background reclaim per worker that pushes its resident memory into swap.
    Parked attempts stay stealable via revoke_inputs_for_idle_peer and
    killable by the RAM monitor's backstops."""
    if not selected_workers:
        return
    reclaim_workers = []
    async with SELF["dynamic_retire_lock"]:
        unthrottled_active = [
            worker
            for worker in SELF["workers"]
            if not worker.retired and not worker.throttled
        ]
        running_unthrottled = [
            worker
            for worker in unthrottled_active
            if not worker.is_idle and worker.current_input is not None
        ]
        # Same invariant as the CPU path: at least one RUNNING unthrottled
        # worker must remain.
        max_park_count = max(0, len(running_unthrottled) - 1)
        selected_workers = [
            (rss_bytes, worker)
            for rss_bytes, worker in selected_workers
            if not worker.retired
            and not worker.throttled
            and worker.current_input is not None
        ][:max_park_count]
        if not selected_workers:
            return

        old_parallelism = len(unthrottled_active)
        new_parallelism = old_parallelism - len(selected_workers)
        input_indexes = [worker.current_input[0] for _, worker in selected_workers]
        rss_bytes_list = [int(rss_bytes) for rss_bytes, _ in selected_workers]
        # Reuses the retirement cooldown so the recovery loop, slot trading,
        # and trade grants all hold off while pressure is being shed.
        SELF["last_pressure_retirement_at"] = time.time()
        SELF["last_memory_shed_at"] = time.time()
        SELF["readd_gates_last_green_at"] = None
        record_park_event()
        for _, worker in selected_workers:
            await worker.park_for_memory()
            worker.reclaim_in_progress = True
            reclaim_workers.append(worker)

        msg = (
            f"Node parallelism decreased from {old_parallelism} to {new_parallelism} "
            f"due to {reason}: parked {len(selected_workers)} worker(s) at ~1% CPU and "
            "began moving their memory to swap. Their in-flight inputs are paused, "
            "not killed, and resume (here or on another node) when memory frees up."
        )
        await Logger().log(
            msg,
            severity="WARNING",
            job_id=SELF["current_job"],
            input_indexes=input_indexes,
            old_parallelism=old_parallelism,
            new_parallelism=new_parallelism,
        )
        await debug_log(
            "workers_throttled",
            reason=reason,
            input_indexes=input_indexes,
            rss_bytes=rss_bytes_list,
            old_parallelism=old_parallelism,
            new_parallelism=new_parallelism,
        )
    for worker in reclaim_workers:
        asyncio.create_task(_reclaim_parked_worker_memory(worker))


async def _reclaim_parked_worker_memory(worker: "WorkerClient"):
    input_index = worker.current_input[0] if worker.current_input else None
    try:
        cgroup_dir = _worker_cgroup_dir(worker)
        slice_dir = _workers_cgroup_slice_dir(worker)
        metrics = await asyncio.to_thread(worker.reclaim_memory_sync, cgroup_dir)
        memory_psi_full_avg10 = None
        if slice_dir is not None:
            memory_psi_full_avg10 = _read_memory_psi_full_avg10(slice_dir)
    except OSError:
        return  # worker killed/revoked mid-reclaim and its cgroup vanished
    finally:
        worker.reclaim_in_progress = False
    if metrics["reclaimed_total_bytes"] >= SWAP_RATE_MIN_SAMPLE_BYTES:
        _observe_swap_rate(metrics["reclaimed_total_bytes"] / metrics["duration_sec"])
    await debug_log(
        "worker_swap_reclaim",
        input_index=input_index,
        memory_psi_full_avg10=memory_psi_full_avg10,
        **metrics,
    )


async def revoke_inputs_for_idle_peer(max_inputs: int) -> list[tuple[int, bytes]]:
    """Kill up to max_inputs workers and hand their in-flight inputs to a
    stealing peer that reported idle capacity, least attempt-CPU first (the
    youngest attempts are nearly free to move). Parked workers always qualify:
    they make no progress here. Running workers qualify only while this
    node's cores are saturated (see CPU_UTILIZATION_SATURATED), and at most
    half of them per request so the next utilization sample reflects the
    transfer before the peer asks again. Reuses the pressure-retirement
    mechanics, so exactly-once holds the same way it does there: once
    retired/current_input are cleared under the lock, a late local result is
    dropped by _process_inputs, and a failed transfer ACK requeues the batch
    locally."""
    async with SELF["dynamic_retire_lock"]:
        in_flight = [
            worker
            for worker in SELF["workers"]
            if not worker.retired and worker.current_input is not None
        ]
        candidates = [worker for worker in in_flight if worker.throttled]
        if SELF["cpu_utilization"] >= CPU_UTILIZATION_SATURATED:
            running = sorted(
                (worker for worker in in_flight if not worker.throttled),
                key=lambda worker: worker.attempt_cpu_seconds(),
            )
            candidates += running[: len(running) // 2]
        if not candidates:
            return []
        candidates.sort(key=lambda worker: worker.attempt_cpu_seconds())
        victims = candidates[:max_inputs]

        revoked_inputs = []
        attempt_cpu_seconds = []
        parked_count = sum(worker.throttled for worker in victims)
        for worker in victims:
            revoked_inputs.append(worker.current_input)
            attempt_cpu_seconds.append(round(worker.attempt_cpu_seconds(), 3))
            worker.retired = True
            worker.is_idle = True
            worker.current_input = None
        SELF["reboot_containers_after_job"] = True
        SELF["last_pressure_retirement_at"] = time.time()

        input_indexes = [input_index for input_index, _ in revoked_inputs]
        await Logger().log(
            f"Revoked {len(revoked_inputs)} in-flight input(s) for a peer node "
            "with idle workers.",
            job_id=SELF["current_job"],
            input_indexes=input_indexes,
        )
        await debug_log(
            "inputs_revoked_for_peer",
            input_indexes=input_indexes,
            attempt_cpu_seconds=attempt_cpu_seconds,
            parked_count=parked_count,
            cpu_utilization=round(SELF["cpu_utilization"], 3),
        )
        await asyncio.gather(*(worker.retire_for_pressure() for worker in victims))
        return revoked_inputs


class WorkerClient:
    def __init__(self, image: str, gpu_index: int | None = None):
        self.gpu_index = gpu_index
        self.container_name = f"worker_{uuid4().hex[:8]}"
        self.port = None
        self.image = image
        self.docker = aiodocker.Docker()
        self.is_idle = True
        self.python_version = None
        self.container = None
        self.container_id = None
        self.logstream_task = None
        self.reader = None
        self.writer = None
        self.process_inputs_task = None
        self.log_writer = None
        self.worker_host_pid = None
        self.oom_kill_marker_count = 0
        self.retired = False
        self.current_input = None
        self.throttled = False
        self.throttled_at = None
        self.swap_parked = False
        self.reclaim_in_progress = False
        self.last_reclaim_progress_at = 0.0
        self.attempt_cpu_baseline = None
        self.attempt_started_at = None
        self.attempt_peak_rss_bytes = 0
        self._activity_anchor = None
        self.network_bound = False

    def _worker_server_host_path(self):
        return str(Path(__file__).resolve().parent / "worker_server.py")

    async def _start_container(self):
        binds = [f"{self._worker_server_host_path()}:/opt/burla/worker_server.py"]

        host_config = {
            "PortBindings": {f"{WORKER_INTERNAL_PORT}/tcp": [{"HostIp": "127.0.0.1"}]},
            "ShmSize": 16 * 1024**3,
        }

        host_config["CgroupParent"] = "burla-workers.slice"
        if self.gpu_index is not None:
            # One GPU per worker: without pinning, every worker on a
            # multi-GPU machine (AWS sells A100s only 8-per-VM) sees all
            # GPUs and user code piles onto GPU 0.
            host_config["DeviceRequests"] = [
                {"DeviceIDs": [str(self.gpu_index)], "Capabilities": [["gpu"]]}
            ]
            host_config["Runtime"] = "nvidia"
        binds.extend(
            [
                # One mount lets uv hardlink prepared package files into the
                # environment instead of copying gigabytes between mounts.
                "/worker_service_storage:/worker_service_storage",
                "/workspace/shared:/workspace/shared",
                # node_auth bind: see NODE_AUTH_DIR in node_service/__init__.py.
                "/opt/burla/node_auth:/root/.config/burla",
                # worker_server.py installs burla from this checkout when
                # the pre-populated env is missing - installing from PyPI
                # instead would break any unreleased version.
                "/opt/burla/client:/opt/burla/client:ro",
                # public CAs + the cluster CA, so nested rpm calls can
                # reach the head (cluster-CA cert) without breaking
                # public-internet TLS for user code.
                "/etc/burla/tls/ca-bundle.pem:/etc/burla/ca-bundle.pem:ro",
            ]
        )

        host_config["Binds"] = binds

        # Shell loop keeps PID 1 alive so os.killpg against worker_server.py's process group from
        # the host only restarts Python, not the whole container. sleep 0.1 guards a crash loop.
        command = [
            "sh",
            "-lc",
            (
                "rm -rf /worker_service_python_env /uv_cache; "
                "ln -s /worker_service_storage/python_env /worker_service_python_env; "
                "ln -s /worker_service_storage/uv_cache /uv_cache; "
                "export PYTHONUNBUFFERED=1; "
                "export PYTHONPATH=/worker_service_python_env; "
                'export PATH="/worker_service_python_env/bin:$PATH"; '
                "oom_kill_count() { awk '$1 == \"oom_kill\" {print $2}' /sys/fs/cgroup/memory.events; }; "
                "oom_kills=$(oom_kill_count); "
                f"while true; do python /opt/burla/worker_server.py {WORKER_INTERNAL_PORT} {__version__}; "
                "next_oom_kills=$(oom_kill_count); "
                'if [ "$next_oom_kills" != "$oom_kills" ]; then '
                f"echo '{OOM_KILL_MARKER_PREFIX}'\"$oom_kills->$next_oom_kills\"; "
                "fi; "
                "oom_kills=$next_oom_kills; "
                "sleep 0.1; done"
            ),
        ]

        config = {
            "Image": self.image,
            "Cmd": command,
            "WorkingDir": "/workspace",
            "ExposedPorts": {f"{WORKER_INTERNAL_PORT}/tcp": {}},
            "HostConfig": host_config,
            "Labels": {
                "burla-cluster": BURLA_CLUSTER_NAME,
                "burla-cluster-member": BURLA_CLUSTER_NAME,
            },
            # The bundle is public CAs + the cluster CA (just public CAs when
            # there is no cluster CA), so pointing every TLS stack at it
            # (requests ignores SSL_CERT_FILE) changes nothing for public hosts.
            # BURLA_IN_WORKER tells a nested rpm's client it can reach node
            # hosts directly instead of via a local-dev localhost rewrite.
            "Env": [
                "SSL_CERT_FILE=/etc/burla/ca-bundle.pem",
                "REQUESTS_CA_BUNDLE=/etc/burla/ca-bundle.pem",
                "CURL_CA_BUNDLE=/etc/burla/ca-bundle.pem",
                "BURLA_IN_WORKER=1",
                # Nested rpm calls import the client in here, and that client
                # sends telemetry too; forward the node's kill switch.
                f"DISABLE_BURLA_TELEMETRY={os.environ.get('DISABLE_BURLA_TELEMETRY', '')}",
                "UV_CACHE_DIR=/uv_cache",
                "UV_LINK_MODE=hardlink",
            ],
        }

        try:
            self.container = await self.docker.containers.run(
                config=config, name=self.container_name
            )
        except aiodocker.DockerContainerError as error:
            if (
                "got `canceled`" not in error.message
                and "disconnected from message bus" not in error.message
            ):
                raise
            # systemd sometimes drops a scope start during a mass worker
            # boot (observed: `canceled`, and dbus disconnect on a 64-wide
            # GovDocs boot). One transient failure used to fail the whole
            # node and delete it. run() creates the named container before
            # starting it, so the leftover must be removed or the retry
            # 409s on the name.
            leftover = self.docker.containers.container(error.container_id)
            await leftover.delete(force=True)
            self.container = await self.docker.containers.run(
                config=config, name=self.container_name
            )
        self.container_id = self.container.id

    async def _get_host_port(self):
        for _ in range(20):
            port_info = await self.container.port(WORKER_INTERNAL_PORT)
            if port_info:
                return int(port_info[0]["HostPort"])
            await asyncio.sleep(0.5)
        raise RuntimeError(
            f"Failed to get port for container {self.container_name} in 10s"
        )

    async def _get_worker_host_pid(self) -> int:
        # Docker's /top endpoint returns host PIDs of every process in the container.
        # aiodocker doesn't expose a wrapper for it so we call it via the internal client.
        data = await self.docker._query_json(
            f"containers/{self.container_id}/top", method="GET"
        )
        for row in data.get("Processes", []):
            cmd = row[-1]
            # The shell wrapper's CMD also contains worker_server.py because the script text
            # embeds that path. Skip the wrapper and match only the actual python invocation.
            if "while true" in cmd:
                continue
            if "worker_server.py" in cmd:
                return int(row[1])
        raise RuntimeError(f"worker_server.py not found in {self.container_name}")

    def memory_rss_bytes(self) -> int:
        return psutil.Process(self.worker_host_pid).memory_info().rss

    def cpu_seconds(self) -> float:
        """Summed over the worker's container cgroup, not worker_server.py's
        own process: a call that forks (ocrmypdf, multiprocessing) burns its
        CPU in children, which the parent's own counters never see."""
        return _read_cpu_usage_usec(_worker_cgroup_dir(self)) / 1_000_000

    def attempt_cpu_seconds(self) -> float:
        """CPU consumed by the current attempt: the "least progress lost"
        ranking for parking, revoking and resuming. A missing baseline or a
        vanished process reads 0 (no preservable progress)."""
        if self.attempt_cpu_baseline is None:
            return 0.0
        try:
            return max(0.0, self.cpu_seconds() - self.attempt_cpu_baseline)
        except OSError:
            return 0.0

    def set_cpu_weight(self, weight: int):
        try:
            (_worker_cgroup_dir(self) / "cpu.weight").write_text(str(weight))
        except OSError:
            pass  # worker mid-relaunch or teardown; the next tick rewrites it

    def attempt_elapsed_seconds(self) -> float:
        return time.time() - self.attempt_started_at

    def _rx_bytes(self) -> int:
        # The container's own network namespace, so this is its traffic only.
        total = 0
        for line in Path(f"/proc/{self.worker_host_pid}/net/dev").read_text().splitlines()[2:]:
            interface, _, counters = line.partition(":")
            if interface.strip() != "lo":
                total += int(counters.split()[0])
        return total

    def sample_activity(self):
        """Refresh `network_bound` every NETWORK_BOUND_WINDOW_SECONDS: bytes
        still arriving means a download is in flight. CPU is deliberately not
        consulted: this workload decodes each raster as it lands, so a
        downloading worker never looks idle (observed: parked mid-download
        anyway, then failed on resume). Called each RAM-monitor tick."""
        try:
            rx_bytes = self._rx_bytes()
        except OSError:
            return  # worker_server.py mid-relaunch
        now = time.time()
        if self._activity_anchor is None:
            self._activity_anchor = (now, rx_bytes)
            return
        anchor_at, anchor_rx_bytes = self._activity_anchor
        if now - anchor_at < NETWORK_BOUND_WINDOW_SECONDS:
            return
        self.network_bound = rx_bytes - anchor_rx_bytes >= NETWORK_BOUND_MIN_RX_BYTES
        self._activity_anchor = (now, rx_bytes)

    async def _get_python_version(self):
        for _ in range(20):
            logs = await self._get_logs()
            if logs:
                return logs.splitlines()[0].strip()
            await asyncio.sleep(0.1)
        raise RuntimeError(f"Failed to get python version for {self.container_name}.")

    async def _handle_container_logs(self):
        async for log_line in self.container.log(
            stdout=True, stderr=True, follow=True, timestamps=True
        ):
            self._capture_container_output_chunk(log_line)

    def _capture_container_output_chunk(self, container_output_chunk: str):
        if self.log_writer is None:
            return
        asyncio.create_task(
            self.log_writer.capture_container_output(container_output_chunk)
        )

    async def _ensure_log_writer(self):
        current_job = SELF["current_job"]
        if current_job is None:
            return None
        if self.log_writer is not None and self.log_writer.job_id == current_job:
            return self.log_writer
        if self.log_writer is not None:
            await self.log_writer.stop()
        self.log_writer = JobLogWriter(current_job)
        return self.log_writer

    def _traceback_string(self, error: Exception):
        if isinstance(error, WorkerFunctionError):
            return error.traceback_str
        return "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        )

    async def boot(self):
        await self._start_container()
        self.python_version = await self._get_python_version()
        self.port = await self._get_host_port()
        boot_started_at = time.perf_counter()
        while True:
            try:
                self.reader, self.writer = await asyncio.open_connection(
                    "127.0.0.1", self.port
                )
                worker_socket = self.writer.get_extra_info("socket")
                worker_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self.writer.write(b"s")
                await self.writer.drain()
                await self.reader.readexactly(1)
                break
            except (
                ConnectionRefusedError,
                ConnectionResetError,
                asyncio.IncompleteReadError,
            ):
                if self.writer is not None:
                    self.writer.close()
                    self.writer = None
                container_info = await self.container.show()
                if not container_info["State"]["Running"]:
                    await self._log_container_failure()
                    raise RuntimeError(
                        f"Container {self.container_name} stopped while booting."
                    )
                if time.perf_counter() - boot_started_at > WORKER_BOOT_TIMEOUT_SECONDS:
                    raise _worker_boot_timeout_error(await self._get_logs())
                await asyncio.sleep(0.1)
        self.is_idle = True
        self.logstream_task = asyncio.create_task(self._handle_container_logs())
        self.worker_host_pid = await self._get_worker_host_pid()
        self._deny_swap()

    def _deny_swap(self):
        # Workers must not swap organically: with node swap present, the
        # workers slice's memory.max would push overruns into swap (thrash)
        # instead of the prompt OOM kill users expect. park_for_memory()
        # flips this to "max" for workers being parked. Re-applied after
        # every container (re)start because restarts recreate the cgroup.
        if not IN_LOCAL_DEV_MODE:
            (_worker_cgroup_dir(self) / "memory.swap.max").write_text("0")

    async def _raise_if_worker_failed(self):
        for _ in range(10):
            container_info = await self.container.show()
            if container_info["State"]["OOMKilled"]:
                raise _worker_oom_error()
            if not container_info["State"]["Running"]:
                await self._log_container_failure()
                raise RuntimeError("\n\nWorker container stopped unexpectedly.\n")
            await asyncio.sleep(0.1)
        current_oom_kill_marker_count = oom_kill_marker_count(await self._get_logs())
        if current_oom_kill_marker_count > self.oom_kill_marker_count:
            self.oom_kill_marker_count = current_oom_kill_marker_count
            raise _worker_process_oom_error()
        self.oom_kill_marker_count = current_oom_kill_marker_count
        raise WorkerProcessTerminatedError(
            "\n\nWorker process ended unexpectedly while the container was still healthy.\n"
            "This usually means the user function called `os._exit`, `sys.exit`, raised\n"
            "`SystemExit`/`KeyboardInterrupt`, or crashed a C extension (segfault / OOM of\n"
            "the worker subprocess specifically). The cluster itself is fine.\n"
        )

    async def _retire_after_dynamic_worker_failure(
        self,
        input_index: int,
        input_pkl: bytes,
        error: WorkerOutOfMemoryError | WorkerProcessTerminatedError,
    ):
        async with SELF["dynamic_retire_lock"]:
            # A pressure retirement already requeued this input (it clears
            # current_input before killing the process). Requeueing or
            # delivering here too would run the input twice.
            if self.current_input is None:
                return None
            # The monitor loop may have already retired this worker when its
            # process died (NoSuchProcess), so "terminal" means no OTHER
            # active worker is left to retry the input.
            other_active_workers = [
                worker
                for worker in SELF["workers"]
                if not worker.retired and worker is not self
            ]
            if not other_active_workers:
                if isinstance(error, WorkerOutOfMemoryError):
                    error = _dynamic_terminal_oom_error()
                self.retired = True
                SELF["reboot_containers_after_job"] = True
                return (input_index, True, self._serialize_error(error))

            old_parallelism = len(other_active_workers) + 1
            new_parallelism = old_parallelism - 1
            self.retired = True
            self.is_idle = True
            SELF["reboot_containers_after_job"] = True
            SELF["last_pressure_retirement_at"] = time.time()
            await SELF["inputs_queue"].put((input_index, input_pkl), len(input_pkl))

            reason = (
                "worker process exit"
                if isinstance(error, WorkerProcessTerminatedError)
                else "worker OOM"
            )
            msg = (
                f"Node parallelism decreased from {old_parallelism} to {new_parallelism} "
                "due to memory pressure."
            )
            await Logger().log(
                msg,
                severity="WARNING",
                job_id=SELF["current_job"],
                input_index=input_index,
                reason=reason,
                old_parallelism=old_parallelism,
                new_parallelism=new_parallelism,
            )

            await self._delete_container()
            return None

    async def retire_for_pressure(self):
        await self._kill_worker_process()

    async def throttle(self):
        """Park this worker at the kernel-minimum CPU quota (1% of one core):
        enough to keep TCP, heartbeats, and library timers alive, not enough
        to make meaningful progress. The quota covers the container's whole
        cgroup, UDF children included. aiodocker has no wrapper for the
        update endpoint (which mutates a running container's cgroup limits by
        design), so call it via the internal client like /top."""
        await self.docker._query_json(
            f"containers/{self.container_id}/update",
            method="POST",
            data={
                "CpuQuota": THROTTLED_CPU_QUOTA_USEC,
                "CpuPeriod": CPU_QUOTA_PERIOD_USEC,
            },
        )
        self.throttled = True
        self.throttled_at = time.time()

    async def unthrottle(self):
        await self.docker._query_json(
            f"containers/{self.container_id}/update",
            method="POST",
            data={"CpuQuota": -1, "CpuPeriod": CPU_QUOTA_PERIOD_USEC},
        )
        if self.swap_parked:
            try:
                # Re-deny swap; already-swapped pages just fault back on use.
                (_worker_cgroup_dir(self) / "memory.swap.max").write_text("0")
            except OSError:
                pass  # worker process/container mid-teardown or relaunch
            self.swap_parked = False
        self.throttled = False
        self.throttled_at = None

    async def park_for_memory(self):
        """Park like throttle(), then grant this container swap access (all
        workers boot with memory.swap.max=0) so the reclaim task and the
        kernel's memory.high reclaim can move its pages out of RAM."""
        await self.throttle()
        try:
            (_worker_cgroup_dir(self) / "memory.swap.max").write_text("max")
            self.swap_parked = True
        except OSError:
            pass  # worker process mid-relaunch; stays CPU-parked only

    def reclaim_memory_sync(self, cgroup_dir: Path) -> dict:
        """Blocking (run in a thread): push this parked container's resident
        memory to swap with chunked memory.reclaim writes. The kernel may
        reclaim less than asked (EAGAIN on shortfall), so progress is
        measured from memory.current instead of trusting requested counts."""
        started_at = time.perf_counter()
        self.last_reclaim_progress_at = time.time()
        anon_before = _read_memory_stat_anon(cgroup_dir)
        current_before = int((cgroup_dir / "memory.current").read_text())
        requested_bytes = current_before
        remaining_bytes = requested_bytes
        no_progress_chunks = 0
        outcome = "reclaimed"
        while remaining_bytes > 0:
            if self.retired or not self.swap_parked:
                outcome = "aborted"  # killed, revoked, or resumed mid-reclaim
                break
            chunk_start_bytes = int((cgroup_dir / "memory.current").read_text())
            chunk_bytes = min(MEMORY_RECLAIM_CHUNK_BYTES, remaining_bytes)
            try:
                (cgroup_dir / "memory.reclaim").write_text(str(chunk_bytes))
            except OSError as error:
                # EAGAIN just means this pass reclaimed less than requested.
                if error.errno != errno.EAGAIN:
                    raise
            chunk_end_bytes = int((cgroup_dir / "memory.current").read_text())
            if chunk_start_bytes - chunk_end_bytes < 1024**2:
                no_progress_chunks += 1
                if no_progress_chunks >= 2:
                    outcome = "stalled"  # only unreclaimable pages remain
                    break
            else:
                no_progress_chunks = 0
                self.last_reclaim_progress_at = time.time()
            remaining_bytes -= chunk_bytes
        return {
            "outcome": outcome,
            "requested_bytes": requested_bytes,
            "reclaimed_anon_bytes": anon_before - _read_memory_stat_anon(cgroup_dir),
            "reclaimed_total_bytes": (
                current_before - int((cgroup_dir / "memory.current").read_text())
            ),
            "swap_used_bytes": int((cgroup_dir / "memory.swap.current").read_text()),
            "node_swap_free_bytes": psutil.swap_memory().free,
            "duration_sec": round(time.perf_counter() - started_at, 3),
        }

    async def _read_response(self):
        try:
            status = await self.reader.readexactly(1)
        except (ConnectionResetError, asyncio.IncompleteReadError):
            await self._raise_if_worker_failed()
        if status == b"s":
            payload_size = int.from_bytes(await self.reader.readexactly(8), "big")
            payload = await self.reader.readexactly(payload_size)
            if payload:
                return payload
            return None
        if status == b"e":
            error_size = int.from_bytes(await self.reader.readexactly(8), "big")
            error_response = pickle.loads(await self.reader.readexactly(error_size))
            raise WorkerFunctionError(
                error_response["error_info_pkl"], error_response["traceback_str"]
            )
        raise Exception(f"unknown response status: {status}")

    def _serialize_error(self, error: Exception):
        if isinstance(error, WorkerFunctionError):
            return error.error_info_pkl
        traceback_str = "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        )
        return pickle.dumps(
            {"traceback_str": traceback_str, "is_infrastructure_error": True}
        )

    async def _process_inputs(self):
        while True:
            self.is_idle = True
            # Mint-epoch rollback: between inputs is the one moment a busy
            # worker can retire without stranding an attempt (see
            # shed_slots_pending in head_client.push_state).
            if SELF["shed_slots_pending"] > 0 and not self.retired:
                SELF["shed_slots_pending"] -= 1
                self.retired = True
                SELF["reboot_containers_after_job"] = True
                await self.retire_for_pressure()
                await debug_log(
                    "worker_shed_self",
                    still_pending=SELF["shed_slots_pending"],
                    target_now=SELF["target_parallelism"],
                )
                return
            # Parked attempts have absolute priority over fresh inputs: a
            # finishing worker's freed capacity goes straight to the
            # most-progressed parked attempt, draining the parked count at
            # task-completion speed so workers spend moments, not minutes,
            # throttled. The queue may only be popped while nothing is
            # parked, so any capacity reduction shows up as idle-ready
            # workers instead of frozen in-flight attempts.
            if _parked_workers_exist():
                await _unthrottle_one_parked_worker(
                    reason="a worker went idle", via="idle_handoff"
                )
                while _parked_workers_exist():
                    await asyncio.sleep(0.25)
            while SELF["results_queue"].size_bytes > RESULTS_QUEUE_RAM_LIMIT_BYTES:
                await asyncio.sleep(0.1)
            input_index, input_pkl = await SELF["inputs_queue"].get()

            self.is_idle = False
            self.current_input = (input_index, input_pkl)
            self.attempt_started_at = time.time()
            self._activity_anchor = None
            self.network_bound = False
            self.attempt_peak_rss_bytes = 0
            try:
                self.attempt_cpu_baseline = self.cpu_seconds()
            except OSError:
                self.attempt_cpu_baseline = None  # mid-relaunch: no progress yet
            await self._ensure_log_writer()
            # Exact call tracking: this is the moment the input is handed to
            # the worker, and the finally below is the moment this attempt
            # stops for any reason (result, error, worker death, cancel).
            job_id = SELF["current_job"]
            attempt = uuid4().hex[:12]
            record_call_event("start", job_id, input_index, attempt)
            stop_after_result = False
            try:
                result_pkl = await self.call_function(input_index, input_pkl)
                result = (input_index, False, result_pkl)
                _observe_attempt_peak(self.attempt_peak_rss_bytes)
            except asyncio.CancelledError:
                raise
            except WorkerFunctionError as error:
                if self.log_writer is not None:
                    await self.log_writer.write_error(input_index, error.traceback_str)
                result = (input_index, True, error.error_info_pkl)
                _observe_attempt_peak(self.attempt_peak_rss_bytes)
            except (WorkerOutOfMemoryError, WorkerProcessTerminatedError) as error:
                # Pressure retirement / revoke clears current_input and sets
                # retired before killing us; that kill must not be logged as a
                # per-call infrastructure failure (the input was requeued).
                if self.current_input is None or self.retired:
                    return
                if SELF["dynamic_func_ram"]:
                    result = await self._retire_after_dynamic_worker_failure(
                        input_index, input_pkl, error
                    )
                    if result is None:
                        return
                    # Terminal: no other worker is left to retry this input.
                    # Deliver here, bypassing the `self.retired` early-return
                    # below: the RAM monitor races this handler (it retires a
                    # worker the moment its process disappears) and used to
                    # win, swallowing the error and hanging the job.
                    await SELF["results_queue"].put(result, len(result[2]))
                    SELF["num_results_received"] += 1
                    return
                else:
                    if self.log_writer is not None:
                        await self.log_writer.write_error(
                            input_index, self._traceback_string(error)
                        )
                    result = (input_index, True, self._serialize_error(error))
                stop_after_result = True
            except BaseException as error:
                # Same intentional-teardown signal as above. Without this,
                # job-end container kills surface as
                # "Worker container stopped unexpectedly" on every in-flight
                # call and look like Burla broke when a different call's UDF
                # actually failed.
                if self.current_input is None or self.retired:
                    return
                if self.log_writer is not None:
                    await self.log_writer.write_error(
                        input_index, self._traceback_string(error)
                    )
                result = (input_index, True, self._serialize_error(error))
            finally:
                record_call_event("end", job_id, input_index, attempt)
                if self.log_writer is not None:
                    await self.log_writer.finish_input(input_index)
                self.current_input = None

            if self.retired:
                self.current_input = None
                return
            await SELF["results_queue"].put(result, len(result[2]))
            SELF["num_results_received"] += 1
            if stop_after_result:
                return

    async def install_packages(self, packages: dict):
        try:
            payload = pickle.dumps(packages)
            self.writer.write(b"i")
            self.writer.write(len(payload).to_bytes(8, "big"))
            self.writer.write(payload)
            await self.writer.drain()
            return pickle.loads(await self._read_response())
        except (BrokenPipeError, ConnectionResetError):
            await self._raise_if_worker_failed()

    async def load_function(self, function_bytes: bytes):
        try:
            self.writer.write(b"l")
            self.writer.write(len(function_bytes).to_bytes(8, "big"))
            self.writer.write(function_bytes)
            await self.writer.drain()
            await self._read_response()
            if self.process_inputs_task is None:
                self.process_inputs_task = asyncio.create_task(self._process_inputs())
        except (BrokenPipeError, ConnectionResetError):
            await self._raise_if_worker_failed()

    async def call_function(self, input_index: int, argument_bytes: bytes):
        try:
            payload = pickle.dumps(
                {"input_index": input_index, "argument_bytes": argument_bytes}
            )
            self.writer.write(b"c")
            self.writer.write(len(payload).to_bytes(8, "big"))
            self.writer.write(payload)
            await self.writer.drain()
            return await self._read_response()
        except (BrokenPipeError, ConnectionResetError):
            await self._raise_if_worker_failed()

    async def reset(self):
        # The quota lives on the container, which survives worker_server.py
        # restarts (_restart_container keeps it), so without this the next
        # job would inherit a ~1%-CPU worker.
        if self.throttled:
            await self.unthrottle()
        if self.process_inputs_task is not None:
            self.process_inputs_task.cancel()
            try:
                await self.process_inputs_task
            except asyncio.CancelledError:
                pass
            self.process_inputs_task = None
        if not self.is_idle:
            # Worker is mid-UDF. The worker_server.py main thread is blocked inside the
            # user's function and can't service the 'r' byte over TCP until the call returns.
            # Waiting on the UDF can take arbitrarily long, so kill the container and
            # boot a fresh one instead.
            await self._restart_container()
            return
        if self.writer is not None:
            self.writer.write(b"r")
            self.writer.write((0).to_bytes(8, "big"))
            await self.writer.drain()
            await self._read_response()
        if self.log_writer is not None:
            await self.log_writer.stop()
            self.log_writer = None
        self.is_idle = True

    async def _reconnect(self):
        reconnect_started_at = time.perf_counter()
        while True:
            try:
                self.reader, self.writer = await asyncio.open_connection(
                    "127.0.0.1", self.port
                )
                worker_socket = self.writer.get_extra_info("socket")
                worker_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self.writer.write(b"s")
                await self.writer.drain()
                await self.reader.readexactly(1)
                break
            except (
                ConnectionRefusedError,
                ConnectionResetError,
                asyncio.IncompleteReadError,
            ):
                if self.writer is not None:
                    self.writer.close()
                    self.writer = None
                if (
                    time.perf_counter() - reconnect_started_at
                    > WORKER_BOOT_TIMEOUT_SECONDS
                ):
                    raise _worker_boot_timeout_error(await self._get_logs())
                await asyncio.sleep(0.05)
        self.is_idle = True
        self.logstream_task = asyncio.create_task(self._handle_container_logs())
        self.worker_host_pid = await self._get_worker_host_pid()
        self._deny_swap()

    async def _restart_container(self):
        if self.writer is not None:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
            self.writer = None
            self.reader = None
        if self.logstream_task is not None:
            self.logstream_task.cancel()
            try:
                await self.logstream_task
            except asyncio.CancelledError:
                pass
            self.logstream_task = None
        if self.log_writer is not None:
            await self.log_writer.stop()
            self.log_writer = None
        try:
            os.killpg(self.worker_host_pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        await self._reconnect()

    async def _kill_worker_process(self):
        if self.writer is not None:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
            self.writer = None
            self.reader = None
        if self.logstream_task is not None:
            self.logstream_task.cancel()
            try:
                await self.logstream_task
            except asyncio.CancelledError:
                pass
            self.logstream_task = None
        if self.log_writer is not None:
            await self.log_writer.stop()
            self.log_writer = None
        os.killpg(self.worker_host_pid, signal.SIGKILL)
        container_id = self.container_id
        self.container = None
        self.container_id = None
        self.worker_host_pid = None
        if container_id is not None:
            asyncio.create_task(self._remove_retired_container(container_id))

    async def _delete_container(self):
        if self.writer is not None:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
            self.writer = None
            self.reader = None
        if self.logstream_task is not None:
            self.logstream_task.cancel()
            try:
                await self.logstream_task
            except asyncio.CancelledError:
                pass
            self.logstream_task = None
        if self.log_writer is not None:
            await self.log_writer.stop()
            self.log_writer = None
        await self.container.delete(force=True)
        self.container = None
        self.container_id = None
        self.worker_host_pid = None

    async def _remove_retired_container(self, container_id: str):
        docker = aiodocker.Docker()
        try:
            container = docker.containers.container(container_id)
            await container.delete(force=True)
        except Exception:
            pass
        finally:
            await docker.close()

    async def _container_exists(self):
        if not self.container_id:
            return False
        try:
            await self.container.show()
            return True
        except aiodocker.DockerError as e:
            if e.status == 404:
                return False
            raise

    async def _get_logs(self):
        log_lines = await self.container.log(stdout=True, stderr=True)
        return "".join(log_lines)

    async def _log_container_failure(self):
        if await self._container_exists():
            print(await self._get_logs(), end="")
