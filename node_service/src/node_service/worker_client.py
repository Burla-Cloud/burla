import asyncio
import os
import pickle
import signal
import socket
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import aiodocker
import psutil
from tblib import Traceback

from node_service import (
    SELF,
    BURLA_CLUSTER_NAME,
    IN_LOCAL_DEV_MODE,
    Logger,
    __version__,
    head_client,
)

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
DYNAMIC_RAM_MAX_WORKER_MEMORY_USED_FRACTION = 0.97
DYNAMIC_RAM_TARGET_WORKER_MEMORY_USED_FRACTION = 0.92
DYNAMIC_RAM_MONITOR_INTERVAL_SECONDS = 0.25
DYNAMIC_RAM_STARTUP_MONITOR_INTERVAL_SECONDS = 0.05
DYNAMIC_RAM_STARTUP_MONITOR_SECONDS = 30

# Read from the cgroup root instead of /proc/pressure/cpu: identical on real
# VMs (node_service is a host process), but inside a local-dev DinD node the
# private cgroup namespace scopes it to that fake VM instead of the whole
# docker-desktop VM shared by every cluster on the machine.
CPU_PRESSURE_FILE = Path("/sys/fs/cgroup/cpu.pressure")
# 2s is PSI's own internal update cadence; 1s windows are too noisy.
CPU_PRESSURE_MONITOR_INTERVAL_SECONDS = 2
CPU_PRESSURE_MAX_STALL_FRACTION = 0.10
CPU_PRESSURE_MAX_KILL_FRACTION = 0.5


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
            problems.append(f"{WORKERS_CGROUP_SLICE} has no memory cap (memory.max=max)")
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
        await logger.log(
            f"Worker cgroup isolation verified: {len(workers)} workers in "
            f"{WORKERS_CGROUP_SLICE} (memory.max={memory_max}, "
            f"cpu.weight={cpu_weight}), node_service in {NODE_SERVICE_CGROUP_SLICE}."
        )


async def dynamic_ram_monitor_loop():
    started_at = time.perf_counter()
    worker_memory_limit_bytes = None
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
            worker_memory_limit_bytes = _workers_memory_limit_bytes(active_workers[0])

        worker_memory = []
        for worker in active_workers:
            try:
                worker_memory.append((worker.memory_rss_bytes(), worker))
            except psutil.NoSuchProcess:
                await _relocate_worker_process_or_retire(worker)
        if not worker_memory:
            continue

        active_worker_memory_bytes = sum(rss_bytes for rss_bytes, _ in worker_memory)
        active_worker_memory_fraction = (
            active_worker_memory_bytes / worker_memory_limit_bytes
        )
        if active_worker_memory_fraction < DYNAMIC_RAM_MAX_WORKER_MEMORY_USED_FRACTION:
            continue

        if len(worker_memory) <= 1:
            continue

        target_used_bytes = int(
            worker_memory_limit_bytes * DYNAMIC_RAM_TARGET_WORKER_MEMORY_USED_FRACTION
        )
        bytes_to_free = max(0, active_worker_memory_bytes - target_used_bytes)
        running_worker_memory = [
            (rss_bytes, worker)
            for rss_bytes, worker in worker_memory
            if not worker.is_idle and worker.current_input is not None
        ]
        if not running_worker_memory:
            continue
        # Smallest first: retiring small workers gives large tasks more room
        # while losing the least in-flight progress to the requeue.
        running_worker_memory.sort(key=lambda item: item[0])

        selected_worker_memory = []
        selected_rss_bytes = 0
        for rss_bytes, worker in running_worker_memory:
            if len(worker_memory) - len(selected_worker_memory) <= 1:
                break
            selected_worker_memory.append((rss_bytes, worker))
            selected_rss_bytes += rss_bytes
            if selected_rss_bytes >= bytes_to_free:
                break

        await retire_workers_for_pressure(
            selected_worker_memory, reason="memory pressure"
        )


def _read_cpu_stall_usec() -> int:
    # `some` line, `total` field: cumulative microseconds during which at
    # least one runnable task sat waiting for a core.
    some_line = CPU_PRESSURE_FILE.read_text().splitlines()[0]
    return int(some_line.rsplit("total=", 1)[1])


async def cpu_pressure_monitor_loop():
    if not CPU_PRESSURE_FILE.exists():
        await Logger().log(
            f"{CPU_PRESSURE_FILE} does not exist (kernel without PSI?), "
            "dynamic CPU is disabled for this job.",
            severity="WARNING",
        )
        return

    # Prime every worker's cpu_percent handle: psutil measures CPU use since
    # the previous call on the same handle, and a fresh handle reads 0.0,
    # which would make victim ranking garbage on the first pressured tick.
    for worker in _active_dynamic_workers():
        try:
            worker.cpu_percent()
        except psutil.NoSuchProcess:
            pass  # worker_server.py mid-relaunch; the loop below re-locates it

    last_stall_usec = _read_cpu_stall_usec()
    last_read_at = time.perf_counter()
    while SELF["dynamic_func_cpu"]:
        await asyncio.sleep(CPU_PRESSURE_MONITOR_INTERVAL_SECONDS)
        active_workers = _active_dynamic_workers()
        if not active_workers:
            return

        # Sample every tick so each reading covers exactly the last tick.
        worker_cpu = []
        for worker in active_workers:
            try:
                worker_cpu.append((worker.cpu_percent(), worker))
            except psutil.NoSuchProcess:
                await _relocate_worker_process_or_retire(worker)

        stall_usec = _read_cpu_stall_usec()
        read_at = time.perf_counter()
        elapsed_usec = (read_at - last_read_at) * 1_000_000
        stall_fraction = (stall_usec - last_stall_usec) / elapsed_usec
        last_stall_usec, last_read_at = stall_usec, read_at

        if stall_fraction < CPU_PRESSURE_MAX_STALL_FRACTION:
            continue
        if len(worker_cpu) <= 1:
            continue

        running_worker_cpu = [
            (cpu, worker)
            for cpu, worker in worker_cpu
            if not worker.is_idle and worker.current_input is not None
        ]
        if not running_worker_cpu:
            continue
        # Least CPU first: cheapest to evict and requeue, and the biggest
        # tasks keep the cores they are clearly using.
        running_worker_cpu.sort(key=lambda item: item[0])

        # Batch scales with how far past the threshold pressure is, capped at
        # half the running workers per tick, so a slammed node converges in a
        # few ticks while mild pressure sheds one worker at a time.
        kill_fraction = min(
            CPU_PRESSURE_MAX_KILL_FRACTION,
            stall_fraction - CPU_PRESSURE_MAX_STALL_FRACTION,
        )
        n_to_retire = max(1, int(len(running_worker_cpu) * kill_fraction))
        await retire_workers_for_pressure(
            running_worker_cpu[:n_to_retire], reason="CPU pressure"
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

        old_parallelism = len(active_workers)
        new_parallelism = old_parallelism - len(selected_workers)
        input_indexes = []
        for _, worker in selected_workers:
            input_index, input_pkl = worker.current_input
            input_indexes.append(input_index)
            worker.retired = True
            worker.is_idle = True
            await SELF["inputs_queue"].put((input_index, input_pkl), len(input_pkl))

        SELF["reboot_containers_after_job"] = True
        msg = (
            f"Node parallelism decreased from {old_parallelism} to {new_parallelism} "
            f"due to {reason}."
        )
        await Logger().log(
            msg,
            severity="WARNING",
            job_id=SELF["current_job"],
            input_indexes=input_indexes,
            old_parallelism=old_parallelism,
            new_parallelism=new_parallelism,
        )

        current_inputs = []
        for _, worker in selected_workers:
            input_index = worker.current_input[0]
            current_inputs.append((worker, input_index))
        for worker, input_index in current_inputs:
            worker.current_input = None

        await asyncio.gather(
            *(worker.retire_for_pressure() for _, worker in selected_workers)
        )


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
        self._psutil_process = None
        self.oom_kill_marker_count = 0
        self.retired = False
        self.current_input = None

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
                "/worker_service_python_env:/worker_service_python_env",
                # A worker env can be the client's whole environment (GBs); the
                # cache bind makes every install after the first on a host
                # near-network-free.
                "/uv_cache:/uv_cache",
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
                # Copy mode: hardlinks can't cross the cache/env bind mounts.
                "UV_CACHE_DIR=/uv_cache",
                "UV_LINK_MODE=copy",
            ],
        }

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

    def cpu_percent(self) -> float:
        # psutil measures CPU use since the previous call on the same handle
        # (a fresh handle reads 0.0), so the handle must persist across calls.
        # Rebuild it when worker_server.py was relaunched under a new pid.
        process = self._psutil_process
        if process is None or process.pid != self.worker_host_pid:
            process = psutil.Process(self.worker_host_pid)
            self._psutil_process = process
        return process.cpu_percent()

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
        error_info = getattr(error, "burla_error_info", None)
        if error_info and error_info.get("traceback_dict"):
            traceback_object = Traceback.from_dict(
                error_info["traceback_dict"]
            ).as_traceback()
            return "".join(
                traceback.format_exception(type(error), error, traceback_object)
            )
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
            while SELF["results_queue"].size_bytes > RESULTS_QUEUE_RAM_LIMIT_BYTES:
                await asyncio.sleep(0.1)
            input_index, input_pkl = await SELF["inputs_queue"].get()

            self.is_idle = False
            self.current_input = (input_index, input_pkl)
            await self._ensure_log_writer()
            stop_after_result = False
            try:
                result_pkl = await self.call_function(input_index, input_pkl)
                result = (input_index, False, result_pkl)
            except asyncio.CancelledError:
                raise
            except WorkerFunctionError as error:
                if self.log_writer is not None:
                    await self.log_writer.write_error(input_index, error.traceback_str)
                result = (input_index, True, error.error_info_pkl)
            except (WorkerOutOfMemoryError, WorkerProcessTerminatedError) as error:
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
                if self.log_writer is not None:
                    await self.log_writer.write_error(
                        input_index, self._traceback_string(error)
                    )
                result = (input_index, True, self._serialize_error(error))
            finally:
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
            await self._read_response()
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
