import asyncio
from pathlib import Path
from time import monotonic, time

import psutil
import pynvml

from node_service import INSTANCE_N_CPUS, SELF, head_client

SAMPLE_INTERVAL_SEC = 1
BATCH_INTERVAL_SEC = 5
BATCH_MAX_ROWS = 5_000
MICROSECONDS_PER_SECOND = 1_000_000


def _gpu_handles() -> list:
    try:
        pynvml.nvmlInit()
    except pynvml.NVMLError:
        # No NVIDIA driver: a CPU-only node.
        return []
    count = pynvml.nvmlDeviceGetCount()
    return [pynvml.nvmlDeviceGetHandleByIndex(index) for index in range(count)]


def _gpu_readings(handles: list) -> list[dict]:
    """NVML utilization is already averaged over its last sample period
    (~1s), so unlike the cpu/net/disk counters no previous value is needed."""
    readings = []
    for handle in handles:
        utilization = pynvml.nvmlDeviceGetUtilizationRates(handle)
        memory = pynvml.nvmlDeviceGetMemoryInfo(handle)
        readings.append(
            {
                "gpu_percent": float(utilization.gpu),
                "memory_used_bytes": int(memory.used),
                "memory_total_bytes": int(memory.total),
            }
        )
    return readings


def _node_gpu_fields(gpu_readings: list[dict]) -> dict:
    if not gpu_readings:
        return {"gpu_percent": None, "gpu_memory_bytes": None, "gpu_memory_percent": None}
    used_bytes = sum(reading["memory_used_bytes"] for reading in gpu_readings)
    total_bytes = sum(reading["memory_total_bytes"] for reading in gpu_readings)
    mean_percent = sum(r["gpu_percent"] for r in gpu_readings) / len(gpu_readings)
    return {
        "gpu_percent": mean_percent,
        "gpu_memory_bytes": used_bytes,
        "gpu_memory_percent": 100 * used_bytes / total_bytes,
    }


def _node_cpu_counters() -> tuple[float, float]:
    cpu_times = psutil.cpu_times()
    total_seconds = sum(cpu_times) - cpu_times.guest - cpu_times.guest_nice
    busy_seconds = total_seconds - cpu_times.idle - cpu_times.iowait
    return total_seconds, busy_seconds


def _node_counters() -> dict:
    total_cpu_seconds, busy_cpu_seconds = _node_cpu_counters()
    memory = psutil.virtual_memory()
    network = psutil.net_io_counters(nowrap=True)
    disk = psutil.disk_io_counters(nowrap=True)
    return {
        "total_cpu_seconds": total_cpu_seconds,
        "busy_cpu_seconds": busy_cpu_seconds,
        "memory_bytes": memory.total - memory.available,
        "memory_total_bytes": memory.total,
        "network_rx_bytes": network.bytes_recv,
        "network_tx_bytes": network.bytes_sent,
        "disk_read_bytes": disk.read_bytes,
        "disk_write_bytes": disk.write_bytes,
    }


def _node_sample(
    current: dict,
    previous: dict,
    sampled_at: float,
    duration_sec: float,
    job_id: str | None,
    gpu_readings: list[dict],
) -> dict:
    cpu_seconds = current["busy_cpu_seconds"] - previous["busy_cpu_seconds"]
    total_cpu_seconds = (
        current["total_cpu_seconds"] - previous["total_cpu_seconds"]
    )
    return {
        "timestamp": sampled_at,
        "duration_sec": duration_sec,
        "scope": "node",
        "job_id": job_id,
        "input_index": None,
        "worker_id": "",
        "cpu_seconds": cpu_seconds,
        "cpu_percent": 100 * cpu_seconds / total_cpu_seconds,
        "memory_bytes": current["memory_bytes"],
        "memory_percent": (
            100 * current["memory_bytes"] / current["memory_total_bytes"]
        ),
        # max(0, ...): the node-wide network/disk totals include worker veth
        # interfaces and devices that vanish when containers exit, so the
        # aggregate counter can go backwards between samples.
        "network_rx_bytes": max(
            0, current["network_rx_bytes"] - previous["network_rx_bytes"]
        ),
        "network_tx_bytes": max(
            0, current["network_tx_bytes"] - previous["network_tx_bytes"]
        ),
        "disk_read_bytes": max(
            0, current["disk_read_bytes"] - previous["disk_read_bytes"]
        ),
        "disk_write_bytes": max(
            0, current["disk_write_bytes"] - previous["disk_write_bytes"]
        ),
        **_node_gpu_fields(gpu_readings),
    }


def _worker_counters(worker_pid: int) -> dict:
    cgroup_path = (
        Path(f"/proc/{worker_pid}/cgroup").read_text().strip().split(":", 2)[2]
    )
    cgroup_dir = Path("/sys/fs/cgroup", cgroup_path.lstrip("/"))
    cpu = dict(
        line.split() for line in (cgroup_dir / "cpu.stat").read_text().splitlines()
    )
    memory = dict(
        line.split() for line in (cgroup_dir / "memory.stat").read_text().splitlines()
    )

    disk_read_bytes = 0
    disk_write_bytes = 0
    for line in (cgroup_dir / "io.stat").read_text().splitlines():
        _, *fields = line.split()
        counters = dict(field.split("=") for field in fields)
        disk_read_bytes += int(counters["rbytes"])
        disk_write_bytes += int(counters["wbytes"])

    network_rx_bytes = 0
    network_tx_bytes = 0
    network_lines = Path(f"/proc/{worker_pid}/net/dev").read_text().splitlines()[2:]
    for line in network_lines:
        interface, counters = line.split(":", 1)
        if interface.strip() == "lo":
            continue
        fields = counters.split()
        network_rx_bytes += int(fields[0])
        network_tx_bytes += int(fields[8])

    memory_bytes = int((cgroup_dir / "memory.current").read_text()) - int(
        memory["inactive_file"]
    )
    return {
        "cpu_usage_usec": int(cpu["usage_usec"]),
        "memory_bytes": memory_bytes,
        "network_rx_bytes": network_rx_bytes,
        "network_tx_bytes": network_tx_bytes,
        "disk_read_bytes": disk_read_bytes,
        "disk_write_bytes": disk_write_bytes,
    }


def _worker_snapshot(worker, job_id: str | None):
    container_id = worker.container_id
    current_input = worker.current_input
    input_index = current_input[0] if current_input is not None else None
    try:
        counters = _worker_counters(worker.worker_host_pid)
    except FileNotFoundError:
        return None
    return {
        "container_id": container_id,
        "worker_id": worker.container_name,
        "job_id": job_id,
        "input_index": input_index,
        "gpu_index": worker.gpu_index,
        "counters": counters,
    }


def _task_sample(
    snapshot: dict,
    previous: dict,
    sampled_at: float,
    duration_sec: float,
    node_memory_bytes: int,
    gpu_readings: list[dict],
) -> dict:
    current = snapshot["counters"]
    # On GPU nodes each worker owns exactly one GPU, so that device's stats
    # ARE this task's GPU usage.
    gpu_index = snapshot["gpu_index"]
    gpu = gpu_readings[gpu_index] if gpu_index is not None and gpu_readings else None
    cpu_seconds = (
        current["cpu_usage_usec"] - previous["cpu_usage_usec"]
    ) / MICROSECONDS_PER_SECOND
    return {
        "timestamp": sampled_at,
        "duration_sec": duration_sec,
        "scope": "task",
        "job_id": snapshot["job_id"],
        "input_index": snapshot["input_index"],
        "worker_id": snapshot["worker_id"],
        "cpu_seconds": cpu_seconds,
        "cpu_percent": (
            100 * cpu_seconds / (duration_sec * INSTANCE_N_CPUS)
        ),
        "memory_bytes": current["memory_bytes"],
        "memory_percent": 100 * current["memory_bytes"] / node_memory_bytes,
        "network_rx_bytes": (
            current["network_rx_bytes"] - previous["network_rx_bytes"]
        ),
        "network_tx_bytes": (
            current["network_tx_bytes"] - previous["network_tx_bytes"]
        ),
        "disk_read_bytes": (
            current["disk_read_bytes"] - previous["disk_read_bytes"]
        ),
        "disk_write_bytes": (
            current["disk_write_bytes"] - previous["disk_write_bytes"]
        ),
        "gpu_percent": gpu["gpu_percent"] if gpu else None,
        "gpu_memory_bytes": gpu["memory_used_bytes"] if gpu else None,
        "gpu_memory_percent": (
            100 * gpu["memory_used_bytes"] / gpu["memory_total_bytes"] if gpu else None
        ),
    }


async def resource_metrics_loop():
    gpu_handles = _gpu_handles()
    previous_node_counters = _node_counters()
    previous_node_at = monotonic()
    previous_worker_counters = {}
    pending_samples = []
    next_sample_at = previous_node_at
    last_flush_at = previous_node_at

    while True:
        next_sample_at += SAMPLE_INTERVAL_SEC
        await asyncio.sleep(max(0, next_sample_at - monotonic()))

        sampled_at = time()
        sampled_monotonic = monotonic()
        current_node_counters = _node_counters()
        gpu_readings = _gpu_readings(gpu_handles)
        pending_samples.append(
            _node_sample(
                current_node_counters,
                previous_node_counters,
                sampled_at,
                sampled_monotonic - previous_node_at,
                SELF["current_job"],
                gpu_readings,
            )
        )
        previous_node_counters = current_node_counters
        previous_node_at = sampled_monotonic

        job_id = SELF["current_job"]
        snapshots = [
            _worker_snapshot(worker, job_id)
            for worker in SELF["workers"]
            if worker.container_id is not None and worker.worker_host_pid is not None
        ]
        worker_sampled_at = monotonic()
        current_container_ids = set()
        for snapshot in snapshots:
            if snapshot is None:
                continue
            container_id = snapshot["container_id"]
            current_container_ids.add(container_id)
            previous = previous_worker_counters.get(container_id)
            if previous is not None and snapshot["input_index"] is not None:
                previous_at, previous_counters = previous
                pending_samples.append(
                    _task_sample(
                        snapshot,
                        previous_counters,
                        sampled_at,
                        worker_sampled_at - previous_at,
                        current_node_counters["memory_total_bytes"],
                        gpu_readings,
                    )
                )
            previous_worker_counters[container_id] = (
                worker_sampled_at,
                snapshot["counters"],
            )
        previous_worker_counters = {
            container_id: previous_worker_counters[container_id]
            for container_id in current_container_ids
        }

        should_flush = (
            monotonic() - last_flush_at >= BATCH_INTERVAL_SEC
            or len(pending_samples) >= BATCH_MAX_ROWS
        )
        if should_flush:
            try:
                await head_client.post_resource_metrics(pending_samples)
            except Exception as error:
                print(
                    f"failed to forward {len(pending_samples)} resource metrics "
                    f"to head: {error}"
                )
            else:
                pending_samples = []
            last_flush_at = monotonic()
