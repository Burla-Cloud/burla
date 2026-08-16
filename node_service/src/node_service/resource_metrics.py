import asyncio
from time import monotonic, time

import aiodocker
import psutil

from node_service import INSTANCE_N_CPUS, SELF, head_client

SAMPLE_INTERVAL_SEC = 1
BATCH_INTERVAL_SEC = 5
BATCH_MAX_ROWS = 5_000
NANOSECONDS_PER_SECOND = 1_000_000_000


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
    }


def _container_counters(stats: dict) -> dict:
    memory = stats["memory_stats"]
    memory_bytes = memory["usage"] - memory["stats"]["inactive_file"]
    network_interfaces = stats["networks"].values()
    block_io = stats["blkio_stats"]["io_service_bytes_recursive"]
    return {
        "cpu_usage_ns": stats["cpu_stats"]["cpu_usage"]["total_usage"],
        "memory_bytes": memory_bytes,
        "network_rx_bytes": sum(
            interface["rx_bytes"] for interface in network_interfaces
        ),
        "network_tx_bytes": sum(
            interface["tx_bytes"] for interface in stats["networks"].values()
        ),
        "disk_read_bytes": sum(
            entry["value"] for entry in block_io if entry["op"].lower() == "read"
        ),
        "disk_write_bytes": sum(
            entry["value"] for entry in block_io if entry["op"].lower() == "write"
        ),
    }


async def _worker_snapshot(worker, job_id: str | None):
    container = worker.container
    container_id = worker.container_id
    current_input = worker.current_input
    input_index = current_input[0] if current_input is not None else None
    try:
        stats = (await container.stats(stream=False))[0]
    except aiodocker.DockerError as error:
        if error.status == 404:
            return None
        raise
    return {
        "container_id": container_id,
        "worker_id": worker.container_name,
        "job_id": job_id,
        "input_index": input_index,
        "counters": _container_counters(stats),
    }


def _task_sample(
    snapshot: dict,
    previous: dict,
    sampled_at: float,
    duration_sec: float,
    node_memory_bytes: int,
) -> dict:
    current = snapshot["counters"]
    cpu_seconds = (
        current["cpu_usage_ns"] - previous["cpu_usage_ns"]
    ) / NANOSECONDS_PER_SECOND
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
    }


async def resource_metrics_loop():
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
        pending_samples.append(
            _node_sample(
                current_node_counters,
                previous_node_counters,
                sampled_at,
                sampled_monotonic - previous_node_at,
                SELF["current_job"],
            )
        )
        previous_node_counters = current_node_counters
        previous_node_at = sampled_monotonic

        job_id = SELF["current_job"]
        snapshots = await asyncio.gather(
            *(
                _worker_snapshot(worker, job_id)
                for worker in SELF["workers"]
                if worker.container is not None
            )
        )
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
