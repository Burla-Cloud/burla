"""
Authoritative in-memory live cluster state.

main_service is a single always-on process; every node pushes its state here
over HTTP (see endpoints/nodes.py) and every reader (job start, dashboards,
the burla client) is served from these dicts. Firestore is gone - the only
persistence is the SQLite history store, written for durable mutations so
in-flight jobs can recover after a head restart.

Mutations can arrive from the event loop (endpoints) and from worker threads
(Node.start runs in a ThreadPoolExecutor), so a threading.Lock guards state
and pub/sub events are delivered with call_soon_threadsafe.
"""

import asyncio
import threading
from time import time

from main_service import history

_lock = threading.RLock()

# instance_name -> node dict (same field names the firestore docs used).
# DELETED entries are kept (not dropped) so a deleted VM whose push loop is
# still running can't resurrect itself; readers filter them out. Instance
# names are never reused, and the head reloads only active nodes at startup,
# so this can't grow unboundedly.
NODES: dict[str, dict] = {}

# job_id -> job dict. Same fields the firestore job docs used, plus
# "assigned_nodes": {instance_name: {"current_num_results", "client_contact_last_1s",
# "last_push_at"}}. Terminal jobs stay in memory (they're small) and are
# persisted to history on every durable mutation.
JOBS: dict[str, dict] = {}

TERMINAL_JOB_STATUSES = ("COMPLETED", "FAILED", "CANCELED")
NODE_FRESHNESS_SEC = 15

_loop: asyncio.AbstractEventLoop | None = None
_node_event_queues: set[asyncio.Queue] = set()
_job_event_queues: set[asyncio.Queue] = set()
_node_log_queues: dict[str, set[asyncio.Queue]] = {}


def set_event_loop(loop: asyncio.AbstractEventLoop):
    global _loop
    _loop = loop


def load_from_history():
    """Rebuild live state after a head restart. Node pushes (~1s cadence)
    correct any staleness within seconds; jobs whose nodes died while the
    head was down are cleaned up by the reaper."""
    with _lock:
        for node in history.active_nodes():
            node.pop("last_push_at", None)
            node["loaded_from_history"] = True
            NODES.setdefault(node["instance_name"], node)
        for job_id, job in history.running_jobs():
            job.pop("n_results", None)
            job["assigned_nodes"] = {}
            JOBS.setdefault(job_id, job)


def _publish(queues, event: dict):
    if _loop is None:
        return
    for queue in list(queues):
        _loop.call_soon_threadsafe(queue.put_nowait, event)


# ------------------------------------------------------------------ subscriptions


def subscribe_node_events() -> asyncio.Queue:
    queue = asyncio.Queue()
    with _lock:
        _node_event_queues.add(queue)
    return queue


def subscribe_job_events() -> asyncio.Queue:
    queue = asyncio.Queue()
    with _lock:
        _job_event_queues.add(queue)
    return queue


def subscribe_node_logs(instance_name: str) -> asyncio.Queue:
    queue = asyncio.Queue()
    with _lock:
        _node_log_queues.setdefault(instance_name, set()).add(queue)
    return queue


def unsubscribe(queue: asyncio.Queue):
    with _lock:
        _node_event_queues.discard(queue)
        _job_event_queues.discard(queue)
        for queues in _node_log_queues.values():
            queues.discard(queue)


# ------------------------------------------------------------------ nodes


def list_nodes() -> list[dict]:
    with _lock:
        return [
            dict(node) for node in NODES.values() if node.get("status") != "DELETED"
        ]


def get_node(instance_name: str) -> dict | None:
    """Live view: DELETED nodes read as absent (the client treats a 404 on a
    node it was polling as FAILED, same as the old cache behavior)."""
    with _lock:
        node = NODES.get(instance_name)
        if node is None or node.get("status") == "DELETED":
            return None
        return dict(node)


def node_is_fresh(node: dict, now: float | None = None) -> bool:
    now = time() if now is None else now
    return now - node.get("last_push_at", 0) <= NODE_FRESHNESS_SEC


def update_node(instance_name: str, updates: dict) -> dict:
    """Merge `updates` into the node's state and return the merged view.

    Rules (previously enforced by scattered read-then-write firestore code):
    - BOOTING/READY/RUNNING never overwrite a terminal DELETED/FAILED status.
    - DELETED never overwrites FAILED (failed nodes stay visible for debugging).
    - DELETED nodes are persisted to history then dropped from memory.
    """
    with _lock:
        node = NODES.get(instance_name, {"instance_name": instance_name})
        current_status = node.get("status")
        new_status = updates.get("status")

        if new_status:
            terminal = current_status in ("DELETED", "FAILED")
            downgrade = new_status in ("BOOTING", "READY", "RUNNING")
            if terminal and downgrade:
                updates = {k: v for k, v in updates.items() if k != "status"}
                new_status = None
            if new_status == "DELETED" and current_status == "FAILED":
                updates = {k: v for k, v in updates.items() if k != "status"}
                new_status = None

        node.update(updates)
        status_changed = new_status is not None and new_status != current_status
        merged = dict(node)
        durable_fields = {
            "host",
            "peer_host",
            "public_ip",
            "private_ip",
            "zone",
            "current_job",
            "reserved_for_job",
        }
        durable_changed = status_changed or bool(durable_fields.intersection(updates))
        if durable_changed:
            history.upsert_node(instance_name, merged)
        NODES[instance_name] = node

    if status_changed or "host" in updates or "current_job" in updates:
        deleted = merged.get("status") == "DELETED"
        _publish(_node_event_queues, {"deleted": deleted, **merged})
    return merged


def record_node_push(instance_name: str, updates: dict) -> dict:
    return update_node(
        instance_name,
        {**updates, "last_push_at": time(), "loaded_from_history": False},
    )


def remove_node(instance_name: str):
    """Drop a node from live state without marking it DELETED (dev cleanup)."""
    with _lock:
        node = NODES.pop(instance_name, None)
    if node:
        _publish(_node_event_queues, {"deleted": True, **node})


def add_node_log(instance_name: str, msg: str, ts: float | None = None):
    ts = ts if ts is not None else time()
    history.add_node_logs(instance_name, [{"msg": msg, "ts": ts}])
    with _lock:
        queues = set(_node_log_queues.get(instance_name, ()))
    _publish(queues, {"msg": msg, "ts": ts})


def add_node_logs(instance_name: str, logs: list[dict]):
    history.add_node_logs(instance_name, logs)
    with _lock:
        queues = set(_node_log_queues.get(instance_name, ()))
    for log in logs:
        _publish(queues, {"msg": log.get("msg", ""), "ts": log.get("ts")})


# ------------------------------------------------------------------ jobs

# Set while `burla deploy` snapshots this head's history db for migration to a
# new deployed cluster: a job admitted mid-snapshot would write history the
# migration never sees.
_job_admission_paused = False


def pause_job_admission_if_idle() -> bool:
    """Pause admission, refusing if any job is RUNNING. Shares the state lock
    with `admit_job`, so after this returns True no job can slip in."""
    global _job_admission_paused
    with _lock:
        if any(job.get("status") == "RUNNING" for job in JOBS.values()):
            return False
        _job_admission_paused = True
        return True


def resume_job_admission():
    global _job_admission_paused
    with _lock:
        _job_admission_paused = False


def job_admission_paused() -> bool:
    return _job_admission_paused


def admit_job(job_id: str, job: dict, selected_instance_names: list[str]) -> bool:
    job = dict(job)
    job["assigned_nodes"] = {}
    with _lock:
        if _job_admission_paused:
            return False
        selected = [NODES.get(name) for name in selected_instance_names]
        if any(
            node is None
            or node.get("status") != "READY"
            or node.get("current_job")
            or node.get("reserved_for_job")
            or not node_is_fresh(node)
            for node in selected
        ):
            return False

        node_snapshots = []
        for node in selected:
            snapshot = dict(node)
            snapshot["reserved_for_job"] = job_id
            node_snapshots.append(snapshot)

        history.upsert_job_and_nodes(job_id, job, node_snapshots)
        for snapshot in node_snapshots:
            NODES[snapshot["instance_name"]] = snapshot
        JOBS[job_id] = job
        snapshot = dict(job)

    for node in node_snapshots:
        _publish(_node_event_queues, {"deleted": False, **node})
    _publish(_job_event_queues, {"job_id": job_id, **_job_summary(snapshot)})
    return True


def _get_or_load_job(job_id: str) -> dict | None:
    """Must be called with _lock held. Lazily reloads a job from history
    after a head restart so in-flight jobs keep working."""
    job = JOBS.get(job_id)
    if job is not None:
        return job
    stored = history.get_job(job_id)
    if stored is None:
        return None
    stored.pop("n_results", None)
    stored["assigned_nodes"] = {}
    JOBS[job_id] = stored
    return stored


def get_job(job_id: str) -> dict | None:
    with _lock:
        job = _get_or_load_job(job_id)
        if job is None:
            return None
        view = {k: v for k, v in job.items() if k != "assigned_nodes"}
        view["n_results"] = sum(
            n.get("current_num_results", 0) for n in job["assigned_nodes"].values()
        )
        return view


def update_job(
    job_id: str, updates: dict, append_fail_reason: str | None = None
) -> bool:
    """Merge updates into a job. Returns False if the job doesn't exist.

    Status rule: FAILED/CANCELED always apply; COMPLETED only applies while
    the job is RUNNING (a node reporting completion must not overwrite a
    cancellation another writer already recorded).
    """
    with _lock:
        job = _get_or_load_job(job_id)
        if job is None:
            return False
        updates = dict(updates)
        new_status = updates.get("status")
        if new_status == "COMPLETED" and job.get("status") != "RUNNING":
            updates.pop("status")
            new_status = None
        became_failed = new_status == "FAILED" and job.get("status") != "FAILED"
        job.update(updates)
        if append_fail_reason is not None:
            reasons = job.setdefault("fail_reason", [])
            if append_fail_reason not in reasons:
                reasons.append(append_fail_reason)
        released_nodes = []
        if job.get("status") in TERMINAL_JOB_STATUSES:
            for instance_name, node in NODES.items():
                if node.get("reserved_for_job") == job_id:
                    node["reserved_for_job"] = None
                    released_nodes.append(dict(node))
        snapshot = dict(job)
        history.upsert_job_and_nodes(job_id, snapshot, released_nodes)

    for node in released_nodes:
        _publish(_node_event_queues, {"deleted": False, **node})
    _publish(_job_event_queues, {"job_id": job_id, **_job_summary(snapshot)})

    if became_failed:
        # Lazy import: helpers imports from the main_service package, which
        # is mid-initialization when this module is first imported.
        from main_service.helpers import ship_job_debug_logs

        threading.Thread(
            target=ship_job_debug_logs,
            args=(job_id, snapshot.get("fail_reason")),
            daemon=True,
        ).start()
    return True


# Result counts live in memory (per-node progress) and normally only reach
# history on status transitions. Flushing them every few seconds bounds how
# many counts an ungraceful head death (kill -9, SIGTERM mid-job) can lose;
# the MAX() in history's upsert keeps the stored count monotonic.
COUNTS_FLUSH_INTERVAL_SEC = 5
_counts_flushed_at: dict[str, float] = {}


def update_job_progress(
    job_id: str,
    instance_name: str,
    current_num_results: int | None = None,
    client_contact_last_1s: bool | None = None,
):
    with _lock:
        job = _get_or_load_job(job_id)
        if job is None:
            return
        progress = job["assigned_nodes"].setdefault(
            instance_name, {"current_num_results": 0, "client_contact_last_1s": True}
        )
        if current_num_results is not None:
            progress["current_num_results"] = current_num_results
        if client_contact_last_1s is not None:
            progress["client_contact_last_1s"] = client_contact_last_1s
        now = time()
        progress["last_push_at"] = now
        if now - _counts_flushed_at.get(job_id, 0) >= COUNTS_FLUSH_INTERVAL_SEC:
            _counts_flushed_at[job_id] = now
            history.upsert_job_and_nodes(job_id, dict(job), [])


def job_view(job_id: str) -> dict:
    """The signal set a node needs each push: replaces the per-job firestore
    on_snapshot watch, the assigned_nodes quorum read, and the completion
    count aggregation."""
    with _lock:
        job = _get_or_load_job(job_id)
        if job is None:
            return {"exists": False}
        assigned = job["assigned_nodes"]
        now = time()
        return {
            "exists": True,
            "status": job.get("status"),
            "grow": bool(job.get("grow")),
            "all_inputs_uploaded": bool(job.get("all_inputs_uploaded")),
            "client_has_all_results": bool(job.get("client_has_all_results")),
            "dashboard_canceled": bool(job.get("dashboard_canceled")),
            "cluster_shutdown": bool(job.get("cluster_shutdown")),
            "cluster_restarted": bool(job.get("cluster_restarted")),
            "any_node_client_contact": any(
                progress.get("client_contact_last_1s")
                and now - progress.get("last_push_at", 0) <= NODE_FRESHNESS_SEC
                for progress in assigned.values()
            ),
            "total_num_results": sum(
                progress.get("current_num_results", 0) for progress in assigned.values()
            ),
            "n_inputs": job.get("n_inputs"),
        }


def running_job_ids() -> list[str]:
    with _lock:
        return [
            job_id for job_id, job in JOBS.items() if job.get("status") == "RUNNING"
        ]


def nodes_for_job(job_id: str) -> list[dict]:
    """Every node running or reserved for this job. The client polls this
    during a job to discover mid-job replacement nodes it must assign."""
    with _lock:
        return [
            {
                "instance_name": name,
                "status": node.get("status"),
                "host": node.get("host"),
                "machine_type": node.get("machine_type"),
                "target_parallelism": node.get("target_parallelism"),
            }
            for name, node in NODES.items()
            if node.get("status") != "DELETED"
            and (
                node.get("current_job") == job_id
                or node.get("reserved_for_job") == job_id
            )
        ]


def record_replacement_request(
    job_id: str, requesting_node: str, request: dict, cpus_booted: int
):
    """Persist a replacement boot under the state lock: the idempotency entry
    (nodes retry with the same request_id when a response is lost) and the
    CPU-budget decrement must not race concurrent requests from other nodes."""
    with _lock:
        job = _get_or_load_job(job_id)
        if job is None:
            return
        job.setdefault("replacement_requests", {})[requesting_node] = request
        if job.get("grow_cpus_remaining") is not None:
            job["grow_cpus_remaining"] = max(
                0, job["grow_cpus_remaining"] - cpus_booted
            )
        history.upsert_job_and_nodes(job_id, dict(job), [])


def peers_for_job(job_id: str) -> dict:
    """RUNNING nodes assigned to this job (the input-stealing ring) plus the
    ids of nodes still BOOTING, so a stealer can tell whether expected nodes
    might still join."""
    with _lock:
        now = time()
        peers = [
            {"instance_name": name, "host": node.get("peer_host") or node.get("host")}
            for name, node in sorted(NODES.items())
            if node.get("status") == "RUNNING"
            and node.get("current_job") == job_id
            and node_is_fresh(node, now)
        ]
        booting = [
            name
            for name, node in NODES.items()
            if node.get("status") == "BOOTING" and not node.get("loaded_from_history")
        ]
    return {"peers": peers, "booting_node_ids": booting}


def _job_summary(job: dict) -> dict:
    assigned = job.get("assigned_nodes") or {}
    return {
        "status": job.get("status"),
        "user": job.get("user", "Unknown"),
        "function_name": job.get("function_name", "Unknown"),
        "n_inputs": job.get("n_inputs", 0),
        "n_results": sum(p.get("current_num_results", 0) for p in assigned.values()),
        "started_at": job.get("started_at"),
    }


def job_summary(job_id: str) -> dict | None:
    with _lock:
        job = JOBS.get(job_id)
        return _job_summary(job) if job else None


# ------------------------------------------------------------------ job reaper

# A RUNNING job whose nodes have all stopped pushing state was previously
# detected by the dashboard's SSE stream (only while someone had it open).
# The head now owns that watchdog. The budget can be short because cgroup
# isolation (verified at node boot) keeps node_service responsive under any
# user workload, so real silence this long means the nodes are gone.
REAPER_JOB_SILENCE_SEC = 150
REAPER_INTERVAL_SEC = 10


async def job_reaper_loop(logger=None):
    while True:
        await asyncio.sleep(REAPER_INTERVAL_SEC)
        now = time()
        with _lock:
            candidates = []
            for job_id, job in JOBS.items():
                if job.get("status") != "RUNNING":
                    continue
                if now - (job.get("started_at") or now) < REAPER_JOB_SILENCE_SEC:
                    continue
                assigned = job["assigned_nodes"]
                last_push = max(
                    (p.get("last_push_at", 0) for p in assigned.values()), default=0
                )
                fresh_nodes_on_job = any(
                    node.get("status") == "RUNNING"
                    and node.get("current_job") == job_id
                    and node_is_fresh(node, now)
                    for node in NODES.values()
                )
                n_results = sum(
                    p.get("current_num_results", 0) for p in assigned.values()
                )
                n_inputs = job.get("n_inputs") or 0
                all_results_in = n_inputs > 0 and n_results >= n_inputs
                dead = (
                    now - last_push > REAPER_JOB_SILENCE_SEC and not fresh_nodes_on_job
                )
                if dead:
                    completed = job.get("client_has_all_results") or (
                        job.get("is_background_job")
                        and job.get("all_inputs_uploaded")
                        and all_results_in
                    )
                    candidates.append((job_id, "COMPLETED" if completed else "FAILED"))

        for job_id, status in candidates:
            if status == "COMPLETED":
                update_job(job_id, {"status": status})
                if logger is not None:
                    logger.log(f"Reaped completed job {job_id}", severity="WARNING")
                continue
            reason = 'main_svc: job is "running" but no nodes working on it ???'
            update_job(job_id, {"status": "FAILED"}, append_fail_reason=reason)
            history.add_job_logs(
                job_id,
                [
                    {
                        "logs": [
                            {
                                "timestamp": now,
                                "message": "Job failed due to internal cluster error.",
                            }
                        ],
                        "timestamp": now,
                        "is_error": True,
                    }
                ],
            )
            if logger is not None:
                logger.log(f"Reaped stalled job {job_id}", severity="WARNING")
