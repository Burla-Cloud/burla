import asyncio
import base64
import hashlib
import json
from datetime import datetime, timezone
from time import time

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from main_service import (
    CLOUD_ACCOUNT_NAME,
    CLOUD_PROVIDER,
    CURRENT_BURLA_VERSION,
    IN_CLIENT_HOSTED_MODE,
    IN_LOCAL_DEV_MODE,
    LOCAL_DEV_CONFIG,
    PROJECT_ID,
    cluster_state,
    history,
)
from main_service.endpoints.cluster_lifecycle import (
    _get_cluster_config,
    _mark_running_jobs_with_lifecycle_event,
    _restart_cluster,
    _shutdown_cluster,
    _start_nodes,
    verify_cloud_credentials,
    verify_nodes_can_reach_head,
)
from main_service.endpoints.usage import build_nodes_daily_hours
from main_service.helpers import Logger
from main_service.providers.catalog import (
    gpu_display,
    image_is_gpu_compatible,
    machine_spec,
    on_demand_hourly_usd,
    settings_options,
)


router = APIRouter(prefix="/v1/management")
SSE_MAX_DURATION_SECONDS = 50


class ManagementAPIError(Exception):
    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        details: dict | None = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.retryable = retryable
        self.details = details or {}


async def management_error_handler(request: Request, error: ManagementAPIError):
    request_id = getattr(request.state, "uuid", None)
    return JSONResponse(
        status_code=error.status_code,
        headers={"X-Request-ID": request_id or ""},
        content={
            "request_id": request_id,
            "error": {
                "code": error.code,
                "message": error.message,
                "retryable": error.retryable,
                "details": error.details,
            },
        },
    )


class SettingsPatch(BaseModel):
    image: str | None = None
    machine_type: str | None = None
    quantity: int | None = None
    region: str | None = None
    disk_gb: int | None = None
    inactivity_timeout_seconds: int | None = None


def _iso(timestamp) -> str | None:
    if timestamp is None:
        return None
    if isinstance(timestamp, datetime):
        value = timestamp
    else:
        value = datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_time(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError as error:
        raise ManagementAPIError(
            422, "INVALID_ARGUMENT", f"Invalid ISO-8601 timestamp: {value}"
        ) from error


def _fingerprint(query: dict) -> str:
    encoded = json.dumps(query, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()[:16]


def _encode_cursor(resource: str, query: dict, key) -> str:
    payload = json.dumps(
        {
            "v": 1,
            "resource": resource,
            "query": _fingerprint(query),
            "direction": query.get("order"),
            "key": key,
        },
        separators=(",", ":"),
    ).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def _decode_cursor(cursor: str | None, resource: str, query: dict):
    if cursor is None:
        return None
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded))
    except Exception as error:
        raise ManagementAPIError(422, "INVALID_CURSOR", "Invalid cursor.") from error
    valid = (
        payload.get("v") == 1
        and payload.get("resource") == resource
        and payload.get("query") == _fingerprint(query)
        and payload.get("direction") == query.get("order")
    )
    if not valid:
        raise ManagementAPIError(
            422, "INVALID_CURSOR", "Cursor does not match this query."
        )
    return tuple(payload["key"])


def _terminal_reason(data: dict) -> dict | None:
    reason = data.get("terminal_reason")
    if reason:
        return reason
    fail_reason = data.get("fail_reason")
    if fail_reason:
        messages = fail_reason if isinstance(fail_reason, list) else [fail_reason]
        return {"code": "failed", "source": "legacy", "message": "\n".join(messages)}
    return None


def _node_dto(node: dict) -> dict:
    machine_type = node.get("machine_type")
    if machine_type is None:
        spec = {"cpus": None, "ram_gb": None, "gpus": 0}
    else:
        try:
            spec = machine_spec(machine_type)
        except ValueError:
            spec = {"cpus": None, "ram_gb": None, "gpus": 0}
    result = {
        "node_id": node.get("instance_name"),
        "status": str(node.get("status") or "unknown").lower(),
        "machine_type": machine_type,
        "region": node.get("gcp_region"),
        "spot": bool(node.get("spot")),
        "vcpu_count": spec["cpus"],
        "memory_bytes": spec["ram_gb"] * 1024**3 if spec["ram_gb"] else None,
        "gpu_display": gpu_display(machine_type) if machine_type else None,
        "disk_gb": node.get("disk_size") or node.get("disk_size_gb"),
        "started_booting_at": _iso(node.get("started_booting_at")),
        "ended_at": _iso(node.get("ended_at")),
        "current_job_id": node.get("current_job"),
        "reserved_job_id": node.get("reserved_for_job"),
        "current_function": node.get("current_function"),
        "terminal_reason": _terminal_reason(node),
    }
    if spec["gpus"]:
        result["gpu_count"] = spec["gpus"]
    return result


def _cluster_dto() -> dict:
    active_statuses = {"BOOTING", "READY", "RUNNING"}
    nodes = [
        node for node in cluster_state.list_nodes() if node.get("status") in active_statuses
    ]
    counts = {}
    total_cpus = 0
    total_memory = 0
    total_gpus = 0
    for node in nodes:
        status = str(node.get("status") or "unknown").lower()
        counts[status] = counts.get(status, 0) + 1
        node_dto = _node_dto(node)
        total_cpus += node_dto["vcpu_count"] or 0
        total_memory += node_dto["memory_bytes"] or 0
        total_gpus += node_dto.get("gpu_count", 0)
    if any(node.get("status") == "RUNNING" for node in nodes):
        status = "running"
    elif any(node.get("status") == "READY" for node in nodes):
        status = "ready"
    elif nodes:
        status = "booting"
    else:
        status = "off"
    config = _get_cluster_config()
    result = {
        "status": status,
        "head_mode": (
            "local_dev"
            if IN_LOCAL_DEV_MODE
            else "client_hosted"
            if IN_CLIENT_HOSTED_MODE
            else "deployed"
        ),
        "desired_node_count": sum(spec.get("quantity", 0) for spec in config["Nodes"]),
        "node_counts": counts,
        "vcpu_count": total_cpus,
        "memory_bytes": total_memory,
        "active_job_count": len(cluster_state.running_job_ids()),
        "active_function_count": len(
            {node.get("current_function") for node in nodes if node.get("current_function")}
        ),
    }
    if total_gpus:
        result["gpu_count"] = total_gpus
    return result


def _cluster_watch_dto() -> dict:
    active = [
        _node_dto(node)
        for node in cluster_state.list_nodes()
        if node.get("status") in {"BOOTING", "READY", "RUNNING"}
    ]
    return {"cluster": _cluster_dto(), "nodes": active}


def _sse(event: str, data: dict, event_id: str | None = None) -> str:
    lines = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event}")
    lines.append(f"data: {json.dumps(data, separators=(',', ':'))}")
    return "\n".join(lines) + "\n\n"


@router.get("/cluster")
def get_cluster():
    return _cluster_dto()


@router.get("/cluster/watch")
def watch_cluster():
    async def stream():
        queue = cluster_state.subscribe_node_events()
        started_at = time()
        try:
            yield _sse("snapshot", _cluster_watch_dto())
            while time() - started_at < SSE_MAX_DURATION_SECONDS:
                try:
                    await asyncio.wait_for(queue.get(), timeout=15)
                    yield _sse("update", _cluster_watch_dto())
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            cluster_state.unsubscribe(queue)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache, no-transform"},
    )


@router.post("/cluster/start")
def start_cluster(request: Request):
    active_count = sum(
        node.get("status") in {"BOOTING", "READY", "RUNNING"}
        for node in cluster_state.list_nodes()
    )
    config = _get_cluster_config()
    desired_count = sum(node["quantity"] for node in config["Nodes"])
    if active_count >= desired_count:
        return {"changed": False, "cluster": _cluster_dto()}
    try:
        verify_nodes_can_reach_head()
        verify_cloud_credentials()
        _start_nodes(
            Logger(request),
            config,
            n_nodes_to_add=desired_count - active_count,
        )
    except Exception as error:
        raise ManagementAPIError(
            503, "PROVIDER_ERROR", str(error), retryable=True
        ) from error
    return {"changed": True, "cluster": _cluster_dto()}


@router.post("/cluster/restart")
def restart_cluster(request: Request):
    try:
        verify_nodes_can_reach_head()
        verify_cloud_credentials()
        _mark_running_jobs_with_lifecycle_event(
            "cluster_restarted", "The cluster was restarted."
        )
        _restart_cluster(Logger(request))
    except Exception as error:
        raise ManagementAPIError(
            503, "PROVIDER_ERROR", str(error), retryable=True
        ) from error
    return {"changed": True, "cluster": _cluster_dto()}


@router.post("/cluster/stop")
def stop_cluster(request: Request):
    active = any(
        node.get("status") in {"BOOTING", "READY", "RUNNING"}
        for node in cluster_state.list_nodes()
    )
    try:
        _mark_running_jobs_with_lifecycle_event(
            "cluster_shutdown", "The cluster was shut down."
        )
        _shutdown_cluster(Logger(request))
    except Exception as error:
        raise ManagementAPIError(
            503, "PROVIDER_ERROR", str(error), retryable=True
        ) from error
    return {"changed": active, "cluster": _cluster_dto()}


@router.get("/nodes")
def list_nodes(
    status: str = "active",
    region: str | None = None,
    job_id: str | None = None,
    started_after: str | None = None,
    ended_after: str | None = None,
    sort: str = "started_at",
    order: str = "desc",
    limit: int = 100,
    cursor: str | None = None,
):
    if status not in {
        "active",
        "booting",
        "ready",
        "running",
        "failed",
        "deleted",
        "all",
    }:
        raise ManagementAPIError(422, "INVALID_ARGUMENT", "Invalid node status.")
    if sort not in {"started_at", "ended_at", "status", "machine_type"}:
        raise ManagementAPIError(422, "INVALID_ARGUMENT", "Invalid node sort.")
    if order not in {"asc", "desc"}:
        raise ManagementAPIError(422, "INVALID_ARGUMENT", "Invalid sort order.")
    started_cutoff = _parse_time(started_after)
    ended_cutoff = _parse_time(ended_after)
    query = {
        "status": status,
        "region": region,
        "job_id": job_id,
        "started_after": started_after,
        "ended_after": ended_after,
        "sort": sort,
        "order": order,
    }
    limit = min(max(1, limit), 1000)
    after_key = _decode_cursor(cursor, "nodes", query)
    result = history.management_nodes_page(
        status=status,
        region=region,
        job_id=job_id,
        started_after=started_cutoff,
        ended_after=ended_cutoff,
        sort=sort,
        descending=order == "desc",
        after_key=after_key,
        limit=limit + 1,
    )
    live_nodes = {node["instance_name"]: node for node in cluster_state.list_nodes()}
    has_more = len(result["items"]) > limit
    raw_page = result["items"][:limit]
    items = [
        _node_dto(live_nodes.get(raw["instance_name"], raw))
        for raw in raw_page
    ]
    return {
        "items": items,
        "next_cursor": (
            _encode_cursor("nodes", query, raw_page[-1]["_cursor_key"])
            if has_more and raw_page
            else None
        ),
        "has_more": has_more,
        "total_count": result["total"],
    }


@router.get("/nodes/{node_id}")
def show_node(node_id: str):
    node = next(
        (
            node
            for node in cluster_state.list_nodes()
            if node.get("instance_name") == node_id
        ),
        None,
    )
    if node is None:
        node = history.management_node(node_id)
    if node is not None:
        return _node_dto(node)
    raise ManagementAPIError(404, "NOT_FOUND", f"Node {node_id!r} was not found.")


def _log_page(
    entries: list[dict],
    resource: str,
    query: dict,
    before: str | None,
    after: str | None,
    limit: int,
):
    if before and after:
        raise ManagementAPIError(
            422, "INVALID_ARGUMENT", "Use only one of before or after."
        )
    limit = min(max(1, limit), 5000)
    cursor = before or after
    last = _decode_cursor(cursor, resource, query)
    if last is not None:
        key = tuple(last)
        entries = [
            entry
            for entry in entries
            if (
                (entry["id"], entry.get("offset", 0)) < key
                if before
                else (entry["id"], entry.get("offset", 0)) > key
            )
        ]
    entries.sort(
        key=lambda entry: (entry["id"], entry.get("offset", 0)),
        reverse=bool(before),
    )
    page = entries[: limit + 1]
    has_more = len(page) > limit
    page = page[:limit]
    if before:
        page.reverse()
    next_cursor = None
    if has_more and page:
        edge = page[0] if before else page[-1]
        next_cursor = _encode_cursor(
            resource, query, [edge["id"], edge.get("offset", 0)]
        )
    return {"items": page, "next_cursor": next_cursor, "has_more": has_more}


@router.get("/nodes/{node_id}/logs")
def node_logs(
    node_id: str,
    before: str | None = None,
    after: str | None = None,
    limit: int = 500,
):
    show_node(node_id)
    resource = f"node_logs:{node_id}"
    cursor = before or after
    decoded = _decode_cursor(cursor, resource, {})
    cursor_id = int(decoded[0]) if decoded else None
    limit = min(max(1, limit), 5000)
    entries = [
        {**entry, "timestamp": _iso(entry["timestamp"])}
        for entry in history.management_node_logs(
            node_id,
            before_id=cursor_id if before else None,
            after_id=cursor_id if after else None,
            limit=limit + 1,
        )
    ]
    return _log_page(
        entries,
        resource,
        {},
        before,
        after,
        limit,
    )


@router.get("/nodes/{node_id}/logs/stream")
def stream_node_logs(
    node_id: str,
    request: Request,
    after: str | None = None,
):
    show_node(node_id)
    resume_cursor = after or request.headers.get("last-event-id")
    last = _decode_cursor(resume_cursor, f"node_logs:{node_id}", {})
    last_id = int(last[0]) if last else 0

    async def stream():
        nonlocal last_id
        started_at = time()
        while time() - started_at < SSE_MAX_DURATION_SECONDS:
            rows = history.node_logs_after(node_id, last_id)
            for row_id, timestamp, message in rows:
                last_id = row_id
                cursor = _encode_cursor(
                    f"node_logs:{node_id}", {}, [row_id, 0]
                )
                yield _sse(
                    "log",
                    {
                        "id": row_id,
                        "timestamp": _iso(timestamp),
                        "message": message,
                    },
                    cursor,
                )
            await asyncio.sleep(0.5)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache, no-transform"},
    )


def _job_dto(raw: dict) -> dict:
    job_id = raw["job_id"]
    live = cluster_state.get_job(job_id)
    stored = history.get_job(job_id)
    job = {**(stored or {}), **raw, **(live or {})}
    result_count = max(
        int((stored or {}).get("n_results") or 0),
        int((live or {}).get("n_results") or 0),
        int(raw.get("n_results") or 0),
    )
    input_count = int(job.get("n_inputs") or 0)
    failed_count = history.job_error_count(job_id)
    status = str(job.get("status") or "unknown").lower()
    if input_count and result_count >= input_count:
        status = "failed" if failed_count else "completed"
    started_at = job.get("started_at")
    ended_at = job.get("ended_at")
    notices = []
    for entry in history.management_job_logs(
        job_id, None, include_notices=True, row_limit=500
    ):
        notices.append(
            {
                "id": f"{entry['row_id']}:{entry['offset']}",
                "timestamp": _iso(entry["timestamp"]),
                "message": entry["message"],
                "is_error": entry["is_error"],
            }
        )
    return {
        "job_id": job_id,
        "status": status,
        "user": job.get("user", "Unknown"),
        "function_name": job.get("function_name", "Unknown"),
        "image": job.get("image"),
        "max_parallelism": job.get("max_parallelism"),
        "resources_per_call": {
            "cpu": job.get("func_cpu"),
            "memory_gb": job.get("func_ram"),
            "gpu": job.get("func_gpu"),
        },
        "requested_region": job.get("region"),
        "requested_disk_gb": job.get("disk_gb"),
        "input_count": input_count,
        "result_count": result_count,
        "success_count": max(0, result_count - failed_count),
        "failed_count": failed_count,
        "remaining_count": max(0, input_count - result_count),
        "started_at": _iso(started_at),
        "ended_at": _iso(ended_at),
        "duration_seconds": (
            max(0, float((ended_at or time())) - float(started_at))
            if started_at is not None
            else None
        ),
        "notices": notices,
        "terminal_reason": None if status == "completed" else _terminal_reason(job),
    }


def _job_or_404(job_id: str) -> dict:
    live = cluster_state.get_job(job_id)
    raw = {"job_id": job_id, **live} if live is not None else history.management_job(job_id)
    if raw is None:
        raise ManagementAPIError(404, "NOT_FOUND", f"Job {job_id!r} was not found.")
    return _job_dto(raw)


@router.get("/jobs")
def list_jobs(
    status: str | None = None,
    user: str | None = None,
    function_name: str | None = None,
    started_after: str | None = None,
    started_before: str | None = None,
    sort: str = "started_at",
    order: str = "desc",
    limit: int = 100,
    cursor: str | None = None,
):
    if status and status not in {"running", "completed", "failed", "canceled"}:
        raise ManagementAPIError(422, "INVALID_ARGUMENT", "Invalid job status.")
    sort_fields = {
        "started_at",
        "ended_at",
        "duration",
        "status",
        "input_count",
        "result_count",
        "failed_count",
    }
    if sort not in sort_fields:
        raise ManagementAPIError(422, "INVALID_ARGUMENT", "Invalid job sort.")
    if order not in {"asc", "desc"}:
        raise ManagementAPIError(422, "INVALID_ARGUMENT", "Invalid sort order.")
    after = _parse_time(started_after)
    before = _parse_time(started_before)
    query = {
        "status": status,
        "user": user,
        "function_name": function_name,
        "started_after": started_after,
        "started_before": started_before,
        "sort": sort,
        "order": order,
    }
    limit = min(max(1, limit), 1000)
    after_key = _decode_cursor(cursor, "jobs", query)
    result = history.management_jobs_page(
        status=status,
        user=user,
        function_name=function_name,
        started_after=after,
        started_before=before,
        sort=sort,
        descending=order == "desc",
        after_key=after_key,
        limit=limit + 1,
    )
    has_more = len(result["items"]) > limit
    raw_page = result["items"][:limit]
    items = [_job_dto(raw) for raw in raw_page]
    return {
        "items": items,
        "next_cursor": (
            _encode_cursor("jobs", query, raw_page[-1]["_cursor_key"])
            if has_more and raw_page
            else None
        ),
        "has_more": has_more,
        "total_count": result["total"],
    }


@router.get("/jobs/watch")
def watch_jobs():
    async def stream():
        queue = cluster_state.subscribe_job_events()
        started_at = time()
        try:
            snapshot = list_jobs(limit=100)["items"]
            yield _sse("snapshot", {"items": snapshot})
            while time() - started_at < SSE_MAX_DURATION_SECONDS:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=1)
                    job_id = event["job_id"]
                    yield _sse("update", _job_or_404(job_id))
                except asyncio.TimeoutError:
                    for job_id in cluster_state.running_job_ids():
                        yield _sse("update", _job_or_404(job_id))
        finally:
            cluster_state.unsubscribe(queue)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache, no-transform"},
    )


@router.get("/jobs/{job_id}")
def show_job(job_id: str):
    return _job_or_404(job_id)


@router.get("/jobs/{job_id}/watch")
def watch_job(job_id: str):
    initial = _job_or_404(job_id)

    async def stream():
        queue = cluster_state.subscribe_job_events()
        last_payload = json.dumps(initial, sort_keys=True)
        started_at = time()
        try:
            yield _sse("snapshot", initial)
            if initial["status"] in {"completed", "failed", "canceled"}:
                return
            while time() - started_at < SSE_MAX_DURATION_SECONDS:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=1)
                    if event["job_id"] != job_id:
                        continue
                    item = _job_or_404(job_id)
                except asyncio.TimeoutError:
                    item = _job_or_404(job_id)
                payload = json.dumps(item, sort_keys=True)
                if payload != last_payload:
                    yield _sse("update", item)
                    last_payload = payload
                if item["status"] in {"completed", "failed", "canceled"}:
                    return
        finally:
            cluster_state.unsubscribe(queue)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache, no-transform"},
    )


@router.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: str, request: Request):
    item = _job_or_404(job_id)
    if item["status"] in {"completed", "failed", "canceled"}:
        return {"changed": False, "job": item}
    email = request.session.get("X-User-Email") or request.headers.get("X-User-Email")
    timestamp = time()
    history.add_job_logs(
        job_id,
        [
            {
                "logs": [
                    {
                        "timestamp": timestamp,
                        "message": f"Job canceled by user: {email}",
                    }
                ],
                "timestamp": timestamp,
                "is_error": True,
            }
        ],
    )
    cluster_state.update_job(
        job_id,
        {
            "status": "CANCELED",
            "dashboard_canceled": True,
            "terminal_reason": {
                "code": "user_canceled",
                "source": "user",
                "message": f"Job canceled by user: {email}",
            },
        },
    )
    return {"changed": True, "job": _job_or_404(job_id)}


@router.get("/jobs/{job_id}/errors")
def job_errors(job_id: str, limit: int = 100, cursor: str | None = None):
    _job_or_404(job_id)
    query = {}
    limit = min(max(1, limit), 1000)
    resource = f"job_errors:{job_id}"
    after_key = _decode_cursor(cursor, resource, query)
    result = history.management_error_groups(job_id, after_key, limit + 1)
    has_more = len(result["items"]) > limit
    items = result["items"][:limit]
    return {
        "items": items,
        "next_cursor": (
            _encode_cursor(
                resource,
                query,
                [items[-1]["count"], items[-1]["signature"]],
            )
            if has_more and items
            else None
        ),
        "has_more": has_more,
        "total_count": result["total"],
    }


def _job_metrics_dto(series: dict) -> dict:
    points = []
    for point in series["points"]:
        item = {
            "timestamp": _iso(point["t"]),
            "node_count": point["nodes"],
            "cpu_percent": point["cpu"],
            "memory_percent": point["mem"],
            "network_rx_bytes_per_second": point["net_rx"],
            "network_tx_bytes_per_second": point["net_tx"],
            "disk_read_bytes_per_second": point["disk_read"],
            "disk_write_bytes_per_second": point["disk_write"],
        }
        if series["has_gpu"]:
            item["gpu_percent"] = point["gpu"]
            item["gpu_memory_percent"] = point["gpu_mem"]
        points.append(item)
    return {
        "has_metrics": series["has_metrics"],
        "bucket_seconds": series["bucket_sec"],
        "points": points,
    }


def _call_metrics_dto(series: dict) -> dict:
    points = []
    for point in series["points"]:
        item = {
            "timestamp": _iso(point["t"]),
            "cpu_cores": point["cpus"],
            "memory_bytes": point["mem"],
            "network_rx_bytes_per_second": point["net_rx"],
            "network_tx_bytes_per_second": point["net_tx"],
            "disk_read_bytes_per_second": point["disk_read"],
            "disk_write_bytes_per_second": point["disk_write"],
        }
        if series["has_gpu"]:
            item["gpu_percent"] = point["gpu"]
            item["gpu_memory_bytes"] = point["gpu_mem"]
        points.append(item)
    return {
        "has_metrics": series["has_metrics"],
        "bucket_seconds": series["bucket_sec"],
        "previous_input_index": series["prev_index"],
        "next_input_index": series["next_index"],
        "attempt_count": series["n_attempts"],
        "points": points,
    }


def _raw_metrics_response(
    job_id: str,
    scope: str,
    input_index: int | None,
    limit: int,
    cursor: str | None,
):
    limit = min(max(1, limit), 100_000)
    resource = f"raw_metrics:{job_id}:{scope}:{input_index}"
    query = {}
    decoded = _decode_cursor(cursor, resource, query)
    after_timestamp = float(decoded[0]) if decoded else 0
    after_id = int(decoded[1]) if decoded else 0
    rows = history.management_raw_metrics(
        job_id, scope, input_index, after_timestamp, after_id, limit
    )

    async def stream():
        for row in rows:
            timestamp = row["timestamp"]
            row["timestamp"] = _iso(row["timestamp"])
            if row["gpu_percent"] is None:
                row.pop("gpu_percent")
                row.pop("gpu_memory_bytes")
                row.pop("gpu_memory_percent")
            next_cursor = _encode_cursor(
                resource, query, [timestamp, row["id"]]
            )
            yield json.dumps(
                {"cursor": next_cursor, **row}, separators=(",", ":")
            ) + "\n"

    return StreamingResponse(stream(), media_type="application/x-ndjson")


@router.get("/jobs/{job_id}/metrics")
def job_metrics(job_id: str):
    _job_or_404(job_id)
    return _job_metrics_dto(history.job_metrics_series(job_id))


@router.get("/jobs/{job_id}/metrics/raw")
def raw_job_metrics(job_id: str, limit: int = 10_000, cursor: str | None = None):
    _job_or_404(job_id)
    return _raw_metrics_response(job_id, "node", None, limit, cursor)


def _call_dto(task: dict, job_status: str) -> dict:
    if task["status"] == "failed":
        status = "failed"
    elif task["status"] == "running":
        status = "running"
    elif task["started_at"] is not None:
        status = "canceled" if job_status == "canceled" else "succeeded"
    elif job_status in {"running", "pending"}:
        status = "pending"
    elif job_status == "canceled":
        status = "not_run"
    else:
        status = "unknown"
    ended_at = None
    if (
        task["started_at"] is not None
        and task["duration_sec"] is not None
        and status != "running"
    ):
        ended_at = task["started_at"] + task["duration_sec"]
    return {
        "input_index": task["index"],
        "status": status,
        "attempt_count": task["attempts"],
        "started_at": _iso(task["started_at"]),
        "ended_at": _iso(ended_at),
        "duration_seconds": task["duration_sec"],
        "has_logs": task["has_logs"],
        "has_error": task["has_error"],
        "has_metrics": task["peak_cpus"] is not None
        or task["peak_mem_bytes"] is not None,
        "peak_cpu_cores": task["peak_cpus"],
        "peak_memory_bytes": task["peak_mem_bytes"],
        "_cursor_key": task["_cursor_key"],
    }


def _call_summary_page(
    job: dict,
    *,
    sort: str,
    descending: bool,
    failed_only: bool,
    logs_only: bool,
    has_metrics: bool,
    status: str | None,
    input_index: int | None,
    offset: int,
    limit: int,
    after_key: tuple | None = None,
) -> dict:
    history_sort = {
        "input_index": "index",
        "started_at": "started",
        "ended_at": "ended",
        "duration": "duration",
        "attempts": "attempts",
        "status": "status",
        "peak_cpu": "peak_cpus",
        "peak_memory": "peak_mem",
    }[sort]
    result = history.job_task_summaries(
        job["job_id"],
        job["input_count"],
        history_sort,
        descending,
        failed_only,
        logs_only,
        input_index,
        offset,
        limit,
        job["status"] in {"running", "pending"},
        has_metrics,
        after_key,
        status,
        job["status"] == "canceled",
    )
    return {
        "total": result["total"],
        "items": [_call_dto(task, job["status"]) for task in result["tasks"]],
    }


@router.get("/jobs/{job_id}/calls")
def list_calls(
    job_id: str,
    input_index: int | None = None,
    status: str | None = None,
    failed_only: bool = False,
    logs_only: bool = False,
    has_metrics: bool = False,
    sort: str = "input_index",
    order: str = "asc",
    limit: int = 100,
    cursor: str | None = None,
):
    statuses = {
        "pending",
        "running",
        "succeeded",
        "failed",
        "canceled",
        "not_run",
        "unknown",
    }
    if status and status not in statuses:
        raise ManagementAPIError(422, "INVALID_ARGUMENT", "Invalid call status.")
    sort_fields = {
        "input_index",
        "started_at",
        "ended_at",
        "duration",
        "attempts",
        "status",
        "peak_cpu",
        "peak_memory",
    }
    if sort not in sort_fields or order not in {"asc", "desc"}:
        raise ManagementAPIError(422, "INVALID_ARGUMENT", "Invalid call sort.")
    query = {
        "job_id": job_id,
        "input_index": input_index,
        "status": status,
        "failed_only": failed_only,
        "logs_only": logs_only,
        "has_metrics": has_metrics,
        "sort": sort,
        "order": order,
    }
    limit = min(max(1, limit), 1000)
    resource = f"calls:{job_id}"
    after_key = _decode_cursor(cursor, resource, query)
    job = _job_or_404(job_id)
    result = _call_summary_page(
        job,
        sort=sort,
        descending=order == "desc",
        failed_only=failed_only,
        logs_only=logs_only,
        has_metrics=has_metrics,
        status=status,
        input_index=input_index,
        offset=0,
        limit=limit + 1,
        after_key=after_key,
    )
    has_more = len(result["items"]) > limit
    page = result["items"][:limit]
    items = [
        {key: value for key, value in item.items() if key != "_cursor_key"}
        for item in page
    ]
    return {
        "items": items,
        "next_cursor": (
            _encode_cursor(resource, query, page[-1]["_cursor_key"])
            if has_more and page
            else None
        ),
        "has_more": has_more,
        "total_count": result["total"],
    }


@router.get("/jobs/{job_id}/calls/{input_index}")
def show_call(job_id: str, input_index: int):
    job = _job_or_404(job_id)
    if input_index < 0 or input_index >= job["input_count"]:
        raise ManagementAPIError(
            404,
            "NOT_FOUND",
            f"Input index {input_index} was not found in job {job_id!r}.",
        )
    result = _call_summary_page(
        job,
        sort="input_index",
        descending=False,
        failed_only=False,
        logs_only=False,
        has_metrics=False,
        status=None,
        input_index=input_index,
        offset=0,
        limit=1,
    )
    return {
        key: value
        for key, value in result["items"][0].items()
        if key != "_cursor_key"
    }


@router.get("/jobs/{job_id}/calls/{input_index}/logs")
def call_logs(
    job_id: str,
    input_index: int,
    errors_only: bool = False,
    before: str | None = None,
    after: str | None = None,
    limit: int = 500,
):
    show_call(job_id, input_index)
    query = {"errors_only": errors_only}
    resource = f"call_logs:{job_id}:{input_index}"
    cursor = before or after
    decoded = _decode_cursor(cursor, resource, query)
    cursor_row_id = int(decoded[0]) if decoded else None
    limit = min(max(1, limit), 5000)
    entries = []
    for entry in history.management_job_logs(
        job_id,
        input_index,
        row_limit=limit + 1,
        before_row_id=cursor_row_id if before else None,
        after_row_id=cursor_row_id if after else None,
        errors_only=errors_only,
    ):
        if errors_only and not entry["is_error"]:
            continue
        entries.append(
            {
                "id": entry["row_id"],
                "offset": entry["offset"],
                "timestamp": _iso(entry["timestamp"]),
                "message": entry["message"],
                "is_error": entry["is_error"],
            }
        )
    return _log_page(
        entries,
        resource,
        query,
        before,
        after,
        limit,
    )


@router.get("/jobs/{job_id}/calls/{input_index}/metrics")
def call_metrics(job_id: str, input_index: int):
    show_call(job_id, input_index)
    return _call_metrics_dto(history.task_metrics_series(job_id, input_index))


@router.get("/jobs/{job_id}/calls/{input_index}/metrics/raw")
def raw_call_metrics(
    job_id: str,
    input_index: int,
    limit: int = 10_000,
    cursor: str | None = None,
):
    show_call(job_id, input_index)
    return _raw_metrics_response(job_id, "task", input_index, limit, cursor)


def _settings_dto() -> dict:
    config = LOCAL_DEV_CONFIG if IN_LOCAL_DEV_MODE else history.get_cluster_config()
    node = config["Nodes"][0]
    container = node["containers"][0]
    return {
        "image": container.get("image", ""),
        "machine_type": node.get("machine_type"),
        "quantity": node.get("quantity", 1),
        "region": node.get("gcp_region"),
        "disk_gb": node.get("disk_size_gb", 50),
        "inactivity_timeout_seconds": node.get(
            "inactivity_shutdown_time_sec", 600
        ),
        "burla_version": CURRENT_BURLA_VERSION,
        "project_id": PROJECT_ID,
        "cloud_account_name": CLOUD_ACCOUNT_NAME,
        "cloud_provider": CLOUD_PROVIDER,
        "options": settings_options(CLOUD_PROVIDER),
    }


@router.get("/settings")
def get_settings():
    return _settings_dto()


def _validate_settings(updates: dict):
    options = settings_options(CLOUD_PROVIDER)
    machine_types = {
        machine["machine_type"] for machine in options["machine_types"]
    }
    if "machine_type" in updates and updates["machine_type"] not in machine_types:
        raise ManagementAPIError(
            422, "INVALID_ARGUMENT", "machine_type is not available on this cloud."
        )
    current = _settings_dto()
    target_machine = updates.get("machine_type", current["machine_type"])
    target_region = updates.get("region", current["region"])
    machine = next(
        item for item in options["machine_types"] if item["machine_type"] == target_machine
    )
    if target_region not in machine["regions"]:
        raise ManagementAPIError(
            422,
            "INVALID_ARGUMENT",
            "region is not available for this machine type.",
        )
    for field in ("quantity", "disk_gb", "inactivity_timeout_seconds"):
        if field not in updates:
            continue
        constraint = options["constraints"][field]
        if not constraint["minimum"] <= updates[field] <= constraint["maximum"]:
            raise ManagementAPIError(
                422,
                "INVALID_ARGUMENT",
                f"{field} must be between {constraint['minimum']} and "
                f"{constraint['maximum']}.",
            )
    target_image = updates.get("image", current["image"])
    if (
        machine_spec(target_machine)["gpus"]
        and not image_is_gpu_compatible(target_image)
    ):
        raise ManagementAPIError(
            422,
            "INVALID_ARGUMENT",
            "The selected GPU machine requires a CUDA-capable container image.",
        )


@router.patch("/settings")
def update_settings(patch: SettingsPatch):
    updates = patch.model_dump(exclude_none=True)
    if not updates:
        raise ManagementAPIError(
            422, "INVALID_ARGUMENT", "At least one setting is required."
        )
    _validate_settings(updates)
    config = history.get_cluster_config()
    node = config["Nodes"][0]
    container = node["containers"][0]
    if "image" in updates:
        container["image"] = updates["image"]
        container.pop("python_command", None)
        container.pop("python_version", None)
    mapping = {
        "machine_type": "machine_type",
        "quantity": "quantity",
        "region": "gcp_region",
        "disk_gb": "disk_size_gb",
        "inactivity_timeout_seconds": "inactivity_shutdown_time_sec",
    }
    for field, config_field in mapping.items():
        if field in updates:
            node[config_field] = updates[field]
    history.save_cluster_config(config)
    if IN_LOCAL_DEV_MODE:
        LOCAL_DEV_CONFIG.update(config)
        LOCAL_DEV_CONFIG["Nodes"][0]["machine_type"] = {
            "gcp": "n4-standard-2",
            "aws": "m7i.large",
            "azure": "Standard_D2as_v5",
        }[CLOUD_PROVIDER]
        LOCAL_DEV_CONFIG["Nodes"][0]["quantity"] = 1
    return _settings_dto()


@router.get("/usage")
def get_usage(month: str | None = None):
    report = build_nodes_daily_hours(month)
    total_cost = 0.0
    unpriced_hours = 0.0
    compute_types = {}
    for day in report["days"]:
        day_cost = 0.0
        day_unpriced = 0.0
        for group in day["groups"]:
            price = on_demand_hourly_usd(group["machine_type"])
            if price is None:
                group["estimated_spend_usd"] = None
                day_unpriced += group["total_node_hours"]
            else:
                group["estimated_spend_usd"] = round(
                    group["total_node_hours"] * price, 6
                )
                day_cost += group["estimated_spend_usd"]
            display = gpu_display(group["machine_type"])
            category = display.split(" ")[1] if display else "CPU"
            bucket = compute_types.setdefault(
                category,
                {
                    "type": category,
                    "compute_hours": 0.0,
                    "estimated_spend_usd": 0.0,
                    "rate_missing": False,
                },
            )
            bucket["compute_hours"] += group["total_compute_hours"]
            if group["estimated_spend_usd"] is None:
                bucket["rate_missing"] = True
            else:
                bucket["estimated_spend_usd"] += group["estimated_spend_usd"]
        day["estimated_spend_usd"] = round(day_cost, 6)
        day["unpriced_node_hours"] = round(day_unpriced, 6)
        total_cost += day_cost
        unpriced_hours += day_unpriced
    report["estimated_spend_usd"] = round(total_cost, 6)
    report["unpriced_node_hours"] = round(unpriced_hours, 6)
    report["compute_types"] = [
        {
            **bucket,
            "compute_hours": round(bucket["compute_hours"], 6),
            "estimated_spend_usd": round(bucket["estimated_spend_usd"], 6),
        }
        for bucket in compute_types.values()
    ]
    return report
