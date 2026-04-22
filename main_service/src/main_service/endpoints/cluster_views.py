import json
import asyncio
from datetime import datetime, timedelta
from time import time
from typing import Optional

import pytz
import textwrap

from fastapi import APIRouter, Depends, Request, HTTPException
from google.cloud import firestore
from starlette.responses import StreamingResponse

from main_service import (
    DB,
    get_logger,
    get_add_background_task_function,
)
from main_service.helpers import Logger
from main_service.node import Node
from main_service.endpoints.usage import _to_epoch_ms

router = APIRouter()

# Cloud Run cuts requests at 60s, which on `while True` SSE generators produces
# a `Truncated response body` warning every cycle. End the stream before that
# cutoff and rely on the client's EventSource `retry: 5000` to reconnect.
SSE_MAX_DURATION_SEC = 50


def _require_auth(request: Request) -> dict:
    email = request.session.get("X-User-Email") or request.headers.get("X-User-Email")
    authorization = request.session.get("Authorization") or request.headers.get("Authorization")
    if not email or not authorization:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return {"Authorization": authorization, "X-User-Email": email}


@router.get("/v1/cluster")
async def cluster_info(request: Request, logger: Logger = Depends(get_logger)):
    _require_auth(request)

    queue = asyncio.Queue()
    current_loop = asyncio.get_running_loop()

    async def node_stream():
        active_filter = firestore.FieldFilter("status", "in", ["BOOTING", "READY", "RUNNING"])
        query = DB.collection("nodes").where(filter=active_filter)
        if len([doc for doc in query.stream()]) == 0:
            yield f"data: {json.dumps({'type': 'empty'})}\n\n"

        def on_snapshot(query_snapshot, changes, read_time):
            for change in changes:
                doc_data = change.document.to_dict() or {}
                instance_name = doc_data.get("instance_name")

                if change.type.name == "REMOVED":
                    event_data = {"nodeId": instance_name, "deleted": True}
                else:
                    # job_id is `f"{function_name}-{uid}"` (see client _remote_parallel_map.py)
                    job_id = doc_data.get("current_job") or doc_data.get("reserved_for_job")
                    current_function = job_id.rsplit("-", 1)[0] if job_id else None
                    event_data = {
                        "nodeId": instance_name,
                        "status": doc_data.get("status"),
                        "type": doc_data.get("machine_type"),
                        "started_booting_at": _to_epoch_ms(doc_data.get("started_booting_at")),
                        "current_function": current_function,
                    }
                current_loop.call_soon_threadsafe(queue.put_nowait, event_data)

        node_watch = DB.collection("nodes").where(filter=active_filter).on_snapshot(on_snapshot)
        stream_started_at = time()
        try:
            yield "retry: 5000\n\n"
            yield ": init\n\n"
            while time() - stream_started_at < SSE_MAX_DURATION_SEC:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=2)
                    yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            node_watch.unsubscribe()

    return StreamingResponse(
        node_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache, no-transform"},
    )


@router.delete("/v1/cluster/{node_id}")
def delete_node(
    node_id: str,
    request: Request,
    add_background_task=Depends(get_add_background_task_function),
    logger: Logger = Depends(get_logger),
):
    auth_headers = _require_auth(request)

    node_doc = DB.collection("nodes").document(node_id).get()
    node = Node.from_snapshot(DB, logger, node_doc, auth_headers)
    add_background_task(node.delete)


@router.get("/v1/cluster/{node_id}/logs")
async def node_log_stream(node_id: str, request: Request):
    _require_auth(request)

    queue = asyncio.Queue()
    current_loop = asyncio.get_running_loop()

    timezone_name = request.cookies.get("timezone", "UTC")
    try:
        tz = pytz.timezone(timezone_name)
    except Exception:
        tz = pytz.timezone("UTC")

    def ts_to_str(timestamp: float) -> str:
        return f"[{datetime.fromtimestamp(timestamp, tz).strftime('%I:%M %p').lstrip('0')}]"

    last_date_str = None
    first_log_processed = False

    def on_snapshot(query_snapshot, changes, read_time):
        nonlocal last_date_str, first_log_processed
        sorted_changes = sorted(changes, key=lambda c: (c.document.to_dict() or {}).get("ts") or 0)

        for change in sorted_changes:
            log_doc_dict = change.document.to_dict() or {}
            timestamp = log_doc_dict.get("ts")
            if not timestamp:
                continue

            current_date_str = datetime.fromtimestamp(timestamp, tz).strftime("%B %d, %Y (%Z)")
            if not first_log_processed or current_date_str != last_date_str:
                padding_size = max(0, (120 - 2 - len(current_date_str)) // 2)
                message = f"{'-' * padding_size} {current_date_str} {'-' * padding_size}"
                current_loop.call_soon_threadsafe(queue.put_nowait, {"message": message})
                last_date_str = current_date_str
                first_log_processed = True

            timestamp_str = ts_to_str(timestamp)
            message_raw = str(log_doc_dict.get("msg") or "").rstrip()
            line_len = max(20, 120 - len(timestamp_str))
            wrapper = textwrap.TextWrapper(line_len, break_long_words=True, break_on_hyphens=True)

            formatted_lines = []
            for original_line in message_raw.splitlines() or [""]:
                wrapped_segments = wrapper.wrap(original_line) or [""]
                for segment in wrapped_segments:
                    if not formatted_lines:
                        formatted_lines.append(f"{timestamp_str} {segment}")
                    else:
                        formatted_lines.append(f" {' ' * len(timestamp_str)}{segment}")

            message_clean = "\n".join(formatted_lines)
            current_loop.call_soon_threadsafe(queue.put_nowait, {"message": message_clean})

    logs_ref = DB.collection("nodes").document(node_id).collection("logs")
    watch = logs_ref.on_snapshot(on_snapshot)

    async def log_generator():
        stream_started_at = time()
        try:
            yield "retry: 5000\n\n"
            yield ": init\n\n"
            while time() - stream_started_at < SSE_MAX_DURATION_SEC:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=2)
                    yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            watch.unsubscribe()

    return StreamingResponse(
        log_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache, no-transform"},
    )


@router.get("/v1/cluster/deleted_recent_paginated")
def get_deleted_recent_paginated(
    request: Request,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
):
    _require_auth(request)

    if offset is None or limit is None:
        page_value = max(int(page or 0), 0)
        page_size_value = max(int(page_size or 15), 1)
        offset = page_value * page_size_value
        limit = page_size_value
    else:
        offset = max(int(offset), 0)
        limit = max(int(limit), 1)

    deleted_statuses = {"DELETED", "FAILED"}
    cutoff_ms = int((datetime.utcnow() - timedelta(days=7)).timestamp() * 1000)

    def _sort_ms_from_doc(data: dict) -> int:
        deleted_ms = _to_epoch_ms(data.get("deleted_at"))
        started_ms = _to_epoch_ms(data.get("started_booting_at"))
        return deleted_ms or started_ms or 0

    def _within_last_7_days(data: dict) -> bool:
        return _sort_ms_from_doc(data) >= cutoff_ms

    nodes = []
    last_doc = None
    scanned = 0
    max_scan = 20000

    while True:
        query = DB.collection("nodes").order_by(
            "started_booting_at", direction=firestore.Query.DESCENDING
        )
        if last_doc:
            query = query.start_after(last_doc)

        docs = list(query.limit(500).stream())
        if not docs:
            break

        last_doc = docs[-1]
        scanned += len(docs)

        for doc in docs:
            data = doc.to_dict() or {}
            status = str(data.get("status") or "").upper()
            if status not in deleted_statuses:
                continue
            if not _within_last_7_days(data):
                continue

            deleted_ms = _to_epoch_ms(data.get("deleted_at"))
            started_ms = _to_epoch_ms(data.get("started_booting_at"))
            sort_ms = deleted_ms or started_ms or 0
            nodes.append(
                {
                    "id": doc.id,
                    "name": data.get("instance_name", doc.id),
                    "status": data.get("status"),
                    "type": data.get("machine_type"),
                    "cpus": data.get("num_cpus"),
                    "gpus": data.get("num_gpus"),
                    "memory": data.get("memory"),
                    "deletedAt": deleted_ms if deleted_ms is not None else sort_ms,
                    "started_booting_at": started_ms,
                    "_sort_ms": sort_ms,
                }
            )

        if scanned >= max_scan:
            break

    nodes.sort(key=lambda node: (node.get("_sort_ms") or 0), reverse=True)
    total = len(nodes)
    paged = nodes[offset : offset + limit]
    for node in paged:
        node.pop("_sort_ms", None)

    return {
        "nodes": paged,
        "total": total,
        "meta": {
            "offset": offset,
            "limit": limit,
            "returned": len(paged),
            "scanned": scanned,
            "max_scan": max_scan,
            "cutoff_days": 7,
        },
    }
