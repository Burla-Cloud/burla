import json
import asyncio
from datetime import datetime, timedelta
from time import time
from typing import Optional

import pytz
import textwrap

from fastapi import APIRouter, Depends, Request, HTTPException
from starlette.responses import StreamingResponse

from main_service import get_logger
from main_service import cluster_state, history
from main_service.helpers import Logger
from main_service.node import Node
from main_service.endpoints.usage import _to_epoch_ms

router = APIRouter()

# Browsers reconnect via EventSource `retry:`; recycling the stream bounds how
# long a dead connection can hold a pub/sub queue open.
SSE_MAX_DURATION_SEC = 50


def _require_auth(request: Request) -> dict:
    email = request.session.get("X-User-Email") or request.headers.get("X-User-Email")
    authorization = request.session.get("Authorization") or request.headers.get("Authorization")
    if not email or not authorization:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return {"Authorization": authorization, "X-User-Email": email}


def _node_event(node: dict) -> dict:
    if node.get("deleted") or node.get("status") in ("DELETED", None):
        return {"nodeId": node.get("instance_name"), "deleted": True}
    # job_id is `f"{function_name}-{uid}"` (see client _remote_parallel_map.py)
    job_id = node.get("current_job") or node.get("reserved_for_job")
    current_function = job_id.rsplit("-", 1)[0] if job_id else None
    return {
        "nodeId": node.get("instance_name"),
        "status": node.get("status"),
        "type": node.get("machine_type"),
        "started_booting_at": _to_epoch_ms(node.get("started_booting_at")),
        "current_function": current_function,
    }


@router.get("/v1/cluster")
async def cluster_info(request: Request, logger: Logger = Depends(get_logger)):
    _require_auth(request)

    async def node_stream():
        queue = cluster_state.subscribe_node_events()
        stream_started_at = time()
        try:
            yield "retry: 5000\n\n"
            yield ": init\n\n"

            active_statuses = ("BOOTING", "READY", "RUNNING")
            current = [n for n in cluster_state.list_nodes() if n.get("status") in active_statuses]
            if not current:
                yield f"data: {json.dumps({'type': 'empty'})}\n\n"
            for node in current:
                yield f"data: {json.dumps(_node_event(node))}\n\n"

            while time() - stream_started_at < SSE_MAX_DURATION_SEC:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=2)
                    yield f"data: {json.dumps(_node_event(event))}\n\n"
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            cluster_state.unsubscribe(queue)

    return StreamingResponse(
        node_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache, no-transform"},
    )


@router.delete("/v1/cluster/{node_id}")
def delete_node(
    node_id: str,
    request: Request,
    logger: Logger = Depends(get_logger),
):
    auth_headers = _require_auth(request)

    node_dict = cluster_state.get_node(node_id)
    if node_dict is None:
        raise HTTPException(status_code=404, detail="node not found")
    node = Node.from_state(logger, node_dict, auth_headers)
    # Threaded (not a BackgroundTask) so slow VM deletion can't delay the response.
    import threading

    threading.Thread(target=node.delete, daemon=True).start()


@router.get("/v1/cluster/{node_id}/logs")
async def node_log_stream(node_id: str, request: Request):
    _require_auth(request)

    timezone_name = request.cookies.get("timezone", "UTC")
    try:
        tz = pytz.timezone(timezone_name)
    except Exception:
        tz = pytz.timezone("UTC")

    def ts_to_str(timestamp: float) -> str:
        return f"[{datetime.fromtimestamp(timestamp, tz).strftime('%I:%M %p').lstrip('0')}]"

    last_date_str = None
    first_log_processed = False

    def format_log(timestamp: float, message_raw: str) -> list[dict]:
        nonlocal last_date_str, first_log_processed
        events = []

        current_date_str = datetime.fromtimestamp(timestamp, tz).strftime("%B %d, %Y (%Z)")
        if not first_log_processed or current_date_str != last_date_str:
            padding_size = max(0, (120 - 2 - len(current_date_str)) // 2)
            message = f"{'-' * padding_size} {current_date_str} {'-' * padding_size}"
            events.append({"message": message})
            last_date_str = current_date_str
            first_log_processed = True

        timestamp_str = ts_to_str(timestamp)
        message_raw = str(message_raw or "").rstrip()
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

        events.append({"message": "\n".join(formatted_lines)})
        return events

    async def log_generator():
        queue = cluster_state.subscribe_node_logs(node_id)
        stream_started_at = time()
        try:
            yield "retry: 5000\n\n"
            yield ": init\n\n"

            stored_logs = await asyncio.to_thread(history.node_logs_after, node_id, 0)
            replayed_ts = None
            for _, ts, msg in stored_logs:
                if ts is None:
                    continue
                for event in format_log(ts, msg):
                    yield f"data: {json.dumps(event)}\n\n"
                replayed_ts = ts

            while time() - stream_started_at < SSE_MAX_DURATION_SEC:
                try:
                    log = await asyncio.wait_for(queue.get(), timeout=2)
                    timestamp = log.get("ts")
                    if timestamp is None:
                        continue
                    # Live events published while we were replaying history
                    # would otherwise appear twice.
                    if replayed_ts is not None and timestamp <= replayed_ts:
                        continue
                    for event in format_log(timestamp, log.get("msg")):
                        yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            cluster_state.unsubscribe(queue)

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

    cutoff_sec = (datetime.utcnow() - timedelta(days=7)).timestamp()
    stored = history.ended_nodes_page(("DELETED", "FAILED"), cutoff_sec)

    # FAILED nodes are still in live state too (kept visible for debugging);
    # prefer the live entry so status flips show immediately.
    live_failed = [n for n in cluster_state.list_nodes() if n.get("status") == "FAILED"]
    seen = {n.get("instance_name") for n in live_failed}
    all_nodes = live_failed + [n for n in stored if n.get("instance_name") not in seen]

    nodes = []
    for data in all_nodes:
        ended_ms = _to_epoch_ms(data.get("ended_at"))
        started_ms = _to_epoch_ms(data.get("started_booting_at"))
        sort_ms = ended_ms or started_ms or 0
        if sort_ms < cutoff_sec * 1000:
            continue
        nodes.append(
            {
                "id": data.get("instance_name"),
                "name": data.get("instance_name"),
                "status": data.get("status"),
                "type": data.get("machine_type"),
                "cpus": data.get("num_cpus"),
                "gpus": data.get("num_gpus"),
                "memory": data.get("memory"),
                "deletedAt": ended_ms if ended_ms is not None else sort_ms,
                "started_booting_at": started_ms,
                "_sort_ms": sort_ms,
            }
        )

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
            "cutoff_days": 7,
        },
    }
