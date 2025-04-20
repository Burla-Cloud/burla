import json
import requests
import asyncio
from datetime import datetime, timezone
from typing import Optional, Callable

from fastapi import APIRouter, Path, Depends, Query, Request
from fastapi.responses import JSONResponse
from starlette.responses import StreamingResponse
from google.cloud import firestore
from google.cloud.firestore import FieldFilter

from main_service import DB, get_logger, get_add_background_task_function
from main_service.helpers import Logger

router = APIRouter()


@router.get("/v1/jobs/{job_id}")
def run_job_healthcheck(
    job_id: str = Path(...),
    logger: Logger = Depends(get_logger),
    add_background_task: Callable = Depends(get_add_background_task_function),
):
    # get all nodes working on this job
    _filter = FieldFilter("current_job", "==", job_id)
    nodes_with_job = [n.to_dict() for n in DB.collection("nodes").where(filter=_filter).stream()]

    # check status of every node / worker working on this job
    for node in nodes_with_job:
        response = requests.get(f"{node['host']}/jobs/{job_id}")
        response.raise_for_status()
        if response.json()["any_workers_failed"]:
            raise Exception(f"Worker failed. Check logs for node {node['instance_name']}")


@router.get("/v1/jobs_paginated")
async def get_recent_jobs(request: Request, page: int = 0, stream: bool = False):
    limit = 15
    offset = page * limit

    # If `stream=true` or `Accept: text/event-stream`, serve SSE
    if stream or request.headers.get("accept") == "text/event-stream":
        queue = asyncio.Queue()
        current_loop = asyncio.get_running_loop()

        def on_snapshot(col_snapshot, changes, read_time):
            for change in changes:
                doc = change.document
                doc_data = doc.to_dict() or {}

                event_data = {
                    "jobId": doc.id,
                    "status": doc_data.get("status"),
                    "user": doc_data.get("user", "Unknown"),
                    "n_inputs": doc_data.get("n_inputs", 0),
                    "started_at": doc_data.get("started_at"),
                    "deleted": change.type.name == "REMOVED",
                }

                current_loop.call_soon_threadsafe(queue.put_nowait, event_data)

        jobs_query = DB.collection("jobs").order_by("started_at", direction="DESCENDING")
        unsubscribe = jobs_query.on_snapshot(on_snapshot)

        async def event_stream():
            try:
                while True:
                    event = await queue.get()
                    yield f"data: {json.dumps(event)}\n\n"
            finally:
                unsubscribe.unsubscribe()

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    # Otherwise return paginated JSON
    jobs_query = (
        DB.collection("jobs")
        .order_by("started_at", direction="DESCENDING")
        .offset(offset)
        .limit(limit)
    )

    jobs = []
    for doc in jobs_query.stream():
        data = doc.to_dict()
        jobs.append(
            {
                "jobId": doc.id,
                "status": data.get("status"),
                "n_inputs": data.get("n_inputs", 0),
                "user": data.get("user", "Unknown"),
                "started_at": data.get("started_at"),
            }
        )

    total = len([doc.id for doc in DB.collection("jobs").stream()])

    return JSONResponse({"jobs": jobs, "page": page, "limit": limit, "total": total})


@router.get("/v1/job_logs/{job_id}/paginated")
def get_paginated_logs(
    job_id: str,
    limit: int = Query(10, ge=1, le=1000),
    start_after_time: Optional[float] = Query(None),
    start_after_id: Optional[str] = Query(None),
):
    logs_ref = (
        DB.collection("jobs")
        .document(job_id)
        .collection("logs")
        .order_by("created_at", direction=firestore.Query.DESCENDING)
        .order_by("__name__", direction=firestore.Query.DESCENDING)
    )

    if start_after_time is not None and start_after_id:
        ts = datetime.fromtimestamp(start_after_time, tz=timezone.utc)
        logs_ref = logs_ref.start_after({"created_at": ts, "__name__": start_after_id})

    docs = list(logs_ref.limit(limit).stream())

    logs = []
    for doc in docs:
        data = doc.to_dict()
        created_at = data.get("created_at")
        if isinstance(created_at, float):
            created_at = datetime.fromtimestamp(created_at, tz=timezone.utc)
        logs.append(
            {
                "id": doc.id,
                "msg": data.get("msg"),
                "time": created_at.timestamp() if created_at else None,
            }
        )

    next_cursor = None
    if len(docs) == limit:
        last = docs[-1]
        last_data = last.to_dict()
        last_created_at = last_data.get("created_at")
        if isinstance(last_created_at, float):
            last_created_at = datetime.fromtimestamp(last_created_at, tz=timezone.utc)
        if last_created_at:
            next_cursor = {
                "start_after_time": last_created_at.timestamp(),
                "start_after_id": last.id,
            }

    return {
        "logs": logs,
        "limit": limit,
        "job_id": job_id,
        "nextCursor": next_cursor,
    }
