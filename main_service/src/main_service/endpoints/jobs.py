import json
import asyncio
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse
from starlette.responses import StreamingResponse
from google.cloud import firestore

from main_service import DB, PROJECT_ID

router = APIRouter()


ASYNC_DB = firestore.AsyncClient(project=PROJECT_ID, database="burla")


async def current_num_results(job_id: str):
    job_doc = ASYNC_DB.collection("jobs").document(job_id)
    assigned_nodes_collection = job_doc.collection("assigned_nodes")
    query_result = await assigned_nodes_collection.sum("current_num_results").get()
    return query_result[0][0].value


@router.get("/v1/jobs_paginated")
async def get_recent_jobs(request: Request, page: int = 0, stream: bool = False):
    docs_per_page = 15
    offset = page * docs_per_page
    accept = request.headers.get("accept", "")

    page_one_docs = list(
        DB.collection("jobs")
        .order_by("started_at", direction=firestore.Query.DESCENDING)
        .offset(offset)
        .limit(docs_per_page)
        .stream()
    )

    if stream or "text/event-stream" in accept:
        queue = asyncio.Queue()
        loop = asyncio.get_running_loop()

        if not page_one_docs:
            return StreamingResponse(iter([]), media_type="text/event-stream")

        def on_snapshot(col_snapshot, changes, read_time):
            for change in changes:
                doc = change.document
                data = doc.to_dict() or {}

                ts = data.get("started_at")
                if hasattr(ts, "timestamp"):
                    ts = ts.timestamp()

                async def build_event_and_put():
                    # I cant figure out how to `current_num_results` synchronously
                    event = {
                        "jobId": doc.id,
                        "status": data.get("status"),
                        "user": data.get("user", "Unknown"),
                        "function_name": data.get("function_name", "Unknown"),
                        "n_inputs": data.get("n_inputs", 0),
                        "n_results": await current_num_results(doc.id),
                        "started_at": ts,
                        "deleted": change.type.name == "REMOVED",
                    }
                    await queue.put(event)

                asyncio.run_coroutine_threadsafe(build_event_and_put(), loop)

        unsubscribe = DB.collection("jobs").on_snapshot(on_snapshot)

        async def event_stream():
            try:
                while True:
                    yield f"data: {json.dumps(await queue.get())}\n\n"
            finally:
                unsubscribe.unsubscribe()

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    # --- fallback for non-stream requests ---
    jobs = []
    for doc in page_one_docs:
        d = doc.to_dict() or {}
        ts = d.get("started_at")
        if hasattr(ts, "timestamp"):
            ts = ts.timestamp()
        jobs.append(
            {
                "jobId": doc.id,
                "status": d.get("status"),
                "user": d.get("user", "Unknown"),
                "function_name": d.get("function_name", "Unknown"),
                "n_inputs": d.get("n_inputs", 0),
                "n_results": await current_num_results(doc.id),
                "started_at": ts,
            }
        )

    total_jobs = await ASYNC_DB.collection("jobs").count().get()
    total_jobs = total_jobs[0][0].value

    return JSONResponse({"jobs": jobs, "page": page, "limit": docs_per_page, "total": total_jobs})


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
