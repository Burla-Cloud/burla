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


def job_stream(jobs_current_page: firestore.CollectionReference):
    queue = asyncio.Queue()
    loop = asyncio.get_running_loop()
    running_jobs = {}

    async def push_n_results_updates(job_id: str, event: dict):
        while True:
            n_results = await current_num_results(job_id)
            print(f"here: {job_id}: {n_results}")
            event["n_results"] = n_results
            await queue.put(event)
            await asyncio.sleep(1)

    def on_changed_job_doc(col_snapshot, changes, read_time):
        for change in changes:
            job = change.document.to_dict()
            event = {
                "jobId": change.document.id,
                "status": job.get("status"),
                "user": job.get("user", "Unknown"),
                "function_name": job.get("function_name", "Unknown"),
                "n_inputs": job.get("n_inputs", 0),
                "n_results": None,
                "started_at": job.get("started_at"),
                "deleted": change.type.name == "REMOVED",
            }

            if (change.document.id in running_jobs) and (job.get("status") != "RUNNING"):
                running_jobs.pop(change.document.id).cancel()
            elif (change.document.id not in running_jobs) and (job.get("status") == "RUNNING"):
                coroutine = push_n_results_updates(change.document.id, event)
                future = asyncio.run_coroutine_threadsafe(coroutine, loop)
                running_jobs[change.document.id] = future

            async def queue_job(job_id: str, event: dict):
                # I cant figure out how to `current_num_results` synchronously
                n_results = await current_num_results(job_id)
                event["n_results"] = n_results
                await queue.put(event)

            coroutine = queue_job(change.document.id, event)
            asyncio.run_coroutine_threadsafe(coroutine, loop)

    job_collection_stream = jobs_current_page.on_snapshot(on_changed_job_doc)

    async def event_stream():
        yield "retry: 5000\n\n"  # <- make browser reconnect after 5s on error
        yield ": init\n\n"
        try:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=10)
                    yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    # heartbeat to keeps proxy from closing connection
                    yield ": keep-alive\n\n"
        finally:
            job_collection_stream.unsubscribe()

    headers = {"Cache-Control": "no-cache, no-transform"}
    return StreamingResponse(event_stream(), media_type="text/event-stream", headers=headers)


@router.get("/v1/jobs")
async def get_jobs(request: Request, page: int = 0, stream: bool = False):
    docs_per_page = 15
    _jobs_ref = DB.collection("jobs").order_by("started_at", direction=firestore.Query.DESCENDING)
    jobs_current_page = _jobs_ref.offset(page * docs_per_page).limit(docs_per_page)
    if stream:
        return job_stream(jobs_current_page)

    jobs = []
    for job_doc in jobs_current_page.stream():
        job = job_doc.to_dict() or {}
        jobs.append(
            {
                "jobId": job_doc.id,
                "status": job.get("status"),
                "user": job.get("user", "Unknown"),
                "function_name": job.get("function_name", "Unknown"),
                "n_inputs": job.get("n_inputs", 0),
                "n_results": await current_num_results(job_doc.id),
                "started_at": job.get("started_at"),
            }
        )
    total_jobs = await ASYNC_DB.collection("jobs").count().get()
    total_jobs = total_jobs[0][0].value
    return JSONResponse({"jobs": jobs, "page": page, "limit": docs_per_page, "total": total_jobs})


@router.get("/v1/jobs/{job_id}/logs")
def get_job_logs(
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
