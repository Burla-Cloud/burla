import json
import asyncio

from fastapi import APIRouter, Request
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
            print(f"pushing new n results: {job_id}: {n_results}")
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
        job = job_doc.to_dict()
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
async def stream_or_fetch_job_logs(
    job_id: str, stream: bool = False, page: int = 0, limit: int = 1000
):
    if not stream:
        # Non-streaming JSON mode for fetching large chunks at once
        logs_sync_ref = DB.collection("jobs").document(job_id).collection("logs")
        # Firestore orders by created_at ascending for natural reading order
        query = logs_sync_ref.order_by("created_at").offset(page * limit).limit(limit)
        items = []
        for doc in query.stream():
            d = doc.to_dict() or {}
            created_at = d.get("created_at")
            try:
                created_at = float(created_at.timestamp())  # timestamp from Firestore Timestamp
            except Exception:
                try:
                    # already a number
                    created_at = float(created_at)
                except Exception:
                    created_at = None
            items.append(
                {
                    "id": doc.id,
                    "message": d.get("msg"),
                    "created_at": created_at,
                }
            )

        # Total count
        total_res = (
            await ASYNC_DB.collection("jobs").document(job_id).collection("logs").count().get()
        )
        total = total_res[0][0].value
        return JSONResponse({"logs": items, "page": page, "limit": limit, "total": total})

    # Streaming (SSE) mode for live updates only
    queue = asyncio.Queue()
    current_loop = asyncio.get_running_loop()
    skip_first_snapshot = True

    def on_snapshot(col_snapshot, changes, read_time):
        def to_ts(value):
            if value is None:
                return None
            if isinstance(value, (int, float)):
                return float(value)
            try:
                return value.timestamp()
            except Exception:
                return None

        nonlocal skip_first_snapshot
        if skip_first_snapshot:
            # Ignore the initial on_snapshot flood (ADDED for all existing docs)
            skip_first_snapshot = False
            return

        # Stream incremental changes as single events
        sorted_changes = sorted(
            changes,
            key=lambda change: to_ts(change.document.to_dict().get("created_at")) or 0.0,
        )

        for change in sorted_changes:
            data = change.document.to_dict() or {}
            created_at_val = data.get("created_at")
            created_at_ts = to_ts(created_at_val)
            event = {
                "id": change.document.id,
                "message": data.get("msg"),
                "created_at": created_at_ts,
            }
            current_loop.call_soon_threadsafe(queue.put_nowait, event)

    logs_ref = DB.collection("jobs").document(job_id).collection("logs")
    watch = logs_ref.on_snapshot(on_snapshot)

    async def event_stream():
        try:
            yield "retry: 5000\n\n"
            yield ": init\n\n"
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=2)
                    yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            watch.unsubscribe()

    headers = {"Cache-Control": "no-cache, no-transform"}
    return StreamingResponse(event_stream(), media_type="text/event-stream", headers=headers)
