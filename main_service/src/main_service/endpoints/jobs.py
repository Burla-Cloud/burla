import json
import asyncio
from time import time
from datetime import datetime, timezone
from typing import Optional, Iterable

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from starlette.responses import StreamingResponse

from google.cloud import firestore
from google.cloud.firestore import ArrayUnion

from main_service import DB, PROJECT_ID

router = APIRouter()
ASYNC_DB = firestore.AsyncClient(project=PROJECT_ID, database="burla")


async def current_num_results(job_id: str) -> int:
    job_doc = ASYNC_DB.collection("jobs").document(job_id)
    assigned_nodes_collection = job_doc.collection("assigned_nodes")
    query_result = await assigned_nodes_collection.sum("current_num_results").get()
    try:
        return int(query_result[0][0].value or 0)
    except Exception:
        return 0


async def current_num_failed(job_id: str) -> int:
    job_doc = ASYNC_DB.collection("jobs").document(job_id)
    logs_collection = job_doc.collection("logs")
    error_documents_query = logs_collection.where(
        filter=firestore.FieldFilter("is_error", "==", True)
    )
    query_result = await error_documents_query.count().get()
    try:
        return int(query_result[0][0].value or 0)
    except Exception:
        return 0


def job_stream(jobs_current_page: firestore.CollectionReference):
    queue = asyncio.Queue()
    loop = asyncio.get_running_loop()
    running_jobs: dict[str, asyncio.Future] = {}

    async def push_n_results_updates(job_id: str, event: dict):
        start = time()
        last_check = 0

        while True:
            n_results = await current_num_results(job_id)
            n_failed = await current_num_failed(job_id)
            event["n_results"] = n_results
            event["n_failed"] = n_failed
            await queue.put(event)
            await asyncio.sleep(1)

            elapsed = int(time() - start)
            if elapsed - last_check >= 10:
                last_check = elapsed
                filter_ = firestore.FieldFilter("current_job", "==", job_id)
                nodes = ASYNC_DB.collection("nodes").where(filter=filter_)
                nodes_working_on_job = await nodes.get()

                if not nodes_working_on_job and elapsed > 300:
                    msg = "Job failed due to internal cluster error."
                    timestamp = datetime.now(timezone.utc)
                    logs = [{"message": msg, "timestamp": timestamp}]

                    job_doc = ASYNC_DB.collection("jobs").document(job_id)
                    await job_doc.collection("logs").add({"logs": logs, "timestamp": timestamp})

                    msg2 = 'main_svc: job is "running" but no nodes working on it ???'
                    await job_doc.update({"status": "FAILED", "fail_reason": ArrayUnion([msg2])})
                    return

    def on_changed_job_doc(col_snapshot, changes, read_time):
        for change in changes:
            job_id = change.document.id
            job = change.document.to_dict() or {}

            event = {
                "jobId": job_id,
                "status": job.get("status"),
                "user": job.get("user", "Unknown"),
                "function_name": job.get("function_name", "Unknown"),
                "n_inputs": job.get("n_inputs", 0),
                "n_results": None,
                "n_failed": None,
                "started_at": job.get("started_at"),
                "deleted": change.type.name == "REMOVED",
            }

            if change.type.name == "REMOVED":
                if job_id in running_jobs:
                    running_jobs[job_id].cancel()
                    del running_jobs[job_id]
            else:
                if (job_id in running_jobs) and (job.get("status") != "RUNNING"):
                    running_jobs[job_id].cancel()
                    del running_jobs[job_id]
                elif (job_id not in running_jobs) and (job.get("status") == "RUNNING"):
                    coroutine = push_n_results_updates(job_id, event)
                    future = asyncio.run_coroutine_threadsafe(coroutine, loop)
                    running_jobs[job_id] = future

            async def queue_job(job_id_: str, event_: dict):
                n_results = await current_num_results(job_id_)
                n_failed = await current_num_failed(job_id_)
                event_["n_results"] = n_results
                event_["n_failed"] = n_failed
                await queue.put(event_)

            asyncio.run_coroutine_threadsafe(queue_job(job_id, event), loop)

    job_collection_stream = jobs_current_page.on_snapshot(on_changed_job_doc)

    async def event_stream():
        yield "retry: 5000\n\n"
        yield ": init\n\n"
        try:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=10)
                    yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
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
                "n_failed": await current_num_failed(job_doc.id),
                "started_at": job.get("started_at"),
            }
        )

    total_jobs = await ASYNC_DB.collection("jobs").count().get()
    total_jobs = total_jobs[0][0].value
    return JSONResponse({"jobs": jobs, "page": page, "limit": docs_per_page, "total": total_jobs})


@router.post("/v1/jobs/{job_id}/stop")
async def stop_job(job_id: str, request: Request):
    email = request.session.get("X-User-Email") or request.headers.get("X-User-Email")
    msg = f"Job canceled by user: {email}"
    timestamp = datetime.now(timezone.utc)
    logs = [{"is_error": True, "message": msg, "timestamp": timestamp}]
    job_doc = ASYNC_DB.collection("jobs").document(job_id)
    await job_doc.collection("logs").add({"logs": logs, "timestamp": timestamp})
    await job_doc.update({"status": "CANCELED"})


@router.get("/v1/jobs/{job_id}/result-stats")
async def get_job_result_stats(job_id: str):
    job_doc_ref = ASYNC_DB.collection("jobs").document(job_id)
    job_doc = await job_doc_ref.get()
    if not job_doc.exists:
        raise HTTPException(status_code=404, detail="Job not found")

    job = job_doc.to_dict() or {}
    n_inputs = int(job.get("n_inputs", 0) or 0)
    n_results = await current_num_results(job_id)
    n_failed = await current_num_failed(job_id)

    return JSONResponse(
        {
            "job_id": job_id,
            "n_inputs": n_inputs,
            "n_results": n_results,
            "n_failed": n_failed,
        }
    )


@router.get("/v1/jobs/{job_id}/logged-input-indexes")
async def get_logged_input_indexes(job_id: str):
    logs_collection = ASYNC_DB.collection("jobs").document(job_id).collection("logs")

    indexes_with_logs = set()
    failed_indexes = set()

    async for log_document in logs_collection.stream():
        payload = log_document.to_dict() or {}
        input_index = payload.get("input_index")
        if input_index is None:
            continue

        normalized_index = int(input_index)
        indexes_with_logs.add(normalized_index)
        if payload.get("is_error") is True:
            failed_indexes.add(normalized_index)

    non_failed_indexes_with_logs = sorted(indexes_with_logs - failed_indexes)

    return JSONResponse(
        {
            "indexes_with_logs": sorted(indexes_with_logs),
            "failed_indexes": sorted(failed_indexes),
            "non_failed_indexes_with_logs": non_failed_indexes_with_logs,
        }
    )


@router.get("/v1/jobs/{job_id}/next-failed-input")
async def get_next_failed_input_index(
    job_id: str,
    index: int,
):
    current_input_index = int(index)
    failed_documents = (
        ASYNC_DB.collection("jobs")
        .document(job_id)
        .collection("logs")
        .where(filter=firestore.FieldFilter("is_error", "==", True))
    )

    failed_input_indexes = set()
    async for failed_document in failed_documents.stream():
        failed_input_index = int(failed_document.to_dict()["input_index"])
        failed_input_indexes.add(failed_input_index)

    ordered_failed_indexes = sorted(failed_input_indexes)
    first_failed_input_index = ordered_failed_indexes[0] if ordered_failed_indexes else None
    next_failed_input_index = None
    for failed_input_index in ordered_failed_indexes:
        if failed_input_index > current_input_index:
            next_failed_input_index = failed_input_index
            break

    return JSONResponse(
        {
            "next_failed_input_index": (
                next_failed_input_index
                if next_failed_input_index is not None
                else first_failed_input_index
            ),
            "failed_input_indexes": ordered_failed_indexes,
        }
    )


@router.get("/v1/jobs/{job_id}/logs")
async def stream_or_fetch_job_logs(
    job_id: str,
    index: int,
    oldest_timestamp: Optional[str] = None,
):
    max_logs_per_response = 500
    logs_collection = ASYNC_DB.collection("jobs").document(job_id).collection("logs")
    error_documents_query = logs_collection.where(
        filter=firestore.FieldFilter("is_error", "==", True)
    )

    failed_inputs_count_response = await error_documents_query.count().get()
    failed_inputs_count = int(failed_inputs_count_response[0][0].value or 0)

    oldest_requested_timestamp = float(oldest_timestamp) if oldest_timestamp else None

    logs_query = logs_collection.where(
        filter=firestore.FieldFilter("input_index", "==", int(index))
    )
    matching_logs = []
    async for doc in logs_query.stream():
        log_document = doc.to_dict() or {}
        is_error_document = bool(log_document.get("is_error"))
        for log in log_document.get("logs", []):
            timestamp_value = log.get("timestamp")
            if not timestamp_value:
                continue
            log_timestamp = (
                timestamp_value.timestamp()
                if hasattr(timestamp_value, "timestamp")
                else float(timestamp_value)
            )
            if (
                oldest_requested_timestamp is not None
                and log_timestamp >= oldest_requested_timestamp
            ):
                continue
            matching_logs.append(
                {
                    "message": log.get("message", ""),
                    "log_timestamp": log_timestamp,
                    "is_error": bool(log.get("is_error", False) or is_error_document),
                }
            )

    matching_logs.sort(key=lambda entry: entry["log_timestamp"], reverse=True)
    has_more_older = len(matching_logs) > max_logs_per_response
    newest_window_desc = matching_logs[:max_logs_per_response]
    logs = sorted(newest_window_desc, key=lambda entry: entry["log_timestamp"])
    oldest_returned_log_timestamp = (
        min(entry["log_timestamp"] for entry in newest_window_desc)
        if newest_window_desc
        else None
    )

    return JSONResponse(
        {
            "logs": logs,
            "input_index": int(index),
            "log_document_timestamp": oldest_returned_log_timestamp,
            "truncated": has_more_older,
            "has_more_older": has_more_older,
            "failed_inputs_count": failed_inputs_count,
        }
    )
 
 
