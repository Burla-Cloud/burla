import json
import asyncio
from time import time
from typing import Optional

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from starlette.responses import StreamingResponse

from main_service import cluster_state, history

router = APIRouter()

# Browsers reconnect via EventSource `retry:`; recycling the stream bounds how
# long a dead connection can hold a pub/sub queue open.
SSE_MAX_DURATION_SEC = 50
DOCS_PER_PAGE = 15


async def _job_summaries_page(page: int) -> tuple[list[dict], int]:
    """History is the source for the job list; live in-memory summaries are
    overlaid so RUNNING jobs show fresh n_results without waiting for a
    status transition to persist."""
    stored = await asyncio.to_thread(history.list_jobs, page * DOCS_PER_PAGE, DOCS_PER_PAGE)
    total = await asyncio.to_thread(history.count_jobs)
    jobs = []
    for job in stored:
        job_id = job["job_id"]
        live = cluster_state.job_summary(job_id)
        summary = live or {
            "status": job.get("status"),
            "user": job.get("user", "Unknown"),
            "function_name": job.get("function_name", "Unknown"),
            "n_inputs": job.get("n_inputs", 0),
            "started_at": job.get("started_at"),
        }
        # Live counts restart at 0 when a restarted head reloads a job
        # (per-node progress is memory-only), so history's count still wins.
        summary["n_results"] = max(
            int((live or {}).get("n_results") or 0), int(job.get("n_results") or 0)
        )
        n_failed = await asyncio.to_thread(history.job_error_count, job_id)
        jobs.append({"jobId": job_id, "n_failed": n_failed, **summary})
    return jobs, total


def job_stream(page: int):
    async def event_stream():
        queue = cluster_state.subscribe_job_events()
        stream_started_at = time()
        last_running_tick = 0.0
        try:
            yield "retry: 5000\n\n"
            yield ": init\n\n"

            jobs, _ = await _job_summaries_page(page)
            for job in jobs:
                yield f"data: {json.dumps(job)}\n\n"

            while time() - stream_started_at < SSE_MAX_DURATION_SEC:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=1)
                    job_id = event.pop("job_id")
                    n_failed = await asyncio.to_thread(history.job_error_count, job_id)
                    payload = {"jobId": job_id, "n_failed": n_failed, "deleted": False, **event}
                    yield f"data: {json.dumps(payload)}\n\n"
                except asyncio.TimeoutError:
                    pass

                # RUNNING jobs get a ~1s n_results refresh even without a
                # status event (progress pushes don't publish job events).
                if time() - last_running_tick >= 1:
                    last_running_tick = time()
                    for job_id in cluster_state.running_job_ids():
                        summary = cluster_state.job_summary(job_id)
                        if summary is None:
                            continue
                        n_failed = await asyncio.to_thread(history.job_error_count, job_id)
                        payload = {"jobId": job_id, "n_failed": n_failed, **summary}
                        yield f"data: {json.dumps(payload)}\n\n"
        finally:
            cluster_state.unsubscribe(queue)

    headers = {"Cache-Control": "no-cache, no-transform"}
    return StreamingResponse(event_stream(), media_type="text/event-stream", headers=headers)


@router.get("/v1/jobs")
async def get_jobs(request: Request, page: int = 0, stream: bool = False):
    if stream:
        return job_stream(page)

    jobs, total = await _job_summaries_page(page)
    return JSONResponse({"jobs": jobs, "page": page, "limit": DOCS_PER_PAGE, "total": total})


@router.post("/v1/jobs/{job_id}/stop")
async def stop_job(job_id: str, request: Request):
    email = request.session.get("X-User-Email") or request.headers.get("X-User-Email")
    msg = f"Job canceled by user: {email}"
    timestamp = time()
    logs = [{"is_error": True, "message": msg, "timestamp": timestamp}]
    # The log still exists for the dashboard's log view.
    await asyncio.to_thread(
        history.add_job_logs,
        job_id,
        [{"logs": logs, "timestamp": timestamp, "is_error": True}],
    )
    # `dashboard_canceled` is the signal the client reads: each node picks it
    # up from its next state-push response, caches it into SELF, the next
    # /results response returns it, and the client raises JobCanceled.
    cluster_state.update_job(job_id, {"status": "CANCELED", "dashboard_canceled": True})


@router.get("/v1/jobs/{job_id}/result-stats")
async def get_job_result_stats(job_id: str):
    """Counts plus the summary fields the job page needs, resolved from live
    state first and history second, so a deep link to any job id works even
    when the job is outside the paginated jobs list (and after head restarts,
    which reset live per-node result counts to 0)."""
    live = cluster_state.get_job(job_id)
    stored = await asyncio.to_thread(history.get_job, job_id)
    if live is None and stored is None:
        raise HTTPException(status_code=404, detail="Job not found")

    job = live or stored
    n_results = max(
        int((live or {}).get("n_results") or 0),
        int((stored or {}).get("n_results") or 0),
    )
    n_failed = await asyncio.to_thread(history.job_error_count, job_id)
    return JSONResponse(
        {
            "job_id": job_id,
            "n_inputs": int(job.get("n_inputs", 0) or 0),
            "n_results": n_results,
            "n_failed": n_failed,
            "status": job.get("status"),
            "user": job.get("user", "Unknown"),
            "function_name": job.get("function_name", "Unknown"),
            "started_at": job.get("started_at"),
        }
    )


@router.get("/v1/jobs/{job_id}/metrics")
async def get_job_metrics(job_id: str):
    return JSONResponse(await asyncio.to_thread(history.job_metrics_series, job_id))


@router.get("/v1/jobs/{job_id}/metrics/tasks/{input_index}")
async def get_task_metrics(job_id: str, input_index: int):
    series = await asyncio.to_thread(history.task_metrics_series, job_id, input_index)
    return JSONResponse(series)


@router.get("/v1/jobs/{job_id}/logged-input-indexes")
async def get_logged_input_indexes(job_id: str):
    indexes, failed = await asyncio.to_thread(history.job_logged_input_indexes, job_id)
    non_failed = sorted(set(indexes) - set(failed))
    return JSONResponse(
        {
            "indexes_with_logs": indexes,
            "failed_indexes": failed,
            "non_failed_indexes_with_logs": non_failed,
        }
    )


@router.get("/v1/jobs/{job_id}/next-failed-input")
async def get_next_failed_input_index(
    job_id: str,
    index: int,
):
    current_input_index = int(index)
    _, ordered_failed_indexes = await asyncio.to_thread(
        history.job_logged_input_indexes, job_id
    )

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
    failed_inputs_count = await asyncio.to_thread(history.job_error_count, job_id)
    oldest_requested_timestamp = float(oldest_timestamp) if oldest_timestamp else None

    matching_logs = await asyncio.to_thread(history.job_logs_for_input, job_id, int(index))
    if oldest_requested_timestamp is not None:
        matching_logs = [
            log for log in matching_logs if log["log_timestamp"] < oldest_requested_timestamp
        ]

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
