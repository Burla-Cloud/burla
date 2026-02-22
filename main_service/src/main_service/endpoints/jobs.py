import json
import asyncio
from time import time
from datetime import datetime, timezone
from typing import Optional, Any, Iterable

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


def _ts_to_seconds(ts_val: Any, fallback_ts: Any = None) -> int:
    v = ts_val if ts_val is not None else fallback_ts
    if v is None:
        return 0

    try:
        return int(v.timestamp())
    except Exception:
        pass

    if isinstance(v, (int, float)):
        return int(v)

    if isinstance(v, str):
        try:
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
            return int(dt.timestamp())
        except Exception:
            return 0

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
            event["n_results"] = n_results
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
                event_["n_results"] = n_results
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


@router.get("/v1/jobs/{job_id}/logs")
async def stream_or_fetch_job_logs(
    job_id: str,
    stream: bool = False,
    index: Optional[int] = None,
    index_start: Optional[int] = None,
    index_end: Optional[int] = None,
    include_global: bool = True,
    summary: bool = False,
    limit: int = 5000,
    limit_per_index: int = 5000,
):
    ascending_logs_ref = (
        ASYNC_DB.collection("jobs").document(job_id).collection("logs").order_by("timestamp")
    )

    def _matches_single(idx: Optional[int]) -> bool:
        if index is None:
            return True
        return idx == index

    def _range_mode_active() -> bool:
        return index_start is not None or index_end is not None

    def _validate_range() -> tuple[int, int]:
        if index_start is None or index_end is None:
            raise HTTPException(
                status_code=400, detail="index_start and index_end must both be provided"
            )
        try:
            s = int(index_start)
            e = int(index_end)
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=400, detail="index_start and index_end must be integers"
            )
        if s < 0 or e < 0:
            raise HTTPException(status_code=400, detail="index_start and index_end must be >= 0")
        if e < s:
            raise HTTPException(status_code=400, detail="index_end must be >= index_start")
        if (e - s + 1) > 500:
            raise HTTPException(status_code=400, detail="Range too large (max 500 indexes)")
        return s, e

    if summary and not stream:
        first_error_by_index: dict[int, int] = {}
        seen_indexes: set[int] = set()

        async for doc in ascending_logs_ref.stream():
            d = doc.to_dict() or {}
            fallback_doc_ts = d.get("timestamp")

            for log in d["logs"]:
                idx = int(log["input_index"])
                seen_indexes.add(idx)

                is_err = bool(log.get("is_error", False))
                if is_err:
                    ts = _ts_to_seconds(log.get("timestamp"), fallback_doc_ts)
                    prev = first_error_by_index.get(idx)
                    if prev is None or ts < prev:
                        first_error_by_index[idx] = ts

        failed_indexes = [i for i, _ in sorted(first_error_by_index.items(), key=lambda kv: kv[1])]
        return JSONResponse(
            {"failed_indexes": failed_indexes, "seen_indexes": sorted(seen_indexes)}
        )

    if stream:
        logs_ref = ascending_logs_ref
        if _range_mode_active():
            raise HTTPException(
                status_code=400, detail="Range loading is not supported in stream mode"
            )

        queue: asyncio.Queue = asyncio.Queue()
        current_loop = asyncio.get_running_loop()
        skip_first_snapshot = True

        def on_snapshot(col_snapshot, changes, read_time):
            nonlocal skip_first_snapshot
            if skip_first_snapshot:
                skip_first_snapshot = False
                return

            def sort_key(change):
                d = change.document.to_dict() or {}
                fallback_doc_ts = d.get("timestamp")
                first = None
                for lg in d["logs"]:
                    first = lg
                    break
                if not first:
                    return datetime.min.replace(tzinfo=timezone.utc)
                ts_seconds = _ts_to_seconds(first.get("timestamp"), fallback_doc_ts)
                return datetime.fromtimestamp(ts_seconds, tz=timezone.utc)

            for change in sorted(changes, key=sort_key):
                d = change.document.to_dict() or {}
                doc_id = change.document.id
                fallback_doc_ts = d.get("timestamp")

                for i, log in enumerate(d["logs"]):
                    idx = int(log["input_index"])
                    if not _matches_single(idx):
                        continue

                    event = {
                        "id": f"{doc_id}:{i}",
                        "message": log.get("message"),
                        "created_at": _ts_to_seconds(log.get("timestamp"), fallback_doc_ts),
                        "input_index": idx,
                        "is_error": bool(log.get("is_error", False)),
                    }
                    current_loop.call_soon_threadsafe(queue.put_nowait, event)

        logs_ref_sync = (
            DB.collection("jobs").document(job_id).collection("logs").order_by("timestamp")
        )
        watcher = logs_ref_sync.on_snapshot(on_snapshot)

        async def event_stream():
            try:
                yield "retry: 5000\n\n"
                yield ": init\n\n"
                while True:
                    try:
                        event = await asyncio.wait_for(queue.get(), timeout=2)
                        print(event)
                        yield f"data: {json.dumps(event)}\n\n"
                    except asyncio.TimeoutError:
                        yield ": keep-alive\n\n"
            finally:
                watcher.unsubscribe()

        headers = {"Cache-Control": "no-cache, no-transform"}
        return StreamingResponse(event_stream(), media_type="text/event-stream", headers=headers)

    descending_logs_ref = (
        ASYNC_DB.collection("jobs")
        .document(job_id)
        .collection("logs")
        .order_by("timestamp", direction=firestore.Query.DESCENDING)
    )

    if _range_mode_active():
        s, e = _validate_range()

        logs_by_index: dict[int, list] = {i: [] for i in range(s, e + 1)}
        global_logs: list = []
        per_index_counts: dict[int, int] = {i: 0 for i in range(s, e + 1)}
        global_count = 0

        truncated_indexes: set[int] = set()
        truncated_global = False
        truncated_total = False
        indexes_still_collecting: set[int] = set(range(s, e + 1))

        max_total = 200_000
        total_added = 0

        async for doc in descending_logs_ref.stream():
            d = doc.to_dict() or {}
            doc_id = doc.id
            fallback_doc_ts = d.get("timestamp")

            for i, log in enumerate(d["logs"]):
                if total_added >= max_total:
                    truncated_total = True
                    break

                idx = int(log["input_index"])
                ts = _ts_to_seconds(log.get("timestamp"), fallback_doc_ts)

                payload = {
                    "id": f"{doc_id}:{i}",
                    "message": log.get("message"),
                    "created_at": ts,
                    "input_index": idx,
                    "is_error": bool(log.get("is_error", False)),
                }

                if idx < s or idx > e:
                    continue

                if per_index_counts[idx] >= limit_per_index:
                    truncated_indexes.add(idx)
                    if idx in indexes_still_collecting:
                        indexes_still_collecting.remove(idx)
                    continue

                logs_by_index[idx].append(payload)
                per_index_counts[idx] += 1
                total_added += 1

                if per_index_counts[idx] >= limit_per_index and idx in indexes_still_collecting:
                    indexes_still_collecting.remove(idx)

            if truncated_total:
                break

            if not include_global and not indexes_still_collecting:
                break

        for idx in range(s, e + 1):
            logs_by_index[idx].sort(key=lambda x: x["created_at"])
        global_logs.sort(key=lambda x: x["created_at"])
        return JSONResponse(
            {
                "index_start": s,
                "index_end": e,
                "limit_per_index": limit_per_index,
                "include_global": include_global,
                "global_logs": global_logs,
                "logs_by_index": {str(k): v for k, v in logs_by_index.items()},
                "truncated": bool(truncated_indexes) or truncated_global or truncated_total,
                "truncated_indexes": sorted(list(truncated_indexes)),
                "truncated_global": truncated_global,
                "truncated_total": truncated_total,
            }
        )

    # TODO: I am 90% sure that all code below here is never used in any scenario.

    logs = []
    truncated = False

    async for doc in descending_logs_ref.stream():
        d = doc.to_dict() or {}
        doc_id = doc.id
        fallback_doc_ts = d.get("timestamp")

        for i, log in enumerate(d["logs"]):
            idx = int(log["input_index"])
            if not _matches_single(idx):
                continue

            logs.append(
                {
                    "id": f"{doc_id}:{i}",
                    "message": log.get("message"),
                    "created_at": _ts_to_seconds(log.get("timestamp"), fallback_doc_ts),
                    "input_index": idx,
                    "is_error": bool(log.get("is_error", False)),
                }
            )

            if len(logs) >= limit:
                truncated = True
                break

        if truncated:
            break

    logs.sort(key=lambda x: x["created_at"])
    return JSONResponse(
        {"logs": logs, "input_index": index, "limit": limit, "truncated": truncated}
    )
