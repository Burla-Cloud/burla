# import json
# import asyncio
# from time import time
# from datetime import datetime, timezone

# from fastapi import APIRouter, Request
# from fastapi.responses import JSONResponse
# from starlette.responses import StreamingResponse
# from google.cloud import firestore
# from google.cloud.firestore import ArrayUnion

# from main_service import DB, PROJECT_ID

# router = APIRouter()
# ASYNC_DB = firestore.AsyncClient(project=PROJECT_ID, database="burla")


# async def current_num_results(job_id: str):
#     job_doc = ASYNC_DB.collection("jobs").document(job_id)
#     assigned_nodes_collection = job_doc.collection("assigned_nodes")
#     query_result = await assigned_nodes_collection.sum("current_num_results").get()
#     return query_result[0][0].value


# def job_stream(jobs_current_page: firestore.CollectionReference):
#     queue = asyncio.Queue()
#     loop = asyncio.get_running_loop()
#     running_jobs = {}

#     async def push_n_results_updates(job_id: str, event: dict):
#         start = time()
#         while True:
#             n_results = await current_num_results(job_id)
#             event["n_results"] = n_results
#             await queue.put(event)
#             await asyncio.sleep(1)
#             if (time() - start) % 10 < 1:
#                 # check job has nodes working on it
#                 filter_ = firestore.FieldFilter("current_job", "==", job_id)
#                 nodes = ASYNC_DB.collection("nodes").where(filter=filter_)
#                 nodes_working_on_job = await nodes.get()

#                 # I think this situation is possible when uploading really large functions to many
#                 # nodes, The timeout on that is 300s, so that's how long this has to  be true for
#                 # here to cause an error.
#                 if not nodes_working_on_job and (time() - start) > 300:
#                     msg = "Job failed due to internal cluster error."
#                     timestamp = datetime.now(timezone.utc)
#                     logs = [{"message": msg, "timestamp": timestamp}]
#                     job_doc = ASYNC_DB.collection("jobs").document(job_id)
#                     await job_doc.collection("logs").add({"logs": logs, "timestamp": timestamp})
#                     msg = 'main_svc: job is "running" but no nodes working on it ???'
#                     await job_doc.update({"status": "FAILED", "fail_reason": ArrayUnion([msg])})
#                     return

#     def on_changed_job_doc(col_snapshot, changes, read_time):
#         for change in changes:
#             job = change.document.to_dict()
#             event = {
#                 "jobId": change.document.id,
#                 "status": job.get("status"),
#                 "user": job.get("user", "Unknown"),
#                 "function_name": job.get("function_name", "Unknown"),
#                 "n_inputs": job.get("n_inputs", 0),
#                 "n_results": None,
#                 "started_at": job.get("started_at"),
#                 "deleted": change.type.name == "REMOVED",
#             }

#             if (change.document.id in running_jobs) and (job.get("status") != "RUNNING"):
#                 running_jobs[change.document.id].cancel()
#                 del running_jobs[change.document.id]
#             elif (change.document.id not in running_jobs) and (job.get("status") == "RUNNING"):
#                 coroutine = push_n_results_updates(change.document.id, event)
#                 future = asyncio.run_coroutine_threadsafe(coroutine, loop)
#                 running_jobs[change.document.id] = future

#             async def queue_job(job_id: str, event: dict):
#                 # I cant figure out how to `current_num_results` synchronously
#                 n_results = await current_num_results(job_id)
#                 event["n_results"] = n_results
#                 await queue.put(event)

#             coroutine = queue_job(change.document.id, event)
#             asyncio.run_coroutine_threadsafe(coroutine, loop)

#     job_collection_stream = jobs_current_page.on_snapshot(on_changed_job_doc)

#     async def event_stream():
#         yield "retry: 5000\n\n"  # <- make browser reconnect after 5s on error
#         yield ": init\n\n"
#         try:
#             while True:
#                 try:
#                     event = await asyncio.wait_for(queue.get(), timeout=10)
#                     yield f"data: {json.dumps(event)}\n\n"
#                 except asyncio.TimeoutError:
#                     # heartbeat to keeps proxy from closing connection
#                     yield ": keep-alive\n\n"
#         finally:
#             job_collection_stream.unsubscribe()

#     headers = {"Cache-Control": "no-cache, no-transform"}
#     return StreamingResponse(event_stream(), media_type="text/event-stream", headers=headers)


# @router.get("/v1/jobs")
# async def get_jobs(request: Request, page: int = 0, stream: bool = False):
#     docs_per_page = 15
#     _jobs_ref = DB.collection("jobs").order_by("started_at", direction=firestore.Query.DESCENDING)
#     jobs_current_page = _jobs_ref.offset(page * docs_per_page).limit(docs_per_page)
#     if stream:
#         return job_stream(jobs_current_page)

#     jobs = []
#     for job_doc in jobs_current_page.stream():
#         job = job_doc.to_dict()
#         jobs.append(
#             {
#                 "jobId": job_doc.id,
#                 "status": job.get("status"),
#                 "user": job.get("user", "Unknown"),
#                 "function_name": job.get("function_name", "Unknown"),
#                 "n_inputs": job.get("n_inputs", 0),
#                 "n_results": await current_num_results(job_doc.id),
#                 "started_at": job.get("started_at"),
#             }
#         )
#     total_jobs = await ASYNC_DB.collection("jobs").count().get()
#     total_jobs = total_jobs[0][0].value
#     return JSONResponse({"jobs": jobs, "page": page, "limit": docs_per_page, "total": total_jobs})


# @router.post("/v1/jobs/{job_id}/stop")
# async def stop_job(job_id: str, request: Request):
#     email = request.session.get("X-User-Email") or request.headers.get("X-User-Email")
#     msg = f"Job canceled by user: {email}"
#     timestamp = datetime.now(timezone.utc)
#     logs = [{"is_error": True, "message": msg, "timestamp": timestamp}]
#     job_doc = ASYNC_DB.collection("jobs").document(job_id)
#     await job_doc.collection("logs").add({"logs": logs, "timestamp": timestamp})
#     await job_doc.update({"status": "CANCELED"})


# @router.get("/v1/jobs/{job_id}/logs")
# async def stream_or_fetch_job_logs(
#     job_id: str, stream: bool = False, page: int = 0, limit: int = 1000
# ):
#     logs_ref = ASYNC_DB.collection("jobs").document(job_id).collection("logs")
#     logs_query = logs_ref.order_by("timestamp").offset(page * limit).limit(limit)
#     if not stream:
#         logs = []
#         async for doc in logs_query.stream():
#             for log in doc.to_dict()["logs"]:
#                 ts = int(log["timestamp"].timestamp())
#                 logs.append({"message": log["message"], "created_at": ts})

#         total_res = await logs_ref.count().get()
#         total = total_res[0][0].value
#         return JSONResponse({"logs": logs, "page": page, "limit": limit, "total": total})

#     queue = asyncio.Queue()
#     current_loop = asyncio.get_running_loop()
#     skip_first_snapshot = True

#     def on_snapshot(col_snapshot, changes, read_time):
#         nonlocal skip_first_snapshot
#         if skip_first_snapshot:
#             # Skip the initial on_snapshot flood (ADDED for all existing docs)
#             skip_first_snapshot = False
#             return

#         sort_key = lambda change: change.document.to_dict()["logs"][0]["timestamp"]
#         for change in sorted(changes, key=sort_key):
#             data = change.document.to_dict()
#             for log in data["logs"]:
#                 event = {
#                     "message": log["message"],
#                     "created_at": int(log["timestamp"].timestamp()),
#                 }
#                 current_loop.call_soon_threadsafe(queue.put_nowait, event)

#     logs_ref = DB.collection("jobs").document(job_id).collection("logs")
#     logs_ref_watcher = logs_ref.on_snapshot(on_snapshot)

#     async def event_stream():
#         try:
#             yield "retry: 5000\n\n"
#             yield ": init\n\n"
#             while True:
#                 try:
#                     event = await asyncio.wait_for(queue.get(), timeout=2)
#                     yield f"data: {json.dumps(event)}\n\n"
#                 except asyncio.TimeoutError:
#                     yield ": keep-alive\n\n"
#         finally:
#             logs_ref_watcher.unsubscribe()

#     headers = {"Cache-Control": "no-cache, no-transform"}
#     return StreamingResponse(event_stream(), media_type="text/event-stream", headers=headers)
import json
import asyncio
from time import time
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from starlette.responses import StreamingResponse
from google.cloud import firestore
from google.cloud.firestore import ArrayUnion

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
        start = time()
        while True:
            n_results = await current_num_results(job_id)
            event["n_results"] = n_results
            await queue.put(event)
            await asyncio.sleep(1)
            if (time() - start) % 10 < 1:
                # check job has nodes working on it
                filter_ = firestore.FieldFilter("current_job", "==", job_id)
                nodes = ASYNC_DB.collection("nodes").where(filter=filter_)
                nodes_working_on_job = await nodes.get()

                # I think this situation is possible when uploading really large functions to many
                # nodes, The timeout on that is 300s, so that's how long this has to  be true for
                # here to cause an error.
                if not nodes_working_on_job and (time() - start) > 300:
                    msg = "Job failed due to internal cluster error."
                    timestamp = datetime.now(timezone.utc)
                    logs = [{"message": msg, "timestamp": timestamp}]
                    job_doc = ASYNC_DB.collection("jobs").document(job_id)
                    await job_doc.collection("logs").add({"logs": logs, "timestamp": timestamp})
                    msg = 'main_svc: job is "running" but no nodes working on it ???'
                    await job_doc.update({"status": "FAILED", "fail_reason": ArrayUnion([msg])})
                    return

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
                running_jobs[change.document.id].cancel()
                del running_jobs[change.document.id]
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
        yield "retry: 5000\n\n"  # make browser reconnect after 5s on error
        yield ": init\n\n"
        try:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=10)
                    yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    # heartbeat to keep proxy from closing connection
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
    page: int = 0,
    limit: int = 1000,
):
    logs_ref = ASYNC_DB.collection("jobs").document(job_id).collection("logs")
    logs_query = logs_ref.order_by("timestamp").offset(page * limit).limit(limit)

    def _normalize_index(raw_index):
        if raw_index is None:
            return None
        try:
            return int(raw_index)
        except (TypeError, ValueError):
            # fall back to raw value for weird legacy data
            return raw_index

    if not stream:
        logs = []
        async for doc in logs_query.stream():
            doc_dict = doc.to_dict()
            for log in doc_dict.get("logs", []):
                ts = int(log["timestamp"].timestamp())
                idx = _normalize_index(log.get("index"))
                is_error = bool(log.get("is_error", False))
                logs.append(
                    {
                        "message": log["message"],
                        "created_at": ts,
                        "index": idx,
                        "is_error": is_error,
                    }
                )

        total_res = await logs_ref.count().get()
        total = total_res[0][0].value
        return JSONResponse({"logs": logs, "page": page, "limit": limit, "total": total})

    queue = asyncio.Queue()
    current_loop = asyncio.get_running_loop()
    skip_first_snapshot = True

    def on_snapshot(col_snapshot, changes, read_time):
        nonlocal skip_first_snapshot
        if skip_first_snapshot:
            # Skip the initial on_snapshot flood (ADDED for all existing docs)
            skip_first_snapshot = False
            return

        sort_key = lambda change: change.document.to_dict()["logs"][0]["timestamp"]
        for change in sorted(changes, key=sort_key):
            data = change.document.to_dict()
            for log in data.get("logs", []):
                event = {
                    "message": log["message"],
                    "created_at": int(log["timestamp"].timestamp()),
                    "index": _normalize_index(log.get("index")),
                    "is_error": bool(log.get("is_error", False)),
                }
                current_loop.call_soon_threadsafe(queue.put_nowait, event)

    logs_ref_sync = DB.collection("jobs").document(job_id).collection("logs")
    logs_ref_watcher = logs_ref_sync.on_snapshot(on_snapshot)

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
            logs_ref_watcher.unsubscribe()

    headers = {"Cache-Control": "no-cache, no-transform"}
    return StreamingResponse(event_stream(), media_type="text/event-stream", headers=headers)
