import json
import requests
import asyncio
import datetime 
from time import time
from uuid import uuid4
from typing import Optional, Callable
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter, Path, Depends
from fastapi.responses import JSONResponse
from starlette.responses import StreamingResponse
from google.cloud import firestore 
from google.cloud.firestore import FieldFilter, Increment


from main_service import (
    DB,
    get_user_email,
    get_logger,
    get_request_json,
    get_request_files,
    get_add_background_task_function,
)
from main_service.cluster import (
    parallelism_capacity,
    reboot_nodes_with_job,
    async_ensure_reconcile,
)
from main_service.helpers import validate_create_job_request, Logger

router = APIRouter()


@router.post("/v1/jobs/")
def create_job(
    user_email: dict = Depends(get_user_email),
    request_json: dict = Depends(get_request_json),
    request_files: Optional[dict] = Depends(get_request_files),
    logger: Logger = Depends(get_logger),
    add_background_task: Callable = Depends(get_add_background_task_function),
):
    validate_create_job_request(request_json)

    node_filter = FieldFilter("status", "==", "READY")
    ready_nodes = [n.to_dict() for n in DB.collection("nodes").where(filter=node_filter).stream()]
    if len(ready_nodes) == 0:
        msg = "Zero nodes with state `READY` are currently available."
        content = {"error_type": "NoReadyNodes", "message": msg}
        return JSONResponse(content=content, status_code=503) 

    planned_future_job_parallelism = 0
    nodes_to_assign = []
    for node in ready_nodes:
        parallelism_deficit = request_json["max_parallelism"] - planned_future_job_parallelism
        max_node_parallelism = parallelism_capacity(
            node["machine_type"], request_json["func_cpu"], request_json["func_ram"]
        )

        if max_node_parallelism > 0 and parallelism_deficit > 0:
            node_target_parallelism = min(parallelism_deficit, max_node_parallelism)
            node["target_parallelism"] = node_target_parallelism
            node["starting_index"] = planned_future_job_parallelism  # idx to start work at
            planned_future_job_parallelism += node_target_parallelism
            nodes_to_assign.append(node)

    job_id = str(uuid4())
    job_ref = DB.collection("jobs").document(job_id) 
    job_ref.set(
        {
            "n_inputs": int(request_json["n_inputs"]),
            "inputs_id": request_json["inputs_id"],
            "func_cpu": request_json["func_cpu"],
            "func_ram": request_json["func_ram"],
            "burla_client_version": request_json["burla_version"],
            "user_python_version": request_json["python_version"],
            "target_parallelism": request_json["max_parallelism"],
            "current_parallelism": 0,
            "planned_future_job_parallelism": planned_future_job_parallelism,
            "user": user_email,
            "started_at": time(),
        }
    )

    if len(nodes_to_assign) == 0:
        content = {"error_type": "NoCompatibleNodes", "message": "No compatible nodes available."}
        return JSONResponse(content=content, status_code=503)

    if request_json["max_parallelism"] > planned_future_job_parallelism:
        # TODO: start more nodes here to fill the gap ?
        parallelism_deficit = request_json["max_parallelism"] - planned_future_job_parallelism
        msg = f"Cluster needs {parallelism_deficit} more cpus, "
        msg += f"continuing with a parallelism of {planned_future_job_parallelism}."
        logger.log(msg, severity="WARNING")

    # concurrently ask all ready nodes to start work:
    def assign_node(node: dict):
        """Errors in here are raised correctly!"""
        parallelism = node["target_parallelism"]
        starting_index = node["starting_index"]
        payload = {"parallelism": parallelism, "starting_index": starting_index}
        data = dict(request_json=json.dumps(payload))
        files = dict(function_pkl=request_files["function_pkl"])
        response = requests.post(f"{node['host']}/jobs/{job_id}", files=files, data=data)

        try:
            response.raise_for_status()
        except Exception as e:
            # Any errors returned here should also be raised inside the node service.
            # Errors here shouldn't kill the job because some workers are often able to start.
            # Nodes returning errors here should be restarted.
            msg = f"Node {node['instance_name']} refused job with error: {e}"
            logger.log(msg, severity="WARNING")
            return 0

        logger.log(f"Assigned node {node['instance_name']} to job {job_id}.")
        return node["target_parallelism"]

    with ThreadPoolExecutor(max_workers=32) as executor:
        current_parallelism = sum(list(executor.map(assign_node, nodes_to_assign)))

    if current_parallelism == 0:
        add_background_task(reboot_nodes_with_job, DB, job_id)
        async_ensure_reconcile(DB, logger, add_background_task)
        content = {"error_type": "JobRefused", "message": "Job refused by all available nodes."}
        return JSONResponse(content=content, status_code=503)
    else:
        new_job_info = {"current_parallelism": Increment(current_parallelism)}
        add_background_task(job_ref.update, new_job_info)
        return {"job_id": job_id}


@router.get("/v1/jobs/{job_id}")
def run_job_healthcheck(
    job_id: str = Path(...),
    logger: Logger = Depends(get_logger),
    add_background_task: Callable = Depends(get_add_background_task_function),
):
    # if not already happening, modify current cluster state -> correct/optimal state:
    async_ensure_reconcile(DB, logger, add_background_task)

    # get all nodes working on this job
    _filter = FieldFilter("current_job", "==", job_id)
    nodes_with_job = [n.to_dict() for n in DB.collection("nodes").where(filter=_filter).stream()]

    # check status of every node / worker working on this job
    for node in nodes_with_job:
        response = requests.get(f"{node['host']}/jobs/{job_id}")
        response.raise_for_status()
        if response.json()["any_workers_failed"]:
            raise Exception(f"Worker failed. Check logs for node {node['instance_name']}")
        

# @router.get("/v1/job_context")
# async def job_stream(logger: Logger = Depends(get_logger)):
#     queue = asyncio.Queue()
#     current_loop = asyncio.get_running_loop()

#     def fetch_results_sync(job_id):
#         """Fetch only the `is_error` field from the `results` sub-collection."""
#         results_ref = DB.collection("jobs").document(job_id).collection("results")
#         results_docs = results_ref.stream()
#         return [{"is_error": result.to_dict().get("is_error", None)} for result in results_docs]

#     def determine_job_status(results):
#         """Determine job status based on `is_error` values."""
#         if not results:  
#             return "RUNNING"  # No results means job is still running
#         elif any(result["is_error"] for result in results if result["is_error"] is not None):
#             return "FAILED"  # At least one `is_error` is True
#         else:
#             return "COMPLETED"  # All `is_error` values are False

#     def update_job_status(job_id, new_status):
#         """Update the job status in Firestore and ensure it triggers an update."""
#         job_ref = DB.collection("jobs").document(job_id)
#         job_data = job_ref.get().to_dict() or {}

#         current_status = job_data.get("status") 
#         if current_status != new_status:
#             job_ref.update({
#                 "status": new_status,
#                 "last_updated": firestore.SERVER_TIMESTAMP  # ✅ Forces Firestore to trigger snapshot
#             })
#             logger.log(f"Updated job {job_id} status to {new_status}")


#     def on_snapshot(query_snapshot, changes, read_time):
#         """Firestore listener that fetches `is_error` from results & updates status."""
#         logger.log(f"Firestore snapshot detected {len(changes)} changes at {read_time}")

#         for change in changes:
#             doc_data = change.document.to_dict() or {}
#             job_id = change.document.id
#             logger.log(f"Change detected in job {job_id}: {change.type.name}")

#             results = fetch_results_sync(job_id)
#             new_status = determine_job_status(results)

#             # Update Firestore job status if needed
#             update_job_status(job_id, new_status)

#             event_data = {
#                 "jobId": job_id,
#                 "status": new_status,  # Updated status
#                 "results": results  # Only `is_error` values
#             }

#             # ✅ Ensure safe queue execution in event loop
#             current_loop.call_soon_threadsafe(queue.put_nowait, event_data)
#             logger.log(f"Firestore event pushed: {event_data}")

#     async def stream_jobs():
#         """Streams Firestore job updates via SSE."""
#         query = DB.collection("jobs")
#         unsubscribe = query.on_snapshot(on_snapshot)

#         try:
#             while True:
#                 event = await queue.get()
#                 yield f"data: {json.dumps(event)}\n\n"
#         finally:
#             unsubscribe()  # ✅ Correctly unsubscribing
#             logger.log("Unsubscribed from Firestore snapshot.")

#     return StreamingResponse(stream_jobs(), media_type="text/event-stream")

# @router.get("/v1/job_context")
# async def job_stream(logger: Logger = Depends(get_logger)):
#     queue = asyncio.Queue()
#     current_loop = asyncio.get_running_loop()

#     def fetch_results_sync(job_id):
#         """Fetch only the `is_error` field from the `results` sub-collection."""
#         results_ref = DB.collection("jobs").document(job_id).collection("results")
#         results_docs = results_ref.stream()
#         return [{"is_error": result.to_dict().get("is_error", None)} for result in results_docs]

#     def determine_job_status(results):
#         """Determine job status based on `is_error` values."""
#         if not results or all(result["is_error"] is None for result in results):
#             return "RUNNING"  # No results means job is still running
#         elif any(result["is_error"] for result in results if result["is_error"] is not None):
#             return "FAILED"  # At least one `is_error` is True
#         else:
#             return "COMPLETED"  # All `is_error` values are False

#     def update_job_status(job_id, new_status):
#         """Update the job status in Firestore if it has changed."""
#         job_ref = DB.collection("jobs").document(job_id)
#         job_data = job_ref.get().to_dict() or {}

#         current_status = job_data.get("status")
#         if current_status != new_status:
#             job_ref.update({
#                 "status": new_status,
#                 "last_updated": firestore.SERVER_TIMESTAMP  # ✅ Forces Firestore to trigger snapshot
#             })
#             logger.log(f"Updated job {job_id} status to {new_status}")

#     def on_results_snapshot(job_id):
#         """Creates a listener for a job's `results` subcollection changes."""
#         def callback(query_snapshot, changes, read_time):
#             logger.log(f"🔄 Detected results update for job {job_id} at {read_time}")

#             results = fetch_results_sync(job_id)
#             new_status = determine_job_status(results)

#             update_job_status(job_id, new_status)

#             event_data = {
#                 "jobId": job_id,
#                 "status": new_status,  # Updated status
#                 "results": results  # Only `is_error` values
#             }

#             current_loop.call_soon_threadsafe(queue.put_nowait, event_data)
#             logger.log(f"✅ Firestore event pushed: {event_data}")

#         return callback

#     job_watchers = {}

#     def on_jobs_snapshot(query_snapshot, changes, read_time):
#         """Firestore listener that watches for job document changes and attaches listeners to their results subcollections."""
#         logger.log(f"📝 Detected {len(changes)} job changes at {read_time}")

#         for change in changes:
#             doc_data = change.document.to_dict() or {}
#             job_id = change.document.id

#             if change.type.name in ["ADDED", "MODIFIED"]:
#                 logger.log(f"📌 Job {job_id} detected - Setting up listener for results")

#                 # Ensure there's only one listener per job
#                 if job_id not in job_watchers:
#                     job_watchers[job_id] = DB.collection("jobs").document(job_id).collection("results").on_snapshot(
#                         on_results_snapshot(job_id)
#                     )

#             elif change.type.name == "REMOVED":
#                 logger.log(f"❌ Job {job_id} removed - Cleaning up listener")
#                 if job_id in job_watchers:
#                     job_watchers[job_id].unsubscribe()
#                     del job_watchers[job_id]

#     async def stream_jobs():
#         """Streams Firestore job updates via SSE."""
#         unsubscribe_jobs = DB.collection("jobs").on_snapshot(on_jobs_snapshot)

#         try:
#             while True:
#                 event = await queue.get()
#                 yield f"data: {json.dumps(event)}\n\n"
#         finally:
#             unsubscribe_jobs()
#             logger.log("🛑 Unsubscribed from Firestore job snapshot.")
#             for job_id, watcher in job_watchers.items():
#                 watcher.unsubscribe()
#             job_watchers.clear()

#     return StreamingResponse(stream_jobs(), media_type="text/event-stream")


@router.get("/v1/job_context")
async def job_stream(logger: Logger = Depends(get_logger)):
    queue = asyncio.Queue()
    current_loop = asyncio.get_running_loop()

    def fetch_results_sync(job_id):
        """Fetch only the `is_error` field from the `results` sub-collection."""
        results_ref = DB.collection("jobs").document(job_id).collection("results")
        results_docs = results_ref.stream()
        return [{"is_error": result.to_dict().get("is_error", None)} for result in results_docs]

    def determine_job_status(results, n_inputs):
        """Determine job status based on `is_error` values and number of results."""
        num_results = len(results)

        # If there are no results, or all `is_error` values are None, it's still running
        if not results or all(result["is_error"] is None for result in results):
            return "RUNNING"

        # If number of results is less than n_inputs:
        if num_results < n_inputs:
            if any(result["is_error"] for result in results if result["is_error"] is not None):
                return "FAILED"  # At least one error detected
            else:
                return "RUNNING"  # No errors, but still processing

        # If number of results matches n_inputs:
        if num_results == n_inputs:
            if any(result["is_error"] for result in results if result["is_error"] is not None):
                return "FAILED"  # At least one error in a fully completed batch
            else:
                return "COMPLETED"  # All tasks completed successfully

        return "RUNNING"  # Default case (shouldn't reach here)

    def update_job_status(job_id, new_status):
        """Update the job status in Firestore if it has changed."""
        job_ref = DB.collection("jobs").document(job_id)
        job_data = job_ref.get().to_dict() or {}

        current_status = job_data.get("status")
        if current_status != new_status:
            job_ref.update({
                "status": new_status,
                "last_updated": firestore.SERVER_TIMESTAMP  # ✅ Forces Firestore to trigger snapshot
            })
            logger.log(f"Updated job {job_id} status to {new_status}")

    def on_results_snapshot(job_id):
        """Creates a listener for a job's `results` subcollection changes."""
        def callback(query_snapshot, changes, read_time):
            logger.log(f"🔄 Detected results update for job {job_id} at {read_time}")

            job_ref = DB.collection("jobs").document(job_id)
            job_data = job_ref.get().to_dict() or {}

            n_inputs = job_data.get("n_inputs", 0)  # Default to 0 if not present
            user = job_data.get("user", "unknown")
            started_at_raw = job_data.get("started_at", None)  # Get raw timestamp
            started_at = started_at = started_at_raw if isinstance(started_at_raw, (int, float)) else None


            results = fetch_results_sync(job_id)
            new_status = determine_job_status(results, n_inputs) 
            update_job_status(job_id, new_status)

            event_data = {
                "jobId": job_id,
                "status": new_status, 
                "results": results, 
                "user": user,  
                "started_at": started_at
            } 

            current_loop.call_soon_threadsafe(queue.put_nowait, event_data)
            logger.log(f"✅ Firestore event pushed: {event_data}")

        return callback

    job_watchers = {}

    def on_jobs_snapshot(query_snapshot, changes, read_time):
        """Firestore listener that watches for job document changes and attaches listeners to their results subcollections."""
        logger.log(f"📝 Detected {len(changes)} job changes at {read_time}")

        for change in changes:
            doc_data = change.document.to_dict() or {}
            job_id = change.document.id

            if change.type.name in ["ADDED", "MODIFIED"]:
                logger.log(f"📌 Job {job_id} detected - Setting up listener for results")

                # Ensure there's only one listener per job
                if job_id not in job_watchers:
                    job_watchers[job_id] = DB.collection("jobs").document(job_id).collection("results").on_snapshot(
                        on_results_snapshot(job_id)
                    )

            elif change.type.name == "REMOVED":
                logger.log(f"❌ Job {job_id} removed - Cleaning up listener")
                if job_id in job_watchers:
                    job_watchers[job_id].unsubscribe()
                    del job_watchers[job_id]

    async def stream_jobs():
        """Streams Firestore job updates via SSE."""
        unsubscribe_jobs = DB.collection("jobs").on_snapshot(on_jobs_snapshot)

        try:
            while True:
                event = await queue.get()
                yield f"data: {json.dumps(event)}\n\n"
        finally:
            unsubscribe_jobs()
            logger.log("🛑 Unsubscribed from Firestore job snapshot.")
            for job_id, watcher in job_watchers.items():
                watcher.unsubscribe()
            job_watchers.clear()

    return StreamingResponse(stream_jobs(), media_type="text/event-stream") 

