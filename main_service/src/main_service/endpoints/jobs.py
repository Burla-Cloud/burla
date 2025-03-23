import json
import requests
import asyncio
from threading import Timer 
from datetime import datetime, timezone 
from time import time, sleep
from uuid import uuid4
from typing import Optional, Callable
from concurrent.futures import ThreadPoolExecutor
import logging 

from fastapi import APIRouter, Path, Depends
from fastapi.responses import JSONResponse
from starlette.responses import StreamingResponse
from google.cloud import firestore 
from google.cloud.firestore import FieldFilter, Increment
from google.api_core.exceptions import GoogleAPICallError, NotFound


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

logger = logging.getLogger("uvicorn")
logger.setLevel(logging.INFO)


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


@router.get("/v1/job_context")
async def job_info(logger: Logger = Depends(get_logger)):
    queue = asyncio.Queue()
    current_loop = asyncio.get_running_loop()
    top_jobs_ids = set()
    node_cache = {}

    def determine_job_status(doc_data: dict, job_id: str) -> str:
        now = datetime.now(timezone.utc)
        num_inputs = doc_data.get("n_inputs")
        started_at = doc_data.get("started_at")

        if started_at is not None and isinstance(started_at, (float, int)):
            started_at = datetime.fromtimestamp(started_at, tz=timezone.utc)

        node_docs = DB.collection("nodes").where("current_job", "==", job_id).get()
        node_working = bool(node_docs)

        results_docs = DB.collection("jobs").document(job_id).collection("results").get()
        num_results = len(results_docs)
        any_error = any(doc.to_dict().get("is_error", False) for doc in results_docs)

        if node_working:
            computed_status = "RUNNING"
        else:
            if num_inputs and num_results == num_inputs:
                computed_status = "COMPLETED" if not any_error else "FAILED"
            elif started_at and (now - started_at).total_seconds() > 7.5:
                computed_status = "FAILED"
            else:
                computed_status = "PENDING"

        logger.log(f"Job {job_id}: node_working: {node_working}, num_results: {num_results}, status: {computed_status}")
        return computed_status

    def reassess_job(job_id: str):
        if job_id not in top_jobs_ids:
            return
        job_doc = DB.collection("jobs").document(job_id).get()
        if job_doc.exists:
            doc_data = job_doc.to_dict() or {}
            computed_status = determine_job_status(doc_data, job_id)
            if doc_data.get("status") != computed_status:
                DB.collection("jobs").document(job_id).update({"status": computed_status})
            event_data = {
                "jobId": job_id,
                "status": computed_status,
                "user": doc_data.get("user"),
                "started_at": doc_data.get("started_at")
            }
            current_loop.call_soon_threadsafe(queue.put_nowait, event_data)

    async def job_stream():
        nonlocal top_jobs_ids

        def on_jobs_snapshot(query_snapshot, changes, read_time):
            nonlocal top_jobs_ids
            top_jobs_ids = {doc.id for doc in query_snapshot}
            for doc in query_snapshot:
                reassess_job(doc.id)

        def on_nodes_snapshot(col_snapshot, changes, read_time):
            for change in changes:
                doc = change.document
                node_id = doc.id
                new_data = doc.to_dict() or {}

                new_job = new_data.get("current_job")
                old_job = node_cache.get(node_id)
                node_cache[node_id] = new_job  # Update cache

                if old_job != new_job:
                    logger.log(f"Node {node_id} current_job changed: {old_job} → {new_job}")
                    if old_job:
                        reassess_job(old_job)
                    if new_job:
                        reassess_job(new_job)

        def on_results_snapshot(query_snapshot, changes, read_time):
            for change in changes:
                parent_ref = change.document.reference.parent.parent
                if parent_ref:
                    job_id = parent_ref.id
                    reassess_job(job_id)

        jobs_query = DB.collection("jobs").order_by("started_at", direction="DESCENDING").limit(10)
        unsubscribe_jobs = jobs_query.on_snapshot(on_jobs_snapshot)
        unsubscribe_nodes = DB.collection("nodes").on_snapshot(on_nodes_snapshot)
        unsubscribe_results = DB.collection_group("results").on_snapshot(on_results_snapshot)

        try:
            while True:
                event = await queue.get()
                yield f"data: {json.dumps(event)}\n\n"
        finally:
            unsubscribe_jobs()
            unsubscribe_nodes()
            unsubscribe_results()

    return StreamingResponse(job_stream(), media_type="text/event-stream")