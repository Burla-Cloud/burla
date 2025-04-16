import json
import requests
import random
import asyncio
from threading import Timer 
from datetime import datetime, timezone, timedelta
from time import time, sleep
from queue import Queue
from uuid import uuid4
from typing import Optional, Callable
from concurrent.futures import ThreadPoolExecutor
import logging 

from fastapi import APIRouter, Path, Depends, Query, Request 
from fastapi.responses import JSONResponse
from starlette.responses import StreamingResponse
from google.cloud import firestore
from google.cloud.firestore import FieldFilter, Increment
from google.protobuf.timestamp_pb2 import Timestamp  
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
                    "deleted": change.type.name == "REMOVED"
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
        jobs.append({
            "jobId": doc.id,
            "status": data.get("status"),
            "n_inputs": data.get("n_inputs", 0),
            "user": data.get("user", "Unknown"),
            "started_at": data.get("started_at"),
        })

    total = len([doc.id for doc in DB.collection("jobs").stream()])

    return JSONResponse({
        "jobs": jobs,
        "page": page,
        "limit": limit,
        "total": total
    })


@router.get("/v1/job_logs/{job_id}/paginated")
def get_paginated_logs(
    job_id: str,
    limit: int = Query(10, ge=1, le=250),
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
        logs.append({
            "id": doc.id,
            "msg": data.get("msg"),
            "time": created_at.timestamp() if created_at else None,
        })

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


@router.post("/v1/logs/generate-6200")
def generate_job_2500():
    job_id = str(uuid4())
    job_ref = DB.collection("jobs").document(job_id)
    logs_ref = job_ref.collection("logs")

    # UTC time for consistent behavior (displays as EDT in Firestore)
    base_time = datetime.now(timezone.utc)

    # Job metadata (started_at as UNIX timestamp)
    job_metadata = {
        "user": "joe@burla.dev",
        "n_inputs": 11500,
        "results": 11500,
        "status": "RUNNING",
        "started_at": base_time.timestamp(),  # ✅ UNIX timestamp here
        "func_cpu": 1,
        "func_ram": 256,
        "current_parallelism": 10,
        "target_parallelism": 10,
        "planned_future_job_parallelism": 10,
        "user_python_version": "3.10",
        "burla_client_version": "1.0.0",
        "inputs_id": "dummy_input_id"
    }
    job_ref.set(job_metadata)

    for i in range(11500):
        log_time = base_time + timedelta(milliseconds=i * 5)

        log_entry = {
            "created_at": log_time,  # ✅ Firestore-native timestamp
            "msg": f"Dummy log line {i + 1}: Skipped"
        }
        logs_ref.document(f"log_{i}").set(log_entry)

    return {"job_id": job_id, "message": "Created 1 job with 6,200 logs using proper timestamps."}