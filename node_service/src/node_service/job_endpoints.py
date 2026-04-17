import pickle
from typing import Optional

import asyncio
from google.cloud.firestore_v1.async_client import AsyncClient
from fastapi import APIRouter, Path, Query, Depends, Response, Request

from node_service import (
    SELF,
    PROJECT_ID,
    INSTANCE_NAME,
    get_request_json,
    get_logger,
    get_request_files,
)
from node_service.helpers import Logger
from node_service.job_watcher import job_watcher_logged

router = APIRouter()


@router.get("/jobs/{job_id}/get_inputs")
async def get_inputs(
    job_id: str = Path(...),
    transfer_id: str = Query(...),
    requester_queue_size: int = Query(0),
):
    if job_id != SELF["current_job"]:
        return Response("job not found", status_code=404)

    if transfer_id in SELF["pending_transfers"]:
        items = SELF["pending_transfers"][transfer_id]
    else:
        difference = SELF["inputs_queue"].qsize() - requester_queue_size
        target_num = max(difference, 1) // 2
        items = []
        total_bytes = 0
        while len(items) < target_num:
            try:
                input_index, input_pkl = SELF["inputs_queue"].get_nowait()
            except asyncio.QueueEmpty:
                break
            if total_bytes + len(input_pkl) > 3_000_000 and items:
                SELF["inputs_queue"].put_nowait((input_index, input_pkl), len(input_pkl))
                break
            items.append((input_index, input_pkl))
            total_bytes += len(input_pkl)
        SELF["pending_transfers"][transfer_id] = items

    return Response(
        content=pickle.dumps(items),
        media_type="application/octet-stream",
    )


@router.post("/jobs/{job_id}/ack_transfer")
async def ack_transfer(
    job_id: str = Path(...),
    transfer_id: str = Query(...),
    received: bool = Query(...),
):
    if job_id != SELF["current_job"]:
        return Response("job not found", status_code=404)

    items = SELF["pending_transfers"].pop(transfer_id, None)
    if items is None:
        return Response(status_code=200)

    if not received:
        for input_index, input_pkl in items:
            SELF["inputs_queue"].put_nowait((input_index, input_pkl), len(input_pkl))
    return Response(status_code=200)


@router.post("/jobs/{job_id}/inputs")
async def upload_inputs(
    job_id: str = Path(...),
    request_files: Optional[dict] = Depends(get_request_files),
):
    if job_id != SELF["current_job"]:
        return Response("job not found", status_code=404)

    inputs_pkl_with_idx = pickle.loads(request_files["inputs_pkl_with_idx"])
    await asyncio.sleep(0)
    for input_pkl_with_idx in inputs_pkl_with_idx:
        await SELF["inputs_queue"].put(input_pkl_with_idx, len(input_pkl_with_idx[1]))


@router.get("/jobs/{job_id}/results")
async def get_results(job_id: str = Path(...)):
    if job_id != SELF["current_job"]:
        print(f"job {job_id} not found, current job: {SELF['current_job']}")
        return Response("job not found", status_code=404)

    results = []
    total_bytes = 0
    while (not SELF["results_queue"].empty()) and (total_bytes < (1_000_000 * 1)):
        try:
            result = SELF["results_queue"].get_nowait()
            results.append(result)
            total_bytes += len(result[2])
        except asyncio.QueueEmpty:
            break

    response_json = {
        "results": results,
        "current_parallelism": SELF["current_parallelism"],
    }

    data = pickle.dumps(response_json)
    headers = {"Content-Disposition": 'attachment; filename="results.pkl"'}
    return Response(content=data, media_type="application/octet-stream", headers=headers)


@router.post("/jobs/{job_id}")
async def execute(
    request: Request,
    job_id: str = Path(...),
    request_json: dict = Depends(get_request_json),
    request_files: Optional[dict] = Depends(get_request_files),
    logger: Logger = Depends(get_logger),
):
    await logger.log(f"Executing job {job_id} ...")
    # The `on_job_start` function in __init__.py is run as soon as upload to this endpoint starts.
    # It exists to set `SELF["current_job"]` and set this node to RUNNING in the db as soon as
    # upload starts if the user's function is big.

    # determine which workers to call
    workers_to_assign = []
    workers_to_leave_idle = []
    future_parallelism = 0
    is_background_job = request_json["is_background_job"]
    user_python_version = request_json["user_python_version"]

    for worker in SELF["workers"]:
        correct_python_version = worker.python_version == user_python_version
        need_more_parallelism = future_parallelism < request_json["parallelism"]

        if correct_python_version and need_more_parallelism:
            workers_to_assign.append(worker)
            future_parallelism += 1
        else:
            workers_to_leave_idle.append(worker)

    if not workers_to_assign:
        # possible the `on_job_start` in __init__ hasn't run yet because async.
        # if it runs after this returns node will be stuck running.
        # switch coroutines until this node is running, then reset back.
        # using async in `on_job_start` because it's maybe slightly faster ?
        n = 0
        while not SELF["RUNNING"]:
            n += 1
            if n > 10:
                raise Exception("this is theoretically impossible")
            await asyncio.sleep(0)

        SELF["RUNNING"] = False
        SELF["current_job"] = None
        async_db = AsyncClient(project=PROJECT_ID, database="burla")
        node_doc = async_db.collection("nodes").document(INSTANCE_NAME)
        await node_doc.update({"status": "READY", "current_job": None})

        msg = "No compatible containers.\n"
        msg += f"User is running python version {user_python_version}, "
        versions = list(set([e.python_version for e in SELF["workers"]]))
        msg += f"containers in the cluster are running: {', '.join(versions)}.\n"
        msg += "To fix this you can either:\n"
        msg += f" - update the cluster to run containers with python{user_python_version}\n"
        msg += f" - update your local python version to be one of {versions}"
        return Response(msg, status_code=409)

    packages = request_json["packages"]
    if packages:
        # installing in one installs in all, they share volume-mounted python env
        await workers_to_assign[0].install_packages(packages)

    function_pkl = request_files["function_pkl"]
    await asyncio.gather(*(w.load_function(function_pkl) for w in workers_to_assign))

    SELF["workers"] = workers_to_assign
    SELF["idle_workers"] = workers_to_leave_idle
    SELF["current_parallelism"] = 0
    # user specific, assign to self to use for node <-> node requests only during this job.
    SELF["auth_headers"] = {
        "Authorization": request.headers.get("Authorization", ""),
        "X-User-Email": request.headers.get("X-User-Email", ""),
    }

    SELF["job_watcher_stop_event"].clear()  # is initalized as set by default
    job_watcher_coroutine = job_watcher_logged(
        request_json["n_inputs"],
        is_background_job,
        request_json["start_time"],
        request_json["node_ids_expected"],
    )
    SELF["job_watcher_task"] = asyncio.create_task(job_watcher_coroutine)
    return Response(status_code=200)
