import pickle
import asyncio
import aiohttp
from typing import Optional
from queue import Empty
from fastapi import APIRouter, Path, Response, Depends, Query

from worker_service import SELF, get_request_json, get_request_files
from worker_service.udf_executor import execute_job
from worker_service.helpers import ThreadWithExc

router = APIRouter()


@router.get("/")
async def get_status():
    # A worker can also be "IDLE" but that is returned when checking for results (more efficient)
    if SELF["STARTED"]:
        return {"status": "BUSY"}
    else:
        return {"status": "READY"}


def _check_udf_executor_thread():
    if SELF["current_job"] and not SELF["STOP_PROCESSING_EVENT"].is_set():
        thread = SELF.get("udf_executor_thread")
        if not (thread and thread.is_alive()):
            raise Exception(f"UDF executor thread failed! traceback:\n{thread.traceback_str}")


@router.get("/jobs/{job_id}/results")
async def get_results(job_id: str = Path(...)):
    _check_udf_executor_thread()
    if SELF["current_job"] != job_id:
        return Response("job not found", status_code=404)

    results = []
    total_bytes = 0
    while not SELF["results_queue"].empty() and (total_bytes < (1_000_000 * 0.2)):
        try:
            result = SELF["results_queue"].get_nowait()
            results.append(result)
            total_bytes += len(result[2])
        except Empty:
            break

    await asyncio.sleep(0)
    response_json = {
        "results": results,
        "is_idle": SELF["IDLE"],  # <- used to determine if job is done
        "is_empty": SELF["results_queue"].empty(),
    }
    data = pickle.dumps(response_json)
    await asyncio.sleep(0)
    headers = {"Content-Disposition": 'attachment; filename="results.pkl"'}
    return Response(content=data, media_type="application/octet-stream", headers=headers)


@router.get("/jobs/{job_id}/inputs")
async def get_inputs(job_id: str = Path(...), min_reply_size: float = Query(...)):
    _check_udf_executor_thread()
    if SELF["current_job"] != job_id:
        return Response("job not found", status_code=404)

    inputs = []
    total_bytes = 0
    while not SELF["inputs_queue"].empty() and (total_bytes < min_reply_size):
        try:
            input_pkl_with_idx = SELF["inputs_queue"].get_nowait()
            inputs.append(input_pkl_with_idx)
            total_bytes += len(input_pkl_with_idx[1])
        except Empty:
            break

    await asyncio.sleep(0)
    data = pickle.dumps(inputs)
    headers = {"Content-Disposition": 'attachment; filename="inputs.pkl"'}
    return Response(content=data, media_type="application/octet-stream", headers=headers)


@router.post("/jobs/{job_id}/inputs")
async def upload_inputs(
    job_id: str = Path(...),
    request_files: Optional[dict] = Depends(get_request_files),
):
    _check_udf_executor_thread()
    if SELF["current_job"] != job_id:
        return Response("job not found", status_code=404)
    if SELF["STOP_PROCESSING_EVENT"].is_set():
        return Response("Cannot accept inputs - worker is shutting down", status_code=409)

    SELF["INPUT_UPLOAD_IN_PROGRESS"] = True
    pickled_inputs_pkl_with_idx = request_files["inputs_pkl_with_idx"]
    inputs_pkl_with_idx = pickle.loads(pickled_inputs_pkl_with_idx)
    await asyncio.sleep(0)

    total_data = sum(len(input_pkl) for input_pkl in inputs_pkl_with_idx)
    msg = f"Received {len(inputs_pkl_with_idx)} inputs for job {job_id} ({total_data} bytes)."
    SELF["logs"].append(msg)

    for input_pkl_with_idx in inputs_pkl_with_idx:
        SELF["inputs_queue"].put(input_pkl_with_idx)
    SELF["INPUT_UPLOAD_IN_PROGRESS"] = False


@router.post("/jobs/{job_id}")
async def start_job(
    job_id: str = Path(...),
    request_files: Optional[dict] = Depends(get_request_files),
):
    # only one job should ever be executed by this service
    # then it should be restarted (to clear/reset the filesystem)
    if SELF["STARTED"]:
        msg = f"ERROR: Received request to start job {job_id}, but this worker was previously "
        SELF["logs"].append(msg + f"assigned to job {SELF['job_id']}! Returning 409.")
        return Response("Already started.", status_code=409)

    SELF["logs"].append(f"Assigned to job {job_id}.")
    function_pkl = request_files["function_pkl"]
    thread = ThreadWithExc(target=execute_job, args=(job_id, function_pkl), daemon=True)
    thread.start()

    SELF["current_job"] = job_id
    SELF["udf_executor_thread"] = thread
    SELF["STARTED"] = True
    SELF["logs"].append(f"Successfully started job {job_id}.")


@router.post("/jobs/{job_id}/transfer_inputs")
async def transfer_inputs(
    job_id: str = Path(...),
    request_json: dict = Depends(get_request_json),
):
    """Stop processing the current job and transfer remaining inputs to another node"""
    if SELF["current_job"] != job_id:
        return Response("job not found", status_code=404)

    SELF["STOP_PROCESSING_EVENT"].set()
    if SELF["INPUT_UPLOAD_IN_PROGRESS"]:
        while SELF["INPUT_UPLOAD_IN_PROGRESS"]:
            await asyncio.sleep(1)

    target_node_url = request_json["target_node_url"]
    SELF["logs"].append(f"Received request to transfer inputs to {target_node_url}.")

    async with aiohttp.ClientSession() as session:
        chunk = [SELF["in_progress_input"]]
        total_bytes = len(SELF["in_progress_input"][1])
        total_inputs = 0
        while not SELF["inputs_queue"].empty():
            input_pkl_with_idx = SELF["inputs_queue"].get_nowait()
            chunk.append(input_pkl_with_idx)
            total_inputs += 1
            total_bytes += len(input_pkl_with_idx[1])

            if total_bytes > 1_000_000 * 0.2:
                data = aiohttp.FormData()
                data.add_field("inputs_pkl_with_idx", pickle.dumps(chunk))
                url = f"{target_node_url}/jobs/{job_id}/inputs"
                async with session.post(url, data=data) as response:
                    response.raise_for_status()
                    chunk = []
                    total_bytes = 0
    SELF["logs"].append(f"Transferred {total_inputs} remaining inputs to {target_node_url}.")
