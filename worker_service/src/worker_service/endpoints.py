import pickle
from io import BytesIO
import requests
from typing import Optional
from queue import Empty
from fastapi import APIRouter, Path, Response, Depends, Request
from fastapi.responses import StreamingResponse

from worker_service import SELF, get_request_json, get_request_files
from worker_service.udf_executor import execute_job
from worker_service.helpers import ThreadWithExc

router = APIRouter()


@router.get("/")
def get_status():
    # A worker can also be "IDLE" but that is returned when checking for results (more efficient)
    if SELF["STARTED"]:
        return {"status": "BUSY"}
    else:
        return {"status": "READY"}


def _check_udf_executor_thread():
    if SELF["current_job"]:
        thread = SELF.get("udf_executor_thread")
        if not (thread and thread.is_alive()):
            raise Exception(f"UDF executor thread failed! traceback:\n{thread.traceback_str}")


@router.get("/jobs/{job_id}/results")
def get_results(job_id: str = Path(...)):
    _check_udf_executor_thread()
    if SELF["current_job"] != job_id:
        return Response("job not found", status_code=404)

    results = []
    while not SELF["result_queue"].empty():
        try:
            results.append(SELF["result_queue"].get_nowait())
        except Empty:
            break

    # `IDLE` is used to determine if job is done
    data = BytesIO(pickle.dumps({"results": results, "is_idle": SELF["IDLE"]}))
    data.seek(0)  # ensure file pointer is at the beginning of the file.
    headers = {"Content-Disposition": 'attachment; filename="results.pkl"'}
    return StreamingResponse(data, media_type="application/octet-stream", headers=headers)


@router.get("/jobs/{job_id}/inputs")
def get_half_inputs(job_id: str = Path(...)):
    _check_udf_executor_thread()
    if SELF["current_job"] != job_id:
        return Response("job not found", status_code=404)

    # gather half of the inputs from the queue
    inputs = []
    qsize = SELF["inputs_queue"].qsize()

    n_inputs_to_send = qsize if qsize <= 1 else int(qsize // 1.5)  # <- actually takes more like 2/3
    for _ in range(n_inputs_to_send):
        try:
            inputs.append(SELF["inputs_queue"].get_nowait())
        except Empty:
            break

    if not inputs:
        return Response(status_code=204)
    else:
        SELF["logs"].append(f"queue had size {qsize} sending {len(inputs)} inputs to another node")

    data = BytesIO(pickle.dumps(inputs))
    data.seek(0)  # ensure file pointer is at the beginning of the file.
    headers = {"Content-Disposition": 'attachment; filename="inputs.pkl"'}
    return StreamingResponse(data, media_type="application/octet-stream", headers=headers)


@router.post("/jobs/{job_id}/inputs")
def upload_inputs(
    job_id: str = Path(...),
    request_files: Optional[dict] = Depends(get_request_files),
):
    _check_udf_executor_thread()
    if SELF["current_job"] != job_id:
        return Response("job not found", status_code=404)

    pickled_inputs_pkl_with_idx = request_files["inputs_pkl_with_idx"]
    inputs_pkl_with_idx = pickle.loads(pickled_inputs_pkl_with_idx)

    total_data = sum(len(input_pkl) for input_pkl in inputs_pkl_with_idx)
    msg = f"Received {len(inputs_pkl_with_idx)} inputs for job {job_id} ({total_data} bytes)."
    SELF["logs"].append(msg)

    for input_pkl_with_idx in inputs_pkl_with_idx:
        SELF["inputs_queue"].put(input_pkl_with_idx)


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
def transfer_inputs(
    job_id: str = Path(...),
    request_json: dict = Depends(get_request_json),
):
    """Stop processing the current job and transfer remaining inputs to another node"""
    if SELF["current_job"] != job_id:
        return Response("job not found", status_code=404)

    SELF["STOP_PROCESSING_EVENT"].set()
    target_node_url = request_json["target_node_url"]
    SELF["logs"].append(f"Received request to transfer inputs to {target_node_url}.")

    remaining_inputs = []
    while not SELF["inputs_queue"].empty():
        remaining_inputs.append(SELF["inputs_queue"].get())

    if SELF["current_in_progress_input"]:
        remaining_inputs.append(SELF["current_in_progress_input"])

    files = {"inputs_pkl_with_idx": pickle.dumps(remaining_inputs)}
    response = requests.post(f"{target_node_url}/jobs/{job_id}/inputs", files=files)
    response.raise_for_status()
    SELF["logs"].append(f"Sent {len(remaining_inputs)} inputs to {target_node_url}.")
