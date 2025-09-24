import os
import pickle
import asyncio
import signal
import aiohttp
from typing import Optional
from queue import Empty

from fastapi import APIRouter, Path, Response, Depends, Query

from worker_service import SELF, get_request_json, get_request_files
from worker_service.udf_executor import install_pkgs_and_execute_job
from worker_service.helpers import ThreadWithExc

router = APIRouter()


@router.get("/")
async def get_status():
    # A worker can also be "IDLE" but that is returned when checking for results (more efficient)
    if SELF["STARTED"]:
        return {"status": "BUSY"}
    else:
        return {"status": "READY"}


@router.get("/restart")
async def restart():
    # Used to cancel running user jobs.
    # User jobs currently run in a thread (not cancelable)
    # I don't want to make them run in process (cancelable) because it's slower and annoying.
    # Here as a hack I just kill the entire worker service, from inside itself, it's automatically
    # restarted by the while loop in the bash script the container was started with.

    # dont need to append to logs because restart wipes it anyway
    print(f"Restarting worker service, killing process: {os.getpid()}", flush=True)

    os.kill(os.getpid(), signal.SIGTERM)
    # Can't use SELF["STOP_PROCESSING_EVENT"] because it dosent force restart immmediately.


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
    while not SELF["results_queue"].empty() and (total_bytes < (1_000_000 * 0.1)):
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
        "currently_installing_package": SELF["CURRENTLY_INSTALLING_PACKAGE"],
        "all_packages_installed": SELF["ALL_PACKAGES_INSTALLED"],  # required, see udf_executor
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

    # will this make the input queue too big?
    size_limit_gb = SELF["io_queues_ram_limit_gb"] / 2
    new_inputs_size_gb = len(pickled_inputs_pkl_with_idx) / (1024**3)
    future_queue_size_gb = SELF["inputs_queue"].size_gb + new_inputs_size_gb
    if future_queue_size_gb > size_limit_gb:
        msg = f"Cannot accept {new_inputs_size_gb}GB input chunk, "
        msg += f"input queue would exceed size limit of {size_limit_gb} GB"
        return Response(msg, status_code=409)

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
    request_json: dict = Depends(get_request_json),
):
    SELF["logs"].append(f"Assigned to job {job_id}.")
    args = (job_id, request_files["function_pkl"], request_json["packages"])
    thread = ThreadWithExc(target=install_pkgs_and_execute_job, args=args, daemon=True)
    thread.start()

    SELF["current_job"] = job_id
    SELF["udf_executor_thread"] = thread
    SELF["io_queues_ram_limit_gb"] = request_json["io_queues_ram_limit_gb"]
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

        # TODO: it's possible to finish this input and return it after it's been sent to another
        # node causing duplicate execution?
        # instead check at the end if this input is done and send it last if not then immediately
        # cancel so it can't finish
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
