import pickle
from io import BytesIO
import requests

from flask import jsonify, Blueprint, request, send_file, Response

from worker_service import SELF
from worker_service.udf_executor import execute_job
from worker_service.helpers import ThreadWithExc

BP = Blueprint("endpoints", __name__)
ERROR_ALREADY_LOGGED = False


@BP.get("/")
def get_status():
    # A worker can also be "IDLE" but that is returned when checking for results (more efficient)
    if SELF["STARTED"]:
        return jsonify({"status": "BUSY"})
    else:
        return jsonify({"status": "READY"})


def _check_udf_executor_thread():
    if not SELF["subjob_thread"].is_alive():
        traceback_str = getattr(SELF["subjob_thread"], "traceback_str", None)
        if traceback_str:
            raise Exception(f"UDF executor thread failed with traceback:\n{traceback_str}")
        else:
            raise Exception(f"UDF executor thread died with no errors")


def _check_correct_job(job_id: str):
    if SELF["current_job"] != job_id:
        raise Exception(f"Job {job_id} is not the current job")


@BP.get("/jobs/<job_id>/results")
def get_results(job_id: str):
    _check_udf_executor_thread()
    _check_correct_job(job_id)

    results = []
    while not SELF["result_queue"].empty():
        results.append(SELF["result_queue"].get())

    # `IDLE` is used to determine if job is done
    data = BytesIO(pickle.dumps({"results": results, "is_idle": SELF["IDLE"]}))
    data.seek(0)  # ensure file pointer is at the beginning of the file.
    mimetype = "application/octet-stream"
    return send_file(data, mimetype=mimetype, as_attachment=True, download_name="results.pkl")


@BP.get("/jobs/<job_id>/inputs")
def get_half_inputs(job_id: str):
    _check_udf_executor_thread()
    _check_correct_job(job_id)
    if SELF["inputs_queue"].empty():
        return Response(status_code=204)

    # gather half of the inputs from the queue
    inputs = []
    for _ in range(SELF["inputs_queue"].qsize() // 2):
        if not SELF["inputs_queue"].empty():
            inputs.append(SELF["inputs_queue"].get())

    if not inputs:
        return Response(status_code=204)

    data = BytesIO(pickle.dumps(inputs))
    data.seek(0)  # ensure file pointer is at the beginning of the file.
    mimetype = "application/octet-stream"
    return send_file(data, mimetype=mimetype, as_attachment=True, download_name="inputs.pkl")


@BP.post("/jobs/<job_id>/inputs")
def upload_inputs(job_id: str):
    _check_udf_executor_thread()
    _check_correct_job(job_id)

    pickled_inputs_pkl_with_idx = request.files["inputs_pkl_with_idx"].read()
    inputs_pkl_with_idx = pickle.loads(pickled_inputs_pkl_with_idx)

    total_data = sum(len(input_pkl) for input_pkl in inputs_pkl_with_idx)
    msg = f"Received {len(inputs_pkl_with_idx)} inputs for job {job_id} ({total_data} bytes)."
    SELF["logs"].append(msg)

    for input_pkl_with_idx in inputs_pkl_with_idx:
        SELF["inputs_queue"].put(input_pkl_with_idx)

    return "Success"


@BP.post("/jobs/<job_id>")
def start_job(job_id: str):
    # only one job should ever be executed by this service
    # then it should be restarted (to clear/reset the filesystem)
    if SELF["STARTED"]:
        msg = f"ERROR: Received request to start job {job_id}, but this worker was previously "
        SELF["logs"].append(msg + f"assigned to job {SELF['job_id']}! Returning 409.")
        return "STARTED", 409

    SELF["logs"].append(f"Assigned to job {job_id}.")
    function_pkl = request.files.get("function_pkl")
    function_pkl = function_pkl.read()
    SELF["logs"].append("Successfully downloaded user function.")

    # ThreadWithExc is a thread wrapper that catches and stores errors.
    args = (job_id, function_pkl)
    thread = ThreadWithExc(target=execute_job, args=args, daemon=True)
    thread.start()

    SELF["current_job"] = job_id
    SELF["subjob_thread"] = thread
    SELF["STARTED"] = True

    SELF["logs"].append(f"Successfully started job {job_id}.")
    return "Success"


@BP.post("/jobs/<job_id>/transfer_inputs")
def transfer_inputs(job_id: str):
    """Stop processing the current job and transfer remaining inputs to another node"""
    _check_correct_job(job_id)
    SELF["STOP_PROCESSING_EVENT"].set()
    target_node_url = request.json.get("target_node_url")
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
    return "Success"
