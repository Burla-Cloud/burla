import pickle
from io import BytesIO

from flask import jsonify, Blueprint, request, send_file

from worker_service import SELF
from worker_service.udf_executor import execute_job
from worker_service.helpers import ThreadWithExc

BP = Blueprint("endpoints", __name__)
ERROR_ALREADY_LOGGED = False


@BP.get("/")
def get_status():
    if not SELF["STARTED"]:
        return jsonify({"status": "READY"})
    else:
        return jsonify({"status": "BUSY"})


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

    data = BytesIO(pickle.dumps(results))
    data.seek(0)  # <- artificial intelligence told me to put this here idk why
    mimetype = "application/octet-stream"
    return send_file(data, mimetype=mimetype, as_attachment=True, download_name="results.pkl")


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
    thread = ThreadWithExc(target=execute_job, args=args)
    thread.start()

    SELF["current_job"] = job_id
    SELF["subjob_thread"] = thread
    SELF["STARTED"] = True

    SELF["logs"].append(f"Successfully started job {job_id}.")
    return "Success"
