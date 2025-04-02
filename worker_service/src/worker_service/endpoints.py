import pickle
from io import BytesIO
from time import time

from flask import jsonify, Blueprint, request, send_file

from worker_service import SELF
from worker_service.udf_executor import execute_job
from worker_service.helpers import ThreadWithExc

BP = Blueprint("endpoints", __name__)
ERROR_ALREADY_LOGGED = False


@BP.get("/")
def get_status():
    global ERROR_ALREADY_LOGGED
    traceback_str = SELF["subjob_thread"].traceback_str if SELF["subjob_thread"] else None
    thread_died = SELF["subjob_thread"] and (not SELF["subjob_thread"].is_alive())

    READY = not SELF["STARTED"]
    FAILED = traceback_str or thread_died

    if READY:
        return jsonify({"status": "READY"})
    elif FAILED:
        return jsonify({"status": "FAILED"})
    else:
        return jsonify({"status": "BUSY"})


@BP.get("/jobs/<job_id>/results")
def get_results(job_id: str):
    results = []
    while not SELF["result_queue"].empty():
        results.append(SELF["result_queue"].get())

    data = BytesIO(pickle.dumps(results))
    data.seek(0)  # <- ai told me to put this here idk why
    mimetype = "application/octet-stream"
    return send_file(data, mimetype=mimetype, as_attachment=True, download_name="results.pkl")


@BP.post("/jobs/<job_id>/inputs")
def upload_inputs(job_id: str):
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

    # ThreadWithExc is a thread that catches and stores errors.
    # We need so we can save the error until the status of this service is checked.
    args = (job_id, function_pkl)
    thread = ThreadWithExc(target=execute_job, args=args)
    thread.start()

    SELF["current_job"] = job_id
    SELF["subjob_thread"] = thread
    SELF["STARTED"] = True
    SELF["started_at"] = time()

    SELF["logs"].append(f"Successfully started job {job_id}.")
    return "Success"
