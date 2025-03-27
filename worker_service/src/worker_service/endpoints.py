import sys
import pickle
from time import time

from flask import jsonify, Blueprint, request

from worker_service import SELF, LOGGER, IN_LOCAL_DEV_MODE
from worker_service.udf_executor import execute_job
from worker_service.helpers import ThreadWithExc

BP = Blueprint("endpoints", __name__)
ERROR_ALREADY_LOGGED = False


@BP.get("/")
def get_status():
    global ERROR_ALREADY_LOGGED
    no_tb_msg = "UDF Executor thread is dead with no errors."
    traceback_str = SELF["subjob_thread"].traceback_str if SELF["subjob_thread"] else no_tb_msg
    thread_died = SELF["subjob_thread"] and (not SELF["subjob_thread"].is_alive())

    READY = not SELF["STARTED"]
    FAILED = traceback_str or thread_died

    if FAILED and (not ERROR_ALREADY_LOGGED):
        # Log all the logs that led up to this error:
        # We can't always log to GCL because so many workers are running at once it just breaks.
        # -> We only save the logs when there is an error (and pray they dont all error at once).
        if not IN_LOCAL_DEV_MODE:
            for log in SELF["logs"]:
                LOGGER.log(log)
            LOGGER.log_struct({"severity": "ERROR", "exception": traceback_str})

        print(traceback_str, file=sys.stderr)
        ERROR_ALREADY_LOGGED = True

    if READY:
        return jsonify({"status": "READY"})
    elif FAILED:
        return jsonify({"status": "FAILED"})
    else:
        return jsonify({"status": "BUSY"})


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
