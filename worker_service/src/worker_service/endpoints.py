import sys
import pickle

from flask import jsonify, Blueprint, request

from worker_service import SELF, LOGGER, IN_LOCAL_DEV_MODE
from worker_service.udf_executor import execute_job
from worker_service.helpers import ThreadWithExc

BP = Blueprint("endpoints", __name__)
ERROR_ALREADY_LOGGED = False

from time import time


@BP.get("/")
def get_status():
    """
    There are four possible states:
     1. “READY”: This service is ready to start processing a subjob.
     2. “RUNNING”: This service is processing a subjob.
     3. “FAILED”: This service had an internal error and failed to process the subjob.
     4. “DONE”: This worker successfully processed the subjob.
    """
    global ERROR_ALREADY_LOGGED
    bar = "----------------------------------"
    status_log = f"{bar}\nReceived request to get worker status.\n"

    thread_is_running = SELF["subjob_thread"] and SELF["subjob_thread"].is_alive()
    thread_traceback_str = SELF["subjob_thread"].traceback_str if SELF["subjob_thread"] else None
    thread_died = SELF["subjob_thread"] and (not SELF["subjob_thread"].is_alive())

    READY = not SELF["STARTED"]
    RUNNING = thread_is_running and (not thread_traceback_str) and (not SELF["DONE"])
    FAILED = thread_traceback_str or (thread_died and not SELF["DONE"])
    DONE = SELF["DONE"]

    # print error if in development so I dont need to go to google cloud logging to see it
    if IN_LOCAL_DEV_MODE and SELF["subjob_thread"] and SELF["subjob_thread"].traceback_str:
        status_log += "ERROR DETECTED IN WORKER THREAD (printing in stderr).\n"
        print(thread_traceback_str, file=sys.stderr)

    if FAILED and not ERROR_ALREADY_LOGGED:
        status_log += "ERROR DETECTED IN WORKER THREAD (logging in GCL).\n"
        struct = {"severity": "ERROR", "worker_logs": SELF["WORKER_LOGS"]}
        if thread_traceback_str:
            struct.update({"traceback": thread_traceback_str})
        else:
            struct.update({"message": "Subjob thread died without error!"})
        LOGGER.log_struct(struct)
        ERROR_ALREADY_LOGGED = True

    if READY:
        status = "READY"
    elif RUNNING:
        status = "RUNNING"
    elif FAILED:
        status = "FAILED"
    elif DONE:
        status = "DONE"

    status_log += f"Status = {status}"
    SELF["WORKER_LOGS"].append(f"{status_log}\n{bar}")
    return jsonify({"status": status})


@BP.post("/jobs/<job_id>")
def start_job(job_id: str):
    # only one job will ever be executed by this service
    if SELF["STARTED"]:
        return "STARTED", 409

    request_json = pickle.loads(request.files["request_json"].read())
    function_pkl = request.files.get("function_pkl")
    if function_pkl:
        function_pkl = function_pkl.read()

    SELF["WORKER_LOGS"].append(f"Executing job {job_id}.")
    SELF["WORKER_LOGS"].append(f"STARTING WORK AT INDEX #{request_json['starting_index']}")

    # ThreadWithExc is a thread that catches and stores errors.
    # We need so we can save the error until the status of this service is checked.
    args = (
        job_id,
        request_json["inputs_id"],
        request_json["n_inputs"],
        request_json["starting_index"],
        request_json["planned_future_job_parallelism"],
        function_pkl,
    )
    thread = ThreadWithExc(target=execute_job, args=args)
    thread.start()

    SELF["current_job"] = job_id
    SELF["subjob_thread"] = thread
    SELF["STARTED"] = True
    SELF["started_at"] = time()
    SELF["starting_index"] = request_json["starting_index"]

    return "Success"
