import os
import sys
import requests
import traceback
from queue import Queue


from flask import Flask, request, abort
import google.auth
from google.cloud import logging


# Defined before importing helpers/endpoints to prevent cyclic imports
IN_LOCAL_DEV_MODE = os.environ.get("IN_LOCAL_DEV_MODE") == "True"
SEND_LOGS_TO_GCL = os.environ.get("SEND_LOGS_TO_GCL") == "True"

CREDENTIALS, PROJECT_ID = google.auth.default()  # need `CREDENTIALS` so token can be refreshed
BURLA_BACKEND_URL = "https://backend.burla.dev"

name = os.environ.get("WORKER_NAME", "unknown_worker")
LOGGER = logging.Client().logger("worker_service", labels={"worker_name": name})
if SEND_LOGS_TO_GCL and (not IN_LOCAL_DEV_MODE):
    LOGGER.log(f"Worker {name} has booted and will send all logs to GCL.")

from worker_service.helpers import VerboseList  # <- same as a list but prints/logs stuff you append

# we append all logs to a list instead of sending them to google cloud logging because
# there are so many logs that logging them all causes issues and slowness.
# By adding them to a list we can write the logs out if an an error occurs,
# or simply do nothing with them when there is no error.
verbose_list = VerboseList(log_on_append=SEND_LOGS_TO_GCL, print_on_append=IN_LOCAL_DEV_MODE)
SELF = {
    "STARTED": False,
    "DONE": False,
    "job_id": None,
    "subjob_thread": None,
    "inputs_queue": Queue(),
    "result_queue": Queue(),
    "started_at": None,
    "logs": verbose_list,
}

from worker_service.endpoints import BP as endpoints_bp

app = Flask(__name__)
app.register_blueprint(endpoints_bp)


@app.errorhandler(Exception)
def log_exception(exception):
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
    traceback_str = "".join(traceback_details)
    print(traceback_str, file=sys.stderr)

    try:
        request_json = json.dumps(vars(request))
    except:
        request_json = "Unable to serialize request."

    # Log all the logs that led up to this error:
    # We can't always log to GCL because so many workers are running at once it just breaks.
    # Therefore we only save the logs when there is an error (and pray they dont all error at once)
    if not IN_LOCAL_DEV_MODE:
        for log in SELF["logs"]:
            LOGGER.log(log)
        LOGGER.log_struct(dict(severity="ERROR", exception=traceback_str, request=request_json))

    # Report errors back to Burla's cloud.
    try:
        json = {"project_id": PROJECT_ID, "message": exc_type, "traceback": traceback_str}
        requests.post(f"{BURLA_BACKEND_URL}/v1/telemetry/alert", json=json, timeout=1)
    except Exception:
        pass

    abort(500)
