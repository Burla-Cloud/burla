import os
import sys
import json
import traceback

from flask import Flask, request, abort
from google.cloud import logging

# Defined before importing helpers/endpoints to prevent cyclic imports
IN_LOCAL_DEV_MODE = os.environ.get("IN_LOCAL_DEV_MODE") == "True"
PROJECT_ID = os.environ.get("PROJECT_ID")
JOBS_BUCKET = f"burla-jobs--{PROJECT_ID}"

from worker_service.helpers import VerboseList  # <- same as a list but prints stuff you append.

# we append all logs to a list instead of sending them to google cloud logging because
# there are so many logs that logging them all causes issues and slowness.
# By adding them to a list we can write the logs out if an an error occurs,
# or simply do nothing with them when there is no error.
SELF = {
    "STARTED": False,
    "DONE": False,
    "job_id": None,
    "subjob_thread": None,
    "WORKER_LOGS": VerboseList() if IN_LOCAL_DEV_MODE else list(),
    "started_at": None,
    "starting_index": None,
}
LOGGER = logging.Client().logger("worker_service")

from worker_service.endpoints import BP as endpoints_bp

app = Flask(__name__)
app.register_blueprint(endpoints_bp)


@app.errorhandler(Exception)
def log_exception(exception):
    """
    Logs any exceptions thrown inside a request.
    """
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
    traceback_str = "".join(traceback_details)

    try:
        request_json = json.dumps(vars(request))
    except:
        request_json = "Unable to serialize request."

    if exception and IN_LOCAL_DEV_MODE:
        print(traceback_str, file=sys.stderr)
    elif exception:
        log = {"severity": "ERROR", "exception": traceback_str, "request": request_json}
        LOGGER.log_struct(log)

    # Report errors back to Burla's cloud.
    try:
        json = {"project_id": PROJECT_ID, "message": exc_type, "traceback": traceback_str}
        requests.post(f"{BURLA_BACKEND_URL}/v1/private/log_error", json=json, timeout=1)
    except Exception:
        pass

    abort(500)
