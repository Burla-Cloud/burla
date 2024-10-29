import os
import sys
import json
import traceback

from flask import Flask, request, abort
from google.cloud import logging

# Defined before importing endpoints to prevent cyclic imports
IN_DEV = os.environ.get("IN_DEV") == "True"
PROJECT_ID = os.environ.get("PROJECT_ID")
JOBS_BUCKET = f"burla-jobs--{PROJECT_ID}"

SELF = {
    "STARTED": False,
    "DONE": False,
    "job_id": None,
    "subjob_thread": None,
    "WORKER_LOGS": [],
    "started_at": None,
    "starting_index": None,
}
LOGGER = logging.Client().logger("container_service")

from container_service.endpoints import BP as endpoints_bp

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

    if exception and IN_DEV:
        print(traceback_str, file=sys.stderr)
    elif exception:
        log = {"severity": "ERROR", "exception": traceback_str, "request": request_json}
        LOGGER.log_struct(log)

    abort(500)
