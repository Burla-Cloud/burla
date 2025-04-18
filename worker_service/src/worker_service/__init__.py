import os
import sys
import json
import requests
import traceback
from queue import Queue
from threading import Event
import logging as python_logging

import google.auth
from google.cloud import logging
from fastapi import FastAPI, Request, Response
from starlette.datastructures import UploadFile

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
    "IDLE": False,
    "job_id": None,
    "udf_executor_thread": None,
    "inputs_queue": Queue(),
    "in_progress_input": None,  # needed so we can send ALL inputs elsewhere on shutdown
    "result_queue": Queue(),
    "started_at": None,
    "logs": verbose_list,
    "STOP_PROCESSING_EVENT": Event(),
}


# Silence fastapi logs coming from the /results endpoint, there are so many it slows stuff down.
class ResultsEndpointFilter(python_logging.Filter):
    def filter(self, record):
        return not record.args[2].endswith("/results")


python_logging.getLogger("uvicorn.access").addFilter(ResultsEndpointFilter())


async def get_request_json(request: Request):
    try:
        return await request.json()
    except:
        form_data = await request.form()
        return json.loads(form_data["request_json"])


async def get_request_files(request: Request):
    """
    If request is multipart/form data load all files and returns as dict of {filename: bytes}
    """
    form_data = await request.form()
    files = {}
    for key, value in form_data.items():
        if isinstance(value, UploadFile):
            files.update({key: await value.read()})
    if files:
        return files


from worker_service.endpoints import router as endpoints_router

app = FastAPI(docs_url=None, redoc_url=None)
app.include_router(endpoints_router)


@app.middleware("http")
async def log_and_time_requests__log_errors(request: Request, call_next):
    """
    Fastapi `@app.exception_handler` will completely hide errors if middleware is used.
    Catching errors in a `Depends` function will not distinguish http errors
        originating here vs from other services.
    """
    try:
        # Important to note that HTTP exceptions do not raise errors here!
        response = await call_next(request)
    except Exception:
        response = Response(status_code=500, content="Internal server error.")
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
        traceback_str = "".join(traceback_details)
        print(traceback_str, file=sys.stderr)

        # Log all the logs that led up to this error:
        # We can't always log to GCL because so many workers are running at once it just breaks.
        # Therefore we only save the logs when error (and pray they dont all error at once)
        if not IN_LOCAL_DEV_MODE:
            for log in SELF["logs"]:
                LOGGER.log(log)
            LOGGER.log_struct(dict(severity="ERROR", exception=traceback_str))

        # Report errors back to Burla's cloud.
        try:
            json = {"project_id": PROJECT_ID, "message": exc_type, "traceback": traceback_str}
            requests.post(f"{BURLA_BACKEND_URL}/v1/telemetry/alert", json=json, timeout=1)
        except Exception:
            pass

    return response
