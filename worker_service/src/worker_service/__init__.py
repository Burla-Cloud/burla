import os
import sys
import json
import requests
import traceback
from pathlib import Path
from threading import Event
import logging as python_logging

from fastapi import FastAPI, Request, Response
from starlette.datastructures import UploadFile


# silence logs so they are not picked up and sent to user
python_logging.getLogger("uvicorn").disabled = True
python_logging.getLogger("uvicorn.error").disabled = True
python_logging.getLogger("uvicorn.access").disabled = True
python_logging.getLogger("fastapi").disabled = True
python_logging.getLogger("starlette").disabled = True

# Defined before importing helpers/endpoints to prevent cyclic imports
IN_LOCAL_DEV_MODE = os.environ["IN_LOCAL_DEV_MODE"] == "True"
PROJECT_ID = os.environ["GOOGLE_CLOUD_PROJECT"]
INSTANCE_NAME = os.environ["INSTANCE_NAME"]
BURLA_BACKEND_URL = "https://backend.burla.dev"

# must be same path on node service!
ENV_IS_READY_PATH = Path("/worker_service_python_env/.ALL_PACKAGES_INSTALLED")

from worker_service.helpers import VerboseList, SizedQueue

SELF = {
    "STARTED": False,
    "IDLE": False,
    "current_job": None,
    "udf_executor_thread": None,
    "inputs_queue": SizedQueue(),
    "in_progress_input": None,  # needed so we can send ALL inputs elsewhere on shutdown
    "results_queue": SizedQueue(),
    "logs": VerboseList(print_on_append=IN_LOCAL_DEV_MODE),  # Buffer in list and flush on error
    "STOP_PROCESSING_EVENT": Event(),
    "INPUT_UPLOAD_IN_PROGRESS": False,
    "CURRENTLY_INSTALLING_PACKAGE": None,
    "ALL_PACKAGES_INSTALLED": False,
    "io_queues_ram_limit_gb": None,
    "udf_start_latency": None,
    "packages_to_install": None,
}


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
print(f"Worker {os.environ.get('WORKER_NAME', 'unknown_worker')} has booted.")


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

        # Log all the logs that led up to this error:
        # We can't always log to GCL because so many workers are running at once it just breaks.
        # Therefore we only save the logs when error (and pray they dont all error at once)
        # printing stuff here sends it to gcl from the node service.
        if not IN_LOCAL_DEV_MODE:
            print("\n".join(SELF["logs"]))
        print(traceback_str, file=sys.stderr)

        # Report errors back to Burla's cloud.
        try:
            json = {"project_id": PROJECT_ID, "message": exc_type, "traceback": traceback_str}
            requests.post(f"{BURLA_BACKEND_URL}/v1/telemetry/log/ERROR", json=json, timeout=1)
        except Exception:
            pass

    return response
