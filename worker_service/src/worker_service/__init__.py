import os
import sys
import json
import requests
import traceback
from queue import Queue
from threading import Event
import logging as python_logging

from fastapi import FastAPI, Request, Response
from starlette.datastructures import UploadFile

# Defined before importing helpers/endpoints to prevent cyclic imports
IN_LOCAL_DEV_MODE = os.environ.get("IN_LOCAL_DEV_MODE") == "True"
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
BURLA_BACKEND_URL = "https://backend.burla.dev"

from worker_service.helpers import VerboseList  # <- same as a list but prints stuff you append


def REINIT_SELF(SELF):
    # we append all logs to a list instead of sending them to google cloud logging because
    # there are so many logs that logging them all causes issues and slowness.
    # By adding them to a list we can write the logs out if an an error occurs,
    # or simply do nothing with them when there is no error.
    verbose_list = VerboseList(print_on_append=IN_LOCAL_DEV_MODE)

    if SELF.get("udf_executor_thread"):
        SELF["STOP_PROCESSING_EVENT"].set()
        SELF["udf_executor_thread"].join()

    SELF["STARTED"] = False
    SELF["IDLE"] = False
    SELF["current_job"] = None
    SELF["udf_executor_thread"] = None
    SELF["inputs_queue"] = Queue()
    SELF["in_progress_input"] = None  # needed so we can send ALL inputs elsewhere on shutdown
    SELF["results_queue"] = Queue()
    SELF["logs"] = verbose_list
    SELF["STOP_PROCESSING_EVENT"] = Event()
    SELF["INPUT_UPLOAD_IN_PROGRESS"] = False


SELF = {}
REINIT_SELF(SELF)


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
