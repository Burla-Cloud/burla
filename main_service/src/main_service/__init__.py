import sys
import os
import json
import traceback
from uuid import uuid4
from time import time, sleep
from typing import Callable
from pathlib import Path
from requests.exceptions import HTTPError
from contextlib import asynccontextmanager

import google.auth
from google.cloud import firestore, logging
from fastapi.responses import Response, FileResponse
from fastapi import FastAPI, Request, BackgroundTasks, Depends
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from starlette.datastructures import UploadFile

os.environ["GRPC_VERBOSITY"] = "ERROR"
os.environ["GLOG_minloglevel"] = "2"

# This is the only possible alternative "mode".
# In this mode everything runs locally in docker containers.
IN_LOCAL_DEV_MODE = os.environ.get("IN_LOCAL_DEV_MODE") == "True"

CREDENTIALS, PROJECT_ID = google.auth.default()
BURLA_BACKEND_URL = "https://backend.burla.dev"
GCL_CLIENT = logging.Client().logger("main_service")
DB = firestore.Client(database="burla")

LOCAL_DEV_CONFIG = {  # <- config used only in local dev mode
    "Nodes": [
        {
            "containers": [
                {
                    "image": "us-docker.pkg.dev/burla-test/burla-worker-service/burla-worker-service:latest",
                    "python_executable": "python3.11",
                    "python_version": "3.11",
                },
            ],
            "machine_type": "n4-standard-2",  # <- means nothing here, this is set in node __init__
            "quantity": 2,
            "inactivity_shutdown_time_sec": 60 * 10,
        }
    ]
}
DEFAULT_CONFIG = {  # <- config used only when config is missing from firestore
    "Nodes": [
        {
            "containers": [
                {
                    "image": "jakezuliani/burla_worker_service:latest",
                    "python_executable": "python3.11",
                    "python_version": "3.11",
                },
            ],
            "machine_type": "n4-standard-2",
            "quantity": 2,
            "inactivity_shutdown_time_sec": 60 * 10,
        }
    ]
}

from main_service.helpers import validate_headers_and_login, Logger, format_traceback


async def get_request_json(request: Request):
    try:
        return await request.json()
    except:
        form_data = await request.form()
        return json.loads(form_data["request_json"])


async def get_request_files(request: Request):
    """Used to send UDF, returns as dict of {filename: bytes}"""
    form_data = await request.form()
    files = {}
    for key, value in form_data.items():
        if isinstance(value, UploadFile):
            files.update({key: await value.read()})

    if files:
        return files


def get_logger(request: Request):
    return Logger(request)


def get_user_email(request: Request):
    return request.state.user_email


def get_add_background_task_function(
    background_tasks: BackgroundTasks, logger: Logger = Depends(get_logger)
):
    def add_logged_background_task(func: Callable, *a, **kw):
        tb_details = traceback.format_list(traceback.extract_stack()[:-1])
        parent_traceback = "Traceback (most recent call last):\n" + format_traceback(tb_details)

        def func_logged(*a, **kw):
            try:
                return func(*a, **kw)
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
                local_traceback_no_title = "\n".join(format_traceback(tb_details).split("\n")[1:])
                traceback_str = parent_traceback + local_traceback_no_title
                logger.log(message=str(e), severity="ERROR", traceback=traceback_str)

        background_tasks.add_task(func_logged, *a, **kw)

    return add_logged_background_task


from main_service.endpoints.cluster import router as cluster_router


@asynccontextmanager
async def lifespan(app: FastAPI):

    if IN_LOCAL_DEV_MODE:

        def frontend_built_successfully(attempt=1):
            if attempt == 3:
                return False
            else:
                frontend_built_at = float(Path(".frontend_last_built_at.txt").read_text().strip())
                frontend_rebuilt = time() - frontend_built_at < 4
                if not frontend_rebuilt:
                    sleep(2)  # wait a couple sec then try again (could still be building)
                    return frontend_built_successfully(attempt=attempt + 1)
                return True

        if frontend_built_successfully():
            print(f"Successfully rebuilt frontend.")
        else:
            print(f"FAILED to rebuild frontend?, check logs with `Cmd + Shift + U`.")

    yield


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None)
app.include_router(cluster_router)
app.add_middleware(SessionMiddleware, secret_key=uuid4().hex)


# don't move this function! must be declared before static files are mounted to the same path below.
@app.get("/")
def dashboard():
    return FileResponse("src/main_service/static/index.html")


# must be mounted after the above endpoint (`/`) is declared, or this will overwrite that endpoint.
app.mount("/", StaticFiles(directory="src/main_service/static"), name="static")


@app.middleware("http")
async def login__log_and_time_requests__log_errors(request: Request, call_next):
    """
    Fastapi `@app.exception_handler` will completely hide errors if middleware is used.
    Catching errors in a `Depends` function will not distinguish
        http errors originating here vs other services.
    """
    start = time()
    request.state.uuid = uuid4().hex
    url_path = Path(request.url.path)

    # Check if requesting a static file from root URL
    static_dir = Path("src/main_service/static")
    requested_file = static_dir / url_path.relative_to("/")
    requesting_static_file = requested_file.exists() and requested_file.is_file()

    public_endpoints = ["/", "/v1/cluster", "/v1/cluster/restart", "/v1/cluster/shutdown"]
    requesting_public_endpoint = str(url_path) in public_endpoints
    request_requires_auth = not (requesting_public_endpoint or requesting_static_file)

    if request_requires_auth:
        try:
            user_info = validate_headers_and_login(request)
            request.state.user_email = user_info.get("email")
        except HTTPError as e:
            if "401" in str(e):
                return Response(status_code=401, content="Unauthorized.")
            else:
                raise e

    # If `get_logger` was a dependency this will be the second time a Logger is created.
    # This is fine because creating this object only attaches the `request` to a function.
    logger = Logger(request)

    # Important to note that HTTP exceptions do not raise errors here!
    try:
        response = await call_next(request)
    except Exception as e:
        # create new response object to return gracefully.
        response = Response(status_code=500, content="Internal server error.")
        response.background = BackgroundTasks()
        add_background_task = get_add_background_task_function(response.background, logger=logger)

        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
        traceback_str = format_traceback(tb_details)
        add_background_task(logger.log, str(e), "ERROR", traceback=traceback_str)

    response_contains_background_tasks = getattr(response, "background") is not None
    if not response_contains_background_tasks:
        response.background = BackgroundTasks()
    add_background_task = get_add_background_task_function(response.background, logger=logger)

    if not IN_LOCAL_DEV_MODE:
        msg = f"Received {request.method} at {request.url}"
        add_background_task(logger.log, msg)

        status = response.status_code
        latency = time() - start
        msg = f"{request.method} to {request.url} returned {status} after {latency} seconds."
        add_background_task(logger.log, msg, latency=latency)

    return response
