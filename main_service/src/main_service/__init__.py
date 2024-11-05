import sys
import os
import json
import traceback
from uuid import uuid4
from time import time
from typing import Callable
from requests.exceptions import HTTPError
from contextlib import asynccontextmanager

from google.cloud import firestore, logging
from fastapi.responses import Response, FileResponse, RedirectResponse
from fastapi import FastAPI, Request, BackgroundTasks, Depends
from fastapi.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool

from starlette.middleware.sessions import SessionMiddleware
from starlette.datastructures import UploadFile

os.environ["GRPC_VERBOSITY"] = "ERROR"
os.environ["GLOG_minloglevel"] = "2"

ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")  # <- only used in dev
PROJECT_ID = os.environ.get("PROJECT_ID")
BURLA_BACKEND_URL = "https://backend.burla.dev"

# reduces number of instances / saves across some requests as opposed to using Depends
GCL_CLIENT = logging.Client().logger("main_service")
DB = firestore.Client(project=PROJECT_ID)

IN_LOCAL_DEV_MODE = os.environ.get("IN_LOCAL_DEV_MODE") == "True"  # Cluster is run 100% locally
IN_REMOTE_DEV_MODE = os.environ.get("IN_REMOTE_DEV_MODE") == "True"  # Only main_svc is run locally
IN_DEV = IN_LOCAL_DEV_MODE or IN_REMOTE_DEV_MODE
IN_PROD = os.environ.get("IN_PROD") == "True"

if not (IN_LOCAL_DEV_MODE or IN_REMOTE_DEV_MODE or IN_PROD):
    raise Exception("One of [IN_LOCAL_DEV_MODE, IN_REMOTE_DEV_MODE, IN_PROD] must be set to `True`")

job_env_repo = f"us-docker.pkg.dev/{PROJECT_ID}/burla-job-containers/default"
LOCAL_DEV_CONFIG = {
    "Nodes": [
        {
            "containers": [
                {
                    "image": f"{job_env_repo}/image-nogpu:latest",
                    "python_executable": "/.pyenv/versions/3.11.*/bin/python3.11",
                    "python_version": "3.11",
                },
            ],
            "machine_type": "n4-standard-2",  # <- means nothing, num containers set in node init
            "quantity": 2,
            "inactivity_shutdown_time_sec": 60 * 15,
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


from main_service.endpoints.jobs import router as jobs_router
from main_service.endpoints.cluster import router as cluster_router, restart_cluster


@asynccontextmanager
async def lifespan(app: FastAPI):

    # Start cluster strait away if in dev:
    if IN_LOCAL_DEV_MODE:
        logger = Logger()
        try:
            background_tasks = BackgroundTasks()
            add_background_task = get_add_background_task_function(background_tasks, logger=logger)
            await run_in_threadpool(
                restart_cluster, add_background_task=add_background_task, logger=logger
            )
            for task in background_tasks.tasks:
                await task()
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
            traceback_str = format_traceback(tb_details)
            logger.log(str(e), "ERROR", traceback=traceback_str)

    yield


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None)
app.include_router(jobs_router)
app.include_router(cluster_router)
app.add_middleware(SessionMiddleware, secret_key=uuid4().hex)
app.mount("/static", StaticFiles(directory="src/main_service/static"), name="static")


@app.get("/")
def dashboard():
    return FileResponse("src/main_service/static/dashboard.html")


@app.get("/favicon.ico")
async def favicon():
    return RedirectResponse(url="/static/favicon.ico")


@app.middleware("http")
async def login__log_and_time_requests__log_errors(request: Request, call_next):
    """
    Fastapi `@app.exception_handler` will completely hide errors if middleware is used.
    Catching errors in a `Depends` function will not distinguish
        http errors originating here vs other services.
    """

    start = time()
    request.state.uuid = uuid4().hex

    public_endpoints = ["/", "/favicon.ico", "/v1/cluster", "/v1/cluster/restart"]
    requesting_public_endpoint = request.url.path in public_endpoints
    requesting_static_file = request.url.path.startswith("/static")
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

    if not IN_DEV:
        msg = f"Received {request.method} at {request.url}"
        add_background_task(logger.log, msg)

        status = response.status_code
        latency = time() - start
        msg = f"{request.method} to {request.url} returned {status} after {latency} seconds."
        add_background_task(logger.log, msg, latency=latency)

    return response
