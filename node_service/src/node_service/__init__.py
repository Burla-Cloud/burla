import os
import sys
import json
import asyncio
import traceback
import requests
from uuid import uuid4
from time import time
from typing import Callable
from contextlib import asynccontextmanager

from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool
from fastapi import FastAPI, Request, BackgroundTasks, Depends
from fastapi.responses import Response
from starlette.datastructures import UploadFile
from google.cloud import logging
from google.cloud.compute_v1 import InstancesClient

__version__ = "0.8.18"
IN_LOCAL_DEV_MODE = os.environ.get("IN_LOCAL_DEV_MODE") == "True"  # Cluster is runing 100% locally
IN_DEV = os.environ.get("IN_DEV") == "True"

PROJECT_ID = os.environ["PROJECT_ID"]
INSTANCE_NAME = os.environ["INSTANCE_NAME"]
INACTIVITY_SHUTDOWN_TIME_SEC = os.environ.get("INACTIVITY_SHUTDOWN_TIME_SEC")
JOBS_BUCKET = f"burla-jobs--{PROJECT_ID}"
INSTANCE_N_CPUS = 2 if IN_DEV else os.cpu_count()
GCL_CLIENT = logging.Client().logger("node_service", labels=dict(INSTANCE_NAME=INSTANCE_NAME))

url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
if IN_DEV:
    ACCESS_TOKEN = os.popen("gcloud auth print-access-token").read().strip()
else:
    response = requests.get(url, headers={"Metadata-Flavor": "Google"})
    response.raise_for_status()
    ACCESS_TOKEN = response.json().get("access_token")

# This MUST be set to the same value as `JOB_HEALTHCHECK_FREQUENCY_SEC` in the client.
# Nodes will restart themself if they dont get a new healthcheck from the client every X seconds.
JOB_HEALTHCHECK_FREQUENCY_SEC = 3

# no real reason I picked +8 for `time_until_client_disconnect_shutdown`, except that 3 didnt work
SELF = {
    "workers": [],
    "job_watcher_thread": None,
    "current_job": None,
    "current_container_config": [],
    "time_until_inactivity_shutdown": None,
    "time_until_client_disconnect_shutdown": JOB_HEALTHCHECK_FREQUENCY_SEC + 8,
    "BOOTING": False,
    "RUNNING": False,
    "FAILED": False,
}

from node_service.helpers import Logger


class Container(BaseModel):
    image: str
    python_executable: str
    python_version: str


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


def get_logger(request: Request):
    return Logger(request=request)


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


from node_service.helpers import Logger, format_traceback
from node_service.endpoints import reboot_containers, router as endpoints_router


async def shutdown_if_idle_for_too_long():
    """WARNING: Errors/stdout from this function are completely hidden!"""

    # this is in a for loop so the wait time can be extended while waiting
    while SELF["time_until_inactivity_shutdown"] > 1:
        await asyncio.sleep(1)
        SELF["time_until_inactivity_shutdown"] -= 1

    if not IN_DEV:
        msg = f"SHUTTING DOWN NODE DUE TO INACTIVITY: {INSTANCE_NAME}"
        struct = dict(message=msg)
        GCL_CLIENT.log_struct(struct, severity="WARNING")

        instance_client = InstancesClient()
        silly_response = instance_client.aggregated_list(project=PROJECT_ID)
        vms_per_zone = [getattr(vms_in_zone, "instances", []) for _, vms_in_zone in silly_response]
        vms = [vm for vms_in_zone in vms_per_zone for vm in vms_in_zone]
        vm = next((vm for vm in vms if vm.name == INSTANCE_NAME), None)

        if vm is None:
            struct = dict(message=f"INSTANCE NOT FOUND?? UNABLE TO DELETE: {INSTANCE_NAME}")
            GCL_CLIENT.log_struct(struct, severity="ERROR")
        else:
            zone = vm.zone.split("/")[-1]
            instance_client.delete(project=PROJECT_ID, zone=zone, instance=INSTANCE_NAME)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger = Logger()
    logger.log(f"Booting node service version {__version__}")

    # In dev all the workers restart everytime I hit save (server is in "reload" mode)
    # This is annoying but you must leave it like this, otherwise stuff won't restart correctly!
    # (you tried skipping the worker restarts here when reloading,
    # this won't work because this whole file re-runs, and SELF is reset when reloading.)

    try:
        # boot containers before accepting any requests.
        containers = [Container(**c) for c in json.loads(os.environ["CONTAINERS"])]
        await run_in_threadpool(reboot_containers, new_container_config=containers, logger=logger)

        if INACTIVITY_SHUTDOWN_TIME_SEC:
            SELF["time_until_inactivity_shutdown"] = int(INACTIVITY_SHUTDOWN_TIME_SEC)
            asyncio.create_task(shutdown_if_idle_for_too_long())
            logger.log(f"Set to shutdown if idle for {INACTIVITY_SHUTDOWN_TIME_SEC} sec.")

    except Exception as e:
        SELF["FAILED"] = True
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
        traceback_str = format_traceback(tb_details)
        logger.log(str(e), "ERROR", traceback=traceback_str)

    yield


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None)  #
app.include_router(endpoints_router)


@app.get("/")
def get_status():
    if SELF["FAILED"]:
        return {"status": "FAILED"}
    elif SELF["BOOTING"]:
        return {"status": "BOOTING"}
    elif SELF["RUNNING"]:
        return {"status": "RUNNING"}
    else:
        return {"status": "READY"}


@app.middleware("http")
async def log_and_time_requests__log_errors(request: Request, call_next):
    """
    Fastapi `@app.exception_handler` will completely hide errors if middleware is used.
    Catching errors in a `Depends` function will not distinguish
        http errors originating here vs other services.
    """
    start = time()
    request.state.uuid = uuid4().hex

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

    if response.status_code != 200 and hasattr(response, "body"):
        response_text = response.body.decode("utf-8", errors="ignore")
        logger.log(f"non-200 status response: {response.status_code}: {response_text}", "WARNING")
    elif response.status_code != 200 and hasattr(response, "body_iterator"):
        body = b"".join([chunk async for chunk in response.body_iterator])
        response_text = body.decode("utf-8", errors="ignore")
        logger.log(f"non-200 status response: {response.status_code}: {response_text}", "WARNING")

        # repair original response before returning (we read/emptied it's body_iterator)
        async def body_stream():
            yield body

        response.body_iterator = body_stream()

    response_contains_background_tasks = getattr(response, "background") is not None
    if not response_contains_background_tasks:
        response.background = BackgroundTasks()

    if not IN_DEV:
        add_background_task = get_add_background_task_function(response.background, logger=logger)
        msg = f"Received {request.method} at {request.url}"
        add_background_task(logger.log, msg)

        status = response.status_code
        latency = time() - start
        msg = f"{request.method} to {request.url} returned {status} after {latency} seconds."
        add_background_task(logger.log, msg, latency=latency)

    if INACTIVITY_SHUTDOWN_TIME_SEC:
        SELF["time_until_inactivity_shutdown"] = int(INACTIVITY_SHUTDOWN_TIME_SEC)
    return response
