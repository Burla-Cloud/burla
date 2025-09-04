import os
import sys
import json
import asyncio
import traceback
from uuid import uuid4
from time import time
from queue import Queue
from typing import Callable
import logging as python_logging
from contextlib import asynccontextmanager
from threading import Event

import aiohttp
import google.auth
from google.auth.transport.requests import Request
from starlette.concurrency import run_in_threadpool
from fastapi import FastAPI, Request, BackgroundTasks, Depends
from fastapi.responses import Response
from starlette.requests import ClientDisconnect
from starlette.datastructures import UploadFile
from google.cloud import logging, secretmanager, firestore
from google.cloud.compute_v1 import InstancesClient

__version__ = "1.2.7"
CREDENTIALS, PROJECT_ID = google.auth.default()
BURLA_BACKEND_URL = "https://backend.burla.dev"

IN_LOCAL_DEV_MODE = os.environ.get("IN_LOCAL_DEV_MODE") == "True"  # Cluster running locally

NUM_GPUS = int(os.environ.get("NUM_GPUS"))
INSTANCE_NAME = os.environ["INSTANCE_NAME"]
INACTIVITY_SHUTDOWN_TIME_SEC = int(os.environ.get("INACTIVITY_SHUTDOWN_TIME_SEC"))
INSTANCE_N_CPUS = 2 if IN_LOCAL_DEV_MODE else os.cpu_count()
GCL_CLIENT = logging.Client().logger("node_service", labels=dict(INSTANCE_NAME=INSTANCE_NAME))

secret_client = secretmanager.SecretManagerServiceClient()
secret_name = f"projects/{PROJECT_ID}/secrets/burla-cluster-id-token/versions/latest"
response = secret_client.access_secret_version(request={"name": secret_name})
CLUSTER_ID_TOKEN = response.payload.data.decode("UTF-8")


# SELF = state of this current instance of the node service
def REINIT_SELF(SELF):
    SELF["workers"] = []
    SELF["index_of_last_worker_given_inputs"] = 0
    SELF["results_queue"] = Queue()
    SELF["current_job"] = None
    SELF["current_parallelism"] = 0
    SELF["job_watcher_stop_event"] = Event()
    SELF["BOOTING"] = False
    SELF["RUNNING"] = False
    SELF["FAILED"] = False
    SELF["SHUTTING_DOWN"] = False
    SELF["last_activity_timestamp"] = time()
    SELF["current_container_config"] = []
    SELF["job_watcher_stop_event"].set()  # needs to be default set so it definitely dies on reboot
    SELF["all_inputs_uploaded"] = False
    SELF["current_input_batch_forwarded"] = True
    SELF["num_results_received"] = 0


SELF = {}
REINIT_SELF(SELF)
from node_service.helpers import ResultsEndpointFilter, Logger

# Silence fastapi logs coming from the `/results` endpoint, there are so many it slows stuff down.
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
from node_service.job_endpoints import router as job_endpoints_router
from node_service.lifecycle_endpoints import (
    reboot_containers,
    router as lifecycle_endpoints_router,
    Container,
)


async def shutdown_if_idle_for_too_long(logger: Logger):
    """WARNING: Errors from this function are completely hidden!"""

    time_since_last_activity = 0
    while time_since_last_activity < INACTIVITY_SHUTDOWN_TIME_SEC:
        await asyncio.sleep(5)
        time_since_last_activity = time() - SELF["last_activity_timestamp"]
        if SELF["current_job"]:
            SELF["last_activity_timestamp"] = time()

    if not IN_LOCAL_DEV_MODE:
        msg = f"Node has been idle for {INACTIVITY_SHUTDOWN_TIME_SEC // 60} minutes.\n"
        msg += f"SHUTTING DOWN NODE {INSTANCE_NAME} DUE TO INACTIVITY."
        logger.log(msg, severity="WARNING")

        client = firestore.Client(project=PROJECT_ID, database="burla")
        node_doc = client.collection("nodes").document(INSTANCE_NAME)
        node_doc.update({"idle_for_too_long": True})

        instance_client = InstancesClient()
        silly_response = instance_client.aggregated_list(project=PROJECT_ID)
        vms_per_zone = [getattr(vms_in_zone, "instances", []) for _, vms_in_zone in silly_response]
        vms = [vm for vms_in_zone in vms_per_zone for vm in vms_in_zone]
        vm = next((vm for vm in vms if vm.name == INSTANCE_NAME), None)
        if vm:
            zone = vm.zone.split("/")[-1]
            instance_client.delete(project=PROJECT_ID, zone=zone, instance=INSTANCE_NAME)
    else:
        logger.log(f"Would have deleted vm due to inactivity here! {INSTANCE_NAME}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger = Logger()
    logger.log(f"Starting node service v{__version__} ...")

    # In dev all the workers restart everytime I hit save (server is in "reload" mode)
    # This is annoying but you must leave it like this, otherwise stuff won't restart correctly!
    # (you tried skipping the worker restarts here when reloading,
    # this won't work because this whole file re-runs, and SELF is reset when reloading.)

    try:
        if INACTIVITY_SHUTDOWN_TIME_SEC:
            asyncio.create_task(shutdown_if_idle_for_too_long(logger=logger))
            logger.log(f"Set to shutdown if idle for {INACTIVITY_SHUTDOWN_TIME_SEC} sec.")

        # boot containers before accepting any requests.
        containers = [Container(**c) for c in json.loads(os.environ["CONTAINERS"])]
        await run_in_threadpool(reboot_containers, new_container_config=containers, logger=logger)

    except Exception as e:
        SELF["FAILED"] = True
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
        traceback_str = format_traceback(tb_details)
        logger.log(str(e), "ERROR", traceback=traceback_str)

        client = firestore.Client(project=PROJECT_ID, database="burla")
        node_doc = client.collection("nodes").document(INSTANCE_NAME)
        node_doc.update({"status": "FAILED"})
        msg = f"Error from Node-Service: {traceback.format_exc()}"
        node_doc.collection("logs").document().set({"msg": msg, "ts": time()})

        instance_client = InstancesClient()
        silly = instance_client.aggregated_list(project=PROJECT_ID)
        vms_per_zone = [getattr(vms_in_zone, "instances", []) for _, vms_in_zone in silly]
        vms = [vm for vms_in_zone in vms_per_zone for vm in vms_in_zone]
        vm = next((vm for vm in vms if vm.name == INSTANCE_NAME), None)
        if vm:
            zone = vm.zone.split("/")[-1]
            instance_client.delete(project=PROJECT_ID, zone=zone, instance=INSTANCE_NAME)

    yield


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None)
app.include_router(job_endpoints_router)
app.include_router(lifecycle_endpoints_router)


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
async def handle_errors(request: Request, call_next):
    """
    Fastapi `@app.exception_handler` will completely hide errors if middleware is used.
    Catching errors in a `Depends` function will not distinguish
        http errors originating here vs other services.
    """
    try:
        # Important to note that HTTP exceptions do not raise errors here!
        response = await call_next(request)
    except ClientDisconnect:
        response = Response(status_code=499, content="client closed request")
    except Exception as exception:
        # create new response object to return gracefully.
        response = Response(status_code=500, content="Internal server error.")
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
        traceback_str = format_traceback(tb_details)
        logger = Logger(request)
        logger.log(str(exception), "ERROR", traceback=traceback_str)

    # handle response failure/success:
    if response.status_code == 500 and not str(request.url).endswith("/shutdown"):
        has_background_tasks = getattr(response, "background") is not None
        response.background = response.background if has_background_tasks else BackgroundTasks()
        add_background_task = get_add_background_task_function(response.background, logger=logger)
        add_background_task(reboot_containers, logger=logger)
    if response.status_code == 200:
        SELF["last_activity_timestamp"] = time()

    return response


@app.middleware("http")
async def validate_requests(request: Request, call_next):
    """
    How request validation works:
    - SELF["authorized_users"] is pre-loaded in the reboot endpoint.
    - If user/token doesn't match any authorized_users, refresh and try again before returning 401
    - Shutdown endpoint only callable from localhost (inside the shutdown script in the main_svc).
    """
    # validate shutdown requests
    is_shutdown_request = str(request.url).endswith("/shutdown")
    if is_shutdown_request:
        if request.client.host == "127.0.0.1":
            return await call_next(request)
        else:
            return Response("Shutdown endpoint can only be called from localhost", status_code=403)

    # validate all other requests:
    invalid_headers = True
    email = request.headers.get("X-User-Email")
    token = request.headers.get("Authorization", "").replace("Bearer ", "")

    for user_dict in SELF["authorized_users"]:
        if email == user_dict["email"] and token == user_dict["token"]:
            invalid_headers = False

    if invalid_headers:
        # refresh and try again:
        headers = {"Authorization": f"Bearer {CLUSTER_ID_TOKEN}"}
        url = f"{BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}/users"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                response_json = await response.json()
                SELF["authorized_users"] = response_json["authorized_users"]

        for user_dict in SELF["authorized_users"]:
            if email == user_dict["email"] and token == user_dict["token"]:
                invalid_headers = False

    if invalid_headers:
        return Response(status_code=401, content="Unauthorized.")

    return await call_next(request)


@app.middleware("http")
async def log_and_time_requests(request: Request, call_next):
    start = time()
    request.state.uuid = uuid4().hex
    not_requesting_udf_results = not str(request.url).endswith("/results")  # too many to log
    not_requesting_udf_results = True if IN_LOCAL_DEV_MODE else not_requesting_udf_results
    logger = Logger(request)

    try:
        response = await call_next(request)
    except RuntimeError as e:
        # Thrown when client disconnects during request, cannot be caught elsewhere
        if not "No response returned." in str(e):
            raise e
        response = Response(status_code=499, content="Client disconnected.")

    # ensure background tasks are availabe:
    has_background_tasks = getattr(response, "background") is not None
    response.background = response.background if has_background_tasks else BackgroundTasks()
    add_background_task = get_add_background_task_function(response.background, logger=logger)

    # Log response
    is_non_2xx_response = response.status_code < 200 or response.status_code >= 300
    if is_non_2xx_response and hasattr(response, "body"):
        response_text = response.body.decode("utf-8", errors="ignore")
        logger.log(f"non-2xx status response: {response.status_code}: {response_text}", "WARNING")
    elif is_non_2xx_response and hasattr(response, "body_iterator"):
        body = b"".join([chunk async for chunk in response.body_iterator])
        response_text = body.decode("utf-8", errors="ignore")
        logger.log(f"non-2xx status response: {response.status_code}: {response_text}", "WARNING")

        async def body_stream():  #  <- it has to be ugly like this :(
            yield body

        response.body_iterator = body_stream()
    elif response.status_code == 200 and not_requesting_udf_results and not IN_LOCAL_DEV_MODE:
        latency = time() - start
        msg = f"{request.method} to {request.url} returned 200 after {latency}s."
        add_background_task(logger.log, msg, latency=latency)

    return response
