import os
import sys
import json
import inspect
import asyncio
import traceback
from collections import deque
from pathlib import Path
from uuid import uuid4
from time import time
from typing import Callable
import logging as python_logging
from contextlib import asynccontextmanager
from threading import Event

# throws some uncatchable, unimportant, warnings
python_logging.getLogger("google.api_core.bidi").setLevel(python_logging.ERROR)
# prevent some annoying grpc logs / warnings
os.environ["GRPC_VERBOSITY"] = "ERROR"  # only log ERROR/FATAL
os.environ["GLOG_minloglevel"] = "2"  # 0-INFO, 1-WARNING, 2-ERROR, 3-FATAL

import google.auth
from google.auth.transport.requests import Request
from google.cloud import logging, secretmanager
from google.cloud.compute_v1 import InstancesClient
from google.cloud.firestore_v1.async_client import AsyncClient
import aiohttp
from fastapi import FastAPI, Request, BackgroundTasks, Depends
from fastapi.responses import Response
from starlette.requests import ClientDisconnect
from starlette.datastructures import UploadFile


__version__ = "1.5.6"
CREDENTIALS, PROJECT_ID = google.auth.default()
BURLA_BACKEND_URL = "https://backend.burla.dev"

ASYNC_DB = AsyncClient(project=PROJECT_ID, database="burla")
IN_LOCAL_DEV_MODE = os.environ.get("IN_LOCAL_DEV_MODE") == "True"  # Cluster running locally

NUM_GPUS = int(os.environ.get("NUM_GPUS"))
INSTANCE_NAME = os.environ["INSTANCE_NAME"]
_raw_inactivity = os.environ.get("INACTIVITY_SHUTDOWN_TIME_SEC")
INACTIVITY_SHUTDOWN_TIME_SEC = int(_raw_inactivity) if _raw_inactivity is not None else None
RESERVED_FOR_JOB = os.environ.get("RESERVED_FOR_JOB") or None
INSTANCE_N_CPUS = 2 if IN_LOCAL_DEV_MODE else os.cpu_count()
GCL_CLIENT = logging.Client().logger("node_service", labels=dict(INSTANCE_NAME=INSTANCE_NAME))

secret_client = secretmanager.SecretManagerServiceClient()
secret_name = f"projects/{PROJECT_ID}/secrets/burla-cluster-id-token/versions/latest"
response = secret_client.access_secret_version(request={"name": secret_name})
CLUSTER_ID_TOKEN = response.payload.data.decode("UTF-8")

# Bind-mounted into every worker container at /root/.config/burla (where
# platformdirs resolves burla.CONFIG_PATH), so a UDF calling
# remote_parallel_map authenticates without a prior `burla login`.
NODE_AUTH_DIR = Path("/opt/burla/node_auth")
NODE_AUTH_CREDENTIALS_PATH = NODE_AUTH_DIR / "burla_credentials.json"

from node_service.helpers import ResultsEndpointFilter, Logger, SizedQueue


# Upper bound on how many UDF log documents we'll buffer in memory
# between /results polls. If the client stops polling this caps
# memory usage at ~20k docs (<= 2 GB given the 100 KB per-doc cap
# enforced in worker_client.py).
MAX_PENDING_LOGS = 20_000


# SELF = state of this current instance of the node service
def REINIT_SELF(SELF):
    SELF["workers"] = []
    SELF["idle_workers"] = []
    SELF["inputs_queue"] = SizedQueue()
    SELF["results_queue"] = SizedQueue()
    SELF["current_job"] = None
    SELF["current_parallelism"] = 0
    SELF["job_watcher_stop_event"] = Event()
    SELF["job_watcher_stop_event"].set()  # needs to be default set so it definitely dies on reboot
    SELF["job_watcher_task"] = None
    SELF["on_job_start_task"] = None
    SELF["BOOTING"] = False
    SELF["RUNNING"] = False
    SELF["FAILED"] = False
    SELF["current_container_config"] = []
    SELF["auth_headers"] = {}
    SELF["all_inputs_uploaded"] = False
    SELF["num_results_received"] = 0
    SELF["pending_transfers"] = {}
    SELF["pending_logs"] = deque(maxlen=MAX_PENDING_LOGS)
    SELF["pending_cluster_shutdown"] = False
    SELF["pending_cluster_restarted"] = False
    SELF["pending_dashboard_canceled"] = False
    SELF["active_client_request_count"] = 0
    SELF["last_client_activity_timestamp"] = time()
    SELF["reserved_for_job"] = None
    SELF["SHUTTING_DOWN"] = False


SELF = {}
REINIT_SELF(SELF)
SELF["reserved_for_job"] = RESERVED_FOR_JOB

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

        async def func_logged(*a, **kw):
            try:
                result = func(*a, **kw)
                if inspect.isawaitable(result):
                    return await result
                return result
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
                local_traceback_no_title = "\n".join(format_traceback(tb_details).split("\n")[1:])
                traceback_str = parent_traceback + local_traceback_no_title
                await logger.log(message=str(e), severity="ERROR", traceback=traceback_str)

        background_tasks.add_task(func_logged, *a, **kw)

    return add_logged_background_task


from node_service.helpers import Logger, format_traceback
from node_service.job_endpoints import router as job_endpoints_router
from node_service.lifecycle_endpoints import (
    reboot_containers,
    router as lifecycle_endpoints_router,
)


async def shutdown_if_idle_for_too_long(logger: Logger):
    """WARNING: Errors from this function are completely hidden!"""

    time_since_last_activity = 0
    while (
        time_since_last_activity <= INACTIVITY_SHUTDOWN_TIME_SEC
        or SELF["active_client_request_count"] > 0
        or SELF["current_job"]
        or SELF["reserved_for_job"]
        or SELF["BOOTING"]
    ):
        await asyncio.sleep(5)
        time_since_last_activity = time() - SELF["last_client_activity_timestamp"]

    SELF["SHUTTING_DOWN"] = True

    node_doc = ASYNC_DB.collection("nodes").document(INSTANCE_NAME)
    snapshot = await node_doc.get()
    if snapshot.exists and snapshot.to_dict().get("status") != "FAILED":
        await node_doc.update({"status": "DELETED", "ended_at": time()})

    msg = f"Node has been idle for {INACTIVITY_SHUTDOWN_TIME_SEC // 60} minutes.\n"
    msg += f"SHUTTING DOWN NODE {INSTANCE_NAME} DUE TO INACTIVITY."
    await logger.log(msg, severity="WARNING")

    instance_client = InstancesClient()
    silly_response = instance_client.aggregated_list(project=PROJECT_ID)
    vms_per_zone = [getattr(vms_in_zone, "instances", []) for _, vms_in_zone in silly_response]
    vms = [vm for vms_in_zone in vms_per_zone for vm in vms_in_zone]
    vm = next((vm for vm in vms if vm.name == INSTANCE_NAME), None)
    if vm:
        zone = vm.zone.split("/")[-1]
        instance_client.delete(project=PROJECT_ID, zone=zone, instance=INSTANCE_NAME)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger = Logger()
    await logger.log(f"Started node service v{__version__}")

    # Must exist before `reboot_containers` since worker containers bind-mount
    # this dir. Unlink guards against stale creds from a crashed prior run.
    NODE_AUTH_DIR.mkdir(parents=True, exist_ok=True)
    NODE_AUTH_CREDENTIALS_PATH.unlink(missing_ok=True)

    # In dev all the workers restart everytime I hit save (server is in "reload" mode)
    # This is annoying but you must leave it like this, otherwise stuff won't restart correctly!
    # (you tried skipping the worker restarts here when reloading,
    # this won't work because this whole file re-runs, and SELF is reset when reloading.)

    if INACTIVITY_SHUTDOWN_TIME_SEC is not None and not IN_LOCAL_DEV_MODE:
        asyncio.create_task(shutdown_if_idle_for_too_long(logger=logger))
        msg = f"This node will shutdown if idle for {INACTIVITY_SHUTDOWN_TIME_SEC//60} minutes!"
        await logger.log(msg)

    # boot containers before accepting any requests.
    # `reboot_containers` will delete VM's if it fails, no need to do that here.
    containers = [c["image"] for c in json.loads(os.environ["CONTAINERS"])]
    await reboot_containers(new_container_config=containers, logger=logger)

    yield


async def on_job_start(scope, first_event):
    # SELF is set synchronously so the middleware's next-request 409 guard and
    # the execute endpoint's rollback (which reads SELF, not firestore) see the
    # new state immediately. The firestore write happens in the background; the
    # rollback awaits `on_job_start_task` so the two writes cannot race.
    job_id = scope.get("path", "").split("/jobs/")[-1]
    SELF["RUNNING"] = True
    SELF["current_job"] = job_id
    SELF["reserved_for_job"] = None
    node_doc = ASYNC_DB.collection("nodes").document(INSTANCE_NAME)
    update_fields = {"status": "RUNNING", "current_job": job_id, "reserved_for_job": None}
    SELF["on_job_start_task"] = asyncio.create_task(node_doc.update(update_fields))


class CallHookOnJobStartMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        is_post_request = scope.get("method") == "POST"
        path_parts = scope.get("path", "").strip("/").split("/")
        is_job_execution_request = (
            is_post_request and len(path_parts) == 2 and path_parts[0] == "jobs"
        )

        if is_job_execution_request:
            started = False
            if SELF["SHUTTING_DOWN"]:
                msg = "Node is shutting down due to inactivity."
                return await Response(msg, status_code=503)(scope, receive, send)
            if SELF["RUNNING"] or SELF["BOOTING"]:
                msg = "Node currently running or booting, request refused."
                return await Response(msg, status_code=409)(scope, receive, send)

            async def wrapped_receive():
                nonlocal started
                event = await receive()
                job_starting = event.get("type") == "http.request" and not started

                if job_starting:
                    started = True
                    await on_job_start(scope, event)
                return event

            return await self.app(scope, wrapped_receive, send)
        return await self.app(scope, receive, send)


class TrackOpenRequestMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        request_done = False

        def mark_request_done():
            nonlocal request_done
            if request_done:
                return
            request_done = True
            SELF["active_client_request_count"] -= 1
            SELF["last_client_activity_timestamp"] = time()

        SELF["active_client_request_count"] += 1

        async def wrapped_receive():
            event = await receive()
            if event["type"] == "http.disconnect":
                mark_request_done()
            return event

        async def wrapped_send(message):
            if message["type"] == "http.response.body" and not message.get("more_body", False):
                mark_request_done()
            await send(message)

        try:
            await self.app(scope, wrapped_receive, wrapped_send)
        finally:
            mark_request_done()


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None)
app.add_middleware(TrackOpenRequestMiddleware)
app.add_middleware(CallHookOnJobStartMiddleware)
app.include_router(job_endpoints_router)
app.include_router(lifecycle_endpoints_router)


@app.get("/")
async def get_status():
    if SELF["FAILED"]:
        return {"status": "FAILED"}
    elif SELF["BOOTING"]:
        return {"status": "BOOTING"}
    elif SELF["RUNNING"]:
        return {"status": "RUNNING"}
    else:
        return {"status": "READY"}


@app.post("/client-heartbeat")
async def client_heartbeat(request: Request, logger: Logger = Depends(get_logger)):
    last_ping_received_at = None
    async for _ in request.stream():
        now = time()
        seconds_since_last_ping = now - (last_ping_received_at or now)
        if seconds_since_last_ping > 2:
            await logger.log(
                f"high heartbeat gap: {seconds_since_last_ping:.3f}s", severity="WARNING"
            )
        last_ping_received_at = now
        await asyncio.sleep(0)
    return Response(status_code=204)


@app.middleware("http")
async def handle_errors(request: Request, call_next):
    """
    Fastapi `@app.exception_handler` will completely hide errors if middleware is used.
    Catching errors in a `Depends` function will not distinguish
        http errors originating here vs other services.
    """
    logger = Logger(request)
    try:
        # Important to note that HTTP exceptions do not raise errors here!
        response = await call_next(request)
    except ClientDisconnect:
        response = Response(status_code=499, content="client closed request")
        # If disconnect hit POST /jobs/{id} before job_watcher started, reset SELF
        # so the client's retry is accepted instead of being refused with 409.
        disconnected_mid_assign = (
            request.method == "POST"
            and request.url.path == f"/jobs/{SELF['current_job']}"
            and SELF["job_watcher_task"] is None
        )
        if disconnected_mid_assign:
            SELF["RUNNING"] = False
            SELF["current_job"] = None
    except Exception as exception:
        # create new response object to return gracefully.
        response = Response(status_code=500, content="Internal server error.")
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
        traceback_str = format_traceback(tb_details)
        await logger.log(str(exception), "ERROR", traceback=traceback_str)

    # handle response failure/success:
    if response.status_code == 500 and not str(request.url).endswith("/shutdown"):
        has_background_tasks = getattr(response, "background") is not None
        response.background = response.background if has_background_tasks else BackgroundTasks()
        add_background_task = get_add_background_task_function(response.background, logger=logger)
        add_background_task(reboot_containers, logger=logger)
    if response.status_code == 200:
        SELF["last_client_activity_timestamp"] = time()

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
    chatty_endpoint = request.url.path.endswith(("/results", "/ack_transfer", "/get_inputs"))

    logger = Logger(request)
    # Don't use this ^ (except in `get_add_background_task_function`) because it logs to firestore
    # and that can't be turned off without affecting other class instances currently because they
    # all share a python logger.

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
        msg = f"non-2xx status response: {response.status_code}: {response_text}"
        GCL_CLIENT.log_text(msg, severity="WARNING")
    elif is_non_2xx_response and hasattr(response, "body_iterator"):
        body = b"".join([chunk async for chunk in response.body_iterator])
        response_text = body.decode("utf-8", errors="ignore")
        msg = f"non-2xx status response: {response.status_code}: {response_text}"
        GCL_CLIENT.log_text(msg, severity="WARNING")

        async def body_stream():  #  <- it has to be ugly like this :(
            yield body

        response.body_iterator = body_stream()
    elif response.status_code == 200 and not chatty_endpoint and not IN_LOCAL_DEV_MODE:
        latency = time() - start
        msg = f"{request.method} to {request.url} returned 200 after {latency}s."
        add_background_task(GCL_CLIENT.log_text, msg)

    return response
