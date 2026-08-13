import asyncio
import inspect
import json
import logging as python_logging
import os
import sys
import traceback
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Event
from time import monotonic, time
from typing import Callable
from uuid import uuid4

import aiohttp
from fastapi import BackgroundTasks, Depends, FastAPI, Request
from fastapi.responses import Response
from starlette.datastructures import UploadFile
from starlette.requests import ClientDisconnect

__version__ = "1.6.4"
PROJECT_ID = os.environ["PROJECT_ID"]
BURLA_BACKEND_URL = os.environ.get(
    "BURLA_BACKEND_URL", "https://backend.burla.dev"
).rstrip("/")

IN_LOCAL_DEV_MODE = (
    os.environ.get("IN_LOCAL_DEV_MODE") == "True"
)  # Cluster running locally

# The head (main_service). Every piece of cluster state this node reads or
# writes goes through it over HTTP - there is no database here.
MAIN_SERVICE_URL = os.environ["MAIN_SERVICE_URL"].rstrip("/")
CLUSTER_ID_TOKEN = os.environ["CLUSTER_ID_TOKEN"]

NUM_GPUS = int(os.environ.get("NUM_GPUS"))
INSTANCE_NAME = os.environ["INSTANCE_NAME"]

# local-dev only: which checkout's cluster this node belongs to. Workers get
# labeled with it purely for identification; they live on this node's own
# docker daemon so they cannot collide with other clusters regardless.
BURLA_CLUSTER_NAME = os.environ.get("BURLA_CLUSTER_NAME", "default")
_raw_inactivity = os.environ.get("INACTIVITY_SHUTDOWN_TIME_SEC")
INACTIVITY_SHUTDOWN_TIME_SEC = (
    int(_raw_inactivity) if _raw_inactivity is not None else None
)
RESERVED_FOR_JOB = os.environ.get("RESERVED_FOR_JOB") or None
INSTANCE_N_CPUS = 2 if IN_LOCAL_DEV_MODE else os.cpu_count()

# Bind-mounted into every worker at burla.CONFIG_PATH so a UDF's nested
# remote_parallel_map call finds its creds without a prior `burla login`.
NODE_AUTH_DIR = Path("/opt/burla/node_auth")
NODE_AUTH_CREDENTIALS_PATH = NODE_AUTH_DIR / "burla_credentials.json"
AZURE_DELETE_LEASE_PATH = Path("/etc/burla/azure-delete-lease.json")

from node_service.helpers import Logger, ResultsEndpointFilter, SizedQueue

# Upper bound on how many UDF log documents we'll buffer in memory
# between /results polls. If the client stops polling this caps
# memory usage at ~20k docs (<= 2 GB given the 100 KB per-doc cap
# enforced in worker_client.py).
MAX_PENDING_LOGS = 20_000

STATE_PUSH_INTERVAL_SEC = 1


# SELF = state of this current instance of the node service
def REINIT_SELF(SELF):
    SELF["workers"] = []
    SELF["idle_workers"] = []
    SELF["inputs_queue"] = SizedQueue()
    SELF["results_queue"] = SizedQueue()
    SELF["current_job"] = None
    SELF["current_parallelism"] = 0
    SELF["job_watcher_stop_event"] = Event()
    SELF[
        "job_watcher_stop_event"
    ].set()  # needs to be default set so it definitely dies on reboot
    SELF["job_watcher_task"] = None
    SELF["on_job_start_task"] = None
    SELF["BOOTING"] = False
    SELF["RUNNING"] = False
    SELF["FAILED"] = False
    SELF["current_container_config"] = []
    SELF["auth_headers"] = {}
    SELF["all_inputs_uploaded"] = False
    SELF["dynamic_func_ram"] = False
    SELF["dynamic_func_cpu"] = False
    SELF["dynamic_retire_lock"] = asyncio.Lock()
    SELF["dynamic_ram_monitor_task"] = None
    SELF["cpu_pressure_monitor_task"] = None
    SELF["reboot_containers_after_job"] = False
    SELF["num_results_received"] = 0
    SELF["pending_transfers"] = {}
    SELF["pending_result_batch"] = None
    SELF["pending_logs"] = deque(maxlen=MAX_PENDING_LOGS)
    SELF["pending_cluster_shutdown"] = False
    SELF["pending_cluster_restarted"] = False
    SELF["pending_dashboard_canceled"] = False
    SELF["active_client_request_count"] = 0
    SELF["last_client_activity_timestamp"] = time()
    # Whether the client's heartbeat channel has ever been up on this node.
    # Silence only means "disconnected" after it has, otherwise a node that the
    # client hasn't gotten around to pinging yet fails the job out from under it.
    SELF["client_heartbeat_received"] = False
    SELF["reserved_for_job"] = None
    SELF["watch_reservation_task"] = None
    SELF["SHUTTING_DOWN"] = False
    # Populated from backend.burla.dev in reboot_containers; initialized so a
    # request arriving before that fetch gets the middleware's re-fetch path
    # (or a clean 401) instead of a KeyError 500.
    SELF["authorized_users"] = []
    # State reported to / received from the head over the push exchange.
    SELF["reported_status"] = "BOOTING"
    SELF["client_contact_last_1s"] = True
    SELF["job_view"] = None
    SELF["host"] = None
    SELF["delete_lease_expires_at"] = 0
    # How long the head has been unreachable, per the state-push loop. Drives
    # orphan self-deletion (see ORPHANED_SHUTDOWN_TIME_SEC).
    SELF["head_unreachable_sec"] = 0


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
        parent_traceback = "Traceback (most recent call last):\n" + format_traceback(
            tb_details
        )

        async def func_logged(*a, **kw):
            try:
                result = func(*a, **kw)
                if inspect.isawaitable(result):
                    return await result
                return result
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                tb_details = traceback.format_exception(
                    exc_type, exc_value, exc_traceback
                )
                local_traceback_no_title = "\n".join(
                    format_traceback(tb_details).split("\n")[1:]
                )
                traceback_str = parent_traceback + local_traceback_no_title
                await logger.log(
                    message=str(e), severity="ERROR", traceback=traceback_str
                )

        background_tasks.add_task(func_logged, *a, **kw)

    return add_logged_background_task


from node_service import head_client
from node_service.helpers import Logger, format_traceback
from node_service.job_endpoints import router as job_endpoints_router
from node_service.lifecycle_endpoints import (
    reboot_containers,
    watch_reservation,
)
from node_service.lifecycle_endpoints import (
    router as lifecycle_endpoints_router,
)


def _poweroff_self():
    """Last-resort shutdown that needs zero cloud credentials: on AWS the
    instance terminates itself (InstanceInitiatedShutdownBehavior=terminate);
    on GCP it stops, billing only its disk until a head reaps it
    (delete_stopped_instances). On Azure a stopped VM still bills for compute,
    so this runs only after the managed identity and short-lived delete lease."""
    import subprocess

    subprocess.Popen(["systemctl", "poweroff"])


_GCP_METADATA = "http://metadata.google.internal/computeMetadata/v1"
_GCP_METADATA_HEADERS = {"Metadata-Flavor": "Google"}
# Set by a node that wanted to be deleted but could only stop itself, and read
# by the head's GCP reaper (`delete_stopped_instances`) to tell that apart from
# a VM someone stopped on purpose. Keep in sync with providers/gcp.py.
SELF_DELETE_GUEST_ATTRIBUTE = "burla/self-delete-requested"


async def _gcp_metadata(path: str) -> str | None:
    """A value from the GCP metadata server, or None when this isn't a GCP VM."""
    try:
        timeout = aiohttp.ClientTimeout(total=3)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            url = f"{_GCP_METADATA}/{path}"
            async with session.get(url, headers=_GCP_METADATA_HEADERS) as response:
                return await response.text() if response.status == 200 else None
    except Exception:
        return None


async def _mark_self_delete_requested():
    """Leave a marker saying Burla wanted this VM gone.

    Guest attributes are the one thing a credential-less GCP VM can write, and
    they outlive the stop, so a future head can finish deleting this VM without
    touching one a person stopped deliberately. Needs
    `enable-guest-attributes` on the instance (set by the head). No-op off GCP.
    """
    try:
        timeout = aiohttp.ClientTimeout(total=3)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            url = f"{_GCP_METADATA}/instance/guest-attributes/{SELF_DELETE_GUEST_ATTRIBUTE}"
            async with session.put(
                url, headers=_GCP_METADATA_HEADERS, data="true"
            ) as response:
                await response.read()
    except Exception:
        pass


async def _delete_self_via_cloud_api() -> bool:
    """Delete this instance outright, using whatever instance credentials
    this VM carries: a GCP service account, a deployed Azure cluster's managed
    identity, or a client-hosted Azure cluster's short-lived delete lease."""
    if await _delete_self_via_gcp():
        return True
    if await _delete_self_via_azure():
        return True
    return await _delete_self_via_azure_lease()


async def _delete_self_via_gcp() -> bool:
    token_json = await _gcp_metadata("instance/service-accounts/default/token")
    zone_path = await _gcp_metadata("instance/zone")
    if not token_json or not zone_path:
        return False
    try:
        access_token = json.loads(token_json)["access_token"]
    except Exception:
        return False

    zone = zone_path.rsplit("/", 1)[-1]
    url = (
        f"https://compute.googleapis.com/compute/v1/projects/{PROJECT_ID}"
        f"/zones/{zone}/instances/{INSTANCE_NAME}"
    )
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            headers = {"Authorization": f"Bearer {access_token}"}
            async with session.delete(url, headers=headers) as response:
                return response.status < 300
    except Exception:
        return False


_AZURE_IMDS = "http://169.254.169.254/metadata"
_AZURE_IMDS_HEADERS = {"Metadata": "true"}


async def _azure_imds(path: str) -> dict | None:
    """A JSON value from the Azure metadata service, or None off Azure."""
    try:
        timeout = aiohttp.ClientTimeout(total=3)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            url = f"{_AZURE_IMDS}/{path}"
            async with session.get(url, headers=_AZURE_IMDS_HEADERS) as response:
                return await response.json() if response.status == 200 else None
    except Exception:
        return None


async def _delete_self_via_azure() -> bool:
    token_info = await _azure_imds(
        "identity/oauth2/token?api-version=2018-02-01"
        "&resource=https%3A%2F%2Fmanagement.azure.com%2F"
    )
    compute_info = await _azure_imds("instance/compute?api-version=2021-02-01")
    if not token_info or not compute_info:
        return False

    url = (
        "https://management.azure.com/subscriptions/"
        f"{compute_info['subscriptionId']}/resourceGroups/"
        f"{compute_info['resourceGroupName']}/providers/Microsoft.Compute"
        f"/virtualMachines/{compute_info['name']}"
        "?api-version=2024-07-01&forceDeletion=true"
    )
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            headers = {"Authorization": f"Bearer {token_info['access_token']}"}
            async with session.delete(url, headers=headers) as response:
                return response.status < 300
    except Exception:
        return False


async def _delete_self_via_azure_lease() -> bool:
    if not AZURE_DELETE_LEASE_PATH.exists():
        return False
    lease = json.loads(AZURE_DELETE_LEASE_PATH.read_text())
    url = (
        f"https://management.azure.com{lease['vm_id']}"
        "?api-version=2024-07-01&forceDeletion=true"
    )
    headers = {"Authorization": f"Bearer {lease['access_token']}"}
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.delete(url, headers=headers) as response:
                return response.status < 300 or response.status == 404
    except Exception:
        return False


async def _shutdown_self():
    """Get this VM fully deleted, escalating through whatever is reachable.

    The head owns the cloud APIs, so ask it first. If it's gone, delete
    ourselves when this VM happens to carry credentials. Failing both, record
    the intent and power off: that terminates the instance on AWS, but only
    stops it on GCP, where the marker is what lets a later head finish up.
    """
    try:
        await head_client.request_self_delete()
    except Exception:
        await _delete_self_via_cloud_api()
    finally:
        await _mark_self_delete_requested()
        # The lease daemon must stay alive until Azure confirms deletion.
        if not AZURE_DELETE_LEASE_PATH.exists():
            _poweroff_self()


# If the head stays unreachable this long, the node assumes it's orphaned (the
# laptop hosting the head was closed, the head crashed) and deletes itself so it
# can't run up a bill forever. Long enough to sit through a head restart or
# redeploy, which nodes are expected to survive.
ORPHANED_SHUTDOWN_TIME_SEC = 3 * 60


def _head_is_gone() -> bool:
    """True once the head has been unreachable long enough to call this node
    orphaned. Job state is only meaningful while the head is around to update
    it, so callers use this to stop trusting a stale `current_job`."""
    return SELF["head_unreachable_sec"] >= ORPHANED_SHUTDOWN_TIME_SEC


async def shutdown_if_idle_for_too_long(logger: Logger):
    """WARNING: Errors from this function are completely hidden!"""

    time_since_last_activity = 0
    # A job that never got finalized (the head vanished mid-completion) would
    # otherwise hold this node open forever, so stale job state stops counting
    # once the head is gone.
    while (
        time_since_last_activity <= INACTIVITY_SHUTDOWN_TIME_SEC
        or SELF["active_client_request_count"] > 0
        or (not _head_is_gone() and (SELF["current_job"] or SELF["reserved_for_job"]))
        or SELF["BOOTING"]
    ):
        await asyncio.sleep(5)
        time_since_last_activity = time() - SELF["last_client_activity_timestamp"]

    SELF["SHUTTING_DOWN"] = True

    try:
        if not SELF["FAILED"]:
            SELF["reported_status"] = "DELETED"
            await head_client.push_state(status="DELETED", ended_at=time())

        msg = f"Node has been idle for {INACTIVITY_SHUTDOWN_TIME_SEC // 60} minutes.\n"
        msg += f"SHUTTING DOWN NODE {INSTANCE_NAME} DUE TO INACTIVITY."
        await logger.log(msg, severity="WARNING")
    finally:
        await _shutdown_self()


async def _state_push_loop(logger: Logger):
    """Continuous exchange with the head: reports status + job progress up,
    carries `host` and the job signal set down. Replaces this service's
    firestore writes and its per-job on_snapshot watch."""
    consecutive_failures = 0
    head_unreachable_since = None
    while True:
        await asyncio.sleep(STATE_PUSH_INTERVAL_SEC)
        if SELF["SHUTTING_DOWN"]:
            continue
        attempt_started_at = monotonic()
        try:
            # Progress only counts once the job watcher is live; registering
            # it earlier would leave a stale progress entry behind if the
            # assignment is rolled back before the watcher starts.
            watcher_active = not SELF["job_watcher_stop_event"].is_set()
            view = await head_client.push_state(
                status=SELF["reported_status"],
                include_job_progress=watcher_active,
            )
            consecutive_failures = 0
            head_unreachable_since = None
            SELF["head_unreachable_sec"] = 0
            SELF["host"] = view.get("host")
            reservation = view.get("reserved_for_job")
            if not SELF["current_job"] and reservation != SELF["reserved_for_job"]:
                watch_task = SELF["watch_reservation_task"]
                if watch_task and not watch_task.done():
                    watch_task.cancel()
                SELF["reserved_for_job"] = reservation
                SELF["watch_reservation_task"] = (
                    asyncio.create_task(watch_reservation(reservation))
                    if reservation
                    else None
                )
            if view.get("status") == "DELETED":
                # The head deleted this node (dashboard delete, or a cluster
                # shutdown that raced our boot) and refuses to resurrect it.
                # Without this the VM would serve forever with the head blind
                # to it (observed with grow nodes booted mid-shutdown).
                SELF["SHUTTING_DOWN"] = True
                SELF["job_watcher_stop_event"].set()
                print("Head reports this node as DELETED; requesting VM deletion.")
                if not IN_LOCAL_DEV_MODE:
                    await _shutdown_self()
                continue
            if SELF["current_job"]:
                head_client.apply_job_signals(view.get("job"))
        except Exception as e:
            consecutive_failures += 1
            if head_unreachable_since is None:
                head_unreachable_since = attempt_started_at
            # The head being briefly down (restart/redeploy) is survivable -
            # nodes keep working and re-sync on the next successful push.
            if consecutive_failures in (1, 10, 60):
                print(f"state push to head failed ({consecutive_failures}x): {e}")
            head_gone_sec = int(monotonic() - head_unreachable_since)
            SELF["head_unreachable_sec"] = head_gone_sec
            # No head means nobody is coming to collect results or delete this
            # VM, so being mid-job is not a reason to stay alive.
            if _head_is_gone() and not IN_LOCAL_DEV_MODE:
                SELF["SHUTTING_DOWN"] = True
                print(f"Head unreachable for {head_gone_sec}s; deleting self.")
                await _shutdown_self()


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

    asyncio.create_task(_state_push_loop(logger=logger))

    # boot containers before accepting any requests.
    # `reboot_containers` will ask the head to delete this VM if it fails, no need to do that here.
    containers = [c["image"] for c in json.loads(os.environ["CONTAINERS"])]
    await reboot_containers(new_container_config=containers, logger=logger)

    certificate_renewal_task = None
    if not IN_LOCAL_DEV_MODE:
        from node_service.transport_tls import certificate_renewal_loop

        certificate_renewal_task = asyncio.create_task(certificate_renewal_loop())

    yield

    if certificate_renewal_task is not None:
        certificate_renewal_task.cancel()


def on_job_start(scope):
    job_id = scope.get("path", "").split("/jobs/")[-1]
    SELF["RUNNING"] = True
    SELF["current_job"] = job_id
    SELF["reserved_for_job"] = None
    SELF["reported_status"] = "RUNNING"
    SELF["job_view"] = None
    # `watch_reservation` is obsolete once assignment arrives - cancel it so
    # it stops polling the head without writing a redundant clear (the push
    # below already clears `reserved_for_job`).
    watch_task = SELF.get("watch_reservation_task")
    if watch_task and not watch_task.done():
        watch_task.cancel()
    push = head_client.push_state(
        status="RUNNING", current_job=job_id, reserved_for_job=None
    )
    SELF["on_job_start_task"] = asyncio.create_task(push)


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
            if SELF["SHUTTING_DOWN"]:
                msg = "Node is shutting down due to inactivity."
                return await Response(msg, status_code=503)(scope, receive, send)
            if SELF["RUNNING"] or SELF["BOOTING"]:
                msg = "Node currently running or booting, request refused."
                return await Response(msg, status_code=409)(scope, receive, send)

            on_job_start(scope)
            return await self.app(scope, receive, send)
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
            if message["type"] == "http.response.body" and not message.get(
                "more_body", False
            ):
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
    SELF["client_heartbeat_received"] = True
    async for _ in request.stream():
        now = time()
        seconds_since_last_ping = now - (last_ping_received_at or now)
        if seconds_since_last_ping > 2:
            await logger.log(
                f"high heartbeat gap: {seconds_since_last_ping:.3f}s",
                severity="WARNING",
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
            await SELF["on_job_start_task"]
            SELF["RUNNING"] = False
            SELF["current_job"] = None
            SELF["reported_status"] = "READY"
            await head_client.push_state(
                status="READY", current_job=None, reserved_for_job=None
            )
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
        response.background = (
            response.background if has_background_tasks else BackgroundTasks()
        )
        add_background_task = get_add_background_task_function(
            response.background, logger=logger
        )
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
    - /shutdown gets no special treatment: in relayed setups every request arrives
      from loopback, so trusting request.client.host would let anyone shut nodes down.
      The in-VM shutdown hooks send the cluster token like everything else.
    """
    # The head authenticates with the cluster token (same check the head runs
    # for node traffic). Every node gets the identical token at boot, so this
    # is a local comparison, no backend round-trip.
    if request.headers.get("Authorization") == f"Bearer {CLUSTER_ID_TOKEN}":
        return await call_next(request)

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
    chatty_endpoint = request.url.path.endswith(
        ("/results", "/ack_transfer", "/get_inputs")
    )

    try:
        response = await call_next(request)
    except RuntimeError as e:
        # Thrown when client disconnects during request, cannot be caught elsewhere
        if "No response returned." not in str(e):
            raise
        response = Response(status_code=499, content="Client disconnected.")

    # Log response
    is_non_2xx_response = response.status_code < 200 or response.status_code >= 300
    if is_non_2xx_response and hasattr(response, "body"):
        response_text = response.body.decode("utf-8", errors="ignore")
        print(f"non-2xx status response: {response.status_code}: {response_text}")
    elif is_non_2xx_response and hasattr(response, "body_iterator"):
        body = b"".join([chunk async for chunk in response.body_iterator])
        response_text = body.decode("utf-8", errors="ignore")
        print(f"non-2xx status response: {response.status_code}: {response_text}")

        # Logging consumed Starlette's iterator, so the response needs a replacement.
        async def body_stream():
            yield body

        response.body_iterator = body_stream()
    elif response.status_code == 200 and not chatty_endpoint and not IN_LOCAL_DEV_MODE:
        latency = time() - start
        print(f"{request.method} to {request.url} returned 200 after {latency}s.")

    return response
