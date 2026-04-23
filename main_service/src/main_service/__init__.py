import sys
import os
import json
import asyncio
import threading
import traceback
import aiohttp
import logging as python_logging
from uuid import uuid4
from time import time, sleep
from typing import Callable, Optional
from pathlib import Path
from contextlib import asynccontextmanager

import google.auth
from google.cloud import firestore, logging, secretmanager
from fastapi.responses import Response, FileResponse, RedirectResponse
from fastapi import FastAPI, Request, BackgroundTasks, Depends, status
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.datastructures import UploadFile
from jinja2 import Environment, FileSystemLoader

os.environ["GRPC_VERBOSITY"] = "ERROR"
os.environ["GLOG_minloglevel"] = "2"

CURRENT_BURLA_VERSION = "1.5.8"
MIN_COMPATIBLE_CLIENT_VERSION = "1.5.8"

# In this mode EVERYTHING runs locally in docker containers.
# possible modes: local-dev-mode (everything local), remote-dev-mode (only main-service local), prod
IN_LOCAL_DEV_MODE = os.environ.get("IN_LOCAL_DEV_MODE") == "True"
# This is needed because remote-dev-mode is not local-dev-mode, and needs local redirect on login.
REDIRECT_LOCALLY_ON_LOGIN = os.environ.get("REDIRECT_LOCALLY_ON_LOGIN") == "True"

CREDENTIALS, PROJECT_ID = google.auth.default()
BURLA_BACKEND_URL = "https://backend.burla.dev"
GCL_CLIENT = logging.Client().logger("main_service")
DB = firestore.Client(database="burla")

STATIC_FILES_ENV = Environment(loader=FileSystemLoader("src/main_service/static"))
secret_client = secretmanager.SecretManagerServiceClient()
secret_name = f"projects/{PROJECT_ID}/secrets/burla-cluster-id-token/versions/latest"
response = secret_client.access_secret_version(request={"name": secret_name})
CLUSTER_ID_TOKEN = response.payload.data.decode("UTF-8")

DEFAULT_CONFIG = {  # <- config used only when config is missing from firestore
    "Nodes": [
        {
            "containers": [
                {
                    "image": "python:3.12",
                },
            ],
            "machine_type": "n4-standard-4",
            "gcp_region": "us-central1",
            "quantity": 1,
            "inactivity_shutdown_time_sec": 60 * 10,
        }
    ],
    # Same bucket that `burla install` creates via `gcloud storage buckets create ...`
    # Used by storage.py and node boot to mount /workspace/shared in every container.
    "gcs_bucket_name": f"{PROJECT_ID}-burla-shared-workspace",
}

# `burla install` no longer seeds `cluster_config`, so main_service is the
# only thing that can guarantee it exists. Seed at startup before anything
# (LOCAL_DEV_CONFIG below, storage.py at import, etc.) reads it.
_cluster_config_ref = DB.collection("cluster_config").document("cluster_config")
if not _cluster_config_ref.get().exists:
    _cluster_config_ref.set(DEFAULT_CONFIG)

LOCAL_DEV_CONFIG = None
if IN_LOCAL_DEV_MODE:
    LOCAL_DEV_CONFIG = _cluster_config_ref.get().to_dict()
    LOCAL_DEV_CONFIG["Nodes"][0]["machine_type"] = "n4-standard-2"
    LOCAL_DEV_CONFIG["Nodes"][0]["quantity"] = 2


# ------------------------------------------------------------------
# In-process caches backed by firestore on_snapshot listeners.
#
# These kill the per-request firestore query on the burla client's hot path:
# - NODES_CACHE: `POST /v1/jobs/{id}/start`, `GET /v1/cluster/state`, and
#                `GET /v1/cluster/nodes/{id}` all answer from this dict.
# - CLUSTER_CONFIG_CACHE: read by `_get_cluster_config` when sizing growth
#                         inside `POST /v1/jobs/{id}/start`.
#
# The listeners run in firestore's own thread pool (not the asyncio loop).
# Accessors hold the corresponding threading.Lock briefly when reading or
# mutating the cache so snapshot callbacks and endpoint handlers can coexist.
#
# A listener's first fire delivers every currently-matching doc, so cold
# start is warm within a few hundred ms of process boot.
# ------------------------------------------------------------------

# Keyed by instance_name. Holds every node the client might ask about:
# active (BOOTING/RUNNING/READY) plus FAILED so a client polling a node that
# fell over between ticks can still see the FAILED status.
NODES_CACHE: dict[str, dict] = {}
_nodes_cache_lock = threading.Lock()
_nodes_cache_watcher = None
# Set once the nodes listener's first fire completes. Lifespan waits on this
# so no endpoint request is served while NODES_CACHE is still empty from
# warm-up and could return spurious "no nodes" 404s.
_nodes_cache_ready = threading.Event()
_NODES_CACHE_READY_TIMEOUT_SEC = 10

CLUSTER_CONFIG_CACHE: Optional[dict] = None
_config_cache_lock = threading.Lock()
_config_cache_watcher = None

_ACTIVE_NODE_STATUSES = ["BOOTING", "RUNNING", "READY", "FAILED"]


def _on_nodes_snapshot(_query_snapshot, changes, _read_time):
    with _nodes_cache_lock:
        for change in changes:
            doc_id = change.document.id
            data = change.document.to_dict() or {}
            if change.type.name == "REMOVED":
                NODES_CACHE.pop(doc_id, None)
                continue
            if data.get("status") in _ACTIVE_NODE_STATUSES:
                NODES_CACHE[doc_id] = data
            else:
                NODES_CACHE.pop(doc_id, None)
    # First fire delivers the full matching set; everything after is deltas.
    # Either way the cache is now valid to serve.
    _nodes_cache_ready.set()


def _on_config_snapshot(_query_snapshot, changes, _read_time):
    global CLUSTER_CONFIG_CACHE
    with _config_cache_lock:
        for change in changes:
            if change.document.id == "cluster_config":
                CLUSTER_CONFIG_CACHE = change.document.to_dict() or {}


def _start_caches():
    global _nodes_cache_watcher, _config_cache_watcher
    nodes_filter = firestore.FieldFilter("status", "in", _ACTIVE_NODE_STATUSES)
    _nodes_cache_watcher = (
        DB.collection("nodes").where(filter=nodes_filter).on_snapshot(_on_nodes_snapshot)
    )
    _config_cache_watcher = DB.collection("cluster_config").on_snapshot(_on_config_snapshot)


from main_service.helpers import (
    ChattyClientEndpointFilter,
    Logger,
    format_traceback,
    is_chatty_client_path,
)

# Silence uvicorn access logs for the two endpoints the burla client polls
# on a tight loop during every job (cluster state + per-node status).
python_logging.getLogger("uvicorn.access").addFilter(ChattyClientEndpointFilter())


# Converts null-byte probe paths into 404s instead of 500s.
# Eg: GET /phpbb/%00phpinfo.php raised 500 (because %00 is null byte) but should be 404
class SafeStaticFiles(StaticFiles):
    def lookup_path(self, path):
        try:
            return super().lookup_path(path)
        except ValueError:
            return "", None


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


def get_auth_headers(request: Request):
    authorization = request.session.get("Authorization") or request.headers.get("Authorization")
    email = request.session.get("X-User-Email") or request.headers.get("X-User-Email")
    return {"Authorization": authorization, "X-User-Email": email}


async def get_welcome_name(session: aiohttp.ClientSession):
    try:
        url = f"{BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}/users:welcome_name"
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data["first_name"]
            if response.status != 204:
                response.raise_for_status()
    except aiohttp.ClientError:
        return None


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


from main_service.endpoints.cluster_lifecycle import router as cluster_lifecycle_router
from main_service.endpoints.cluster_views import router as cluster_views_router
from main_service.endpoints.usage import router as usage_router
from main_service.endpoints.settings import router as settings_router
from main_service.endpoints.jobs import router as jobs_router
from main_service.endpoints.storage import router as storage_router
from main_service.endpoints.client import router as client_router


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

    _start_caches()
    # Block traffic until the nodes listener has delivered its first fire.
    # Without this, requests arriving within the ~100-500ms cache warm-up
    # window see an empty NODES_CACHE and can spuriously 404. If firestore
    # is unreachable we time out and proceed anyway (degraded to old race
    # behavior rather than failing startup outright).
    warmed = await asyncio.to_thread(_nodes_cache_ready.wait, _NODES_CACHE_READY_TIMEOUT_SEC)
    if not warmed:
        print(
            f"NODES_CACHE did not warm within {_NODES_CACHE_READY_TIMEOUT_SEC}s; "
            "serving with empty cache.",
            file=sys.stderr,
        )
    try:
        yield
    finally:
        if _nodes_cache_watcher is not None:
            _nodes_cache_watcher.unsubscribe()
        if _config_cache_watcher is not None:
            _config_cache_watcher.unsubscribe()


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None)
app.include_router(cluster_lifecycle_router)
app.include_router(cluster_views_router)
app.include_router(usage_router)
app.include_router(settings_router)
app.include_router(jobs_router)
app.include_router(storage_router)
app.include_router(client_router)

# Allow cross-origin requests for local development and to satisfy Syncfusion preflights
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/user")
async def get_user_info(request: Request):
    return {
        "email": request.session.get("X-User-Email"),
        "name": request.session.get("name"),
        "profile_pic": request.session.get("profile_pic"),
        "timezone": request.session.get("timezone"),
    }


@app.post("/api/logout")
async def logout(request: Request, response: Response):
    request.session.clear()
    response.delete_cookie(key="session", path="/")
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/v3/login/dashboard")
def redirect_google_login(request: Request):
    """This is required to make google not classify us as phishing!
    (login buttons that go to other websites = bad, same website + redirect = good)
    """
    query_string = request.url.query
    url = f"{BURLA_BACKEND_URL}/v3/login/dashboard"
    if query_string:
        url = f"{url}?{query_string}"
    return RedirectResponse(url=url, status_code=307)


@app.get("/v1/login/microsoft/dashboard")
def redirect_microsoft_login(request: Request):
    """This is required to make google not classify us as phishing!
    (login buttopns that go to other websites = bad, same website + redirect = good)
    """
    query_string = request.url.query
    url = f"{BURLA_BACKEND_URL}/v1/login/microsoft/dashboard"
    if query_string:
        url = f"{url}?{query_string}"
    return RedirectResponse(url=url, status_code=307)


@app.get("/version")
def version():
    return {"version": CURRENT_BURLA_VERSION, "project": PROJECT_ID}


# don't move this! must be declared before static files are mounted to the same path below.
@app.get("/")
@app.get("/jobs")
@app.get("/jobs/{job_id}")
@app.get("/settings")
@app.get("/filesystem")
def dashboard():
    return FileResponse("src/main_service/static/index.html")


@app.get("/favicon.png")
def favicon():
    headers = {"Cache-Control": "no-store"}
    path = "src/main_service/static/favicon.png"
    return FileResponse(path, media_type="image/png", headers=headers)


# must be mounted after the above endpoint (`/`) is declared, or this will overwrite that endpoint.
app.mount("/", SafeStaticFiles(directory="src/main_service/static"), name="static")


@app.middleware("http")
async def catch_errors(request: Request, call_next):
    """
    Fastapi `@app.exception_handler` will completely hide errors if middleware is used.
    Catching errors in a `Depends` function will not distinguish
        http errors originating here vs other services.
    """
    try:
        # Important to note that HTTP exceptions do not raise errors here!
        return await call_next(request)
    except Exception as exception:
        # create new response object to return gracefully.
        response = Response(status_code=500, content="Internal server error.")
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
        traceback_str = format_traceback(tb_details)
        Logger(request).log(str(exception), "ERROR", traceback=traceback_str)
        return response


# ------------------------------------------------------------------
# Auth validation cache.
#
# The burla client hits main_service many times during a single
# `remote_parallel_map` (heartbeat every 2 s, plus result polls,
# node polls, etc.). Without this cache, each one triggers a
# round-trip to `backend.burla.dev/users:validate` (~100-200 ms), which
# both slows every client call AND loads up the central auth service.
#
# A successful validation is trusted for `_AUTH_CACHE_TTL_SEC`. On a cache
# miss (never validated, or the entry expired) the middleware re-validates
# against the backend before deciding - so a request that looks "invalid"
# from cache state alone always gets a fresh backend check first rather
# than being rejected outright. We only cache successes; a 401 from the
# backend is never cached, so a user whose access is fixed will get in on
# their very next request.
#
# A revoked user can still hit main_service for up to TTL seconds after
# revocation, which we accept in exchange for the latency win.
# ------------------------------------------------------------------

_AUTH_CACHE_TTL_SEC = 60 * 60  # 1 hour
_auth_cache: dict[tuple[str, str], float] = {}  # (email, authorization) -> expires_at
_auth_cache_lock = threading.Lock()


def _cached_auth_ok(email: str, authorization: str) -> bool:
    key = (email, authorization)
    with _auth_cache_lock:
        expires_at = _auth_cache.get(key)
        if expires_at is None or time() >= expires_at:
            # Drop the stale entry so a subsequent successful backend
            # validation rewrites it cleanly instead of racing against
            # a dangling expired one.
            if expires_at is not None:
                _auth_cache.pop(key, None)
            return False
        return True


def _remember_auth_ok(email: str, authorization: str) -> None:
    with _auth_cache_lock:
        _auth_cache[(email, authorization)] = time() + _AUTH_CACHE_TTL_SEC
        # Lightweight eviction: if the cache grows past a few hundred entries
        # (unlikely in practice), drop anything already expired.
        if len(_auth_cache) > 500:
            now = time()
            for cached_key, cached_expiry in list(_auth_cache.items()):
                if cached_expiry <= now:
                    _auth_cache.pop(cached_key, None)


@app.middleware("http")
async def validate_requests(request: Request, call_next):
    """
    Login flow for totally new user:
      - no `client_id` or `auth_cookie` user goes to login page
      - login page -> backend svc -> google login -> backend svc -> here again but with client_id
      - use client_id to get auth info, set auth cookie -> redirect here again but with auth cookie
      - here again with auth cookie -> access granted
    """
    # Local-dev bypass: the auth middleware normally validates every request
    # against backend.burla.dev, which requires a Google/Microsoft login. In
    # local-dev there is no real user flow, so stamp a fake session and let
    # everything through. NEVER runs in prod because IN_LOCAL_DEV_MODE is only
    # set by the `make local-dev` target.
    if IN_LOCAL_DEV_MODE:
        if not request.session.get("X-User-Email"):
            request.session["X-User-Email"] = "local-dev@burla.dev"
            request.session["Authorization"] = "Bearer local-dev"
            request.session["name"] = "Local Dev"
            request.session["profile_pic"] = ""
        return await call_next(request)

    # Allow unauthenticated access for storage stub endpoints and resumable signing during development
    # These are non-privileged helpers used by the storage UI.
    if request.url.path.startswith("/api/sf/") or request.url.path == "/signed-resumable":
        return await call_next(request)
    if request.url.path in ["/v3/login/dashboard", "/v1/login/microsoft/dashboard"]:
        return await call_next(request)

    # Allow Server-Sent Events to pass through without auth to prevent proxy/login HTML from breaking the stream
    # These endpoints read from Firestore only and do not perform privileged actions.
    accept_header = request.headers.get("accept", "")
    if "text/event-stream" in accept_header:
        return await call_next(request)
    # allow static asset requests (js/css/images) to pass through
    last_segment = request.url.path.rstrip("/").split("/")[-1]
    if "." in last_segment:
        return await call_next(request)

    client_id = request.query_params.get("client_id")
    email = request.session.get("X-User-Email") or request.headers.get("X-User-Email")
    authorization = request.session.get("Authorization") or request.headers.get("Authorization")
    auth_cookie_exists = email and authorization

    # Short-circuit the backend round-trip if we validated this same
    # (email, auth_token) pair recently.
    if auth_cookie_exists and not client_id and _cached_auth_ok(email, authorization):
        return await call_next(request)

    async with aiohttp.ClientSession() as session:
        if client_id:
            url = f"{BURLA_BACKEND_URL}/v2/login/dashboard/{client_id}/token"
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    request.session["X-User-Email"] = data["email"]
                    request.session["Authorization"] = f"Bearer {data['token']}"
                    request.session["profile_pic"] = data["profile_pic"]
                    request.session["name"] = data["name"]
                    base_url = f"{request.url.scheme}://{request.url.netloc}{request.url.path}"
                    return RedirectResponse(url=base_url, status_code=303)
                elif response.status == 403:
                    data = await response.json()
                    first_name = await get_welcome_name(session)
                    rendered = STATIC_FILES_ENV.get_template("login.html.j2").render(
                        redirect_locally=REDIRECT_LOCALLY_ON_LOGIN,
                        project_id=PROJECT_ID,
                        user_email=data["detail"]["email"],
                        first_name=first_name,
                    )
                    return Response(content=rendered, status_code=403, media_type="text/html")
        elif auth_cookie_exists:
            url = f"{BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}/users:validate"
            headers = {"Authorization": authorization, "X-User-Email": email}
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    _remember_auth_ok(email, authorization)
                    return await call_next(request)
                elif response.status != 401:
                    response.raise_for_status()
                else:
                    first_name = await get_welcome_name(session)
                    rendered = STATIC_FILES_ENV.get_template("login.html.j2").render(
                        redirect_locally=REDIRECT_LOCALLY_ON_LOGIN,
                        project_id=PROJECT_ID,
                        user_email=email,
                        first_name=first_name,
                    )
                    return Response(content=rendered, status_code=200, media_type="text/html")

        first_name = await get_welcome_name(session)
        rendered = STATIC_FILES_ENV.get_template("login.html.j2").render(
            redirect_locally=REDIRECT_LOCALLY_ON_LOGIN,
            project_id=PROJECT_ID,
            first_name=first_name,
        )
        return Response(content=rendered, status_code=200, media_type="text/html")


@app.middleware("http")
async def log_and_time_requests(request: Request, call_next):
    start = time()
    request.state.uuid = uuid4().hex

    response = await call_next(request)

    if not IN_LOCAL_DEV_MODE and not is_chatty_client_path(request.url.path):

        response_contains_background_tasks = getattr(response, "background") is not None
        if not response_contains_background_tasks:
            response.background = BackgroundTasks()

        logger = Logger(request)
        add_background_task = get_add_background_task_function(response.background, logger=logger)
        add_background_task(logger.log, f"Received {request.method} at {request.url}")

        status = response.status_code
        latency = time() - start
        msg = f"{request.method} to {request.url} returned {status} after {latency} seconds."
        add_background_task(logger.log, msg, latency=latency)

    return response


@app.middleware("http")
async def set_timezone_middleware(request: Request, call_next):
    timezone_header = request.headers.get("X-User-Timezone")
    if timezone_header:
        request.session["timezone"] = timezone_header
    return await call_next(request)


app.add_middleware(SessionMiddleware, secret_key=CLUSTER_ID_TOKEN, same_site="lax")
