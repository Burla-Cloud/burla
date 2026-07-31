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
from typing import Callable
from pathlib import Path
from contextlib import asynccontextmanager
from urllib.parse import urlparse

from fastapi.responses import Response, FileResponse, HTMLResponse, RedirectResponse
from fastapi import FastAPI, Request, BackgroundTasks, Depends, status
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.datastructures import UploadFile
from jinja2 import Environment, FileSystemLoader

CURRENT_BURLA_VERSION = "1.6.1"
MIN_COMPATIBLE_CLIENT_VERSION = "1.6.1"
NODE_SOURCE_REF = os.environ.get("BURLA_NODE_SOURCE_REF", CURRENT_BURLA_VERSION)

# In this mode EVERYTHING runs locally in docker containers.
# possible modes: local-dev-mode (everything local), remote-dev-mode (only main-service local), prod
IN_LOCAL_DEV_MODE = os.environ.get("IN_LOCAL_DEV_MODE") == "True"
# This is needed because remote-dev-mode is not local-dev-mode, and needs local redirect on login.
REDIRECT_LOCALLY_ON_LOGIN = os.environ.get("REDIRECT_LOCALLY_ON_LOGIN") == "True"

# The default way Burla runs: main_service lives inside the `burla` pip
# package on the user's machine (started by `remote_parallel_map` or
# `burla dashboard`), boots real cloud VMs with the user's own credentials,
# and is reachable by nodes through the relay. No head VM, no service
# accounts, no buckets - see client/src/burla/_local_head.py.
IN_CLIENT_HOSTED_MODE = os.environ.get("IN_CLIENT_HOSTED_MODE") == "True"

# The owner's real backend credentials (from burla_credentials.json). Local
# requests to a client-hosted head are stamped with these instead of being
# sent through the login flow - they must be real because head -> node calls
# replay them, and nodes validate them against the backend's user list.
LOCAL_USER_EMAIL = os.environ.get("BURLA_LOCAL_USER_EMAIL", "")
LOCAL_USER_TOKEN = os.environ.get("BURLA_LOCAL_USER_TOKEN", "")

# "gcp" or "aws" - which cloud this cluster boots node VMs in.
CLOUD_PROVIDER = os.environ.get("CLOUD_PROVIDER", "gcp")

BURLA_BACKEND_URL = os.environ.get(
    "BURLA_BACKEND_URL", "https://backend.burla.dev"
).rstrip("/")

# Clients reach nodes and the dashboard through Burla's frp relay: nodes dial
# out to it, so user projects need zero inbound firewall rules. Dev clusters
# override this to a test relay (see Makefile).
BURLA_RELAY_HOST = (
    os.environ.get("BURLA_RELAY_HOST", "relay.burla.dev").strip().lower().rstrip(".")
)
BURLA_RELAY_SERVER_ADDR = os.environ.get("BURLA_RELAY_SERVER_ADDR") or BURLA_RELAY_HOST
BURLA_RELAY_SERVER_PORT = int(os.environ.get("BURLA_RELAY_SERVER_PORT", 7000))
FRP_VERSION = "0.70.1"
print(f"Using Burla backend: {BURLA_BACKEND_URL}")


def _resolve_project_id() -> str:
    """The cluster identifier used by backend.burla.dev and cloud APIs.
    On GCP it's the GCP project id; on AWS it's set at install time."""
    project_id = os.environ.get("PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    if project_id:
        return project_id
    import google.auth

    _, project_id = google.auth.default()
    return project_id


PROJECT_ID = _resolve_project_id()


def relay_fqdn(instance_name: str) -> str:
    """Hostname the relay routes to this node, e.g.
    burla-node-1a2b3c4d--my-project.relay.burla.dev"""
    return f"{instance_name}--{PROJECT_ID}.{BURLA_RELAY_HOST}"


# Package-relative so the vendored copy inside the burla pip package finds
# its static files no matter what the working directory is.
STATIC_DIR = Path(__file__).parent / "static"
STATIC_FILES_ENV = Environment(loader=FileSystemLoader(str(STATIC_DIR)))


def _resolve_cluster_id_token() -> str:
    token = os.environ.get("CLUSTER_ID_TOKEN")
    if token:
        return token
    if IN_LOCAL_DEV_MODE:
        return "local-dev-token"
    raise RuntimeError("CLUSTER_ID_TOKEN env var is required outside local-dev mode.")


CLUSTER_ID_TOKEN = _resolve_cluster_id_token()

# Base URL node VMs use to reach this service. Nodes run in the same VPC as
# the head VM, so this is the head's internal IP (or the docker network
# hostname in local-dev). The public dashboard URL is separate.
MAIN_SERVICE_PORT = int(os.environ.get("PORT", 5001))
INTERNAL_TLS_PORT = int(os.environ.get("INTERNAL_TLS_PORT", 8443))


def _resolve_self_url_for_nodes() -> str:
    if IN_LOCAL_DEV_MODE:
        return f"http://main_service:{MAIN_SERVICE_PORT}"
    override = os.environ.get("MAIN_SERVICE_URL_FOR_NODES")
    if override:
        return override.rstrip("/")
    import requests as _requests

    if CLOUD_PROVIDER == "aws":
        token_response = _requests.put(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"},
            timeout=5,
        )
        ip_response = _requests.get(
            "http://169.254.169.254/latest/meta-data/local-ipv4",
            headers={"X-aws-ec2-metadata-token": token_response.text},
            timeout=5,
        )
        internal_ip = ip_response.text.strip()
    else:
        ip_response = _requests.get(
            "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip",
            headers={"Metadata-Flavor": "Google"},
            timeout=5,
        )
        internal_ip = ip_response.text.strip()
    return f"https://{internal_ip}:{INTERNAL_TLS_PORT}"


MAIN_SERVICE_URL_FOR_NODES = _resolve_self_url_for_nodes()
if not IN_LOCAL_DEV_MODE:
    from main_service.transport_tls import ensure_cluster_tls

    ensure_cluster_tls(urlparse(MAIN_SERVICE_URL_FOR_NODES).hostname)

# Bucket FUSE-mounted at /workspace/shared in every container (GCS on GCP,
# S3 on AWS). Empty/unset disables the shared filesystem entirely - the
# default in client-hosted mode so users need zero storage permissions.
# `burla deploy` passes the bucket it created; local-dev keeps the old name
# because node containers bind-mount a local dir under the same config key.
def _default_shared_workspace_bucket():
    bucket = os.environ.get("SHARED_WORKSPACE_BUCKET")
    if bucket:
        return bucket
    if IN_CLIENT_HOSTED_MODE:
        return None
    return f"{PROJECT_ID}-burla-shared-workspace"


DEFAULT_CONFIG = {  # <- config used only when no config has ever been saved
    "Nodes": [
        {
            "containers": [
                {
                    "image": "python:3.12",
                },
            ],
            "machine_type": (
                "n4-standard-4" if CLOUD_PROVIDER == "gcp" else "m7i.2xlarge"
            ),
            # Region nodes boot in. Field is named gcp_region for historical
            # reasons; on AWS it holds an AWS region (e.g. us-east-1).
            "gcp_region": (
                "us-central1"
                if CLOUD_PROVIDER == "gcp"
                else os.environ.get("AWS_REGION", "us-east-1")
            ),
            "quantity": 1,
            "inactivity_shutdown_time_sec": 60 * 10,
        }
    ],
    "gcs_bucket_name": _default_shared_workspace_bucket(),
}

from main_service import history

if history.get_cluster_config() is None:
    history.save_cluster_config(DEFAULT_CONFIG)

LOCAL_DEV_CONFIG = None
if IN_LOCAL_DEV_MODE:
    LOCAL_DEV_CONFIG = history.get_cluster_config()
    LOCAL_DEV_CONFIG["Nodes"][0]["machine_type"] = "n4-standard-2"
    LOCAL_DEV_CONFIG["Nodes"][0]["quantity"] = 2

from main_service import cluster_state
from main_service.helpers import (
    ChattyClientEndpointFilter,
    Logger,
    format_traceback,
    is_chatty_client_path,
)

# Silence uvicorn access logs for the endpoints polled on a tight loop during
# every job (cluster state + per-node status + node state pushes).
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
    authorization = request.session.get("Authorization") or request.headers.get(
        "Authorization"
    )
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
        parent_traceback = "Traceback (most recent call last):\n" + format_traceback(
            tb_details
        )

        def func_logged(*a, **kw):
            try:
                return func(*a, **kw)
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                tb_details = traceback.format_exception(
                    exc_type, exc_value, exc_traceback
                )
                local_traceback_no_title = "\n".join(
                    format_traceback(tb_details).split("\n")[1:]
                )
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
from main_service.endpoints.nodes import router as nodes_router


async def _dashboard_lease_loop():
    headers = {"Authorization": f"Bearer {CLUSTER_ID_TOKEN}"}
    url = f"{BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}/dashboard/lease"
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.post(url, headers=headers) as response:
                    response.raise_for_status()
            except Exception as error:
                print(f"Dashboard DNS lease renewal failed: {error}")
            await asyncio.sleep(6 * 60 * 60)


async def _stopped_instance_reaper_loop():
    from main_service.providers import get_provider

    provider = get_provider()
    while True:
        await asyncio.to_thread(provider.delete_stopped_instances)
        await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):

    if IN_LOCAL_DEV_MODE:

        def frontend_built_successfully(attempt=1):
            if attempt == 3:
                return False
            else:
                frontend_built_at = float(
                    Path(".frontend_last_built_at.txt").read_text().strip()
                )
                frontend_rebuilt = time() - frontend_built_at < 4
                if not frontend_rebuilt:
                    sleep(
                        2
                    )  # wait a couple sec then try again (could still be building)
                    return frontend_built_successfully(attempt=attempt + 1)
                return True

        if frontend_built_successfully():
            print(f"Successfully rebuilt frontend.")
        else:
            print(f"FAILED to rebuild frontend?, check logs with `Cmd + Shift + U`.")

    cluster_state.set_event_loop(asyncio.get_running_loop())
    cluster_state.load_from_history()
    reaper_task = asyncio.create_task(cluster_state.job_reaper_loop(logger=Logger()))
    # Client-hosted dashboards are localhost-only; there is no public DNS
    # lease to renew.
    run_lease_loop = not IN_LOCAL_DEV_MODE and not IN_CLIENT_HOSTED_MODE
    dashboard_lease_task = (
        asyncio.create_task(_dashboard_lease_loop()) if run_lease_loop else None
    )

    tls_proxy_server = None
    if IN_CLIENT_HOSTED_MODE:
        # Replaces the head VM's Caddy sidecar: terminates cluster-CA TLS for
        # node traffic arriving through the relay tunnel.
        from main_service.tls_proxy import start_tls_proxy

        tls_proxy_server = await start_tls_proxy(
            listen_port=INTERNAL_TLS_PORT, forward_port=MAIN_SERVICE_PORT
        )

    stopped_instance_reaper_task = None
    if not IN_LOCAL_DEV_MODE:
        # Credential-less nodes can only stop themselves (a stopped GCP VM
        # still bills for its disk); actually deleting them requires cloud
        # credentials, which live here.
        stopped_instance_reaper_task = asyncio.create_task(
            _stopped_instance_reaper_loop()
        )

    try:
        yield
    finally:
        reaper_task.cancel()
        if dashboard_lease_task is not None:
            dashboard_lease_task.cancel()
        if stopped_instance_reaper_task is not None:
            stopped_instance_reaper_task.cancel()
        if tls_proxy_server is not None:
            tls_proxy_server.close()


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None)
app.include_router(cluster_lifecycle_router)
app.include_router(cluster_views_router)
app.include_router(usage_router)
app.include_router(settings_router)
app.include_router(jobs_router)
app.include_router(storage_router)
app.include_router(client_router)
app.include_router(nodes_router)

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


# Injected at request time so the key never lives in the public repo's committed bundles.
SYNCFUSION_LICENSE_KEY = os.environ.get("SYNCFUSION_LICENSE_KEY", "")


# don't move this! must be declared before static files are mounted to the same path below.
@app.get("/")
@app.get("/jobs")
@app.get("/jobs/{job_id}")
@app.get("/settings")
@app.get("/filesystem")
def dashboard():
    html = (STATIC_DIR / "index.html").read_text()
    filesystem_enabled = bool((history.get_cluster_config() or {}).get("gcs_bucket_name"))
    inject = f'<script>window.__SYNCFUSION_LICENSE_KEY__ = "{SYNCFUSION_LICENSE_KEY}";'
    inject += f"window.__BURLA_FILESYSTEM_ENABLED__ = {json.dumps(filesystem_enabled)};</script>"
    return HTMLResponse(html.replace("</head>", f"{inject}</head>"))


@app.get("/favicon.png")
def favicon():
    headers = {"Cache-Control": "no-store"}
    return FileResponse(
        STATIC_DIR / "favicon.png", media_type="image/png", headers=headers
    )


# must be mounted after the above endpoint (`/`) is declared, or this will overwrite that endpoint.
app.mount("/", SafeStaticFiles(directory=str(STATIC_DIR)), name="static")


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
# `remote_parallel_map` (result polls, node polls, etc). Without this cache,
# each one triggers a round-trip to `backend.burla.dev/users:validate`
# (~100-200 ms), which both slows every client call AND loads up the central
# auth service.
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
    # Node services (and VM startup scripts) authenticate with the cluster
    # token - they push state / logs and read job + peer views.
    if request.headers.get("Authorization") == f"Bearer {CLUSTER_ID_TOKEN}":
        return await call_next(request)

    # Local-dev bypass: the auth middleware normally validates every request
    # against backend.burla.dev, which requires a Google/Microsoft login. In
    # local-dev there is no real user flow, so stamp a fake session and let
    # everything through. NEVER runs in prod because IN_LOCAL_DEV_MODE is only
    # set by the `make local-dev` target.
    if IN_LOCAL_DEV_MODE:
        if not request.session.get("X-User-Email"):
            header_email = request.headers.get("X-User-Email")
            header_auth = request.headers.get("Authorization")
            if header_email and header_auth:
                request.session["X-User-Email"] = header_email
                request.session["Authorization"] = header_auth
                request.session["name"] = header_email
                request.session["profile_pic"] = ""
            else:
                request.session["X-User-Email"] = "local-dev@burla.dev"
                request.session["Authorization"] = "Bearer local-dev"
                request.session["name"] = "Local Dev"
                request.session["profile_pic"] = ""
        return await call_next(request)

    # Client-hosted bypass: this head only listens on 127.0.0.1, so a local
    # request is the machine's owner - their own dashboard needs no login.
    # Two kinds of traffic also arrive from 127.0.0.1 and must still log in:
    # relay-tunnel connections (they come through the in-process TLS proxy,
    # recognized by their socket address) and cross-site browser requests
    # (a foreign Origin header means some other webpage sent it).
    if IN_CLIENT_HOSTED_MODE:
        from main_service.tls_proxy import RELAY_CLIENT_ADDRESSES

        client_address = (request.client.host, request.client.port)
        origin = request.headers.get("origin")
        origin_is_local = origin is None or urlparse(origin).hostname in (
            "127.0.0.1",
            "localhost",
        )
        is_machine_owner = (
            request.client.host == "127.0.0.1"
            and client_address not in RELAY_CLIENT_ADDRESSES
            and origin_is_local
        )
        if is_machine_owner:
            if not request.session.get("X-User-Email"):
                header_email = request.headers.get("X-User-Email")
                header_auth = request.headers.get("Authorization")
                if header_email and header_auth:
                    request.session["X-User-Email"] = header_email
                    request.session["Authorization"] = header_auth
                else:
                    request.session["X-User-Email"] = LOCAL_USER_EMAIL
                    request.session["Authorization"] = f"Bearer {LOCAL_USER_TOKEN}"
                request.session["name"] = request.session["X-User-Email"]
                request.session["profile_pic"] = ""
            return await call_next(request)

    # Allow unauthenticated access for storage stub endpoints and resumable signing during development
    # These are non-privileged helpers used by the storage UI.
    if (
        request.url.path.startswith("/api/sf/")
        or request.url.path == "/signed-resumable"
    ):
        return await call_next(request)
    if request.url.path in ["/v3/login/dashboard", "/v1/login/microsoft/dashboard"]:
        return await call_next(request)

    # allow static asset requests (js/css/images) to pass through
    last_segment = request.url.path.rstrip("/").split("/")[-1]
    if "." in last_segment:
        return await call_next(request)

    client_id = request.query_params.get("client_id")
    email = request.session.get("X-User-Email") or request.headers.get("X-User-Email")
    authorization = request.session.get("Authorization") or request.headers.get(
        "Authorization"
    )
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
                    base_url = (
                        f"{request.url.scheme}://{request.url.netloc}{request.url.path}"
                    )
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
                    return Response(
                        content=rendered, status_code=403, media_type="text/html"
                    )
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
                    return Response(
                        content=rendered, status_code=200, media_type="text/html"
                    )

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
        add_background_task = get_add_background_task_function(
            response.background, logger=logger
        )
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


app.add_middleware(
    SessionMiddleware,
    secret_key=CLUSTER_ID_TOKEN,
    same_site="lax",
    https_only=not IN_LOCAL_DEV_MODE,
)
