import sys
import os
import json
import traceback
import aiohttp
from uuid import uuid4
from time import time, sleep
from typing import Callable
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

CURRENT_BURLA_VERSION = "1.3.1"

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

LOCAL_DEV_CONFIG = None
if IN_LOCAL_DEV_MODE:
    config_doc = DB.collection("cluster_config").document("cluster_config").get()
    LOCAL_DEV_CONFIG = config_doc.to_dict()
    LOCAL_DEV_CONFIG["Nodes"][0]["machine_type"] = "n4-standard-2"
    LOCAL_DEV_CONFIG["Nodes"][0]["quantity"] = 1

DEFAULT_CONFIG = {  # <- config used only when config is missing from firestore
    "Nodes": [
        {
            "containers": [
                {
                    "image": "python:3.12",
                    "python_version": "3.12",
                },
            ],
            "machine_type": "n4-standard-4",
            "gcp_region": "us-central1",
            "quantity": 1,
            "inactivity_shutdown_time_sec": 60 * 10,
        }
    ]
}
from main_service.helpers import Logger, format_traceback


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
from main_service.endpoints.settings import router as settings_router
from main_service.endpoints.jobs import router as jobs_router
from main_service.endpoints.storage import router as storage_router


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
app.include_router(settings_router)
app.include_router(jobs_router)
app.include_router(storage_router)

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
app.mount("/", StaticFiles(directory="src/main_service/static"), name="static")


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


@app.middleware("http")
async def validate_requests(request: Request, call_next):
    """
    Login flow for totally new user:
      - no `client_id` or `auth_cookie` user goes to login page
      - login page -> backend svc -> google login -> backend svc -> here again but with client_id
      - use client_id to get auth info, set auth cookie -> redirect here again but with auth cookie
      - here again with auth cookie -> access granted
    """
    # Allow unauthenticated access for storage stub endpoints and resumable signing during development
    # These are non-privileged helpers used by the storage UI.
    if request.url.path.startswith("/api/sf/") or request.url.path == "/signed-resumable":
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
    email = request.session.get("X-User-Email")
    authorization = request.session.get("Authorization")
    auth_cookie_exists = email and authorization
    async with aiohttp.ClientSession() as session:

        first_name = None
        url = f"{BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}/users:welcome_name"
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                first_name = data["first_name"]
            elif response.status != 204:
                response.raise_for_status()

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
                    return await call_next(request)
                elif response.status != 401:
                    response.raise_for_status()
                else:
                    rendered = STATIC_FILES_ENV.get_template("login.html.j2").render(
                        redirect_locally=REDIRECT_LOCALLY_ON_LOGIN,
                        project_id=PROJECT_ID,
                        user_email=email,
                        first_name=first_name,
                    )
                    return Response(content=rendered, status_code=401, media_type="text/html")

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

    if not IN_LOCAL_DEV_MODE:

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
