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
from starlette.middleware.sessions import SessionMiddleware
from starlette.datastructures import UploadFile
from jinja2 import Environment, FileSystemLoader

os.environ["GRPC_VERBOSITY"] = "ERROR"
os.environ["GLOG_minloglevel"] = "2"

# This is the only possible alternative "mode".
# In this mode everything runs locally in docker containers.
IN_LOCAL_DEV_MODE = os.environ.get("IN_LOCAL_DEV_MODE") == "True"

CREDENTIALS, PROJECT_ID = google.auth.default()
BURLA_BACKEND_URL = "https://backend.burla.dev"
GCL_CLIENT = logging.Client().logger("main_service")
DB = firestore.Client(database="burla")

env = Environment(loader=FileSystemLoader("src/main_service/static"))
secret_client = secretmanager.SecretManagerServiceClient()
secret_name = f"projects/{PROJECT_ID}/secrets/burla-cluster-id-token/versions/latest"
response = secret_client.access_secret_version(request={"name": secret_name})
CLUSTER_ID_TOKEN = response.payload.data.decode("UTF-8")

LOCAL_DEV_CONFIG = {  # <- config used only in local dev mode
    "Nodes": [
        {
            "containers": [
                {
                    "image": "us-docker.pkg.dev/burla-test/cluster-default/3.10:latest",
                    "python_version": "3.10",
                },
            ],
            "machine_type": "n4-standard-1",  # should match `INSTANCE_N_CPUS` in node svc
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
                    "image": "burlacloud/default-image-py3.12",
                    "python_version": "3.12",
                },
            ],
            "machine_type": "n4-standard-4",
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
from main_service.endpoints.settings import router as settings_router
from main_service.endpoints.jobs import router as jobs_router


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


@app.get("/auth-success")
def auth_success():
    rendered = env.get_template("authorized.html.j2").render()
    return Response(content=rendered, status_code=200, media_type="text/html")


# don't move this! must be declared before static files are mounted to the same path below.
@app.get("/")
@app.get("/jobs")
@app.get("/jobs/{job_id}")
@app.get("/settings")
def dashboard():
    return FileResponse("src/main_service/static/index.html")


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
    # allow static asset requests (js/css/images) to pass through
    last_segment = request.url.path.rstrip("/").split("/")[-1]
    if "." in last_segment:
        return await call_next(request)

    # convert temporary client_id to email/token
    # client_id's are only valid once, and for a very short period of time
    if request.query_params.get("client_id"):
        client_id = request.query_params.get("client_id")
        token_url = f"{BURLA_BACKEND_URL}/v1/login/{client_id}/token"
        async with aiohttp.ClientSession() as session:
            async with session.get(token_url) as response:
                if response.status == 200:
                    data = await response.json()
                    request.session["X-User-Email"] = data["email"]
                    request.session["Authorization"] = f"Bearer {data['token']}"
                    request.session["profile_pic"] = data["profile_pic"]
                    request.session["name"] = data["name"]

        base_url = f"{request.url.scheme}://{request.url.netloc}{request.url.path}"
        response = RedirectResponse(url=base_url, status_code=303)
        session = request.cookies.get("session")
        response.set_cookie(key="session", value=session, httponly=True, samesite="lax")
        return response

    email = request.session.get("X-User-Email") or request.headers.get("X-User-Email")
    authorization = request.session.get("Authorization") or request.headers.get("Authorization")
    if not email or not authorization:
        rendered = env.get_template("login.html.j2").render(user_email=None)
        return Response(content=rendered, status_code=401, media_type="text/html")

    async with aiohttp.ClientSession() as session:
        url = f"{BURLA_BACKEND_URL}/v1/projects/{PROJECT_ID}/users:validate"
        headers = {"Authorization": authorization, "X-User-Email": email}
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                return await call_next(request)
            elif response.status != 401:
                response.raise_for_status()

    rendered = env.get_template("login.html.j2").render(user_email=email)
    return Response(content=rendered, status_code=403, media_type="text/html")


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


app.add_middleware(SessionMiddleware, secret_key=CLUSTER_ID_TOKEN)


# @app.middleware("http")
# async def set_timezone_middleware(request: Request, call_next):
#     timezone_header = request.headers.get("X-User-Timezone")
#     if timezone_header:
#         request.session["timezone"] = timezone_header
#     return await call_next(request)


# @app.post("/api/timezone")
# async def set_timezone(request: Request):
#     data = await request.json()
#     timezone = data.get("timezone")
#     if timezone:
#         request.session["timezone"] = timezone
#     return {"timezone": timezone}
