import sys
import requests
from itertools import groupby
from datetime import datetime, timedelta, timezone

from fastapi import Request, HTTPException
from google.cloud.secretmanager import SecretManagerServiceClient

from main_service import PROJECT_ID, BURLA_BACKEND_URL, IN_DEV, GCL_CLIENT


def get_secret(secret_name: str):
    client = SecretManagerServiceClient()
    secret_path = client.secret_version_path(PROJECT_ID, secret_name, "latest")
    response = client.access_secret_version(request={"name": secret_path})
    return response.payload.data.decode("UTF-8")


def format_traceback(traceback_details: list):
    details = ["  ... (detail hidden)\n" if "/pypoetry/" in d else d for d in traceback_details]
    details = [key for key, _ in groupby(details)]  # <- remove consecutive duplicates
    return "".join(details).split("another exception occurred:")[-1]


class Logger:

    def __init__(self, request: Request):
        self.loggable_request = self.__loggable_request(request)

    def __make_serializeable(self, obj):
        """
        Recursively traverses a nested dict swapping any:
        - tuple -> list
        - !dict or !list or !str -> str
        """
        if isinstance(obj, tuple) or isinstance(obj, list):
            return [self.__make_serializeable(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: self.__make_serializeable(value) for key, value in obj.items()}
        elif not (isinstance(obj, dict) or isinstance(obj, list) or isinstance(obj, str)):
            return str(obj)
        else:
            return obj

    def __loggable_request(self, request: Request):
        keys = ["asgi", "client", "headers", "http_version", "method", "path", "path_params"]
        keys.extend(["query_string", "raw_path", "root_path", "scheme", "server", "state", "type"])
        scope = {key: request.scope.get(key) for key in keys}
        request_dict = {
            "scope": scope,
            "url": str(request.url),
            "base_url": str(request.base_url),
            "headers": request.headers,
            "query_params": request.query_params,
            "path_params": request.path_params,
            "cookies": request.cookies,
            "client": request.client,
            "method": request.method,
        }
        # google cloud logging won't log tuples or bytes objects.
        return self.__make_serializeable(request_dict)

    def log(self, message: str, severity="INFO", **kw):
        if IN_DEV and "traceback" in kw.keys():
            print(f"\nERROR: {message.strip()}\n{kw['traceback'].strip()}\n", file=sys.stderr)
        elif IN_DEV:
            eastern_time = datetime.now(timezone.utc) + timedelta(hours=-4)
            print(f"{eastern_time.strftime('%I:%M:%S.%f %p')}: {message}")
        else:
            struct = dict(message=message, request=self.loggable_request, **kw)
            GCL_CLIENT.log_struct(struct, severity=severity)


def validate_create_job_request(request_json: dict):
    if request_json["python_version"] not in ["3.8", "3.9", "3.10", "3.11", "3.12"]:
        raise HTTPException(400, detail="invalid python version, } [3.8, 3.9, 3.10, 3.11, 3.12]")
    elif (request_json["func_cpu"] > 96) or (request_json["func_cpu"] < 1):
        raise HTTPException(400, detail="invalid func_cpu, must be in [1.. 96]")
    elif (request_json["func_ram"] > 624) or (request_json["func_ram"] < 1):
        raise HTTPException(400, detail="invalid func_ram, must be in [1.. 624]")
    # elif (request_json["func_gpu"] > 4) or (request_json["func_ram"] < 1):
    #     abort(400, "invalid func_gpu, must be in [1.. 4]")


def validate_headers_and_login(request: Request):

    headers = {"authorization": request.headers.get("Authorization")}
    if request.headers.get("Email"):
        headers["Email"] = request.headers.get("Email")

    response = requests.get(f"{BURLA_BACKEND_URL}/v1/private/user_info", headers=headers)
    response.raise_for_status()
    return response.json()
