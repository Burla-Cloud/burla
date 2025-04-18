import sys
import requests
from itertools import groupby
from typing import Optional

from fastapi import Request

from main_service import PROJECT_ID, BURLA_BACKEND_URL, GCL_CLIENT


def format_traceback(traceback_details: list):
    details = ["  ... (detail hidden)\n" if "/pypoetry/" in d else d for d in traceback_details]
    details = [key for key, _ in groupby(details)]  # <- remove consecutive duplicates
    return "".join(details).split("another exception occurred:")[-1]


class Logger:

    def __init__(self, request: Optional[Request] = None):
        self.loggable_request = self.__loggable_request(request) if request else {}

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
        if "traceback" in kw.keys():
            print(f"\nERROR: {message.strip()}\n{kw['traceback'].strip()}\n", file=sys.stderr)
        else:
            print(message)

        struct = dict(message=message, request=self.loggable_request, **kw)
        GCL_CLIENT.log_struct(struct, severity=severity)

        # Report errors back to Burla's cloud.
        if severity == "ERROR" or "traceback" in kw:
            try:
                tb = kw.get("traceback", "")
                json = {"project_id": PROJECT_ID, "message": message, "traceback": tb}
                requests.post(f"{BURLA_BACKEND_URL}/v1/telemetry/alert", json=json, timeout=1)
            except Exception:
                pass


def validate_headers_and_login(request: Request):

    headers = {"authorization": request.headers.get("Authorization")}
    if request.headers.get("Email"):
        headers["Email"] = request.headers.get("Email")

    response = requests.get(f"{BURLA_BACKEND_URL}/v1/private/user_info", headers=headers)
    response.raise_for_status()
    return response.json()
