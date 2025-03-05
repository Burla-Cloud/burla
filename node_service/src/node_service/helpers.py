import sys
import socket
import random
import requests
from itertools import groupby
from typing import Optional, Callable
from requests.exceptions import HTTPError

from fastapi import Request
from docker.errors import APIError, NotFound
from node_service import IN_LOCAL_DEV_MODE, GCL_CLIENT, SELF, PROJECT_ID, BURLA_BACKEND_URL


PRIVATE_PORT_QUEUE = list(range(32768, 60999))  # <- these ports should be mostly free.


def ignore_400_409_404(f: Callable):

    def wrapped(*a, **kw):
        try:
            f(*a, **kw)
        except (APIError, NotFound, HTTPError) as e:
            # ignore errors indicating the desired operation already happened.
            if not ("400" in str(e) or "404" in str(e) or "409" in str(e)):
                raise e

    return wrapped


def startup_error_msg(container_logs, image):
    return {
        "severity": "ERROR",
        "message": "worker timed out.",
        "exception": container_logs,
        "job_id": SELF["current_job_id"],
        "image": image,
    }


def next_free_port():
    """
    pops ports from `PRIVATE_PORT_QUEUE` until free one is found.
    The "correct" way to do this is to bind to port 0 which tells the os to return a random free
    port. This was attempted first, but it kept returning already-in-use ports?
    """
    index = random.randint(0, len(PRIVATE_PORT_QUEUE) - 1)
    port = PRIVATE_PORT_QUEUE.pop(index)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex(("localhost", port)) != 0:
            return port
        else:
            return next_free_port()


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

        if not IN_LOCAL_DEV_MODE:
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
