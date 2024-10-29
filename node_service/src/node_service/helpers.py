import sys
import socket
from itertools import groupby
from typing import Optional
from datetime import datetime, timedelta, timezone

from collections import deque

from fastapi import Request
from node_service import IN_DEV, GCL_CLIENT, SELF


PRIVATE_PORT_QUEUE = deque(range(32768, 60999))  # <- these ports should be mostly free.


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
    port. This was attempted first, but it kept returning already-in-use ports.
    """
    port = PRIVATE_PORT_QUEUE.pop()
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
        if IN_DEV and "traceback" in kw.keys():
            print(f"\nERROR: {message.strip()}\n{kw['traceback'].strip()}\n", file=sys.stderr)
        elif IN_DEV:
            eastern_time = datetime.now(timezone.utc) + timedelta(hours=-4)
            print(f"{eastern_time.strftime('%I:%M:%S.%f %p')}: {message}")
        else:
            struct = dict(message=message, request=self.loggable_request, **kw)
            GCL_CLIENT.log_struct(struct, severity=severity)
