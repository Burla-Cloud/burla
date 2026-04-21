import sys
import requests
import logging as python_logging
from itertools import groupby
from typing import Optional

from fastapi import Request

from main_service import PROJECT_ID, BURLA_BACKEND_URL, GCL_CLIENT


# Paths the burla pypi client polls heavily during a job:
#  - `/v1/cluster/state`              ~every 10-100ms while waiting for nodes to boot
#  - `/v1/cluster/nodes/{instance}`   ~every 2-6s per booting node
# Without filtering these drown real request logs in both stdout (uvicorn.access)
# and Cloud Logging (our `log_and_time_requests` middleware).
_CHATTY_CLIENT_PATH_SUBSTRINGS = ("/v1/cluster/state", "/v1/cluster/nodes/")


def is_chatty_client_path(path: str) -> bool:
    return any(substring in path for substring in _CHATTY_CLIENT_PATH_SUBSTRINGS)


class ChattyClientEndpointFilter(python_logging.Filter):
    """Drop uvicorn access-log records for paths the burla client polls on
    a tight loop during a job."""

    def filter(self, record):
        path = record.args[2]
        return not is_chatty_client_path(path)


def log_telemetry(message, severity="INFO", **kwargs):
    try:
        payload = {"project_id": PROJECT_ID, "message": message, **kwargs}
        requests.post(f"{BURLA_BACKEND_URL}/v1/telemetry/log/{severity}", json=payload, timeout=1)
    except Exception:
        pass


# CPU -> RAM mapping for the n4-standard family; used to size-check that a
# node can physically host the requested per-function resources.
_N_FOUR_STANDARD_CPU_TO_RAM = {
    1: 4, 2: 8, 4: 16, 8: 32, 16: 64, 32: 128, 48: 192, 64: 256, 80: 320,
}


def parallelism_capacity(machine_type: str, func_cpu: int, func_ram: int) -> int:
    """How many copies of a UDF with func_cpu/func_ram fit on one node.

    Used by `POST /v1/jobs/{id}/start` to size which cached ready nodes
    can physically host the requested per-function resources.
    """
    if machine_type.startswith("n4-standard") and machine_type.split("-")[-1].isdigit():
        vm_cpu = int(machine_type.split("-")[-1])
        vm_ram = _N_FOUR_STANDARD_CPU_TO_RAM[vm_cpu]
        return min(vm_cpu // func_cpu, vm_ram // func_ram)
    if machine_type.startswith("a") and machine_type.endswith("g"):
        return 1
    raise ValueError("machine_type must be: n4-standard-X, a3-highgpu-Xg, or a3-ultragpu-8g")


def parse_version(version_str: str) -> tuple[int, ...]:
    """Tuple-compare-friendly version parse. Assumes MAJOR.MINOR.PATCH."""
    return tuple(int(part) for part in version_str.split("."))


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

        if severity == "ERROR" or "traceback" in kw:
            tb = kw.get("traceback", "")
            log_telemetry(message, severity="ERROR", traceback=tb)
