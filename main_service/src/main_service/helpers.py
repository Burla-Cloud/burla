import os
import sys
import requests
import logging as python_logging
from itertools import groupby
from typing import Optional

from fastapi import Request

from main_service import PROJECT_ID, BURLA_BACKEND_URL


# Paths hit on a tight loop during a job:
#  - `/v1/cluster/state`              ~every 10-100ms while waiting for nodes to boot
#  - `/v1/cluster/nodes/{instance}`   ~every 2-6s per booting node
#  - `/v1/nodes/{instance}/state`     ~every 1s per node (state push)
#  - `/v1/jobs/{id}/peers`            ~every 1s per node while stealing
# Without filtering these drown real request logs in stdout (uvicorn.access)
# and our `log_and_time_requests` middleware.
_CHATTY_CLIENT_PATH_SUBSTRINGS = (
    "/v1/cluster/state",
    "/v1/cluster/nodes/",
    "/state",
    "/peers",
    "/logs:batch",
)


def is_chatty_client_path(path: str) -> bool:
    return any(substring in path for substring in _CHATTY_CLIENT_PATH_SUBSTRINGS)


class ChattyClientEndpointFilter(python_logging.Filter):
    """Drop uvicorn access-log records for paths polled on a tight loop
    during a job."""

    def filter(self, record):
        path = record.args[2]
        return not is_chatty_client_path(path)


def log_telemetry(message, severity="INFO", **kwargs):
    # Same kill switch as the client's _reporting.py; test runs set it so
    # they don't spam Slack through the backend's telemetry route.
    if os.environ.get("DISABLE_BURLA_TELEMETRY") == "True":
        return
    try:
        payload = {"project_id": PROJECT_ID, "message": message, **kwargs}
        requests.post(f"{BURLA_BACKEND_URL}/v1/telemetry/log/{severity}", json=payload, timeout=1)
    except Exception:
        pass


def parse_version(version_str: str) -> tuple[int, ...]:
    """Tuple-compare-friendly version parse. Assumes MAJOR.MINOR.PATCH."""
    return tuple(int(part) for part in version_str.split("."))


def format_traceback(traceback_details: list):
    details = ["  ... (detail hidden)\n" if "/pypoetry/" in d else d for d in traceback_details]
    details = [key for key, _ in groupby(details)]  # <- remove consecutive duplicates
    return "".join(details).split("another exception occurred:")[-1]


class Logger:
    """Logs to stdout (journald / docker captures it on the head VM) and
    forwards errors to Burla's telemetry backend."""

    def __init__(self, request: Optional[Request] = None):
        self.request_line = f"{request.method} {request.url}" if request else None

    def log(self, message: str, severity="INFO", **kw):
        prefix = f"[{severity}]" if severity != "INFO" else ""
        suffix = f" ({self.request_line})" if self.request_line else ""
        if "traceback" in kw.keys():
            print(f"\nERROR: {message.strip()}{suffix}\n{kw['traceback'].strip()}\n", file=sys.stderr)
        else:
            print(f"{prefix}{message}{suffix}".strip())

        if severity == "ERROR" or "traceback" in kw:
            tb = kw.get("traceback", "")
            log_telemetry(message, severity="ERROR", traceback=tb)
