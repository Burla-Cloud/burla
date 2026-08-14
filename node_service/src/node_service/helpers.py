import asyncio
import os
import requests
from itertools import groupby
from typing import Optional
import logging as python_logging
from time import time

from fastapi import Request
from node_service import PROJECT_ID, BURLA_BACKEND_URL


def format_traceback(traceback_details: list):
    details = ["  ... (detail hidden)\n" if "/pypoetry/" in d else d for d in traceback_details]
    details = [key for key, _ in groupby(details)]  # <- remove consecutive duplicates
    return "".join(details).split("another exception occurred:")[-1]


class ResultsEndpointFilter(python_logging.Filter):
    def filter(self, record):
        path = record.args[2]
        return not (
            "/results" in path
            or "/client-heartbeat" in path
            or "/get_inputs" in path
            or "/ack_transfer" in path
        )


class SizedQueue(asyncio.Queue):
    # Force user to submit size of their item because it's ususally already available and is slow
    # to calculate for any given generic object, but fast for known objects like input_pkl_with_idx.
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.size_bytes = 0

    async def put(self, item, size_bytes):
        self.put_nowait(item, size_bytes)

    def put_nowait(self, item, size_bytes):
        super().put_nowait((item, size_bytes))

    def _put(self, item_and_size):
        item, size_bytes = item_and_size
        super()._put((item, size_bytes))
        self.size_bytes += size_bytes

    def _get(self):
        item, size_bytes = super()._get()
        self.size_bytes -= size_bytes
        return item


async def debug_log(event: str, **fields):
    """Structured engineering events, the counterpart to Logger.log: they land
    in the head's debug_logs table (retention-pruned, shipped to Burla's
    telemetry backend if the job fails) and are never shown to users, so
    verbosity is a feature here, not noise. Values must be JSON-serializable.
    Never raises."""
    print(f"[debug] {event} {fields}")
    # Lazy import: this module is imported before SELF/head_client exist.
    from node_service import SELF, head_client

    entry = {
        "ts": time(),
        "debug": {"job_id": SELF["current_job"], "event": event, "fields": fields},
    }
    try:
        await head_client.post_node_logs([entry])
    except Exception as e:
        print(f"failed to forward debug log to head: {e}")


class Logger:
    """Prints to stdout (journald / docker captures it) and forwards each line
    to the head so it shows in the dashboard's node-log view. Errors also go
    to Burla's telemetry backend."""

    def __init__(self, request: Optional[Request] = None):
        self.request_line = f"{request.method} {request.url}" if request else None

    async def log(self, message: str, severity="INFO", **kw):
        traceback_str = kw.get("traceback")
        if traceback_str:
            print(traceback_str.strip())
        else:
            print(message)

        # Lazy import: head_client imports SELF from node_service, which
        # imports this module first.
        from node_service import head_client

        head_msg = traceback_str.strip() if traceback_str else message
        try:
            await head_client.post_node_logs([{"msg": head_msg, "ts": time()}])
        except Exception as e:
            print(f"failed to forward log to head: {e}")

        # Same kill switch as the client's _reporting.py; test clusters set it
        # so node errors don't spam Slack through the backend telemetry route.
        if os.environ.get("DISABLE_BURLA_TELEMETRY") == "True":
            return

        if severity == "ERROR" or traceback_str:
            try:
                payload = {"project_id": PROJECT_ID, "message": message, "traceback": traceback_str or ""}
                await asyncio.to_thread(
                    requests.post,
                    f"{BURLA_BACKEND_URL}/v1/telemetry/log/ERROR",
                    json=payload,
                    timeout=1,
                )
            except Exception:
                pass
