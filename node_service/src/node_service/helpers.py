import asyncio
import requests
from itertools import groupby
from typing import Optional
import logging as python_logging
from time import time

from fastapi import Request
from node_service import ASYNC_DB, IN_LOCAL_DEV_MODE, GCL_CLIENT, PROJECT_ID, BURLA_BACKEND_URL, INSTANCE_NAME


def format_traceback(traceback_details: list):
    details = ["  ... (detail hidden)\n" if "/pypoetry/" in d else d for d in traceback_details]
    details = [key for key, _ in groupby(details)]  # <- remove consecutive duplicates
    return "".join(details).split("another exception occurred:")[-1]


class ResultsEndpointFilter(python_logging.Filter):
    def filter(self, record):
        return not record.args[2].endswith(("/results", "/client-heartbeat"))


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

class Logger:

    def __init__(self, request: Optional[Request] = None):
        self.request = request
        self.loggable_request = None
        self.log_collection = ASYNC_DB.collection("nodes").document(INSTANCE_NAME).collection("logs")

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

    async def log(self, message: str, severity="INFO", **kw):
        if (self.loggable_request is None) and self.request:
            self.loggable_request = self.__loggable_request(self.request)

        traceback_str = kw.get("traceback")
        if traceback_str:
            print(traceback_str.strip())
        else:
            print(message)

        firestore_msg = traceback_str.strip() if traceback_str else message
        await self.log_collection.document().set({"msg": firestore_msg, "ts": time()})

        if not IN_LOCAL_DEV_MODE:
            struct = dict(message=message, request=self.loggable_request, **kw)
            await asyncio.to_thread(GCL_CLIENT.log_struct, struct, severity=severity)

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
