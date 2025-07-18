import sys
import requests
from itertools import groupby
from typing import Optional
import logging as python_logging
from time import time

from fastapi import Request
from google.cloud.firestore import Client

from node_service import IN_LOCAL_DEV_MODE, GCL_CLIENT, PROJECT_ID, BURLA_BACKEND_URL, INSTANCE_NAME


def format_traceback(traceback_details: list):
    details = ["  ... (detail hidden)\n" if "/pypoetry/" in d else d for d in traceback_details]
    details = [key for key, _ in groupby(details)]  # <- remove consecutive duplicates
    return "".join(details).split("another exception occurred:")[-1]


class ResultsEndpointFilter(python_logging.Filter):
    def filter(self, record):
        return not record.args[2].endswith("/results")


class FirestoreLogHandler(python_logging.Handler):
    def __init__(self):
        super().__init__()
        client = Client(project=PROJECT_ID, database="burla")
        self.log_collection = client.collection("nodes").document(INSTANCE_NAME).collection("logs")

    def emit(self, record):
        try:
            self.log_collection.document().set({"msg": record.getMessage(), "ts": time()})
        except Exception as e:
            print(f"Error logging to firestore: {e}")


class Logger:

    def __init__(self, request: Optional[Request] = None):
        self.request = request
        self.loggable_request = None

        # using prints instead of logger.info causes a bunch of issues with threading
        self.logger = python_logging.getLogger("node_service")
        if not self.logger.handlers:
            self.logger.setLevel(python_logging.INFO)
            self.logger.addHandler(python_logging.StreamHandler(sys.stdout))
            self.logger.addHandler(FirestoreLogHandler())
            self.logger.propagate = False

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
        if (self.loggable_request is None) and self.request:
            self.loggable_request = self.__loggable_request(self.request)

        if "traceback" in kw.keys():
            self.logger.error(kw["traceback"].strip())
        else:
            self.logger.info(message)

        if not IN_LOCAL_DEV_MODE:
            struct = dict(message=message, request=self.loggable_request, **kw)
            GCL_CLIENT.log_struct(struct, severity=severity)

        # Report errors back to Burla's cloud.
        if severity == "ERROR" or "traceback" in kw:
            try:
                tb = kw.get("traceback", "")
                json = {"project_id": PROJECT_ID, "message": message, "traceback": tb}
                requests.post(f"{BURLA_BACKEND_URL}/v1/telemetry/log/ERROR", json=json, timeout=1)
            except Exception:
                pass
