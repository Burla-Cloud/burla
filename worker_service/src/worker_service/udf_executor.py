import sys
import base64
import pickle
import requests
from queue import Empty
from time import sleep

import cloudpickle
from tblib import Traceback
from google.auth.transport.requests import Request
from worker_service import SELF, PROJECT_ID, CREDENTIALS

FIRESTORE_URL = "https://firestore.googleapis.com"
DB_BASE_URL = f"{FIRESTORE_URL}/v1/projects/{PROJECT_ID}/databases/burla/documents"


class _FirestoreLogger:

    def __init__(self, job_id: str, db_headers: dict):
        self.job_id = job_id
        self.db_headers = db_headers

    def write(self, msg):
        if msg.strip() and (len(msg.encode("utf-8")) > 1_048_376):  # (1mb - est overhead):
            msg_truncated = msg.encode("utf-8")[:1_048_376].decode("utf-8", errors="ignore")
            msg = msg_truncated + "<too-long--remaining-msg-truncated-due-to-length>"
        if msg.strip():
            log_doc_url = f"{DB_BASE_URL}/jobs/{self.job_id}/logs"
            data = {"fields": {"msg": {"stringValue": msg}}}
            response = requests.post(log_doc_url, headers=self.db_headers, json=data)
            response.raise_for_status()

    def flush(self):
        self.original_stdout.flush()

    def __enter__(self):
        self.original_stdout = sys.stdout
        sys.stdout = self

    def __exit__(self, exc_type, exc_value, traceback):
        sys.stdout = self.original_stdout


def _serialize_error(exc_info):
    # exc_info is tuple returned by sys.exc_info()
    exception_type, exception, traceback = exc_info
    pickled_exception_info = pickle.dumps(
        dict(
            type=exception_type,
            exception=exception,
            traceback_dict=Traceback(traceback).to_dict(),
        )
    )
    return pickled_exception_info


def execute_job(job_id: str, function_pkl: bytes):
    CREDENTIALS.refresh(Request())
    db_headers = {
        "Authorization": f"Bearer {CREDENTIALS.token}",
        "Content-Type": "application/json",
    }

    user_defined_function = None
    while True:

        try:
            input_index, input_pkl = SELF["inputs_queue"].get()
        except Empty:
            sleep(2)

        # run UDF:
        exec_info = None
        with _FirestoreLogger(job_id, db_headers):
            try:
                if user_defined_function is None:
                    user_defined_function = cloudpickle.loads(function_pkl)
                input_ = cloudpickle.loads(input_pkl)
                return_value = user_defined_function(input_)
            except Exception:
                exec_info = sys.exc_info()

        # serialize result:
        result_pkl = _serialize_error(exec_info) if exec_info else cloudpickle.dumps(return_value)
        result_too_big = len(result_pkl) > 1_048_376
        if result_too_big:
            noun = "Error" if exec_info else "Return value"
            msg = f"{noun} from input at index {input_index} is greater than 1MB in size."
            raise Exception(f"{msg}\nUnable to store result.")

        # write result:
        result_doc_url = f"{DB_BASE_URL}/jobs/{job_id}/results/{input_index}"
        encoded_result_pkl = base64.b64encode(result_pkl).decode("utf-8")
        data = {
            "fields": {
                "is_error": {"booleanValue": bool(exec_info)},
                "result_pkl": {"bytesValue": encoded_result_pkl},
            }
        }
        response = requests.patch(result_doc_url, headers=db_headers, json=data)
        response.raise_for_status()
