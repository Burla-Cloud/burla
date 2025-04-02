import sys
import base64
import pickle
import requests
import traceback
from queue import Empty
from time import sleep

import cloudpickle
from tblib import Traceback
from google.auth.transport.requests import Request
from worker_service import SELF, PROJECT_ID, CREDENTIALS

FIRESTORE_URL = "https://firestore.googleapis.com"
DB_BASE_URL = f"{FIRESTORE_URL}/v1/projects/{PROJECT_ID}/databases/burla/documents"
CREDENTIALS.refresh(Request())


class _FirestoreLogger:

    def __init__(self, job_id: str, input_index: int):
        self.job_id = job_id
        self.input_index = input_index
        token = CREDENTIALS.token
        self.db_headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def write(self, msg):
        if msg.strip() and (len(msg.encode("utf-8")) > 1_048_376):  # (1mb - est overhead):
            msg_truncated = msg.encode("utf-8")[:1_048_376].decode("utf-8", errors="ignore")
            msg = msg_truncated + "<too-long--remaining-msg-truncated-due-to-length>"
        if msg.strip():
            log_doc_url = f"{DB_BASE_URL}/jobs/{self.job_id}/logs"
            data = {
                "fields": {
                    "msg": {"stringValue": msg},
                    "input_index": {"integerValue": self.input_index},
                }
            }
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
    SELF["logs"].append(f"Starting job {job_id} with func-size {len(function_pkl)} bytes.")

    user_defined_function = None
    while True:
        try:
            input_index, input_pkl = SELF["inputs_queue"].get()
            SELF["logs"].append(f"Popped input #{input_index} from queue.")
        except Empty:
            SELF["logs"].append("No inputs in queue. Sleeping for 2 seconds.")
            sleep(2)

        is_error = False
        with _FirestoreLogger(job_id, input_index):
            try:
                if user_defined_function is None:
                    user_defined_function = cloudpickle.loads(function_pkl)
                input_ = cloudpickle.loads(input_pkl)
                return_value = user_defined_function(input_)
                result_pkl = cloudpickle.dumps(return_value)
                SELF["logs"].append(f"UDF succeded on input #{input_index}.")
            except Exception:
                SELF["logs"].append(f"UDF raised an exception on input #{input_index}.")
                result_pkl = _serialize_error(sys.exc_info())
                is_error = True

        SELF["result_queue"].put((input_index, is_error, result_pkl))
        SELF["logs"].append(f"Successfully enqueued result for input #{input_index}.")
