import sys
import pickle
import requests
from datetime import datetime, timezone
from queue import Empty
from time import sleep
from pathlib import Path

import cloudpickle
from tblib import Traceback
from worker_service import SELF, PROJECT_ID, IN_LOCAL_DEV_MODE

FIRESTORE_URL = "https://firestore.googleapis.com"
DB_BASE_URL = f"{FIRESTORE_URL}/v1/projects/{PROJECT_ID}/databases/burla/documents"


def _get_gcp_auth_token():
    if IN_LOCAL_DEV_MODE:
        token = Path("/burla/.temp_token.txt").read_text().strip()
        url = "https://www.googleapis.com/auth/cloud-platform"
        response = requests.get(url, headers={"Authorization": f"Bearer {token}"})
        if response.status_code == 401:
            raise Exception("EXPIRED GCP TOKEN: please run `make local-dev` to refresh the token.")
        return token

    metadata_svc_host = "http://metadata.google.internal"
    token_url = f"{metadata_svc_host}/computeMetadata/v1/instance/service-accounts/default/token"
    headers = {"Metadata-Flavor": "Google"}
    response = requests.get(token_url, headers=headers)
    response.raise_for_status()
    return response.json()["access_token"]


DB_HEADERS = {
    "Authorization": f"Bearer {_get_gcp_auth_token()}",
    "Content-Type": "application/json",
}


class _FirestoreLogger:

    def __init__(self, job_id: str, input_index: int):
        self.job_id = job_id
        self.input_index = input_index

    def write(self, msg):
        if msg.strip() and (len(msg.encode("utf-8")) > 1_048_376):  # (1mb - est overhead):
            msg_truncated = msg.encode("utf-8")[:1_048_376].decode("utf-8", errors="ignore")
            msg = msg_truncated + "<too-long--remaining-msg-truncated-due-to-length>"
        if msg.strip():
            log_doc_url = f"{DB_BASE_URL}/jobs/{self.job_id}/logs"
            timestamp_str = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            data = {
                "fields": {
                    "msg": {"stringValue": msg},
                    "input_index": {"integerValue": self.input_index},
                    "created_at": {"timestampValue": timestamp_str},
                }
            }
            try:
                response = requests.post(log_doc_url, headers=DB_HEADERS, json=data, timeout=1)
                response.raise_for_status()
            except Exception as e:
                SELF["logs"].append(f"Error writing log to firestore: {e}")

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
    logged_idle = False
    while not SELF["STOP_PROCESSING_EVENT"].is_set():
        try:
            SELF["in_progress_input"] = SELF["inputs_queue"].get_nowait()
            input_index, input_pkl = SELF["in_progress_input"]
            SELF["IDLE"] = False
            # SELF["logs"].append(f"Popped input #{input_index} from queue.")
        except Empty:
            SELF["IDLE"] = True
            if not logged_idle:
                # SELF["logs"].append("Input queue empty, waiting for more inputs ...")
                logged_idle = True
            sleep(0.05)
            continue

        is_error = False
        with _FirestoreLogger(job_id, input_index):
            try:
                if user_defined_function is None:
                    user_defined_function = cloudpickle.loads(function_pkl)
                input_ = cloudpickle.loads(input_pkl)
                return_value = user_defined_function(input_)
                result_pkl = cloudpickle.dumps(return_value)
                # SELF["logs"].append(f"UDF succeded on input #{input_index}.")
            except Exception:
                # SELF["logs"].append(f"UDF raised an exception on input #{input_index}.")
                result_pkl = _serialize_error(sys.exc_info())
                is_error = True

        # we REALLY want to be sure we dont add this result if the stop event got set during the udf
        # because that means the worker is shutting down and the client probably cant get it in time
        #
        # by not adding it to results we gaurentee the client dosent get it, and can send it along
        # with the inputs sitting in the queue to another worker, becore this node shuts down.
        if not SELF["STOP_PROCESSING_EVENT"].is_set():
            SELF["results_queue"].put((input_index, is_error, result_pkl))
            SELF["in_progress_input"] = None
            # SELF["logs"].append(f"Successfully enqueued result for input #{input_index}.")

    SELF["logs"].append(f"STOP_PROCESSING_EVENT has been set!")
