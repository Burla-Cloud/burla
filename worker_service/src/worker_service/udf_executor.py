import os
import sys
import pickle
import requests
import traceback
import subprocess
from datetime import datetime, timezone
from queue import Empty
from time import sleep, time
from pathlib import Path
from threading import Lock, Event, Thread
import importlib.metadata as importlib_metadata

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


class _FirestoreStdout:

    def __init__(self, job_id: str):
        self.job_id = job_id
        self._buffer = []
        self._buffer_size = 0
        self._last_flush_time = time()
        self._max_buffer_size = 1_048_000  # leave extra space for overhead
        self._lock = Lock()
        self._stop_event = Event()
        self._flusher_thread = Thread(target=self._flush_loop, daemon=True)
        self._flusher_thread.start()

    def stop(self):
        self.actually_flush()
        self._stop_event.set()
        self._flusher_thread.join(timeout=1)

    def write(self, msg):
        if msg.strip():
            timestamp_str = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            msg_size = len(msg.encode("utf-8")) + 180  # for timestamp and dict overhead
            msg_too_big = msg_size > self._max_buffer_size
            if msg_too_big:
                truncated_msg_bytes = msg.encode("utf-8")[: self._max_buffer_size]
                truncated_msg = truncated_msg_bytes.decode("utf-8", errors="ignore")
                msg = truncated_msg + "<too-long--remaining-msg-truncated-due-to-length>"
            firestore_formatted_log_msg = {
                "mapValue": {
                    "fields": {
                        "timestamp": {"timestampValue": timestamp_str},
                        "message": {"stringValue": msg},
                    }
                }
            }
            with self._lock:
                future_buffer_size = self._buffer_size + msg_size
                if future_buffer_size > self._max_buffer_size:
                    self.actually_flush()
                self._buffer.append(firestore_formatted_log_msg)
                self._buffer_size += msg_size

    def flush(self):
        # because this is overwriting sys.stdout, other things call flush() often (too often)
        pass

    def actually_flush(self):
        if self._buffer:
            SELF["logs"].append(f"Flushing {len(self._buffer)} logs")
            timestamp_str = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            timestamp_field = {"timestampValue": timestamp_str}
            logs_field = {"arrayValue": {"values": [self._buffer]}}
            data = {"fields": {"logs": logs_field, "timestamp": timestamp_field}}
            try:
                url = f"{DB_BASE_URL}/jobs/{self.job_id}/logs"
                response = requests.post(url, headers=DB_HEADERS, json=data, timeout=5)
                response.raise_for_status()
            except Exception as e:
                if response.status_code == 401 and IN_LOCAL_DEV_MODE:
                    msg = "401 error writing logs, YOU DEV TOKEN IS PROBABLY EXPIRED!\n"
                    msg += "         Re-run `make local-dev` (and reboot!) to refresh the token.\n"
                    SELF["logs"].append(msg)
                else:
                    SELF["logs"].append(f"Error writing log to firestore: {e}")
            finally:
                self._buffer.clear()
                self._buffer_size = 0
        self._last_flush_time = time()

    def __enter__(self):
        self.original_stdout = sys.stdout
        sys.stdout = self

    def __exit__(self, exc_type, exc_value, traceback):
        sys.stdout = self.original_stdout
        sys.stdout.flush()

    def _flush_loop(self):
        while not self._stop_event.wait(1.0):
            if (time() - self._last_flush_time) > 1:
                self.actually_flush()


def _serialize_error(exception_type, exception, traceback):
    # exc_info is tuple returned by sys.exc_info()
    pickled_exception_info = pickle.dumps(
        dict(
            type=exception_type,
            exception=exception,
            traceback_dict=Traceback(traceback).to_dict(),
        )
    )
    return pickled_exception_info


def _packages_are_importable(packages: dict):
    for package, expected_version in packages.items():
        try:
            installed_version = importlib_metadata.version(package)
        except importlib_metadata.PackageNotFoundError:
            installed_version = None
        if installed_version != expected_version:
            SELF["CURRENTLY_INSTALLING_PACKAGE"] = package
            # SELF["logs"].append(f"{package}=={installed_version} but we need {expected_version}")
            return False

    # `ALL_PACKAGES_INSTALLED != CURRENTLY_INSTALLING_PACKAGE == None` because it is none
    # before `_packages_are_importable` is checked causing client to switch spinner to "running"
    # before we know it won't install packages.
    SELF["ALL_PACKAGES_INSTALLED"] = True
    SELF["CURRENTLY_INSTALLING_PACKAGE"] = None
    return True


def _install_packages(packages: dict):
    cmd = ["uv", "pip", "install", "--target", "/worker_service_python_env", "--system"]
    for package, version in packages.items():
        cmd.append(f"{package}=={version}")
    SELF["logs"].append(f"Installing {len(packages)} packages with CMD:\n\t{' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True)
    if result.returncode != 0:
        raise Exception(f"Failed to install packages:\nCMD:{cmd}\nERROR:{result.stderr}\n\n")
        # this should fail the entire node
    else:
        msg = f"Successfully installed {len(packages)} packages:\n\t{result.stderr}"
        SELF["logs"].append(msg)
        SELF["ALL_PACKAGES_INSTALLED"] = True
        SELF["CURRENTLY_INSTALLING_PACKAGE"] = None


def install_pkgs_and_execute_job(job_id: str, function_pkl: bytes, packages: dict):
    SELF["logs"].append(f"Starting job {job_id} with func-size {len(function_pkl)} bytes.")
    all_packages_importable = _packages_are_importable(packages)
    ENV_IS_READY_PATH = Path("/worker_service_python_env/.ALL_PACKAGES_INSTALLED")
    if not all_packages_importable and os.environ.get("ELECTED_INSTALLER") == "True":
        _install_packages(packages)
        ENV_IS_READY_PATH.touch()
    elif not all_packages_importable:
        SELF["logs"].append("Waiting for packages ...")
        while not ENV_IS_READY_PATH.exists():
            # run this to update CURRENTLY_INSTALLING_PACKAGE -> spinner on client!
            _packages_are_importable(packages)
            sleep(0.01)
        SELF["logs"].append("Done waiting for packages.")
    else:
        SELF["logs"].append(f"{len(packages)} packages are already installed.")

    firestore_stdout = _FirestoreStdout(job_id)
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
            sleep(0.01)
            continue

        is_error = False
        with firestore_stdout:  # <- all stdout sent to firestore (where it's grabbed by client)
            try:
                if user_defined_function is None:
                    user_defined_function = cloudpickle.loads(function_pkl)
                input_ = cloudpickle.loads(input_pkl)
                return_value = user_defined_function(input_)
                result_pkl = cloudpickle.dumps(return_value)
                # SELF["logs"].append(f"UDF succeded on input #{input_index}.")
            except Exception:
                # SELF["logs"].append(f"UDF raised an exception on input #{input_index}.")
                exc_type, exc_value, exc_tb = sys.exc_info()
                result_pkl = _serialize_error(exc_type, exc_value, exc_tb)
                is_error = True

        if is_error:
            # write traceback as log message
            tb_str = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
            timestamp_str = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            firestore_formatted_log_msg = {
                "mapValue": {
                    "fields": {
                        "timestamp": {"timestampValue": timestamp_str},
                        "message": {"stringValue": tb_str},
                        "is_error": {"booleanValue": True},
                    }
                }
            }
            logs_field = {"arrayValue": {"values": [firestore_formatted_log_msg]}}
            data = {"fields": {"logs": logs_field, "timestamp": {"timestampValue": timestamp_str}}}
            url = f"{DB_BASE_URL}/jobs/{job_id}/logs"
            response = requests.post(url, headers=DB_HEADERS, json=data, timeout=5)
            response.raise_for_status()
            # mark job failed
            mask = [("updateMask.fieldPaths", "status")]
            data = {"fields": {"status": {"stringValue": "FAILED"}}}
            url = f"{DB_BASE_URL}/jobs/{job_id}"
            response = requests.patch(url, headers=DB_HEADERS, params=mask, json=data)
            response.raise_for_status()

        # we REALLY want to be sure we dont add this result if the stop event got set during the udf
        # because that means the worker is shutting down and the client probably cant get it in time
        #
        # by not adding it to results we gaurentee the client dosent get it, and can send it along
        # with the inputs sitting in the queue to another worker, becore this node shuts down.
        if not SELF["STOP_PROCESSING_EVENT"].is_set():

            if SELF["inputs_queue"].empty():
                # if you don't flush before adding the final result, the worker is restarted
                # before the flush in .stop() can happen.
                firestore_stdout.actually_flush()

            SELF["results_queue"].put((input_index, is_error, result_pkl))
            SELF["in_progress_input"] = None
            # SELF["logs"].append(f"Successfully enqueued result for input #{input_index}.")

    SELF["logs"].append(f"STOP_PROCESSING_EVENT has been set!")
    firestore_stdout.stop()
