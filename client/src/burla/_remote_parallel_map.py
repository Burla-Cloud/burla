import io
import sys
import requests
import pickle
import json
from six import reraise
from threading import Thread, Event
from typing import Callable, Optional
from time import sleep
from queue import Queue
from uuid import uuid4
from requests import Response

import cloudpickle
from yaspin import yaspin
from tblib import Traceback
from google.cloud import firestore

from burla import _BURLA_SERVICE_URL, __version__, _BURLA_GCP_PROJECT
from burla._auth import get_auth_headers, get_gcs_credentials, AuthException
from burla._helpers import (
    upload_inputs,
    print_logs_from_db,
    enqueue_results_from_db,
    periodiocally_healthcheck_job,
)

# increase at your own risk, burla may break.
MAX_PARALLELISM = 1000

# This MUST be set to the same value as `JOB_HEALTHCHECK_FREQUENCY_SEC` in the node service.
# Nodes will restart themself if they dont get a new healthcheck from the client every X seconds.
JOB_HEALTHCHECK_FREQUENCY_SEC = 5

# Try to instantiate clients now to minimize latency when calling `remote_parallel_map`.
# This may not work for a variety of reasons, (user not logged in yet / planning to use api_key).
# If it does, this step can be skipped inside `remote_parallel_map`, lowering e2e latency.
try:
    BURLA_AUTH_HEADERS = get_auth_headers()
    credentials = get_gcs_credentials(BURLA_AUTH_HEADERS)
    DB = firestore.Client(credentials=credentials, project=_BURLA_GCP_PROJECT)
except:
    DB = None
    BURLA_AUTH_HEADERS = None


class UnknownClusterError(Exception):
    def __init__(self):
        msg = "An unknown error occurred inside your Burla cluster, "
        msg += "this is not an error with your code."
        super().__init__(msg)


class MainServiceError(Exception):
    # Error from inside the main_service that should be passed back to the user.
    def __init__(self, response: Response):
        self.__class__.__name__ = response.json().get("error_type")
        super().__init__(response.json().get("message"))


class InputsTooBig(Exception):
    pass


def _start_job(
    function_: Callable,
    inputs: list,
    func_cpu: int,
    func_ram: int,
    max_parallelism: int,
    stop_event: Event,
) -> str:

    inputs_pkl = [cloudpickle.dumps(input_) for input_ in inputs]
    inputs_size = sum([len(input_pkl) for input_pkl in inputs_pkl])
    if inputs_size > 84_866_368:
        raise Exception("Total size of all inputs exceeds current maximum limit of 84.8MB")

    # in separate thread start uploading inputs:
    inputs_id = str(uuid4())
    args = (DB, inputs_id, inputs_pkl, stop_event)
    input_uploader_thread = Thread(target=upload_inputs, args=args, daemon=True)
    input_uploader_thread.start()

    payload = {
        "n_inputs": len(inputs),
        "inputs_id": inputs_id,
        "func_cpu": func_cpu,
        "func_ram": func_ram,
        "max_parallelism": max_parallelism,
        "python_version": f"3.{sys.version_info.minor}",
        "burla_version": __version__,
    }

    # tell service to start job
    url = f"{_BURLA_SERVICE_URL}/v1/jobs/"
    files = {"function_pkl": cloudpickle.dumps(function_)}
    data = dict(request_json=json.dumps(payload))
    response = requests.post(url, files=files, data=data, headers=BURLA_AUTH_HEADERS)
    response_is_json = response.headers.get("Content-Type") == "application/json"

    if response.status_code == 401:
        stop_event.set()
        raise AuthException()
    elif response.status_code != 200 and response_is_json and response.json().get("error_type"):
        stop_event.set()
        raise MainServiceError(response)
    else:
        response.raise_for_status()
        return response.json()["job_id"]


def _watch_job(job_id: str, n_inputs: int, log_msg_stdout: io.TextIOWrapper, stop_event: Event):
    job_doc_ref = DB.collection("jobs").document(job_id)

    # Start printing logs generated by this job from a separate thread.
    args = (job_doc_ref, stop_event, log_msg_stdout)
    log_thread = Thread(target=print_logs_from_db, args=args, daemon=True)
    log_thread.start()

    # Start enqueueing results (either return-values or errors) generated by this job
    # from a separate thread.
    result_queue = Queue()
    args = (job_doc_ref, stop_event, result_queue)
    result_thread = Thread(target=enqueue_results_from_db, args=args, daemon=True)
    result_thread.start()

    # Run periodic healthchecks on the job/cluster from a separate thread.
    cluster_error_event = Event()
    auth_error_event = Event()
    args = (job_id, JOB_HEALTHCHECK_FREQUENCY_SEC, BURLA_AUTH_HEADERS, stop_event)
    args += (cluster_error_event, auth_error_event)
    healthchecker_thread = Thread(target=periodiocally_healthcheck_job, args=args, daemon=True)
    healthchecker_thread.start()

    n_results_received = 0
    while n_results_received < n_inputs:
        sleep(0.05)

        if cluster_error_event.is_set():
            raise UnknownClusterError()
        if auth_error_event.is_set():
            raise AuthException()

        while not result_queue.empty():
            n_results_received += 1
            input_index, is_error, result_pkl = result_queue.get()

            if is_error:
                exc_info = pickle.loads(result_pkl)
                traceback = Traceback.from_dict(exc_info["traceback_dict"]).as_traceback()
                reraise(tp=exc_info["type"], value=exc_info["exception"], tb=traceback)
            else:
                yield cloudpickle.loads(result_pkl)

    stop_event.set()


def remote_parallel_map(
    function_: Callable,
    inputs: list,
    func_cpu: int = 1,
    func_ram: int = 4,
    spinner: bool = True,
    max_parallelism: Optional[int] = None,
    api_key: Optional[str] = None,
):
    """
    TODO: add docstring
    """
    max_parallelism = max_parallelism if max_parallelism else len(inputs)
    max_parallelism = max_parallelism if max_parallelism < MAX_PARALLELISM else MAX_PARALLELISM
    if spinner:
        spinner = yaspin()
        spinner.text = f"Preparing to run {len(inputs)} inputs through `{function_.__name__}`"
        spinner.start()

    global DB, BURLA_AUTH_HEADERS
    if (DB is None) or (BURLA_AUTH_HEADERS is None):
        BURLA_AUTH_HEADERS = get_auth_headers(api_key)
        credentials = get_gcs_credentials(BURLA_AUTH_HEADERS)
        DB = firestore.Client(credentials=credentials, project=_BURLA_GCP_PROJECT)

    # wrap user function with a for loop because sending too many inputs causes firestore issues
    # this is a temporary fix:
    max_inputs = min(len(inputs), 256)
    batch_size = len(inputs) // max_inputs
    remainder = len(inputs) % max_inputs
    start = 0
    input_batches = []
    for i in range(max_inputs):
        end = start + batch_size + (1 if i < remainder else 0)
        input_batches.append(inputs[start:end])
        start = end

    def function_wrapped(input_batch):
        return [function_(input_) for input_ in input_batch]

    #
    #

    stop_event = Event()
    try:
        job_id = _start_job(
            function_=function_wrapped,
            inputs=input_batches,
            func_cpu=func_cpu,
            func_ram=func_ram,
            max_parallelism=max_parallelism,
            stop_event=stop_event,
        )
        if spinner:
            spinner.text = f"Running {len(inputs)} inputs through `{function_.__name__}`"
        log_msg_stdout = spinner if spinner else sys.stdout
        # yield from _watch_job(job_id, len(inputs), log_msg_stdout)
        for output_batch in _watch_job(job_id, len(input_batches), log_msg_stdout, stop_event):
            yield from output_batch

    except Exception as e:
        if spinner:
            spinner.stop()
        raise e
    finally:
        stop_event.set()

    if spinner:
        spinner.text = "Done!"
        spinner.ok("✔")
