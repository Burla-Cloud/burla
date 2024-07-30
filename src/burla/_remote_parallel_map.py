import os
import sys
import requests
import warnings
import pickle
import json
from six import reraise
from threading import Thread, Event
from typing import Callable, Optional
from time import sleep, time
from queue import PriorityQueue

import cloudpickle
import google.auth
from google.auth.transport.requests import Request
from yaspin import yaspin, Spinner
from tblib import Traceback

from burla import _BURLA_SERVICE_URL, __version__
from burla._logstream import print_logs_from_queue
from burla._env_inspection import get_pip_packages, get_function_dependencies
from burla._auth import auth_headers_from_local_config, AuthException, login_required
from burla._helpers import (
    nopath_warning,
    JobTimeoutError,
    InstallError,
    ServerError,
    StatusMessage,
    upload_inputs,
    download_outputs,
)

warnings.formatwarning = nopath_warning

# BURLA_JOBS_BUCKET = "burla-jobs"
BURLA_JOBS_BUCKET = "burla-jobs-prod"
GCR_MAX_BYTES_PER_REQUEST = 32 * 1024 * 1024

MAX_CPUS = 2000  # please ask before you increase this <3
MAX_GPUS = 300  # please ask before you increase this <3
MAX_PARALLELISM = 5000  # remote_parallel_map might break if this is raised.
MAX_CONCURRENCY = 2000

TIMEOUT_MIN = 60 * 12  # max time a Burla job can run for
IN_COLAB = os.getenv("COLAB_RELEASE_TAG") is not None

BYTES_HEADER = {"Content-Type": "application/octet-stream"}


def job_status_poll_rate(seconds_since_job_started: int):
    """
    User wants a response quickly if their job is short (check status often early).
    User wants main_service to not get spammed and be slow (check status less often if job is long).
    """
    if seconds_since_job_started < 10:
        return 0
    elif seconds_since_job_started < 30:
        return 0.5
    elif seconds_since_job_started < 120:
        return 1.5
    elif seconds_since_job_started > 120:
        return 3


def _get_job_info_since(epoch: int, job_id: str, headers: dict):
    """Raises: ServerError, InstallError, HTTPError, or re-raised error from UDF"""

    response = requests.get(f"{_BURLA_SERVICE_URL}/v1/jobs/{job_id}/{epoch}", headers=headers)
    if response.status_code == 401:
        AuthException()
    elif response.status_code >= 500:
        raise ServerError()
    elif response.status_code >= 300:
        response.raise_for_status()

    job = response.json()
    if job.get("udf_error"):
        exception_info = pickle.loads(bytes.fromhex(job.get("udf_error")))
        reraise(
            tp=exception_info["exception_type"],
            value=exception_info["exception"],
            tb=Traceback.from_dict(exception_info["traceback_dict"]).as_traceback(),
        )
    elif job.get("install_error"):
        raise InstallError(job["install_error"])

    return job["udf_started"], job["logs"], job.get("done")


def _start_job(
    rpm_call_time: float,
    function_: Callable,
    inputs: list,
    burla_auth_headers: dict,
    verbose: bool,
    spinner: Spinner,
    func_cpu: int,
    func_ram: int,
    gpu: int,
    parallelism: int,
    image: Optional[str] = None,
    packages: Optional[list[str]] = None,
) -> str:
    if verbose:
        spinner.text = StatusMessage.preparing()
        spinner.start()

    if packages:
        required_packages = [{"name": pkg_name} for pkg_name in packages]
    else:
        installed_packages = list(get_pip_packages())
        imported_modules = list(get_function_dependencies(function_))
        required_packages = [pkg for pkg in installed_packages if pkg["name"] in imported_modules]

    # print(f"installed_packages: {installed_packages}\n\n")
    # print(f"imported_modules: {imported_modules}\n\n")
    # print(f"required_packages: {required_packages}")
    # print(1 / 0)

    function_pkl = cloudpickle.dumps(function_)
    inputs_pkl = cloudpickle.dumps([cloudpickle.dumps(_input) for _input in inputs])
    payload = {
        "rpm_call_time": rpm_call_time,
        "n_inputs": len(inputs),
        "image": image,
        "func_cpu": func_cpu,
        "func_ram": func_ram,
        "gpu": gpu,
        "parallelism": parallelism,
        "python_version": f"3.8" if sys.version_info.minor < 8 else f"3.{sys.version_info.minor}",
        "packages": None,  # if image else required_packages,
        "burla_version": __version__,
    }
    request_size = len(function_pkl) + len(inputs_pkl) + len(cloudpickle.dumps(payload))
    send_inputs_through_gcs = request_size > GCR_MAX_BYTES_PER_REQUEST

    url = f"{_BURLA_SERVICE_URL}/v1/jobs/"
    if send_inputs_through_gcs:
        response = requests.post(url, json=payload, headers=burla_auth_headers)
    else:
        files = {"function_pkl": function_pkl, "inputs_pkl": inputs_pkl}
        data = dict(request_json=json.dumps(payload))
        response = requests.post(url, files=files, data=data, headers=burla_auth_headers)

    if response.status_code == 401:
        raise AuthException()
    else:
        response.raise_for_status()
        job_id = response.json()["job_id"]

    if send_inputs_through_gcs:
        # getting crecentials takes anywhere from .5-1s
        # credentials, _ = google.auth.default()
        # credentials.refresh(Request())
        # gcs_auth_headers = {"Authorization": f"Bearer {credentials.token}", **BYTES_HEADER}

        # bucket is temporarily public:
        gcs_auth_headers = BYTES_HEADER

        # uploading function and inputs from `test_base` consistently takes ~0.8s
        spinner.text = StatusMessage.uploading_function
        function_blob_name = f"{job_id}/function.pkl"
        gcs_base_url = "https://www.googleapis.com/upload/storage"
        function_blob_url_args = f"uploadType=media&name={function_blob_name}"
        function_blob_url = f"{gcs_base_url}/v1/b/{BURLA_JOBS_BUCKET}/o?{function_blob_url_args}"
        requests.post(function_blob_url, headers=gcs_auth_headers, data=function_pkl)

        spinner.text = StatusMessage.uploading_inputs
        upload_inputs(job_id, inputs_pkl, gcs_auth_headers, BURLA_JOBS_BUCKET)

    spinner.text = StatusMessage.preparing()
    return job_id


def _watch_job(job_id: str, job_started_time: int, headers: dict, verbose: bool, spinner: Spinner):
    last_epoch = job_started_time
    epoch = last_epoch
    job_timed_out = False

    # Start printing logs generated by this job using a separate thread.
    print_queue = PriorityQueue()
    stop_event = Event()
    args = (print_queue, stop_event, spinner)
    log_thread = Thread(target=print_logs_from_queue, args=args, daemon=True)
    log_thread.start()
    done = False
    while not (done or job_timed_out):
        sleep(job_status_poll_rate(seconds_since_job_started=time() - job_started_time))

        udf_started, logs, done = _get_job_info_since(last_epoch, job_id, headers)

        for epoch, log_message in logs:
            print_queue.put((epoch, log_message))

        if verbose and udf_started:
            spinner.text = StatusMessage.running()

        last_epoch = epoch
        job_timed_out = (time() - job_started_time) > (TIMEOUT_MIN * 60)

    stop_event.set()
    log_thread.join()

    if job_timed_out:
        raise JobTimeoutError(job_id=job_id, timeout=TIMEOUT_MIN)


@login_required
def remote_parallel_map(
    function_: Callable,
    inputs: list,
    verbose: bool = True,
    image: Optional[str] = None,
    func_cpu: int = 1,
    func_ram: int = 1,
    gpu: int = 0,
    max_parallelism: Optional[int] = None,
    api_key: Optional[str] = None,
    packages: Optional[list[str]] = None,
):
    rpm_call_time = time()
    n_inputs = len(inputs)
    if (func_cpu > 96) or (func_cpu < 1):
        raise ValueError("CPU per function call must be one of [1.. 96]")
    if (func_ram > 624) or (func_ram < 1):
        raise ValueError("RAM per function call must be one of [1.. 624]")
    # if (func_gpu > 4) or (func_gpu < 0):
    #     raise ValueError("GPU per function call must be one of [0.. 4]")

    n_inputs = len(inputs)
    if max_parallelism is None:
        parallelism = min(n_inputs, MAX_PARALLELISM)
    else:
        parallelism = min(max_parallelism, MAX_PARALLELISM)

    requested_cpu = parallelism * func_cpu
    # requested_gpu = parallelism * func_gpu

    if requested_cpu > MAX_CPUS:  # and not (requested_gpu > MAX_GPUS):
        parallelism = MAX_CPUS // func_cpu
        warnings.warn(
            f"Limiting parallelism to {parallelism} to stay under limit of {MAX_CPUS} CPUs."
        )
    # if requested_gpu > MAX_GPUS:
    #     parallelism = MAX_GPUS // func_gpu
    #     warnings.warn(
    #         f"Limiting parallelism to {parallelism} to stay under limit of {MAX_GPUS} GPUs."
    #     )

    spinner = yaspin()
    StatusMessage.function_name = function_.__name__
    StatusMessage.n_inputs = len(inputs) if len(inputs) < MAX_CONCURRENCY else MAX_CONCURRENCY
    StatusMessage.total_cpus = parallelism * func_cpu
    StatusMessage.total_gpus = parallelism if gpu else 0

    if api_key:
        burla_auth_headers = {"Authorization": f"Bearer {api_key}"}
    else:
        burla_auth_headers = auth_headers_from_local_config()

    try:
        polling_start_time = int(time())
        job_id = _start_job(
            rpm_call_time=rpm_call_time,
            function_=function_,
            inputs=inputs,
            burla_auth_headers=burla_auth_headers,
            verbose=verbose,
            spinner=spinner,
            func_cpu=func_cpu,
            func_ram=func_ram,
            gpu=gpu,
            parallelism=parallelism,
            image=image,
            packages=packages,
        )
        _watch_job(job_id, polling_start_time, burla_auth_headers, verbose, spinner)
        spinner.text = StatusMessage.downloading

        # temporary, TODO: only download through gcs if outputs do not fit in a request
        # credentials, _ = google.auth.default()
        # credentials.refresh(Request())
        # gcs_auth_headers = {"Authorization": f"Bearer {credentials.token}", **BYTES_HEADER}
        # consistently takes 0.4-0.5s

        # bucket is temporarily publicly accessible:
        gcs_auth_headers = BYTES_HEADER
        return_values = download_outputs(job_id, len(inputs), gcs_auth_headers, BURLA_JOBS_BUCKET)

    except Exception as e:
        spinner.stop()
        raise e

    if verbose:
        spinner.text = "Done!"
        spinner.ok("✔")

    # report job is done for metrics / benchmarking reasons
    payload = {"job_done_ts": time()}
    url = f"{_BURLA_SERVICE_URL}/v1/jobs/{job_id}/done"
    requests.post(url, json=payload, headers=burla_auth_headers)

    all_return_values_are_none = all(value is None for value in return_values)
    if not all_return_values_are_none:
        return return_values
