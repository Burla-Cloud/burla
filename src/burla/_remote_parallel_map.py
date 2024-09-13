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
from queue import Queue
from uuid import uuid4

import cloudpickle
from yaspin import yaspin, Spinner
from tblib import Traceback
from google.cloud import pubsub
from google.cloud import firestore
from google.oauth2 import service_account

from burla import _BURLA_SERVICE_URL, __version__, _BURLA_JOBS_BUCKET, _BURLA_GCP_PROJECT
from burla._env_inspection import get_pip_packages, get_function_dependencies
from burla._auth import auth_headers_from_local_config, AuthException, login_required
from burla._helpers import (
    nopath_warning,
    JobTimeoutError,
    InstallError,
    ServerError,
    StatusMessage,
    upload_inputs,
    print_logs_from_stream,
    enqueue_outputs_from_stream,
)

warnings.formatwarning = nopath_warning


FUNCTION_SIZE_GCS_THRESHOLD = 25 * 1024 * 1024

MAX_CPUS = 2000
MAX_GPUS = 300
MAX_PARALLELISM = 5000

TIMEOUT_MIN = 60 * 12  # max time a Burla job can run for
IN_COLAB = os.getenv("COLAB_RELEASE_TAG") is not None

BYTES_HEADER = {"Content-Type": "application/octet-stream"}

service_account_info = {
    "type": "service_account",
    "project_id": "burla-prod",
    "private_key_id": "5760535512502e67d3b7184667262d65394f57e3",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCQToCg9XVqBqkT\npyzinS4tleT0bp6mZsgtrFNDTR89LGpTJAeIRnlA2HaUTm5jBHUkFYWwfLopJXjs\nqnNed2nVwV3IQhNSh2kJ4I/ML7h5nAtuPVgKeL5YpTehqvAYdr5LGlqwFfOZoj9T\ndl+QkI705cDl/XTa7KzYVvzFp0dTg5S0RQ4KAuHHz3zTEKWSM6kRHQeBnNLmW5a9\nG2RCz4G587/O8A1FkrEmq6BXMReQkYqjT92yTMHxglraC8LdX5oUwu3quFqu5mwM\nzxZEd2jufr75V4n4nTJpanZebrCDyg78z8jy1jzWiJCqlUjE+XrpfiWoQM7kb/PT\n0ETKbESBAgMBAAECggEACSK/J/GCNm0nhRP/VnVm+AHWVdcu+g/lumZ/evJF+QR3\n0r2kMG9tu7o4f5kbie89T0SBizPKQVKa/jioRyG+NIciXcw5Fu91qedqkx2uSxyi\n6J6/lSIhwtDq3bRJsPLLh0uq1Bz/qAlKgwkqcaeFNWHaPXU3UajMJIIVTJoTfOju\nRi44qpKfitgULBq5EJgkICCGLlP76ZiJdXg/PhpxI0AnW9ZANaxMpGyBBv0B/JtZ\n+95HI7HsV1skXq8d9FHoENbEq15OwXy1IRQ2/BKMXHYdgC8eqhlpoR/RhAPqOODX\nnFVxVOAEbylT050NXMNzqPrwRHkErkWxzgyYskiawQKBgQDHDZA/0+JjJYaIOpap\nIt6OsvM6hthMPvduoAjmLN6nSEDf/PYz7GnkemiKroLA0tLJ72tXkPUGEgSTDuc2\nydmKpRihdlUOkepNqH+NPZDo46Q8DRIhqa03WZtYakQpsQnbv9LWi2yotQGDLji4\ntGkC4BiN5hD5tDJ7/mJFTVE9mQKBgQC5l1+qrxsemO4BmnUZ6KAGqyB2+gRfUG0J\n0VIf6E0VFGNNplC41hLgP9fhjWgfuiVDroIrzP4kEd8pme/AvhNVTFIIsqVvDIsq\nrwKW5GBVh4XrvEpqJViFHek9L1+DgQ3KZXYGryZd0pBum5cOsBMQqOuX6xPPjkAO\nCAAPuT7/KQKBgCujrZxQt7FE6Nm0/pLWMjTWxrxuE72jkFuQemL8M1Q5Yv+4VcHM\ncurEa2b8G25qygu7ka0A+rb5/EbBXa+FUUw0JdJAPyWSl+uupUgx1zM3tSn1M6Rt\nrqwT2RrpMUhyp9all3Ox3YCfLlW0LHtSEjOvLbLuXYphFzBX9PN8n/MBAoGAG40W\nPZ9rFjq7sm88jREUmIjU8/Sfq4qj9T4mw+fXcZaqOz/CYf4dpT61DJ3SZEtc9tQ0\nLM5st+wTRfi9N86/zfzbfMEQgBDLpBWA++eBSZEp11oHbgSHRJOxKU0cD8ibxH0V\nbV6ZAnqcyF6+qQaIfgOlndLfCQPkDHExmSP17ykCgYAqhbkTUSJ7fiScgTHPjy7D\nnYwRSQ1ZDRCXK6HLLr4YK+TAEfxFrrsPds+dguclnL50inxT6cG9ANlH/DVU0dPg\nhSbPs7mvMncAEcVn0xvGH2MD3N6IQcHjDpfiVZrAl05SpLCDborGwbShZHrvQ4AZ\n2Ir7JyTzWVG63F6tAr5yxg==\n-----END PRIVATE KEY-----\n",
    "client_email": "temp-baked-into-pip-pkg@burla-prod.iam.gserviceaccount.com",
    "client_id": "116785145310676148654",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/temp-baked-into-pip-pkg%40burla-prod.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com",
}
credentials = service_account.Credentials.from_service_account_info(service_account_info)
DB = firestore.Client(credentials=credentials, project=_BURLA_GCP_PROJECT)
SUBSCRIBER = pubsub.SubscriberClient(credentials=credentials)


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


def raise_any_errors_from_job(job_id: str, headers: dict):
    """Raises: ServerError, InstallError, HTTPError, or re-raised error from UDF"""

    response = requests.get(f"{_BURLA_SERVICE_URL}/v1/jobs/{job_id}", headers=headers)
    if response.status_code == 401:
        AuthException()
    elif response.status_code >= 500:
        raise ServerError()
    elif response.status_code >= 300:
        response.raise_for_status()

    job = response.json() or {}
    if job.get("udf_error"):
        exception_info = pickle.loads(bytes.fromhex(job.get("udf_error")))
        reraise(
            tp=exception_info["exception_type"],
            value=exception_info["exception"],
            tb=Traceback.from_dict(exception_info["traceback_dict"]).as_traceback(),
        )
    elif job.get("install_error"):
        raise InstallError(job["install_error"])


def _start_job(
    function_: Callable,
    inputs: list,
    auth_headers: dict,
    verbose: bool,
    spinner: Spinner,
    func_cpu: int,
    func_ram: int,
    func_gpu: int,
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

    # in separate thread start uploading inputs:
    inputs_id = str(uuid4())
    input_uploader_thread = Thread(
        target=upload_inputs,
        args=(DB, inputs_id, inputs),
        daemon=True,
    )
    input_uploader_thread.start()

    function_pkl = cloudpickle.dumps(function_)
    payload = {
        "n_inputs": len(inputs),
        "inputs_id": inputs_id,
        "image": image,
        "func_cpu": func_cpu,
        "func_ram": func_ram,
        "func_gpu": func_gpu,
        "parallelism": parallelism,
        "python_version": f"3.10" if sys.version_info.minor < 10 else f"3.{sys.version_info.minor}",
        "packages": None if image else required_packages,
        "burla_version": __version__,
    }
    request_size = len(function_pkl) + len(cloudpickle.dumps(payload))
    send_function_through_gcs = request_size > FUNCTION_SIZE_GCS_THRESHOLD

    # tell service to start job
    url = f"{_BURLA_SERVICE_URL}/v1/jobs/"
    if send_function_through_gcs:
        response = requests.post(url, json=payload, headers=auth_headers)
    else:
        files = {"function_pkl": function_pkl}
        data = dict(request_json=json.dumps(payload))
        response = requests.post(url, files=files, data=data, headers=auth_headers)

    if response.status_code == 401:
        raise AuthException()
    elif response.status_code == 500 and response.text:
        raise requests.exceptions.HTTPError(response.text)
    else:
        response.raise_for_status()
        job_id = response.json()["job_id"]

    if send_function_through_gcs:
        spinner.text = StatusMessage.uploading_function
        function_blob_name = f"{job_id}/function.pkl"
        gcs_base_url = "https://www.googleapis.com/upload/storage"
        function_blob_url_args = f"uploadType=media&name={function_blob_name}"
        function_blob_url = f"{gcs_base_url}/v1/b/{_BURLA_JOBS_BUCKET}/o?{function_blob_url_args}"
        requests.post(function_blob_url, headers=BYTES_HEADER, data=function_pkl)

    spinner.text = StatusMessage.running()
    return job_id, input_uploader_thread


def _watch_job(
    job_id: str,
    n_inputs: int,
    headers: dict,
    verbose: bool,
    spinner: Spinner,
):
    stop_event = Event()

    # Start collecting logs generated by this job using a separate thread.
    args = (SUBSCRIBER, stop_event, spinner)
    log_thread = Thread(target=print_logs_from_stream, args=args, daemon=True)
    log_thread.start()

    # Start collecting outputs generated by this job using a separate thread.
    output_queue = Queue()
    args = (SUBSCRIBER, stop_event, output_queue)
    output_thread = Thread(target=enqueue_outputs_from_stream, args=args, daemon=True)
    output_thread.start()

    if verbose:
        spinner.text = StatusMessage.running()

    start = time()
    n_outputs_received = 0
    while n_outputs_received < n_inputs:
        timed_out = (time() - start) > (TIMEOUT_MIN * 60)
        sleep(job_status_poll_rate(seconds_since_job_started=time() - start))

        raise_any_errors_from_job(job_id, headers)

        while not output_queue.empty():
            n_outputs_received += 1
            yield output_queue.get()

        if timed_out:
            raise JobTimeoutError(job_id=job_id, timeout=TIMEOUT_MIN)

    stop_event.set()
    log_thread.join()
    output_thread.join()


@login_required
def remote_parallel_map(
    function_: Callable,
    inputs: list,
    verbose: bool = True,
    image: Optional[str] = None,
    func_cpu: int = 1,
    func_ram: int = 1,
    func_gpu: int = 0,
    parallelism: Optional[int] = None,
    api_key: Optional[str] = None,
    packages: Optional[list[str]] = None,
):
    rpm_call_time = time()
    n_inputs = len(inputs)
    if (func_cpu > 96) or (func_cpu < 1):
        raise ValueError("CPU per function call must be one of [1.. 80]")
    if (func_ram > 624) or (func_ram < 1):
        raise ValueError("RAM per function call must be one of [1.. 320]")
    if (func_gpu > 4) or (func_gpu < 0):
        raise ValueError("GPU per function call must be one of [0.. 4]")

    parallelism = parallelism if parallelism else n_inputs
    parallelism = parallelism if parallelism < MAX_PARALLELISM else MAX_PARALLELISM
    requested_cpu = parallelism * func_cpu
    requested_gpu = parallelism * func_gpu

    if requested_cpu > MAX_CPUS and not (requested_gpu > MAX_GPUS):
        parallelism = MAX_CPUS // func_cpu
        msg = f"Limiting parallelism to {parallelism} to stay under limit of {MAX_CPUS} CPUs."
        warnings.warn(msg)
    if requested_gpu > MAX_GPUS:
        parallelism = MAX_GPUS // func_gpu
        msg = f"Limiting parallelism to {parallelism} to stay under limit of {MAX_GPUS} GPUs."
        warnings.warn(msg)

    spinner = yaspin()
    StatusMessage.function_name = function_.__name__
    StatusMessage.n_inputs = len(inputs)
    StatusMessage.total_cpus = parallelism * func_cpu
    StatusMessage.total_gpus = parallelism if func_gpu else 0

    if api_key:
        auth_headers = {"Authorization": f"Bearer {api_key}"}
    else:
        auth_headers = auth_headers_from_local_config()

    try:
        job_id = None
        job_id, input_uploader_thread = _start_job(
            function_=function_,
            inputs=inputs,
            auth_headers=auth_headers,
            verbose=verbose,
            spinner=spinner,
            func_cpu=func_cpu,
            func_ram=func_ram,
            func_gpu=func_gpu,
            parallelism=parallelism,
            image=image,
            packages=packages,
        )
        output_generator = _watch_job(job_id, len(inputs), auth_headers, verbose, spinner)
        return_values = list(output_generator)
        input_uploader_thread.join()
    except Exception as e:
        spinner.stop()
        raise e
    finally:
        if job_id:
            payload = {"rpm_call_time": rpm_call_time, "job_ended_ts": time()}
            url = f"{_BURLA_SERVICE_URL}/v1/jobs/{job_id}/ended"
            requests.post(url, json=payload, headers=auth_headers)

    if verbose:
        spinner.text = "Done!"
        spinner.ok("✔")

    return return_values
