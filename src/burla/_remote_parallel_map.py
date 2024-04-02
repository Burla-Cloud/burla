import os
import sys
import requests
import warnings
import pickle
from six import reraise
from threading import Thread, Event
from typing import Callable, Optional
from time import sleep, time
from queue import PriorityQueue

import cloudpickle
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

MAX_CPUS = 2000  # please ask before you increase this <3
MAX_GPUS = 300  # please ask before you increase this <3
MAX_PARALLELISM = 5000  # remote_parallel_map might break if this is raised.
MAX_CONCURRENCY = 2000

TIMEOUT_MIN = 60 * 12  # max time a Burla job can run for
IN_COLAB = os.getenv("COLAB_RELEASE_TAG") is not None

BYTES_HEADER = {"Content-Type": "application/octet-stream"}


def job_status_poll_rate(seconds_since_job_started: int):
    """
    We want to check often if the job is done early on, and progressively check less often so as
    to not spam the service.
    """
    if seconds_since_job_started > 3600:
        return 120
    else:
        # https://www.desmos.com/calculator/adqb1zplc3
        return (
            0.695054758
            + 0.0592058591 * seconds_since_job_started
            - 0.0000218934920 * seconds_since_job_started**2
            + 0.00000000407016749 * seconds_since_job_started**3
            - 0.6
        )


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

    if job.get("output_urls"):
        return job["udf_started"], job["logs"], job["output_urls"]
    else:
        return job["udf_started"], job["logs"], None


def _start_job(
    function_: Callable,
    inputs: list,
    auth_headers: dict,
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

    payload = {
        "n_inputs": len(inputs),
        "image": image,
        "func_cpu": func_cpu,
        "func_ram": func_ram,
        "gpu": gpu,
        "parallelism": parallelism,
        "python_version": f"3.8" if sys.version_info.minor < 8 else f"3.{sys.version_info.minor}",
        "packages": None if image else required_packages,
        "burla_version": __version__,
    }
    response = requests.post(f"{_BURLA_SERVICE_URL}/v1/jobs/", json=payload, headers=auth_headers)
    if response.status_code == 401:
        raise AuthException()
    else:
        response.raise_for_status()
        response_json = response.json()
        job_id = response_json["job_id"]

    # download list of urls to upload to, these urls are big and sometimes there are a lot of them.
    response = requests.get(response_json["input_urls_url"], headers=BYTES_HEADER)
    response.raise_for_status()
    input_urls = pickle.loads(response.content)

    spinner.text = StatusMessage.uploading_inputs
    upload_inputs(input_urls, inputs)

    spinner.text = StatusMessage.uploading_function
    function_pkl = cloudpickle.dumps(function_)
    requests.put(response_json["function_url"], headers=BYTES_HEADER, data=function_pkl)

    spinner.text = StatusMessage.preparing()
    response = requests.post(f"{_BURLA_SERVICE_URL}/v1/jobs/{job_id}", headers=auth_headers)
    response.raise_for_status()
    return job_id


def _watch_job(job_id: str, job_started_time: int, headers: dict, verbose: bool, spinner: Spinner):
    last_epoch = job_started_time
    epoch = last_epoch
    output_urls = None
    job_timed_out = False

    # Start printing logs generated by this job using a separate thread.
    print_queue = PriorityQueue()
    stop_event = Event()
    args = (print_queue, stop_event, spinner)
    log_thread = Thread(target=print_logs_from_queue, args=args, daemon=True)
    log_thread.start()
    while (not output_urls) and (not job_timed_out):
        sleep(job_status_poll_rate(time() - job_started_time))
        udf_started, logs, output_urls = _get_job_info_since(last_epoch, job_id, headers)

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
    return output_urls


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
            f"Limiting parallelism to {parallelism} to stay under current limit of {MAX_CPUS} CPUs."
        )
    # if requested_gpu > MAX_GPUS:
    #     parallelism = MAX_GPUS // func_gpu
    #     warnings.warn(
    #         f"Limiting parallelism to {parallelism} to stay under current limit of {MAX_GPUS} GPUs."
    #     )

    input_batches = [inputs[i : i + MAX_CONCURRENCY] for i in range(0, n_inputs, MAX_CONCURRENCY)]
    if len(input_batches) > 1:
        msg = f"Because a maximum of {MAX_CONCURRENCY} inputs can be processed at a time, "
        msg += f"these inputs will be processed in {len(input_batches)} separate batches.\n"
        msg += "Consider using a lower number of larger inputs, this may reduce runtime.\n"
        warnings.warn(msg)

    spinner = yaspin()
    StatusMessage.function_name = function_.__name__
    StatusMessage.n_inputs = len(inputs) if len(inputs) < MAX_CONCURRENCY else MAX_CONCURRENCY
    StatusMessage.total_cpus = parallelism * func_cpu
    StatusMessage.total_gpus = parallelism if gpu else 0

    if api_key:
        auth_headers = {"Authorization": f"Bearer {api_key}"}
    else:
        auth_headers = auth_headers_from_local_config()

    return_values = []
    for input_batch in input_batches:
        try:
            start_time = int(time())
            job_id = _start_job(
                function_=function_,
                inputs=input_batch,
                auth_headers=auth_headers,
                verbose=verbose,
                spinner=spinner,
                func_cpu=func_cpu,
                func_ram=func_ram,
                gpu=gpu,
                parallelism=parallelism,
                image=image,
                packages=packages,
            )
            output_urls = _watch_job(job_id, start_time, auth_headers, verbose, spinner)

            spinner.text = StatusMessage.downloading
            for output_pkl in download_outputs(output_urls):
                return_values.append(cloudpickle.loads(output_pkl))

        except Exception as e:
            spinner.stop()
            raise e

    if verbose:
        spinner.text = "Done!"
        spinner.ok("✔")

    all_return_values_are_none = all(value is None for value in return_values)
    if not all_return_values_are_none:
        return return_values
