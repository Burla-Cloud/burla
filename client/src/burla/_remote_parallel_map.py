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
from google.cloud import firestore
from yaspin import yaspin
from tblib import Traceback

from burla import __version__
from burla._auth import get_auth_headers, AuthException
from burla._helpers import (
    upload_inputs,
    print_logs_from_db,
    enqueue_results_from_db,
    healthcheck_job,
    get_db,
    get_host,
)

MAX_PARALLELISM = 1000  # outdated.

# This MUST be set to the same value as `JOB_HEALTHCHECK_FREQUENCY_SEC` in the node service.
# Nodes will restart themself if they dont get a new healthcheck from the client every X seconds.
JOB_HEALTHCHECK_FREQUENCY_SEC = 3


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
    db: firestore.Client,
    auth_headers: dict,
) -> str:

    inputs_pkl = [cloudpickle.dumps(input_) for input_ in inputs]
    inputs_size = sum([len(input_pkl) for input_pkl in inputs_pkl])
    if inputs_size > 84_866_368:
        raise Exception("Total size of all inputs exceeds current maximum limit of 84.8MB")

    # in separate thread start uploading inputs:
    inputs_id = str(uuid4())
    args = (db, inputs_id, inputs_pkl, stop_event)
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
    url = f"{get_host()}/v1/jobs/"
    files = {"function_pkl": cloudpickle.dumps(function_)}
    data = dict(request_json=json.dumps(payload))

    response = requests.post(url, files=files, data=data, headers=auth_headers)
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


def _watch_job(
    job_id: str,
    n_inputs: int,
    log_msg_stdout: io.TextIOWrapper,
    stop_event: Event,
    db: firestore.Client,
    auth_headers: dict,
):
    job_doc_ref = db.collection("jobs").document(job_id)

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

    time_since_last_healthcheck = 0
    n_results_received = 0
    while n_results_received < n_inputs:
        sleep(0.05)
        time_since_last_healthcheck += 0.05

        if time_since_last_healthcheck > JOB_HEALTHCHECK_FREQUENCY_SEC:
            healthcheck_job(job_id=job_id, auth_headers=auth_headers)
            time_since_last_healthcheck = 0

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
    generator: bool = False,
    max_parallelism: Optional[int] = None,
    api_key: Optional[str] = None,
):
    """
    Run an arbitrary Python function on many remote computers in parallel.

    Run provided function_ on each item in inputs at the same time, each on a separate CPU,
    up to 256 CPUs (as of 1/3/25). If more than 256 inputs are provided, inputs are queued and
    processed sequentially on each worker. Any exception raised by `function_`
    (including its stack trace) will be re-raised on the client machine.

    Args:
        function_ (Callable):
            A Python function that accepts a single input argument. For example, calling
            `function_(inputs[0])` should not raise an exception.
        inputs (Iterable[Any]):
            An iterable of elements that will be passed to `function_`.
        func_cpu (int, optional):
            The number of CPUs allocated for each instance of `function_`. The maximum allowable
            value is 32. Defaults to 1.
        func_ram (int, optional):
            The amount of RAM (in GB) allocated for each instance of `function_`. The maximum
            allowable value is 128. Defaults to 4.
        spinner (bool, optional):
            If set to False, disables the display of the status indicator/spinner. Defaults to True.
        generator (bool, optional):
            If True, returns a generator that yields outputs as they are produced; otherwise,
            returns a list of outputs once all have been processed. Defaults to False.
        max_parallelism (int, optional):
            The maximum number of `function_` instances allowed to be running at the same time.
            Defaults to the number of available CPUs divided by `func_cpu`.
        api_key (str, optional):
            An API key for use in deployment environments where `burla login` cannot be run.

    Returns:
        List[Any] or Generator[Any, None, None]:
            A list containing the objects returned by `function_` in no particular order.
            If `generator=True`, returns a generator that yields results as they are produced.

    See Also:
        For more info see our overview: https://docs.burla.dev/overview
        or API-Reference: https://docs.burla.dev/api-reference
    """
    max_parallelism = max_parallelism if max_parallelism else len(inputs)
    max_parallelism = max_parallelism if max_parallelism < MAX_PARALLELISM else MAX_PARALLELISM
    kwargs = dict(
        function_=function_,
        inputs=inputs,
        func_cpu=func_cpu,
        func_ram=func_ram,
        spinner=spinner,
        generator=generator,
        max_parallelism=max_parallelism,
        api_key=api_key,
    )
    if spinner:
        with yaspin() as spinner:
            spinner.text = f"Preparing to run {len(inputs)} inputs through `{function_.__name__}`"
            return _rpm(**kwargs)
    else:
        return _rpm(**kwargs)


# temp: something to wrap with the spinner, I seem to be forced to use with statements
def _rpm(
    function_: Callable,
    inputs: list,
    func_cpu: int = 1,
    func_ram: int = 4,
    spinner: bool = True,
    generator: bool = False,
    max_parallelism: Optional[int] = None,
    api_key: Optional[str] = None,
):
    auth_headers = get_auth_headers(api_key)
    db = get_db(auth_headers)

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
            db=db,
            auth_headers=auth_headers,
        )
        log_msg_stdout = spinner if spinner else sys.stdout
        output_batch_generator = _watch_job(
            job_id=job_id,
            n_inputs=len(input_batches),
            log_msg_stdout=log_msg_stdout,
            stop_event=stop_event,
            db=db,
            auth_headers=auth_headers,
        )

        if spinner:
            spinner.text = f"Running {len(inputs)} inputs through `{function_.__name__}`"

        def _output_generator():
            # yield from output_batch_generator
            for output_batch in output_batch_generator:
                yield from output_batch

        if not generator:
            results = [item for item in _output_generator()]

        if spinner:
            spinner.text = "Done!"
            spinner.ok("✔")

        if generator:
            return _output_generator()
        else:
            return results

    finally:
        stop_event.set()
