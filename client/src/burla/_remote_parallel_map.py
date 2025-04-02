import sys
import pickle
import json
import inspect
import aiohttp
import asyncio
from time import sleep, time
from queue import Queue
from six import reraise
from uuid import uuid4
from threading import Thread, Event
from typing import Callable, Optional, Union

import cloudpickle
from google.cloud import firestore
from google.cloud.firestore import FieldFilter
from yaspin import Spinner
from tblib import Traceback

from burla import __version__
from burla._auth import get_auth_headers
from burla._background_threads import (
    upload_inputs,
    print_logs_from_db,
    enqueue_results,
    reboot_nodes,
)
from burla._helpers import (
    get_db,
    prep_graceful_shutdown_with_spinner,
    prep_graceful_shutdown,
    parallelism_capacity,
    ThreadWithExc,
)

START_TIME = None
MAX_PARALLELISM = 1000  # If you increase this Burla will **probably** break.


class UnknownClusterError(Exception):
    def __init__(self):
        msg = "\nAn unknown error occurred inside your Burla cluster, "
        msg += "this is not an error with your code, but with the Burla.\n"
        msg += "If this issue is urgent please don't hesitate to call me (Jake) directly"
        msg += " at 508-320-8778, or email me at jake@burla.dev."
        super().__init__(msg)


def _start_job(
    function_: Callable,
    n_inputs: int,
    func_cpu: int,
    func_ram: int,
    max_parallelism: int,
    db: firestore.Client,
    spinner: Union[bool, Spinner],
) -> str:
    log_msg_stdout = spinner if spinner else sys.stdout
    node_filter = FieldFilter("status", "==", "READY")
    ready_nodes = [n.to_dict() for n in db.collection("nodes").where(filter=node_filter).stream()]
    log_msg_stdout.write(f"Found {len(ready_nodes)} nodes with state `READY`.")

    if len(ready_nodes) == 0:
        raise Exception("Found zero nodes with state `READY`, have you started the Cluster?")

    # When running locally the node service hostname is it's container name. This only works from
    # inside the docker network, not from the host machine (here). If detected, swap to localhost.
    for node in ready_nodes:
        if node["host"].startswith("http://node_"):
            node["host"] = f"http://localhost:{node['host'].split(':')[-1]}"

    planned_future_job_parallelism = 0
    nodes_to_assign = []
    for node in ready_nodes:
        parallelism_deficit = max_parallelism - planned_future_job_parallelism
        max_node_parallelism = parallelism_capacity(node["machine_type"], func_cpu, func_ram)

        if max_node_parallelism > 0 and parallelism_deficit > 0:
            node_target_parallelism = min(parallelism_deficit, max_node_parallelism)
            node["target_parallelism"] = node_target_parallelism
            planned_future_job_parallelism += node_target_parallelism
            nodes_to_assign.append(node)

    if len(nodes_to_assign) == 0:
        raise Exception("No compatible nodes available.")
    log_msg_stdout.write(f"Assigning {len(nodes_to_assign)} nodes to job.")

    job_id = str(uuid4())
    log_msg_stdout.write(f"Job ID: {job_id}")
    job_ref = db.collection("jobs").document(job_id)
    job_ref.set(
        {
            "n_inputs": n_inputs,
            "func_cpu": func_cpu,
            "func_ram": func_ram,
            "burla_client_version": __version__,
            "user_python_version": f"3.{sys.version_info.minor}",
            "target_parallelism": max_parallelism,
            "planned_future_job_parallelism": planned_future_job_parallelism,
            "user": "TEMP-TEST",
            "started_at": time(),
        }
    )

    async def assign_node(node: dict, session: aiohttp.ClientSession):
        request_json = {
            "parallelism": node["target_parallelism"],
            "user_python_version": f"3.{sys.version_info.minor}",
        }
        data = aiohttp.FormData()
        data.add_field("request_json", json.dumps(request_json))
        data.add_field("function_pkl", cloudpickle.dumps(function_))
        url = f"{node['host']}/jobs/{job_id}"

        async with session.post(url, data=data, timeout=5) as response:
            try:
                response.raise_for_status()
                return node
            except Exception as e:
                node_name = node["instance_name"]
                log_msg_stdout.write(f"Failed to assign {node_name}! ignoring error: {e}")
                return None

    async def assign_all_nodes():
        async with aiohttp.ClientSession() as session:
            tasks = [assign_node(node, session) for node in nodes_to_assign]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return [node for node in results if node]

    nodes = asyncio.run(assign_all_nodes())
    if not nodes:
        raise Exception("Job refused by all available Nodes!")
    else:
        return job_id, job_ref, nodes


def _watch_job(
    job_id: str,
    nodes: list,
    inputs: list,
    function_name: str,
    job_ref: firestore.DocumentReference,
    spinner: Union[bool, Spinner],
    stop_event: Event,
    db: firestore.Client,
    auth_headers: dict,
):
    log_msg_stdout = spinner if spinner else sys.stdout

    # In separate thread start uploading inputs:
    args = (job_id, nodes, inputs, stop_event, log_msg_stdout)
    input_thread = ThreadWithExc(target=upload_inputs, args=args, daemon=True)
    input_thread.start()

    # Start printing logs generated by this job from a separate thread.
    args = (job_ref, stop_event, log_msg_stdout)
    log_thread = Thread(target=print_logs_from_db, args=args, daemon=True)
    log_thread.start()

    # Start enqueueing results (either return-values or errors) generated by this job
    # from a separate thread.
    result_queue = Queue()
    args = (job_id, stop_event, nodes, result_queue, log_msg_stdout)
    result_thread = ThreadWithExc(target=enqueue_results, args=args, daemon=True)
    result_thread.start()

    if spinner:
        msg = f"Running {len(inputs)} inputs through `{function_name}` (0/{len(inputs)} completed)"
        spinner.text = msg

    n_results_received = 0
    while n_results_received < len(inputs):
        for t in [input_thread, result_thread]:
            if t.traceback_str:
                raise Exception(f"Error in {t._target.__name__} thread : {t.traceback_str}")

        while not result_queue.empty():
            input_index, is_error, result_pkl = result_queue.get()
            if is_error:
                exc_info = pickle.loads(result_pkl)
                traceback = Traceback.from_dict(exc_info["traceback_dict"]).as_traceback()
                reraise(tp=exc_info["type"], value=exc_info["exception"], tb=traceback)
            else:
                result = cloudpickle.loads(result_pkl)
                n_results_received += 1
                if spinner:
                    msg = f"Running {len(inputs)} inputs through `{function_name}` "
                    msg += f"({n_results_received}/{len(inputs)} completed)"
                    spinner.text = msg
                yield result
        sleep(0.05)

    reboot_thread = Thread(target=reboot_nodes, args=(nodes, auth_headers), daemon=True)
    reboot_thread.start()
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
    sig = inspect.signature(function_)
    if len(sig.parameters) != 1:
        msg = "Function must accept exactly one argument! (even if it does nothing)\n"
        msg += "Email jake@burla.dev if this is really annoying and we will fix it! :)"
        raise ValueError(msg)

    max_parallelism = max_parallelism if max_parallelism else len(inputs)
    max_parallelism = max_parallelism if max_parallelism < MAX_PARALLELISM else MAX_PARALLELISM
    auth_headers = get_auth_headers(api_key) if api_key else get_auth_headers()
    db = get_db(auth_headers)

    stop_event = Event()
    # below functions setup handlers to set `stop_event` (or stop spinner) on os-signals
    # (like when user hits ctrl+c), putting this stuff in a try-finally dosen't always work.
    if spinner:
        spinner = prep_graceful_shutdown_with_spinner(stop_event)
        spinner.start()
        spinner.text = f"Preparing to run {len(inputs)} inputs through `{function_.__name__}`"
    else:
        prep_graceful_shutdown(stop_event)

    try:
        job_id, job_ref, nodes = _start_job(
            function_=function_,
            n_inputs=len(inputs),
            func_cpu=func_cpu,
            func_ram=func_ram,
            max_parallelism=max_parallelism,
            db=db,
            spinner=spinner,
        )
        output_batch_generator = _watch_job(
            job_id=job_id,
            nodes=nodes,
            inputs=inputs,
            function_name=function_.__name__,
            job_ref=job_ref,
            spinner=spinner,
            stop_event=stop_event,
            db=db,
            auth_headers=auth_headers,
        )

        def _output_generator():
            yield from output_batch_generator

        if not generator:
            results = list(_output_generator())

        if spinner:
            msg = f"Done! Ran {len(inputs)} inputs through `{function_.__name__}` "
            msg += f"({len(inputs)}/{len(inputs)} completed)"
            spinner.text = msg
            spinner.ok("✔")

        return _output_generator() if generator else results

    except Exception:
        stop_event.set()
        if spinner:
            spinner.stop()
        raise
