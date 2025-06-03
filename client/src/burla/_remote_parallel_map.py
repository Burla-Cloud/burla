import sys
import pickle
import json
import inspect
import asyncio
import traceback
from asyncio import create_task
from time import time
from six import reraise
from queue import Queue
from uuid import uuid4
from typing import Callable, Optional, Union
from contextlib import AsyncExitStack
from threading import Thread

import aiohttp
import cloudpickle
from tblib import Traceback
import google.auth
from google.cloud.run_v2 import ServicesClient
from google.cloud.firestore import FieldFilter
from google.cloud.firestore_v1.async_client import AsyncClient
from yaspin import Spinner

from burla import __version__
from burla._auth import get_auth_headers
from burla._background_stuff import (
    upload_inputs,
    send_alive_pings_in_background,
)
from burla._helpers import (
    get_db_clients,
    spinner_with_signal_handlers,
    parallelism_capacity,
    has_explicit_return,
    _log_telemetry,
)


class NodeConflict(Exception):
    pass


class NoNodes(Exception):
    pass


class AllNodesBusy(Exception):
    pass


class NoCompatibleNodes(Exception):
    pass


class UnknownClusterError(Exception):
    def __init__(self):
        msg = "\nAn unknown error occurred inside your Burla cluster, "
        msg += "this is not an error with your code, but with the Burla.\n"
        msg += "If this issue is urgent please don't hesitate to call me (Jake) directly"
        msg += " at 508-320-8778, or email me at jake@burla.dev."
        super().__init__(msg)


async def _wait_for_nodes_to_boot(db: AsyncClient, spinner: Union[bool, Spinner]):
    n_booting_nodes = await _num_booting_nodes(db)
    if n_booting_nodes == 0:
        filter_ = FieldFilter("status", "==", "RUNNING")
        running_nodes_generator = db.collection("nodes").where(filter=filter_).stream()
        running_nodes = [n.to_dict() async for n in running_nodes_generator]
        if running_nodes:
            raise AllNodesBusy("All nodes are busy, please try again later.")
        else:
            raise NoNodes("Didn't find any nodes, has the Cluster been turned on?")

    ready_nodes = []
    while n_booting_nodes != 0:
        if spinner:
            msg = f"{len(ready_nodes)} Nodes are ready, waiting for remaining {n_booting_nodes} "
            spinner.text = msg + "to boot before starting ..."
        await asyncio.sleep(0.1)
        n_booting_nodes = await _num_booting_nodes(db)
        ready_nodes = await _get_ready_nodes(db)
    return ready_nodes


async def _num_booting_nodes(db: AsyncClient):
    filter_ = FieldFilter("status", "==", "BOOTING")
    nodes_snapshot = await db.collection("nodes").where(filter=filter_).get()
    return len(nodes_snapshot)


async def _get_ready_nodes(db: AsyncClient):
    filter_ = FieldFilter("status", "==", "READY")
    return [n.to_dict() async for n in db.collection("nodes").where(filter=filter_).stream()]


async def _select_nodes_to_assign_to_job(
    db: AsyncClient,
    max_parallelism: int,
    func_cpu: int,
    func_ram: int,
    spinner: Union[bool, Spinner],
):
    ready_nodes = await _get_ready_nodes(db)
    if not ready_nodes:
        ready_nodes = await _wait_for_nodes_to_boot(db, spinner)

    planned_initial_job_parallelism = 0
    nodes_to_assign = []
    for node in ready_nodes:
        parallelism_deficit = max_parallelism - planned_initial_job_parallelism
        max_node_parallelism = parallelism_capacity(node["machine_type"], func_cpu, func_ram)

        if max_node_parallelism > 0 and parallelism_deficit > 0:
            node_target_parallelism = min(parallelism_deficit, max_node_parallelism)
            node["target_parallelism"] = node_target_parallelism
            planned_initial_job_parallelism += node_target_parallelism
            nodes_to_assign.append(node)

    if len(nodes_to_assign) == 0:
        msg = "No compatible nodes available. Are the machines in your cluster large enough to "
        msg += "support your `func_cpu` and `func_ram` arguments?"
        raise NoCompatibleNodes(msg)

    # When running locally the node service hostname is it's container name. This only works from
    # inside the docker network, not from the host machine (here). If detected, swap to localhost.
    for node in nodes_to_assign:
        if not node.get("host"):
            nodes_to_assign.remove(node)
        elif node["host"].startswith("http://node_"):
            node["host"] = f"http://localhost:{node['host'].split(':')[-1]}"

    return nodes_to_assign, planned_initial_job_parallelism


async def _execute_job(
    job_id: str,
    return_queue: Queue,
    function_: Callable,
    inputs: list,
    func_cpu: int,
    func_ram: int,
    max_parallelism: int,
    background: bool,
    spinner: Union[bool, Spinner],
):
    auth_headers = get_auth_headers()
    sync_db, async_db = get_db_clients()
    log_msg_stdout = spinner if spinner else sys.stdout
    function_pkl = cloudpickle.dumps(function_)

    nodes_to_assign, total_target_parallelism = await _select_nodes_to_assign_to_job(
        async_db, max_parallelism, func_cpu, func_ram, spinner
    )

    job_ref = async_db.collection("jobs").document(job_id)
    await job_ref.set(
        {
            "n_inputs": len(inputs),
            "func_cpu": func_cpu,
            "func_ram": func_ram,
            "status": "RUNNING",
            "burla_client_version": __version__,
            "user_python_version": f"3.{sys.version_info.minor}",
            "max_parallelism": max_parallelism,
            "target_parallelism": total_target_parallelism,
            "user": auth_headers["X-User-Email"],
            "started_at": time(),
            "last_ping_from_client": time(),
            "is_background_job": background,
            "client_has_all_results": False,
        }
    )

    async def assign_node(node: dict, session: aiohttp.ClientSession):
        request_json = {
            "parallelism": node["target_parallelism"],
            "is_background_job": background,
            "user_python_version": f"3.{sys.version_info.minor}",
            "n_inputs": len(inputs),
        }
        data = aiohttp.FormData()
        data.add_field("request_json", json.dumps(request_json))
        data.add_field("function_pkl", function_pkl)

        url = f"{node['host']}/jobs/{job_id}"
        async with session.post(url, data=data, headers=auth_headers) as response:
            try:
                response.raise_for_status()
                return node
            except Exception as e:
                node_name = node["instance_name"]
                if response.status == 409:
                    raise NodeConflict(f"ERROR from {node_name}: {await response.text()}")
                else:
                    log_msg_stdout.write(f"Failed to assign {node_name}! ignoring error: {e}")

    def _on_new_log_message(col_snapshot, changes, read_time):
        for change in changes:
            log_msg_stdout.write(change.document.to_dict()["msg"])

    async with AsyncExitStack() as stack:
        connector = aiohttp.TCPConnector(limit=500, limit_per_host=100)
        session = await stack.enter_async_context(aiohttp.ClientSession(connector=connector))

        ping_exception_queue = None
        if not background:
            # Constantly update firestore to tell nodes client is still listening.
            ping_process, ping_exception_queue = await send_alive_pings_in_background(job_id)
            stack.callback(ping_process.kill)

            # stream stdout back to client
            logs_collection = sync_db.collection("jobs").document(job_id).collection("logs")
            log_stream = logs_collection.on_snapshot(_on_new_log_message)
            stack.callback(log_stream.unsubscribe)

        assign_node_tasks = [assign_node(node, session) for node in nodes_to_assign]
        nodes = [node for node in await asyncio.gather(*assign_node_tasks) if node]
        if not nodes:
            raise Exception("Job refused by all available Nodes!")

        uploader_task = create_task(upload_inputs(job_id, nodes, inputs, session, auth_headers))

        if background:
            if spinner:
                spinner.text = f"Uploading {len(inputs)} inputs to {len(nodes)} nodes ..."
            await uploader_task
            return

        if spinner:
            msg = f"Running {len(inputs)} inputs through `{function_.__name__}` "
            spinner.text = msg + f"(0/{len(inputs)} completed)"

        async def _check_single_node(node: dict):
            url = f"{node['host']}/jobs/{job_id}/results"
            async with session.get(url, headers=auth_headers) as response:
                if response.status == 404:
                    nodes.remove(node)  # <- means node is likely rebooting and failed or is done
                elif response.status != 200:
                    raise Exception(f"Result-check failed for node: {node['instance_name']}")

                return_values = []
                node_status = pickle.loads(await response.content.read())
                for input_index, is_error, result_pkl in node_status["results"]:
                    if is_error:
                        exc_info = pickle.loads(result_pkl)
                        traceback = Traceback.from_dict(exc_info["traceback_dict"]).as_traceback()
                        reraise(tp=exc_info["type"], value=exc_info["exception"], tb=traceback)
                    else:
                        return_values.append(cloudpickle.loads(result_pkl))

                return node_status["is_empty"], node_status["current_parallelism"], return_values

        n_results = 0
        all_nodes_empty = False
        start = time()
        while n_results < len(inputs):

            if all_nodes_empty:
                elapsed_time = time() - start
                if elapsed_time > 3:
                    await asyncio.sleep(0.3)
                else:
                    await asyncio.sleep(0)

            total_parallelism = 0
            all_nodes_empty = True
            nodes_status = await asyncio.gather(*[_check_single_node(n) for n in nodes])
            for is_empty, node_parallelism, return_values in nodes_status:
                total_parallelism += node_parallelism
                all_nodes_empty = all_nodes_empty and is_empty
                for return_value in return_values:
                    return_queue.put_nowait(return_value)
                    n_results += 1

            if uploader_task.done() and uploader_task.exception():
                raise uploader_task.exception()

            if ping_exception_queue and not ping_exception_queue.empty():
                exc_info = pickle.loads(ping_exception_queue.get())
                traceback = Traceback.from_dict(exc_info["traceback_dict"]).as_traceback()
                reraise(tp=exc_info["type"], value=exc_info["exception"], tb=traceback)

            if spinner:
                spinner.text = (
                    f"Running {len(inputs)} inputs through `{function_.__name__}` "
                    f"({n_results}/{len(inputs)} completed) "
                    f"({total_parallelism} function instances running)"
                )

            if len(nodes) == 0 and return_queue.empty():  # nodes removed in _check_single_node
                raise Exception("Zero nodes working on job and we have not received all results!")

        await job_ref.update({"client_has_all_results": True})


def remote_parallel_map(
    function_: Callable,
    inputs: list,
    func_cpu: int = 1,
    func_ram: int = 4,
    background: bool = False,
    generator: bool = False,
    spinner: bool = True,
    max_parallelism: Optional[int] = None,
):
    """
    Run an arbitrary Python function on many remote computers in parallel.

    Run provided function_ on each item in inputs at the same time, each on a separate CPU.
    If more than inputs than there are cpu's are provided, inputs are queued and
    processed sequentially on each worker. Any exception raised by `function_`
    (including its stack trace) will be re-raised here on the client machine.

    Args:
        function_ (Callable):
            A Python function that accepts a single input argument. For example, calling
            `function_(inputs[0])` should not raise an exception.
        inputs (Iterable[Any]):
            An iterable of elements that will be passed to `function_`.
        func_cpu (int, optional):
            The number of CPUs allocated for each instance of `function_`. Defaults to 1.
        func_ram (int, optional):
            The amount of RAM (in GB) allocated for each instance of `function_`. Defaults to 4.
        background (bool, optional):
            If True, returns as soon as all inputs are uploaded and runs the job in the background.
            Defaults to False.
        generator (bool, optional):
            If True, returns a generator that yields outputs as they are produced; otherwise,
            returns a list of outputs once all have been processed. Defaults to False.
        spinner (bool, optional):
            If set to False, disables the display of the status indicator/spinner. Defaults to True.
        max_parallelism (int, optional):
            The maximum number of `function_` instances allowed to be running at the same time.
            Defaults to the number of available CPUs divided by `func_cpu`.

    Returns:
        List[Any] or Generator[Any, None, None]:
            A list containing the objects returned by `function_` in no particular order.
            If `generator=True`, returns a generator that yields results as they are produced.

    See Also:
        For more info see our overview: https://docs.burla.dev/overview
        or API-Reference: https://docs.burla.dev/api-reference
    """
    max_parallelism = max_parallelism if max_parallelism else len(inputs)
    sig = inspect.signature(function_)
    if len(sig.parameters) != 1:
        msg = "Function must accept exactly one argument! (even if it does nothing)\n"
        msg += "Email jake@burla.dev if this is really annoying and we will fix it! :)"
        raise ValueError(msg)

    try:
        if background and has_explicit_return(function_):
            print(
                f"Warning: Function `{function_.__name__}` has an explicit return statement.\n"
                "Because this job is set to run in the background, any returned objects will be lost!"
            )
    except:
        pass

    job_id = str(uuid4())
    _, project_id = google.auth.default()

    msg = f"RPM called with: {len(inputs)} inputs, func_cpu={func_cpu}, func_ram={func_ram}, "
    msg += f"background={background}, generator={generator}, spinner={spinner}, "
    msg += f"max_parallelism={max_parallelism}, job_id={job_id}"
    _log_telemetry(msg, project_id=project_id)

    return_queue = Queue()
    try:
        if spinner:
            spinner = spinner_with_signal_handlers()
            spinner.start()
            spinner.text = f"Preparing to run {len(inputs)} inputs through `{function_.__name__}`"

        def execute_job():
            try:
                asyncio.run(
                    _execute_job(
                        job_id=job_id,
                        return_queue=return_queue,
                        function_=function_,
                        inputs=inputs,
                        func_cpu=func_cpu,
                        func_ram=func_ram,
                        max_parallelism=max_parallelism,
                        background=background,
                        spinner=spinner,
                    )
                )
            except Exception as e:
                execute_job.exc_info = sys.exc_info()

        t = Thread(target=execute_job)
        t.start()
        t.join()

        if hasattr(execute_job, "exc_info"):
            raise execute_job.exc_info[1].with_traceback(execute_job.exc_info[2])

        if background:
            client = ServicesClient()
            service_path = client.service_path(project_id, "us-central1", "burla-main-service")
            job_url = f"{client.get_service(name=service_path).uri}/jobs/{job_id}"

            msg = f"Done uploading inputs.\n"
            msg += f"Job will continue running in the background, monitor progress at: {job_url}"
            spinner.text = msg
            spinner.ok("✔")
            return

        def _output_generator():
            n_results = 0
            while n_results != len(inputs):
                yield return_queue.get()
                n_results += 1

        if spinner:
            msg = f"Done! Ran {len(inputs)} inputs through `{function_.__name__}` "
            msg += f"({len(inputs)}/{len(inputs)} completed)"
            spinner.text = msg
            spinner.ok("✔")

        _log_telemetry(f"Job {job_id} returned successfully.", project_id=project_id)
        return _output_generator() if generator else list(_output_generator())

    except Exception:
        if spinner:
            spinner.stop()

        try:
            sync_db, _ = get_db_clients()
            sync_db.collection("jobs").document(job_id).update({"status": "FAILED"})

            # Report errors back to Burla's cloud.
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
            traceback_str = "".join(traceback_details)
            kwargs = dict(traceback=traceback_str, project_id=project_id, job_id=job_id)
            _log_telemetry(exc_type, severity="ERROR", **kwargs)
        except:
            pass

        raise
