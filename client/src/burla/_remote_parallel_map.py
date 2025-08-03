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
from threading import Thread, Event
from pickle import UnpicklingError

import aiohttp
import cloudpickle
from tblib import Traceback
import google.auth
from google.cloud.run_v2 import ServicesClient
from google.cloud.firestore import FieldFilter
from google.cloud.firestore_v1.async_client import AsyncClient
from yaspin import yaspin, Spinner

from burla import __version__
from burla._auth import get_auth_headers
from burla._background_stuff import upload_inputs, send_alive_pings
from burla._install import main_service_url
from burla._helpers import (
    get_db_clients,
    install_signal_handlers,
    restore_signal_handlers,
    parallelism_capacity,
    has_explicit_return,
    log_telemetry,
    run_in_subprocess,
)


class NodeConflict(Exception):
    pass


class NoNodes(Exception):
    pass


class AllNodesBusy(Exception):
    pass


class NoCompatibleNodes(Exception):
    pass


class FirestoreTimeout(Exception):
    pass


class NodeDisconnected(Exception):
    pass


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
    ready_node_iterator = db.collection("nodes").where(filter=filter_).stream()

    # get first doc, takes 0.1-0.3s independant of number of documents/matches in collection
    try:
        first_ready_node = await asyncio.wait_for(anext(ready_node_iterator), 2)
    except StopAsyncIteration:
        return []
    except asyncio.TimeoutError:
        msg = "\nFirestore request timed out after 2s.\n"
        msg += "This almost always means your Google Cloud credentials have expired!\n"
        msg += "Please run `gcloud auth application-default login` then try again.\n"
        raise FirestoreTimeout(msg)

    return [first_ready_node.to_dict()] + [n.to_dict() async for n in ready_node_iterator]


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
        if node["host"].startswith("http://node_"):
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
    job_canceled_event: Event,
):
    auth_headers = get_auth_headers()
    sync_db, async_db = get_db_clients()
    spinner_compatible_print = lambda msg: spinner.write(msg) if spinner else print(msg)
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
            "function_name": function_.__name__,
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
        timeout = aiohttp.ClientTimeout(total=2)
        request = session.post(url, data=data, headers=auth_headers, timeout=timeout)
        try:
            async with request as response:
                if response.status == 200:
                    return node
                elif response.status == 401:
                    raise Exception("Unauthorized! Please run `burla login` to authenticate.")
                elif response.status == 409:
                    msg = f"ERROR from {node['instance_name']}: {await response.text()}"
                    raise NodeConflict(msg)
                else:
                    msg = f"Failed to assign {node['instance_name']}! ignoring: {response.status}"
                    spinner_compatible_print(msg)
        except asyncio.TimeoutError:
            msg = f"Timeout assigning {node['instance_name']} to job! Failing node ..."
            spinner_compatible_print(msg)
            try:
                # mark first as failed with reason so user can inspect the issue
                node_doc = async_db.collection("nodes").document(node["instance_name"])
                await node_doc.update({"status": "FAILED", "display_in_dashboard": True})
                msg = f"Failed! This node didn't respond (in <2s) to client request to assign job."
                await node_doc.collection("logs").document().set({"msg": msg, "ts": time()})
                # delete node
                url = f"{main_service_url()}/v1/cluster/{node['instance_name']}"
                url += "?hide_if_failed=false"
                async with session.delete(url, headers=auth_headers, timeout=1) as response:
                    if response.status != 200:
                        msg = f"Failed to delete node {node['instance_name']}."
                        spinner_compatible_print(msg + f" ignoring: {response.status}")
            except:
                pass

    async with AsyncExitStack() as stack:
        connector = aiohttp.TCPConnector(limit=500, limit_per_host=100)
        session = await stack.enter_async_context(aiohttp.ClientSession(connector=connector))

        # send function to every node
        assign_node_tasks = [assign_node(node, session) for node in nodes_to_assign]
        nodes = [node for node in await asyncio.gather(*assign_node_tasks) if node]
        if not nodes:
            raise Exception("Job refused by all available Nodes!")

        # start uploading inputs
        upload_inputs_args = (job_id, nodes, inputs, session, auth_headers, job_canceled_event)
        uploader_task = create_task(upload_inputs(*upload_inputs_args))

        if background:
            if spinner:
                spinner.text = f"Uploading {len(inputs)} inputs to {len(nodes)} nodes ..."
            await uploader_task
            return

        if spinner:
            msg = f"Running {len(inputs)} inputs through `{function_.__name__}` "
            spinner.text = msg + f"(0/{len(inputs)} completed)"

        if not background:
            # start sending "alive" pings to nodes
            ping_process = await run_in_subprocess(send_alive_pings, job_id)
            stack.callback(ping_process.kill)

            # start stdout/stderr stream
            def _on_new_log_message(col_snapshot, changes, read_time):
                for change in changes:
                    spinner_compatible_print(change.document.to_dict()["msg"])

            logs_collection = sync_db.collection("jobs").document(job_id).collection("logs")
            log_stream = logs_collection.on_snapshot(_on_new_log_message)
            stack.callback(log_stream.unsubscribe)

        async def _check_single_node(node: dict):
            url = f"{node['host']}/jobs/{job_id}/results"
            async with session.get(url, headers=auth_headers) as response:
                if response.status == 404:
                    nodes.remove(node)  # <- means node is likely rebooting and failed or is done
                elif response.status != 200:
                    raise Exception(f"Result-check failed for node: {node['instance_name']}")

                try:
                    node_status = pickle.loads(await response.content.read())
                except UnpicklingError as e:
                    if not "Memo value not found at index" in str(e):
                        raise e
                    msg = f"Node {node['instance_name']} disconnected while transmitting results.\n"
                    raise NodeDisconnected(msg)

                return_values = []
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

            exit_code = ping_process.poll()
            if exit_code:
                stderr = ping_process.stderr.read().decode("utf-8")
                raise Exception(f"Ping process exited with code: {exit_code}\n{stderr}")

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
    function_signature = inspect.signature(function_)
    if len(function_signature.parameters) != 1:
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

    return_queue = Queue()
    original_signal_handlers = None
    try:
        if spinner:
            spinner = yaspin(sigmap={})  # <- .start will overwrite my handlers without sigmap={}
            spinner.start()
            spinner.text = f"Preparing to run {len(inputs)} inputs through `{function_.__name__}`"
        job_canceled_event = Event()
        original_signal_handlers = install_signal_handlers(job_id, spinner, job_canceled_event)

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
                        job_canceled_event=job_canceled_event,
                    )
                )
            except Exception as e:
                execute_job.exc_info = sys.exc_info()

        t = Thread(target=execute_job, daemon=True)
        t.start()

        msg = f"RPM called with: {len(inputs)} inputs, func_cpu={func_cpu}, func_ram={func_ram}, "
        msg += f"background={background}, generator={generator}, spinner={spinner}, "
        msg += f"max_parallelism={max_parallelism}, job_id={job_id}"
        log_telemetry(msg, project_id=project_id)

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

        log_telemetry(f"Job {job_id} returned successfully.", project_id=project_id)
        return _output_generator() if generator else list(_output_generator())

    except Exception as e:
        if spinner:
            spinner.stop()

        # After a `FirestoreTimeout` further attempts to use firestore will take forever then fail.
        if not isinstance(e, FirestoreTimeout):
            try:
                sync_db, _ = get_db_clients()
                sync_db.collection("jobs").document(job_id).update({"status": "FAILED"})
            except Exception:
                pass

        try:
            # Report errors back to Burla's cloud.
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
            traceback_str = "".join(traceback_details)
            kwargs = dict(traceback=traceback_str, project_id=project_id, job_id=job_id)
            log_telemetry(exc_type, severity="ERROR", **kwargs)
        except:
            pass

        raise
    finally:
        if original_signal_handlers:
            restore_signal_handlers(original_signal_handlers)
