import asyncio
import json
import pickle
import sys
import traceback
from asyncio import create_task
from contextlib import AsyncExitStack
from importlib import metadata
from pickle import UnpicklingError
from queue import Queue
from threading import Event, Thread
from time import time
from typing import Callable, Optional, Union
from uuid import uuid4

import aiohttp
import cloudpickle
from aiohttp import ClientError, ClientOSError, ClientTimeout
from google.cloud.firestore import ArrayUnion, FieldFilter
from google.cloud.firestore_v1.async_client import AsyncClient
from six import reraise
from tblib import Traceback
from yaspin import Spinner, yaspin

from burla import CONFIG_PATH, __version__
from burla._auth import get_auth_headers
from burla._background_stuff import send_alive_pings, upload_inputs
from burla._helpers import (
    get_db_clients,
    get_modules_required_on_remote,
    install_signal_handlers,
    log_telemetry,
    log_telemetry_async,
    parallelism_capacity,
    restore_signal_handlers,
    run_in_subprocess,
)

# load on import and reuse because this is very slow in big envs
PKG_MODULE_MAPPING = metadata.packages_distributions()

LOGIN_TIMEOUT_SEC = 3
BANNED_PACKAGES = ["ipython", "burla", "google-colab"]

# This is here to remind myself why I SHOULDN'T do it (at least for now):
# If I warm up the connections on import like below, then RPM calls that are right next to each
# other, cause GRPC issues. This is possible to fix but not a priority right now.
# try:
#     SYNC_DB, ASYNC_DB = get_db_clients()
# except:
#     SYNC_DB, ASYNC_DB = None, None


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


class JobCanceled(Exception):
    pass


class VersionMismatch(Exception):
    pass


class FunctionTooBig(Exception):
    pass


class UnPickleableUserFunctionException(Exception):
    pass


class InternalClusterError(Exception):
    pass


async def _num_booting_nodes(db: AsyncClient):
    filter_ = FieldFilter("status", "==", "BOOTING")
    nodes_snapshot = await db.collection("nodes").where(filter=filter_).get()
    return len(nodes_snapshot)


async def _num_running_nodes(db: AsyncClient):
    filter_ = FieldFilter("status", "==", "RUNNING")
    nodes_snapshot = await db.collection("nodes").where(filter=filter_).get()
    return len(nodes_snapshot)


async def _wait_for_nodes_to_be_ready(db: AsyncClient, spinner: Union[bool, Spinner]):
    n_booting_nodes = await _num_booting_nodes(db)
    n_running_nodes = await _num_running_nodes(db)

    if n_running_nodes != 0:
        start_time = time()
        time_waiting = 0
        while n_running_nodes != 0:
            if spinner:
                msg = f"Waiting for {n_running_nodes} running nodes to become ready..."
                spinner.text = msg + f" (timeout in {4-time_waiting:.1f}s)"
            await asyncio.sleep(0.01)
            n_running_nodes = await _num_running_nodes(db)
            ready_nodes = await _get_ready_nodes(db)
            time_waiting = time() - start_time
            if time_waiting > 4:
                raise AllNodesBusy("All nodes are busy, please try again later.")

    elif n_booting_nodes != 0:
        ready_nodes = await _get_ready_nodes(db)
        while n_booting_nodes != 0:
            if spinner:
                msg = f"{len(ready_nodes)} Nodes are ready, waiting for remaining {n_booting_nodes}"
                spinner.text = msg + " to boot before starting ..."
            await asyncio.sleep(0.1)
            n_booting_nodes = await _num_booting_nodes(db)
            ready_nodes = await _get_ready_nodes(db)
        if not ready_nodes:
            main_service_url = json.loads(CONFIG_PATH.read_text())["cluster_dashboard_url"]
            msg = "\n\nZero nodes are ready after Booting. Did they fail to boot?\n"
            msg += f"Check your clsuter dashboard at: {main_service_url}\n\n"
            raise NoNodes(msg)

    ready_nodes = await _get_ready_nodes(db)
    if n_booting_nodes == 0 and n_running_nodes == 0 and len(ready_nodes) == 0:
        main_service_url = json.loads(CONFIG_PATH.read_text())["cluster_dashboard_url"]
        msg = "\n\nZero nodes are ready. Is your cluster turned on?\n"
        msg += f'Go to {main_service_url} and hit "⏻ Start" to turn it on!\n\n'
        raise NoNodes(msg)
    return ready_nodes


async def _get_ready_nodes(db: AsyncClient):
    status_filter = FieldFilter("status", "==", "READY")
    ready_nodes_coroutine = db.collection("nodes").where(filter=status_filter).get()
    try:
        docs = await asyncio.wait_for(ready_nodes_coroutine, timeout=LOGIN_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        msg = "\nTimeout waiting for DB.\nPlease run `burla login` and try again.\n"
        raise FirestoreTimeout(msg)
    return [d.to_dict() for d in docs]


async def _select_nodes_to_assign_to_job(
    db: AsyncClient,
    max_parallelism: int,
    func_cpu: int,
    func_ram: int,
    spinner: Union[bool, Spinner],
):
    ready_nodes = await _get_ready_nodes(db)
    if not ready_nodes:
        ready_nodes = await _wait_for_nodes_to_be_ready(db, spinner)

    # it's really important to NOT ignore this check if you are in local dev
    # it should not be necessary to ignore this in local/remote dev and you shouldn't ignore it
    # because it's easy to accidentially start nodes that are on a prod version when you
    # are in dev mode and think they are on your dev version.
    main_svc_version = ready_nodes[0]["main_svc_version"]
    if main_svc_version != __version__:
        msg = "\n\nIncompatible cluster and client versions!\n"
        msg += f"Your cluster is on v{main_svc_version}, but your client is on v{__version__}\n"
        msg += f"To use Burla now please run the command: "
        msg += f"`pip install burla=={main_svc_version}`"
        raise VersionMismatch(msg + "\n")

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
    packages: list,
    func_cpu: int,
    func_ram: int,
    max_parallelism: int,
    background: bool,
    spinner: Union[bool, Spinner],
    job_canceled_event: Event,
    inputs_done_event: Event,
    start_time: float,
    project_id: str,
    generator: bool,
    user_function_error: Event,
):
    if background and spinner:
        msg = f"Running {len(inputs)} inputs through `{function_.__name__}` "
        msg += "with detach mode enabled!\n"
        msg += "This job will continue running on the cluster if canceled locally, "
        msg += "and inputs have finished uploading.\n-"
        spinner.write(msg)

    auth_headers = get_auth_headers()
    SYNC_DB, ASYNC_DB = get_db_clients()

    spinner_compatible_print = lambda msg: spinner.write(msg) if spinner else print(msg)
    function_pkl = cloudpickle.dumps(function_)

    function_size_gb = len(function_pkl) / (1024**3)
    if function_size_gb > 0.1:
        msg = f"\n\nYour function `{function_.__name__}` is referencing some large objects!\n"
        msg += "Functions submitted to Burla, including objects they reference that are defined elsewhere, must be less than 0.1GB.\n"
        msg += "Does your function reference any big numpy arrays, dataframes, or other objects defined elsewhere?\n"
        msg += "Please pass these as inputs to your function, or download them from the internet once inside the function.\n"
        msg += "We apologize for this temporary limitation! If this is confusing or blocking you, please tell us! (jake@burla.dev)\n\n"
        raise FunctionTooBig(msg)

    nodes_to_assign, total_target_parallelism = await _select_nodes_to_assign_to_job(
        ASYNC_DB, max_parallelism, func_cpu, func_ram, spinner
    )

    job_ref = ASYNC_DB.collection("jobs").document(job_id)
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
            "function_size_gb": function_size_gb,
            "started_at": start_time,
            "is_background_job": background,
            "client_has_all_results": False,
            "fail_reason": [],
        }
    )

    async def assign_node(node: dict, session: aiohttp.ClientSession):
        request_json = {
            "parallelism": node["target_parallelism"],
            "is_background_job": background,
            "user_python_version": f"3.{sys.version_info.minor}",
            "n_inputs": len(inputs),
            "packages": packages,
            "start_time": start_time,
        }
        data = aiohttp.FormData()
        data.add_field("request_json", json.dumps(request_json))
        data.add_field("function_pkl", function_pkl)
        url = f"{node['host']}/jobs/{job_id}"
        timeout = ClientTimeout(total=300)
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
                node_doc = ASYNC_DB.collection("nodes").document(node["instance_name"])
                await node_doc.update({"status": "FAILED", "display_in_dashboard": True})
                msg = f"Failed! This node didn't respond (in<300s) to client request to assign job."
                await node_doc.collection("logs").document().set({"msg": msg, "ts": time()})
                # delete node
                main_service_url = json.loads(CONFIG_PATH.read_text())["cluster_dashboard_url"]
                url = f"{main_service_url}/v1/cluster/{node['instance_name']}"
                url += "?hide_if_failed=false"
                async with session.delete(url, headers=auth_headers, timeout=1) as response:
                    if response.status != 200:
                        msg = f"Failed to delete node {node['instance_name']}."
                        spinner_compatible_print(msg + f" ignoring: {response.status}")
            except:
                pass

    async with AsyncExitStack() as stack:
        connector = aiohttp.TCPConnector(
            limit=500,
            limit_per_host=100,
            keepalive_timeout=60,
            enable_cleanup_closed=True,
            use_dns_cache=True,
        )
        client_session = aiohttp.ClientSession(connector=connector, trust_env=True)
        session = await stack.enter_async_context(client_session)

        function_size_str = f" ({function_size_gb:.3f}GB)" if function_size_gb > 0.001 else ""
        msg = f"Calling function `{function_.__name__}`{function_size_str} on {len(inputs)} "
        msg += f"inputs with {len(nodes_to_assign)} {nodes_to_assign[0]['machine_type']} nodes and "
        msg += f"{func_cpu}vCPUs/{func_ram}GB RAM per function.\n"
        msg += f"background={background}, generator={generator}, spinner={bool(spinner)}, "
        msg += f"max_parallelism={max_parallelism}, job_id={job_id}"
        asyncio.create_task(log_telemetry_async(msg, session, project_id=project_id))

        JOB_CANCELED_MSG = ""
        FIRST_LOG_MESSAGE_PRINTED = False
        # start sending "alive" pings to nodes
        ping_process = await run_in_subprocess(send_alive_pings, job_id)
        stack.callback(ping_process.kill)

        # start stdout/stderr stream
        def _on_new_logs_doc(col_snapshot, changes, read_time):
            nonlocal JOB_CANCELED_MSG
            nonlocal FIRST_LOG_MESSAGE_PRINTED
            for change in changes:
                for log in change.document.to_dict()["logs"]:
                    # ignore tb's written as log messages because errors are reraised here
                    if log.get("is_error"):
                        job = SYNC_DB.collection("jobs").document(job_id).get().to_dict()
                        if job["status"] == "CANCELED":
                            JOB_CANCELED_MSG = log["message"]
                    else:
                        msg = log["message"]
                        if msg.endswith("\r\n"):
                            msg = msg[:-2]
                        elif msg.endswith("\n"):
                            msg = msg[:-1]
                        spinner_compatible_print(msg)
                        FIRST_LOG_MESSAGE_PRINTED = True

        logs_collection = SYNC_DB.collection("jobs").document(job_id).collection("logs")
        log_stream = logs_collection.on_snapshot(_on_new_logs_doc)
        stack.callback(log_stream.unsubscribe)

        if spinner:
            function_size_mb = len(function_pkl) / 1024**2
            total_data_gb = function_size_gb * len(nodes_to_assign)
            msg = f"Uploading function `{function_.__name__}` to {len(nodes_to_assign)} nodes ..."
            if total_data_gb > 0.01:
                msg = f"Uploading function `{function_.__name__}` ({(function_size_mb):.2f}MB) "
                msg += f"to {len(nodes_to_assign)} nodes ({total_data_gb:.2f}GB) ..."
            spinner.text = msg

        # send function to every node
        assign_node_tasks = [assign_node(node, session) for node in nodes_to_assign]
        nodes = [node for node in await asyncio.gather(*assign_node_tasks) if node]
        if not nodes:
            raise Exception("Job refused by all available Nodes!")

        # start uploading inputs
        upload_inputs_args = (job_id, nodes, inputs, session, auth_headers, job_canceled_event)
        uploader_task = create_task(upload_inputs(*upload_inputs_args))

        async def _get_with_retries(url: str, headers: dict, max_retries=5):
            try:
                return await session.get(url, headers=headers, timeout=ClientTimeout(total=30))
            except (ClientOSError, ClientError, OSError) as e:
                if max_retries <= 1 or "Protocol wrong type for socket" not in str(e):
                    raise
                await asyncio.sleep(1)
                return await _get_with_retries(url, headers, max_retries=max_retries - 1)

        async def _check_single_node(node: dict):
            url = f"{node['host']}/jobs/{job_id}/results"

            async with await _get_with_retries(url, auth_headers) as response:
                if response.status == 404:
                    nodes.remove(node)  # <- means node is likely rebooting and failed or is done
                    return None
                if response.status != 200:
                    raise Exception(f"Result-check failed for node: {node['instance_name']}")

                try:
                    node_status = pickle.loads(await response.content.read())
                except UnpicklingError as e:
                    if "Memo value not found at index" not in str(e):
                        raise e

                    job_doc = await job_ref.get()
                    if job_doc.to_dict()["status"] == "CANCELED":
                        raise JobCanceled("Job canceled from dashboard.")
                    else:
                        msg = f"Node {node['instance_name']} disconnected while transmitting results.\n"
                        raise NodeDisconnected(msg)

                return_values = []
                for input_index, is_error, result_pkl in node_status["results"]:

                    if not is_error:
                        return_values.append(cloudpickle.loads(result_pkl))
                        continue

                    exc_info = pickle.loads(result_pkl)
                    if exc_info.get("traceback_dict"):
                        traceback = Traceback.from_dict(exc_info["traceback_dict"]).as_traceback()
                        user_function_error.set()
                        msg = f"Job {job_id} failed due to user function error."
                        await log_telemetry_async(msg, session, project_id=project_id)
                        reraise(tp=exc_info["type"], value=exc_info["exception"], tb=traceback)

                    msg = f"\nThis exception had to be sent to your machine as a string:\n\n"
                    msg += f"{exc_info['traceback_str']}\n"
                    raise UnPickleableUserFunctionException(msg)

                status = {
                    "udf_start_latency": node_status.get("udf_start_latency"),
                    "packages_to_install": node_status.get("packages_to_install"),
                    "all_packages_installed": node_status.get("all_packages_installed"),
                    "is_empty": node_status["is_empty"],
                    "current_parallelism": node_status["current_parallelism"],
                    "currently_installing_package": node_status["currently_installing_package"],
                    "return_values": return_values,
                }
                return status

        n_results = 0
        result_loop_start = time()
        all_nodes_empty = False
        udf_start_latency = None
        packages_to_install = None
        all_packages_installed = False
        inputs_done_msg_printed = False
        while n_results < len(inputs):

            if job_canceled_event.is_set():
                # if this is set a nice user message was already printed.
                return

            if all_nodes_empty:
                elapsed_time = time() - result_loop_start
                if elapsed_time > 3:
                    await asyncio.sleep(0.3)
                else:
                    await asyncio.sleep(0)

            if job_canceled_event.is_set():
                # if this is set a nice user message was already printed.
                return
            if JOB_CANCELED_MSG:
                raise JobCanceled(f"\n\n{JOB_CANCELED_MSG}\n")

            total_parallelism = 0
            all_nodes_empty = True
            nodes_status = await asyncio.gather(*[_check_single_node(n) for n in nodes])
            nodes_status = [status for status in nodes_status if status is not None]
            if not nodes_status:
                msg = "\nZero nodes working on job and we have not received all results!\n"
                msg += "This usually means a worker or node crashed, then restarted itself. \n"
                msg += "See node logs in the dashboard for details.\n"
                raise InternalClusterError(msg)

            currently_installing_package = nodes_status[0]["currently_installing_package"]
            if spinner and currently_installing_package:
                spinner.text = f"Installing package: {currently_installing_package} ..."

            if job_canceled_event.is_set():
                # if this is set a nice user message was already printed.
                return

            for status in nodes_status:

                if status.get("udf_start_latency"):
                    udf_start_latency = status["udf_start_latency"]
                if status.get("packages_to_install"):
                    packages_to_install = status["packages_to_install"]
                if status.get("all_packages_installed"):
                    all_packages_installed = status["all_packages_installed"]

                total_parallelism += status["current_parallelism"]
                all_nodes_empty = all_nodes_empty and status["is_empty"]
                for return_value in status["return_values"]:
                    return_queue.put_nowait(return_value)
                    n_results += 1

            if uploader_task.done():
                inputs_done_event.set()

            if uploader_task.done() and uploader_task.exception():
                raise uploader_task.exception()
            elif uploader_task.done() and spinner and background and not inputs_done_msg_printed:
                msg = ""
                if FIRST_LOG_MESSAGE_PRINTED:
                    msg += "-\n"
                msg += "Done uploading inputs! "
                msg += "Job will now continue running if canceled locally.\n-"
                spinner.write(msg)
                inputs_done_msg_printed = True

            exit_code = ping_process.poll()
            if exit_code:
                stderr = ping_process.stderr.read().decode("utf-8")
                raise Exception(f"Ping process exited with code: {exit_code}\n{stderr}")

            if spinner and all_packages_installed:
                spinner.text = (
                    f"Calling `{function_.__name__}`: {n_results}/{len(inputs)} completed, "
                    f"{total_parallelism} running."
                )

            if len(nodes) == 0 and return_queue.empty():  # nodes removed in _check_single_node
                raise Exception("Zero nodes working on job and we have not received all results!")

        total_runtime = time() - start_time
        udf_start_latency = round(udf_start_latency, 2) if udf_start_latency else None
        msg = f"Job {job_id} completed successfully, udf_start_latency={udf_start_latency}s"
        msg += f", total_runtime={total_runtime:.2f}s."
        if packages_to_install:
            msg += f"\nInstalled packages: {packages_to_install}"
        asyncio.create_task(log_telemetry_async(msg, session, project_id=project_id))
        await job_ref.update({"client_has_all_results": True})


def remote_parallel_map(
    function_: Callable,
    inputs: list,
    func_cpu: int = 1,
    func_ram: int = 4,
    detach: bool = False,
    generator: bool = False,
    spinner: bool = True,
    max_parallelism: Optional[int] = None,
):
    """
    Run a Python function on many remote computers in parallel.

    Run provided function_ on each item in inputs at the same time, each on a separate CPU.
    If more than inputs than there are cpu's are provided, inputs are queued and
    processed sequentially on each worker. Any exception raised by `function_`
    (including its stack trace) will be re-raised here on the client machine.

    Args:
        function_ (Callable):
            A Python function that accepts a single input argument. For example, calling
            `function_(inputs[0])` should not raise an exception.
        inputs (List[Any]):
            An iterable of objects that will be passed to `function_`.
            If the iterable contains tuples, they will be unpacked!
            Example: `inputs=[(1, 2)]` -> `function_(1, 2)`
        func_cpu (int, optional):
            The number of CPUs allocated for each instance of `function_`. Defaults to 1.
        func_ram (int, optional):
            The amount of RAM (in GB) allocated for each instance of `function_`. Defaults to 4.
        detach (bool, optional):
            If True, job will continue running on cluster, when canceled locally.
            Defaults to False.
        generator (bool, optional):
            If True, returns a generator that yields outputs as they are produced; otherwise,
            returns a list of outputs once all have been processed. Defaults to False.
        spinner (bool, optional):
            If set to False, disables the display of the status indicator/spinner. Defaults to True.
        max_parallelism (int, optional):
            The maximum number of `function_` instances allowed to be running at the same time.
            Defaults to the number of provided inputs.

    Returns:
        List[Any] or Generator[Any, None, None]:
            A list containing the objects returned by `function_` in no particular order.
            If `generator=True`, returns a generator that yields results as they are produced.

    See Also:
        For more info see our overview: https://docs.burla.dev/overview
        or API-Reference: https://docs.burla.dev/api-reference
    """
    start_time = time()
    user_function_error = Event()

    inputs = [(i,) if not isinstance(i, tuple) else i for i in inputs]
    if not inputs:
        return iter([]) if generator else []

    # TODO: rename internally
    background = detach

    # ------------------------------------------------
    # TODO: implement internally instead of wrapping:
    def wrapped_function_(args_tuple):
        return function_(*args_tuple)

    wrapped_function_.__name__ = function_.__name__

    # Move below code back into `_execute_job` after above todo is done.
    # Needs to operate on function_.__globals__ which cannot be reassigned -> must be done here.
    custom_module_names, package_module_names = get_modules_required_on_remote(function_)
    for module_name in custom_module_names:
        cloudpickle.register_pickle_by_value(sys.modules[module_name])
    packages = {}
    for module_name in package_module_names:
        # some of these are unnecessary since we get all that map to the base module
        # example google.cloud.storage -> google -> every installed google package
        # for now we just install more packages than we need to, it's fast enough
        if not PKG_MODULE_MAPPING.get(module_name):
            continue
        for package_name in PKG_MODULE_MAPPING.get(module_name):
            packages[package_name] = metadata.version(package_name)

    # unnecessary / already installed / will break stuff
    for package in BANNED_PACKAGES:
        packages.pop(package, None)

    # not an official dep
    if packages.get("SQLAlchemy") and "psycopg2-binary" in PKG_MODULE_MAPPING.get("psycopg2", []):
        packages["psycopg2-binary"] = metadata.version("psycopg2-binary")
    # ------------------------------------------------

    max_parallelism = max_parallelism if max_parallelism else len(inputs)
    job_id = str(uuid4())
    project_id = json.loads(CONFIG_PATH.read_text())["project_id"]

    return_queue = Queue()
    original_signal_handlers = None
    try:
        if spinner:
            spinner = yaspin(sigmap={})  # <- .start will overwrite my handlers without sigmap={}
            spinner.start()
            spinner.text = f"Preparing to call `{function_.__name__}` on {len(inputs)} inputs ..."
        job_canceled_event = Event()
        inputs_done_event = Event()
        original_signal_handlers = install_signal_handlers(
            job_id, background, spinner, job_canceled_event, inputs_done_event
        )

        def execute_job():
            try:
                asyncio.run(
                    _execute_job(
                        job_id=job_id,
                        return_queue=return_queue,
                        function_=wrapped_function_,
                        inputs=inputs,
                        packages=packages,
                        func_cpu=func_cpu,
                        func_ram=func_ram,
                        max_parallelism=max_parallelism,
                        background=background,
                        spinner=spinner,
                        job_canceled_event=job_canceled_event,
                        inputs_done_event=inputs_done_event,
                        start_time=start_time,
                        project_id=project_id,
                        generator=generator,
                        user_function_error=user_function_error,
                    )
                )
            except Exception:
                execute_job.exc_info = sys.exc_info()

        t = Thread(target=execute_job, daemon=True)
        t.start()
        t.join()

        if hasattr(execute_job, "exc_info"):
            raise execute_job.exc_info[1].with_traceback(execute_job.exc_info[2])

        if job_canceled_event.is_set() and background and inputs_done_event.is_set():
            return
        elif job_canceled_event.is_set() and background and not inputs_done_event.is_set():
            msg = "\n\nBackground job canceled before all inputs finished uploading to the cluster!"
            msg += '\nPlease wait until the message "Done uploading inputs!" '
            msg += "appears before canceling.\n\n-"
            raise JobCanceled(msg)
        elif job_canceled_event.is_set():
            raise JobCanceled("Job canceled by user.")

        def _output_generator():
            n_results = 0
            while n_results != len(inputs):
                yield return_queue.get()
                n_results += 1

        if spinner:
            msg = f"Done calling `{function_.__name__}`! "
            msg += f"{len(inputs)}/{len(inputs)} completed."
            spinner.text = msg
            spinner.ok("✔")

        return _output_generator() if generator else list(_output_generator())

    except Exception as e:
        if spinner:
            spinner.stop()

        SYNC_DB, _ = get_db_clients()

        # After a `FirestoreTimeout` further attempts to use firestore will take forever then fail.
        if not (isinstance(e, FirestoreTimeout) or background):
            try:
                job_doc = SYNC_DB.collection("jobs").document(job_id)
                if job_doc.get().to_dict()["status"] != "CANCELED":
                    msg = f"client exception: {e}"
                    job_doc.update({"status": "FAILED", "fail_reason": ArrayUnion([msg])})
            except Exception:
                pass

        # Report errors back to Burla's cloud.
        if not user_function_error.is_set():
            exec_types_to_chill = [NoNodes, AllNodesBusy, NoCompatibleNodes, JobCanceled]
            exec_types_to_chill.extend([VersionMismatch, FunctionTooBig, FirestoreTimeout])
            chill_exception = any([isinstance(e, e_type) for e_type in exec_types_to_chill])

            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
            traceback_str = "".join(tb_details)
            kwargs = dict(traceback=traceback_str, project_id=project_id, job_id=job_id)

            try:
                if chill_exception:
                    msg = f"Job {job_id} failed with: {str(e)}"
                    log_telemetry(msg, severity="INFO", **kwargs)
                else:
                    msg = f"Job {job_id} FAILED due to NON-UDF-ERROR:\n```{traceback_str}```"
                    log_telemetry(msg, severity="ERROR", **kwargs)
            except:
                pass

        raise
    finally:
        if original_signal_handlers:
            restore_signal_handlers(original_signal_handlers)
