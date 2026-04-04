import asyncio
import json
import sys
import traceback
import base64
from asyncio import create_task
from contextlib import AsyncExitStack
from importlib import metadata
from queue import Queue
from threading import Event, Thread
from time import time
from typing import Callable, Optional, Union
from uuid import uuid4

import aiohttp
import cloudpickle
from google.cloud.firestore import ArrayUnion
from yaspin import Spinner, yaspin

from burla import CONFIG_PATH, __version__
from burla._auth import get_auth_headers
from burla._background_stuff import send_alive_pings, upload_inputs
from burla._helpers import (
    get_db_clients,
    get_modules_required_on_remote,
    install_signal_handlers,
    restore_signal_handlers,
    run_in_subprocess,
)
from burla._node import (
    AllNodesBusy,
    FirestoreTimeout,
    JobCanceled,
    Node,
    NoCompatibleNodes,
    NoNodes,
    UnauthorizedError,
    VersionMismatch,
    select_nodes_to_assign_to_job,
)
from burla._reporting import RemoteParallelMapReporter

# load on import and reuse because this is very slow in big envs
PKG_MODULE_MAPPING = metadata.packages_distributions()

BANNED_PACKAGES = ["ipython", "burla", "google-colab"]

# This is here to remind myself why I SHOULDN'T do it (at least for now):
# If I warm up the connections on import like below, then RPM calls that are right next to each
# other, cause GRPC issues. This is possible to fix but not a priority right now.
# try:
#     SYNC_DB, ASYNC_DB = get_db_clients()
# except:
#     SYNC_DB, ASYNC_DB = None, None


class FunctionTooBig(Exception):
    def __init__(self, function_name: str):
        msg = f"\n\nYour function `{function_name}` is referencing some large objects!\n"
        msg += "Functions submitted to Burla, including objects they reference that are defined elsewhere, must be less than 0.1GB.\n"
        msg += "Does your function reference any big numpy arrays, dataframes, or other objects defined elsewhere?\n"
        msg += "Please pass these as inputs to your function, or download them from the internet once inside the function.\n"
        msg += "We apologize for this temporary limitation! If this is confusing or blocking you, please tell us! (jake@burla.dev)\n\n"
        super().__init__(msg)


class InternalClusterError(Exception):
    def __init__(self):
        msg = "\nZero nodes working on job and we have not received all results!\n"
        msg += "This usually means a worker or node crashed, then restarted itself. \n"
        msg += "See node logs in the dashboard for details.\n"
        super().__init__(msg)


async def _grow_cluster(auth_headers: dict, current_cpus: int, missing_cpus: int) -> dict:
    request_json = {"current_cpus": current_cpus, "missing_cpus": missing_cpus}
    main_service_url = json.loads(CONFIG_PATH.read_text())["cluster_dashboard_url"]
    url = f"{main_service_url}/v1/cluster/grow"
    async with aiohttp.ClientSession(trust_env=True) as session:
        request = session.post(url, json=request_json, headers=auth_headers)
        async with await request as response:
            if response.status == 200:
                response_json = await response.json()
                return response_json.get("added_node_instance_names", [])
            elif response.status == 401:
                raise UnauthorizedError()
            else:
                raise Exception(f"Failed to grow cluster: {response.status}")


async def _execute_job_wrapped(*args, **kwargs):
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
        reporter = RemoteParallelMapReporter(**kwargs, session=session)
        await _execute_job(*args, **kwargs, session=session, session_stack=stack, reporter=reporter)


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
    user_function_error: Event,
    grow: bool,
    session: aiohttp.ClientSession,
    session_stack: AsyncExitStack,
    reporter: RemoteParallelMapReporter,
):
    JOB_CANCELED_MSG = None
    auth_headers = get_auth_headers()
    sync_db, async_db = get_db_clients()

    if background:
        reporter.print_detach_mode_enabled_message()

    function_pkl = cloudpickle.dumps(function_)
    function_size_gb = len(function_pkl) / (1024**3)
    reporter.function_size_gb = function_size_gb
    if function_size_gb > 0.1:
        raise FunctionTooBig(function_.__name__)

    try:
        nodes_to_assign, target_parallelism = await select_nodes_to_assign_to_job(
            db=async_db,
            max_parallelism=max_parallelism,
            func_cpu=func_cpu,
            func_ram=func_ram,
            spinner=spinner,
            session=session,
            auth_headers=auth_headers,
        )
    except (NoNodes, NoCompatibleNodes, AllNodesBusy):
        nodes_to_assign, target_parallelism = [], 0
        if not grow:
            raise

    sync_job_ref = sync_db.collection("jobs").document(job_id)
    async_job_ref = async_db.collection("jobs").document(job_id)
    await async_job_ref.set(
        {
            "n_inputs": len(inputs),
            "func_cpu": func_cpu,
            "func_ram": func_ram,
            "status": "RUNNING",
            "burla_client_version": __version__,
            "user_python_version": f"3.{sys.version_info.minor}",
            "target_parallelism": target_parallelism,  # <- live: n-nodes * target_parallelism/node
            "user": auth_headers["X-User-Email"],
            "function_name": function_.__name__,
            "function_size_gb": function_size_gb,
            "started_at": start_time,
            "is_background_job": background,
            "client_has_all_results": False,
            "fail_reason": [],
        }
    )

    if grow:
        # assuming static 1:4 cpu/ram ratio, how many more cpus do we need?
        requested_parallelism = min(len(inputs), max_parallelism)
        required_cpus_for_ram = (func_ram + 3) // 4
        required_cpus_per_function_call = max(func_cpu, required_cpus_for_ram)
        target_cpus = requested_parallelism * required_cpus_per_function_call
        current_cpus = target_parallelism * required_cpus_per_function_call
        missing_cpus = max(0, target_cpus - current_cpus)
        if missing_cpus > 0:
            booting_node_instance_names = await _grow_cluster(
                auth_headers=auth_headers,
                current_cpus=current_cpus,  # <- need to pass so we dont cross global max limit
                missing_cpus=missing_cpus,
            )
            if len(booting_node_instance_names) > 0:
                reporter.set_booting_nodes_message(len(booting_node_instance_names))
            if len(nodes_to_assign) == 0 and len(booting_node_instance_names) == 0:
                raise NoNodes("Cluster refused to boot required additional nodes ...")

    # wait until at least one boots to start job.
    if len(nodes_to_assign) == 0:
        start_wait = time()
        while True:
            try:
                nodes_to_assign, target_parallelism = await select_nodes_to_assign_to_job(
                    db=async_db,
                    max_parallelism=max_parallelism,
                    func_cpu=func_cpu,
                    func_ram=func_ram,
                    spinner=spinner,
                    session=session,
                    auth_headers=auth_headers,
                )
                break
            except NoNodes:
                if time() - start_wait > 120:
                    raise
                await asyncio.sleep(1)

    await reporter.log_job_start_telemetry(len(nodes_to_assign), nodes_to_assign[0].machine_type)

    # start stdout/stderr stream
    def _on_new_logs_doc(col_snapshot, changes, read_time):
        nonlocal JOB_CANCELED_MSG
        for log in [log for c in changes for log in c.document.to_dict()["logs"]]:
            if log.get("is_error") and sync_job_ref.get().to_dict()["status"] == "CANCELED":
                JOB_CANCELED_MSG = log["message"]
            else:
                message = log["message"].rstrip("\r\n")
                if spinner:
                    spinner.write(message)
                else:
                    print(message)

    log_stream = sync_job_ref.collection("logs").on_snapshot(_on_new_logs_doc)
    session_stack.callback(log_stream.unsubscribe)

    reporter.set_uploading_function_message(len(nodes_to_assign))

    # assign initial nodes
    nodes = []
    node_last_reply_timestamp = {}
    assigned_nodes = await asyncio.gather(
        *[
            node.assign(
                job_id=job_id,
                background=background,
                n_inputs=len(inputs),
                packages=packages,
                start_time=start_time,
                function_pkl=function_pkl,
            )
            for node in nodes_to_assign
        ]
    )
    nodes.extend([node for node in assigned_nodes if node])
    for node in nodes:
        node_last_reply_timestamp[node.instance_name] = time()
    if len(nodes) == 0:
        raise Exception("Job refused by all available Nodes!")

    # start sending "alive" pings to initial nodes
    ping_process = await run_in_subprocess(send_alive_pings, nodes, auth_headers)
    session_stack.callback(ping_process.kill)

    # start uploading inputs
    upload_nodes = nodes
    upload_inputs_args = (job_id, upload_nodes, inputs, session, auth_headers, job_canceled_event)
    uploader_task = create_task(upload_inputs(*upload_inputs_args))

    n_results = 0
    result_loop_start = time()
    all_nodes_empty = False
    udf_start_latency = None
    packages_to_install = None
    all_packages_installed = False
    inputs_done_msg_printed = False
    while n_results < len(inputs):

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
        nodes_status = await asyncio.gather(
            *[
                node.get_results(
                    job_id=job_id,
                    nodes=nodes,
                    node_last_reply_timestamp=node_last_reply_timestamp,
                    job_ref=async_job_ref,
                    user_function_error_event=user_function_error,
                    project_id=project_id,
                )
                for node in nodes
            ]
        )
        nodes_status = [status for status in nodes_status if status is not None]
        if not nodes_status:
            raise InternalClusterError()

        currently_installing_package = nodes_status[0]["currently_installing_package"]
        if currently_installing_package:
            reporter.set_installing_package_message(currently_installing_package)

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
        elif uploader_task.done() and background and not inputs_done_msg_printed:
            reporter.print_inputs_done_message()
            inputs_done_msg_printed = True

        exit_code = ping_process.poll()
        if exit_code:
            stderr = ping_process.stderr.read().decode("utf-8")
            raise Exception(f"Ping process exited with code: {exit_code}\n{stderr}")

        if all_packages_installed:
            # (len(inputs) - n_results) < total_parallelism is possible happen due to lag
            # it's overwritten here because it's confusing to users.
            reporter.set_running_progress_message(n_results, total_parallelism)

        if len(nodes) == 0 and return_queue.empty():  # nodes removed in Node.get_results
            raise Exception("Zero nodes working on job and we have not received all results!")

    total_runtime = time() - start_time
    udf_start_latency = round(udf_start_latency, 2) if udf_start_latency else None
    await reporter.log_job_success_telemetry(udf_start_latency, total_runtime, packages_to_install)
    await async_job_ref.update({"client_has_all_results": True})


def remote_parallel_map(
    function_: Callable,
    inputs: list,
    func_cpu: int = 1,
    func_ram: int = 4,
    detach: bool = False,
    generator: bool = False,
    spinner: bool = True,
    max_parallelism: Optional[int] = None,
    grow: bool = False,
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
        grow (bool, optional):
            If True, request cluster growth before assignment so enough compute is available
            to process the requested parallelism quickly. Defaults to False.

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

    # temp fix: these are mistetected as PyPI packages but are not!
    if "clim_shift" in package_module_names:
        package_module_names.remove("clim_shift")
        custom_module_names.add("clim_shift")
    if "climate_analysis_toolkit" in package_module_names:
        package_module_names.remove("climate_analysis_toolkit")
        custom_module_names.add("climate_analysis_toolkit")
    # temp fix: these are not listed as dependencies but are often required!
    if "xarray" in package_module_names:
        package_module_names.update(["netcdf4", "h5netcdf", "h5py"])

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

    # manually check for extras until we can support automatic extra detection.
    if packages.get("geopandas"):
        if "geoalchemy2" in PKG_MODULE_MAPPING and not ("geoalchemy2" in packages):
            packages["geoalchemy2"] = metadata.version("geoalchemy2")
        if "geopy" in PKG_MODULE_MAPPING and not ("geopy" in packages):
            packages["geopy"] = metadata.version("geopy")
        if "matplotlib" in PKG_MODULE_MAPPING and not ("matplotlib" in packages):
            packages["matplotlib"] = metadata.version("matplotlib")
        if "mapclassify" in PKG_MODULE_MAPPING and not ("mapclassify" in packages):
            packages["mapclassify"] = metadata.version("mapclassify")
        if "xyzservices" in PKG_MODULE_MAPPING and not ("xyzservices" in packages):
            packages["xyzservices"] = metadata.version("xyzservices")
        if "folium" in PKG_MODULE_MAPPING and not ("folium" in packages):
            packages["folium"] = metadata.version("folium")
        if "pointpats" in PKG_MODULE_MAPPING and not ("pointpats" in packages):
            packages["pointpats"] = metadata.version("pointpats")
        if "scipy" in PKG_MODULE_MAPPING and not ("scipy" in packages):
            packages["scipy"] = metadata.version("scipy")
        if "pyarrow" in PKG_MODULE_MAPPING and not ("pyarrow" in packages):
            packages["pyarrow"] = metadata.version("pyarrow")
        if "SQLAlchemy" in PKG_MODULE_MAPPING and not ("SQLAlchemy" in packages):
            packages["SQLAlchemy"] = metadata.version("SQLAlchemy")

    if packages.get("mapclassify"):
        if "libpysal" in PKG_MODULE_MAPPING and not ("libpysal" in packages):
            packages["libpysal"] = metadata.version("libpysal")
        if "shapely" in PKG_MODULE_MAPPING and not ("shapely" in packages):
            packages["shapely"] = metadata.version("shapely")
        if "matplotlib" in PKG_MODULE_MAPPING and not ("matplotlib" in packages):
            packages["matplotlib"] = metadata.version("matplotlib")
    # ------------------------------------------------

    max_parallelism = max_parallelism if max_parallelism else len(inputs)
    uid = base64.urlsafe_b64encode(uuid4().bytes[:9]).decode()
    job_id = f"{function_.__name__}-{uid}"
    project_id = json.loads(CONFIG_PATH.read_text())["project_id"]

    return_queue = Queue()
    original_signal_handlers = None
    local_reporter = None
    try:
        if spinner:
            spinner = yaspin(sigmap={})  # <- .start will overwrite my handlers without sigmap={}
            spinner.start()
        local_reporter = RemoteParallelMapReporter(
            job_id=job_id,
            return_queue=return_queue,
            function_=function_,
            inputs=inputs,
            packages=packages,
            func_cpu=func_cpu,
            func_ram=func_ram,
            max_parallelism=max_parallelism,
            background=background,
            spinner=spinner,
            job_canceled_event=None,
            inputs_done_event=None,
            start_time=start_time,
            project_id=project_id,
            generator=generator,
            user_function_error=user_function_error,
            grow=grow,
            session=None,
        )
        local_reporter.set_preparing_message()
        job_canceled_event = Event()
        inputs_done_event = Event()
        original_signal_handlers = install_signal_handlers(
            job_id, background, spinner, job_canceled_event, inputs_done_event
        )

        def execute_job():
            try:
                asyncio.run(
                    _execute_job_wrapped(
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
                        grow=grow,
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
            raise JobCanceled(local_reporter.get_background_cancel_before_upload_message())
        elif job_canceled_event.is_set():
            raise JobCanceled("Job canceled by user.")

        def _output_generator():
            n_results = 0
            while n_results != len(inputs):
                yield return_queue.get()
                n_results += 1

        local_reporter.finish_spinner_success()

        return _output_generator() if generator else list(_output_generator())

    except Exception as e:
        if spinner:
            spinner.stop()

        SYNC_DB, _ = get_db_clients()

        # After a `FirestoreTimeout` further attempts to use firestore will take forever then fail.
        if not (isinstance(e, FirestoreTimeout) or background):
            try:
                sync_job_ref = SYNC_DB.collection("jobs").document(job_id)
                if sync_job_ref.get().to_dict()["status"] != "CANCELED":
                    msg = f"client exception: {e}"
                    sync_job_ref.update({"status": "FAILED", "fail_reason": ArrayUnion([msg])})
            except Exception:
                pass

        # Report errors back to Burla's cloud.
        if not user_function_error.is_set():
            exec_types_to_chill = [NoNodes, AllNodesBusy, NoCompatibleNodes, JobCanceled]
            exec_types_to_chill.extend(
                [VersionMismatch, FunctionTooBig, FirestoreTimeout, UnauthorizedError]
            )
            chill_exception = any([isinstance(e, e_type) for e_type in exec_types_to_chill])

            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
            traceback_str = "".join(tb_details)

            try:
                if local_reporter is None:
                    local_reporter = RemoteParallelMapReporter(
                        job_id=job_id,
                        return_queue=return_queue,
                        function_=function_,
                        inputs=inputs,
                        packages=packages,
                        func_cpu=func_cpu,
                        func_ram=func_ram,
                        max_parallelism=max_parallelism,
                        background=background,
                        spinner=spinner,
                        job_canceled_event=None,
                        inputs_done_event=None,
                        start_time=start_time,
                        project_id=project_id,
                        generator=generator,
                        user_function_error=user_function_error,
                        grow=grow,
                        session=None,
                    )
                local_reporter.log_job_failure_telemetry(e, traceback_str, chill_exception)
            except:
                pass

        raise
    finally:
        if original_signal_handlers:
            restore_signal_handlers(original_signal_handlers)
