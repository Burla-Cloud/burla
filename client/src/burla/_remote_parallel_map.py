import asyncio
import random
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
from yaspin import Spinner, yaspin

from burla import __version__
from burla._cluster_client import ClusterClient, NodesBusy, _local_host_from
from burla._heartbeat import run_in_subprocess, send_alive_pings
from burla._helpers import (
    get_modules_required_on_remote,
    install_signal_handlers,
    restore_signal_handlers,
)
from burla._node import (
    AllNodesBusy,
    ClusterRestarted,
    ClusterShutdown,
    JobCanceled,
    MainServiceTimeout,
    Node,
    NoCompatibleNodes,
    NoNodes,
    NodeDisconnected,
    UnauthorizedError,
    VersionMismatch,
    wait_for_nodes_to_be_ready,
)
from burla._reporting import RemoteParallelMapReporter, log_job_failure_telemetry

# load on import and reuse because this is very slow in big envs
PKG_MODULE_MAPPING = metadata.packages_distributions()

BANNED_PACKAGES = ["ipython", "burla", "google-colab"]

class FunctionTooBig(Exception):
    def __init__(self, function_name: str):
        msg = f"\n\nYour function `{function_name}` is referencing some large objects!\n"
        msg += "Functions submitted to Burla, including objects they reference that are defined elsewhere, must be less than 0.1GB.\n"
        msg += "Does your function reference any big numpy arrays, dataframes, or other objects defined elsewhere?\n"
        msg += "Please pass these as inputs to your function, or download them from the internet once inside the function.\n"
        msg += "We apologize for this temporary limitation! If this is confusing or blocking you, please tell us! (jake@burla.dev)\n\n"
        super().__init__(msg)


EXEC_TYPES_TO_NOT_ALERT = [
    NoNodes,
    AllNodesBusy,
    NoCompatibleNodes,
    JobCanceled,
    ClusterRestarted,
    ClusterShutdown,
    VersionMismatch,
    FunctionTooBig,
    MainServiceTimeout,
    UnauthorizedError,
    KeyboardInterrupt,
]


async def _execute_job_wrapped(*args, **kwargs):
    async with AsyncExitStack() as stack:
        connector = aiohttp.TCPConnector(
            limit=300,
            limit_per_host=50,
            keepalive_timeout=60,
            enable_cleanup_closed=True,
            use_dns_cache=True,
        )
        client_session = aiohttp.ClientSession(connector=connector, trust_env=True)
        session = await stack.enter_async_context(client_session)
        reporter = RemoteParallelMapReporter(**kwargs, session=session)
        execute_job_kwargs = dict(kwargs)
        execute_job_kwargs.pop("generator", None)
        await _execute_job(
            *args,
            **execute_job_kwargs,
            session=session,
            session_stack=stack,
            reporter=reporter,
        )


async def _execute_job(
    job_id: str,
    return_queue: Queue,
    function_: Callable,
    inputs: list,
    packages: dict,
    func_cpu: int,
    func_ram: int,
    max_parallelism: int,
    background: bool,
    spinner: Union[bool, Spinner],
    terminal_cancel_event: Event,
    inputs_done_event: Event,
    start_time: float,
    udf_error_event: Event,
    grow: bool,
    session: aiohttp.ClientSession,
    session_stack: AsyncExitStack,
    reporter: RemoteParallelMapReporter,
):
    client = ClusterClient(session)

    if background:
        reporter.print_detach_mode_enabled_message()

    function_pkl = cloudpickle.dumps(function_)
    function_size_gb = len(function_pkl) / (1024**3)
    reporter.function_size_gb = function_size_gb
    if function_size_gb > 0.1:
        raise FunctionTooBig(function_.__name__)

    # Single round-trip: picks nodes from the server's in-memory cache,
    # grows the cluster if `grow=True` and capacity falls short, writes the
    # job doc, and returns the nodes + booting names. Replaces what used to
    # be three separate HTTP calls here.
    start_job_config = {
        "n_inputs": len(inputs),
        "func_cpu": func_cpu,
        "func_ram": func_ram,
        "max_parallelism": max_parallelism,
        "packages": packages,
        "user_python_version": f"3.{sys.version_info.minor}",
        "burla_client_version": __version__,
        "function_name": function_.__name__,
        "function_size_gb": function_size_gb,
        "started_at": start_time,
        "is_background_job": background,
        "grow": grow,
    }
    # On 503 nodes_busy, show boot progress via the polling loop then try
    # once more. Any other known error surfaces as its domain exception
    # (VersionMismatch, NoCompatibleNodes, NoNodes, UnauthorizedError).
    for attempt in range(2):
        try:
            response = await client.start_job(job_id, start_job_config)
            break
        except NodesBusy:
            if attempt == 1:
                raise AllNodesBusy()
            await wait_for_nodes_to_be_ready(client=client, spinner=spinner)

    target_parallelism = int(response.get("target_parallelism") or 0)
    ready_nodes = [
        Node.from_ready(
            instance_name=node_data["instance_name"],
            host=_local_host_from(node_data["host"]),
            machine_type=node_data["machine_type"],
            target_parallelism=int(node_data["target_parallelism"]),
            session=session,
            client=client,
            spinner=spinner,
        )
        for node_data in response.get("ready_nodes", [])
    ]

    booting_names = response.get("booting_node_names", [])
    booting_nodes = []
    if booting_names:
        # Growth-budget per booting node: evenly split the remaining parallelism.
        per_node_parallelism = max(
            1,
            (target_parallelism or len(booting_names)) // max(1, len(booting_names)),
        )
        booting_nodes = [
            Node.from_booting(
                instance_name=name,
                target_parallelism=per_node_parallelism,
                session=session,
                client=client,
                spinner=spinner,
            )
            for name in booting_names
        ]

    nodes = ready_nodes + booting_nodes
    if booting_nodes:
        reporter.set_booting_nodes_message(len(booting_nodes))
    elif not nodes:
        # grow=True but main_service returned no booting names (cap hit) AND
        # no ready nodes - equivalent to the old "Cluster refused to boot"
        # branch.
        raise NoNodes("Cluster refused to boot required additional nodes ...")

    job_start_telemetry_task = create_task(reporter.log_job_start_telemetry(nodes, packages))
    session_stack.callback(job_start_telemetry_task.cancel)
    reporter.set_uploading_function_message(nodes)

    node_tasks = []
    n_inputs = len(inputs)  # <- inputs will be popped from so len(inputs) will start changing
    inputs_with_indicies = list(enumerate(inputs))
    random.shuffle(inputs_with_indicies)
    n_ready_nodes = len(nodes) - len(booting_nodes)
    first_chunk_barrier = asyncio.Barrier(n_ready_nodes) if n_ready_nodes else None
    for node in nodes:
        node_tasks.append(
            create_task(
                node.execute_job(
                    job_id=job_id,
                    background=background,
                    n_inputs=n_inputs,
                    packages=packages,
                    start_time=start_time,
                    function_pkl=function_pkl,
                    udf_error_event=udf_error_event,
                    inputs_with_indicies=inputs_with_indicies,
                    return_queue=return_queue,
                    nodes=nodes,
                    assigned_node_ids=[n.instance_name for n in nodes],
                    first_chunk_barrier=first_chunk_barrier,
                )
            )
        )
    try:
        ping_process = None
        last_status_message_update_time = 0.0
        total_result_count = sum(node.result_count for node in nodes)
        while total_result_count < n_inputs:
            await asyncio.sleep(0.05)

            if terminal_cancel_event.is_set():
                return

            for task, node in zip(node_tasks, nodes):
                exception = task.exception() if task.done() else None
                exception = NodeDisconnected(node) if node.state == "FAILED" else exception
                if exception:
                    # Authoritative check via main_service: if main_service has
                    # already written a lifecycle signal on the job doc, raise
                    # that instead of the bare infrastructure exception.
                    job_dict = await client.get_job(job_id) or {}
                    if job_dict.get("cluster_shutdown"):
                        raise ClusterShutdown()
                    if job_dict.get("cluster_restarted"):
                        raise ClusterRestarted()
                    if job_dict.get("dashboard_canceled"):
                        raise JobCanceled("\n\nJob canceled from dashboard.\n")
                    raise exception

            current_time = time()
            if (current_time - last_status_message_update_time) > 0.05:
                if all([n.state == "BOOTING" for n in nodes]):
                    reporter.set_booting_nodes_message(len(nodes))
                elif any([n.installing_packages for n in nodes]):
                    reporter.set_installing_packages_message()
                else:
                    total_parallelism = sum((n.current_parallelism for n in nodes))
                    booting_nodes = sum(n.state == "BOOTING" for n in nodes)
                    reporter.set_running_progress_message(
                        total_result_count, total_parallelism, booting_nodes
                    )
                last_status_message_update_time = current_time

            if len(inputs_with_indicies) == 0 and not inputs_done_event.is_set():
                inputs_done_event.set()
                await client.patch_job(job_id, {"all_inputs_uploaded": True})
                if background:
                    reporter.print_inputs_done_message()

            if ping_process is None and (time() - start_time) >= 5:
                node_hosts = [node.host for node in nodes]
                ping_process = await run_in_subprocess(send_alive_pings, node_hosts, job_id)
                session_stack.callback(ping_process.kill)

            if ping_process and ping_process.poll():
                stderr = ping_process.stderr.read().decode("utf-8")
                raise Exception(f"Heartbeat process failed!\n{stderr}")

            total_result_count = sum(node.result_count for node in nodes)
            if all([task.done() for task in node_tasks]) and total_result_count < n_inputs:
                raise Exception("Zero nodes working on job and we have not received all results!")

        job_success_telemetry_task = create_task(
            reporter.log_job_success_telemetry(time() - start_time)
        )
        session_stack.callback(job_success_telemetry_task.cancel)
        await client.patch_job(job_id, {"client_has_all_results": True})
    finally:
        [task.cancel() for task in node_tasks]


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
            If True, adds nodes to the cluster (grows) to complete the job as quickly
            as possible. Adds up to 2560 cpus.

    Returns:
        List[Any] or Generator[Any, None, None]:
            A list containing the objects returned by `function_` in no particular order.
            If `generator=True`, returns a generator that yields results as they are produced.

    See Also:
        For more info see our overview: https://docs.burla.dev/overview
        or API-Reference: https://docs.burla.dev/api-reference
    """
    start_time = time()
    udf_error_event = Event()

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

    return_queue = Queue()
    original_signal_handlers = None
    try:
        if spinner:
            spinner = yaspin(sigmap={})  # <- .start will overwrite my handlers without sigmap={}
            spinner.start()
            spinner.text = f"Preparing to call `{function_.__name__}` on {len(inputs)} inputs ..."
        terminal_cancel_event = Event()
        inputs_done_event = Event()
        original_signal_handlers = install_signal_handlers(
            job_id, background, spinner, terminal_cancel_event, inputs_done_event
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
                        terminal_cancel_event=terminal_cancel_event,
                        inputs_done_event=inputs_done_event,
                        start_time=start_time,
                        generator=generator,
                        udf_error_event=udf_error_event,
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

        if terminal_cancel_event.is_set() and background and inputs_done_event.is_set():
            return
        elif terminal_cancel_event.is_set() and background and not inputs_done_event.is_set():
            message = "\n\nBackground job canceled before all inputs finished uploading!"
            message += '\nPlease wait until the message "Done uploading inputs!" '
            message += "appears before canceling.\n\n-"
            raise JobCanceled(message)
        elif terminal_cancel_event.is_set():
            raise JobCanceled("Job canceled by user.")

        def _output_generator():
            n_results = 0
            while n_results != len(inputs):
                yield return_queue.get()
                n_results += 1

        if spinner:
            spinner.text = f"Done! {len(inputs)} `{function_.__name__}` calls completed."
            spinner.ok("✔")

        return _output_generator() if generator else list(_output_generator())

    except Exception as e:
        if spinner:
            spinner.stop()

        # Best-effort: record the failure on the job doc via main_service.
        # main_service's PATCH will apply the FAILED status + ArrayUnion
        # atomically, so no read-then-write is needed. A MainServiceTimeout
        # means main_service itself is unreachable, so skip the write.
        if not (isinstance(e, MainServiceTimeout) or background):
            ClusterClient.patch_job_sync(
                job_id,
                updates={"status": "FAILED"},
                append_fail_reason=f"client exception: {e}",
            )

        # Report errors back to Burla's cloud.
        if not udf_error_event.is_set():
            chill_exception = any([isinstance(e, e_type) for e_type in EXEC_TYPES_TO_NOT_ALERT])

            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
            traceback_str = "".join(tb_details)

            try:
                log_job_failure_telemetry(
                    job_id=job_id,
                    exception=e,
                    traceback_str=traceback_str,
                    chill_exception=chill_exception,
                )
            except:
                pass

        raise
    finally:
        if original_signal_handlers:
            restore_signal_handlers(original_signal_handlers)
