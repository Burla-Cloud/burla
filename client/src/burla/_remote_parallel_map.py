import asyncio
import random
import sys
import traceback
import base64
from asyncio import create_task
from contextlib import AsyncExitStack
from functools import cache
from importlib import metadata
from queue import Queue
from threading import Event, Thread
from time import time
from typing import Callable, Literal, Optional, Union

FuncGpu = Literal["A100", "A100_40G", "A100_80G", "H100", "H100_80G"]
FuncRam = Union[int, Literal["dynamic"]]
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
    JobStalled,
    MainServiceTimeout,
    Node,
    NoCompatibleNodes,
    NoNodes,
    NodeDisconnected,
    UnauthorizedError,
    VersionMismatch,
    wait_for_nodes_to_be_ready,
)
from burla._reporting import (
    RemoteParallelMapReporter,
    log_job_failure_telemetry,
    stdio_supports_unicode,
)


# `metadata.packages_distributions()` takes hundreds of ms to multiple seconds in
# fat notebook envs. Deferring it to first-call time means `import burla` stays
# fast for users who don't call `remote_parallel_map` right away; repeat calls in
# the same session share the cached result.
@cache
def _pkg_module_mapping():
    return metadata.packages_distributions()


BANNED_PACKAGES = ["ipython", "burla", "google-colab"]


def _read_process_stderr(process) -> str:
    stderr_buffer = getattr(process, "stderr_buffer", None)
    if stderr_buffer is not None:
        stderr_buffer.flush()
        stderr_buffer.seek(0)
        return stderr_buffer.read().decode("utf-8", errors="replace")
    if process.stderr is None:
        return ""
    return process.stderr.read().decode("utf-8", errors="replace")


async def _job_lifecycle_exception(client: ClusterClient, job_id: str):
    last_error = None
    for _ in range(3):
        try:
            job_dict = await client.get_job(job_id) or {}
        except Exception as error:
            last_error = error
            await asyncio.sleep(1)
            continue
        if job_dict.get("cluster_shutdown"):
            return ClusterShutdown(), None, job_dict
        if job_dict.get("cluster_restarted"):
            return ClusterRestarted(), None, job_dict
        if job_dict.get("dashboard_canceled"):
            return JobCanceled("\n\nJob canceled from dashboard.\n"), None, job_dict
        return None, None, job_dict
    return None, last_error, None


def _job_diagnostic_summary(job_dict: dict | None) -> str | None:
    if job_dict is None:
        return None
    heartbeat_at = job_dict.get("client_heartbeat_at")
    status = job_dict.get("status")
    all_inputs_uploaded = job_dict.get("all_inputs_uploaded")
    client_has_all_results = job_dict.get("client_has_all_results")
    return (
        f"Job diagnostics: status={status}, client_heartbeat_at={heartbeat_at}, "
        f"all_inputs_uploaded={all_inputs_uploaded}, client_has_all_results={client_has_all_results}"
    )


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
    JobStalled,
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
    func_ram: FuncRam,
    max_parallelism: int,
    background: bool,
    spinner: Union[bool, Spinner],
    terminal_cancel_event: Event,
    inputs_done_event: Event,
    start_time: float,
    udf_error_event: Event,
    grow: bool,
    image: Optional[str],
    func_gpu: Optional[FuncGpu],
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
        "image": image,
        "func_gpu": func_gpu,
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
    booting_nodes = [
        Node.from_booting(
            instance_name=node_data["instance_name"],
            target_parallelism=int(node_data["target_parallelism"]),
            session=session,
            client=client,
            spinner=spinner,
        )
        for node_data in response.get("booting_nodes", [])
    ]

    nodes = ready_nodes + booting_nodes
    if booting_nodes:
        reporter.set_booting_nodes_message(len(booting_nodes))
    elif not nodes:
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
                    func_ram=func_ram,
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
        pinged_hosts: tuple = ()

        def _cleanup_ping_process():
            if ping_process is not None:
                ping_process.kill()
                stderr_buffer = getattr(ping_process, "stderr_buffer", None)
                if stderr_buffer is not None:
                    stderr_buffer.close()

        session_stack.callback(_cleanup_ping_process)
        last_status_message_update_time = 0.0
        total_result_count = sum(node.result_count for node in nodes)
        while total_result_count < n_inputs:
            await asyncio.sleep(0.05)

            if terminal_cancel_event.is_set():
                return

            for task, node in zip(node_tasks, nodes):
                exception = task.exception() if task.done() else None
                if node.state == "FAILED":
                    exception = NodeDisconnected(node, await node._failure_message())
                if exception:
                    exception.add_note(node._diagnostic_summary())
                    lifecycle_exception, lifecycle_error, job_dict = await _job_lifecycle_exception(
                        client, job_id
                    )
                    if lifecycle_exception is not None:
                        raise lifecycle_exception
                    job_note = _job_diagnostic_summary(job_dict)
                    if job_note is not None:
                        exception.add_note(job_note)
                    if lifecycle_error is not None:
                        note = f"Also failed to read job lifecycle state: {lifecycle_error!r}"
                        exception.add_note(note)
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

            # Respawn the ping subprocess whenever the set of ready hosts changes,
            # since it captures its host list at spawn time.
            current_hosts = tuple(sorted(n.host for n in nodes if n.host))
            hosts_changed = current_hosts != pinged_hosts
            if (time() - start_time) >= 5 and current_hosts and hosts_changed:
                if ping_process is not None:
                    ping_process.kill()
                ping_process = await run_in_subprocess(send_alive_pings, list(current_hosts), job_id)
                pinged_hosts = current_hosts

            if ping_process and ping_process.poll():
                stderr = _read_process_stderr(ping_process)
                raise Exception(f"Heartbeat process failed!\n{stderr}")

            total_result_count = sum(node.result_count for node in nodes)
            if all([task.done() for task in node_tasks]) and total_result_count < n_inputs:
                summary = "\n".join([await n._stall_summary_line() for n in nodes])
                msg = (
                    f"Job ended before all results were received "
                    f"({total_result_count}/{n_inputs}).\n"
                    f"Final node states:\n{summary}\n"
                )
                raise JobStalled(msg)

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
    func_ram: FuncRam = "dynamic",
    func_gpu: Optional[FuncGpu] = None,
    image: Optional[str] = None,
    grow: bool = False,
    max_parallelism: Optional[int] = None,
    detach: bool = False,
    generator: bool = False,
    spinner: bool = True,
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
        func_ram (int | "dynamic", optional):
            The amount of RAM (in GB) allocated for each instance of `function_`.
            Defaults to "dynamic", which starts with CPU-bound parallelism and retries
            work at lower node parallelism if workers run out of memory.
        func_gpu (str, optional):
            Allocate one GPU per function call. One of: "A100" / "A100_40G",
            "A100_80G", "H100" / "H100_80G". Defaults to None (no GPU).
        image (str, optional):
            If provided, only nodes running this container image are eligible. When
            `grow=True` and no matching nodes are available, newly booted nodes will
            run this image. Defaults to None. When `grow=True` and `image` is None,
            defaults to the stock `python:X.Y` image matching your local Python
            version (e.g. `python:3.12`) so new nodes can run your pickled function.
        grow (bool, optional):
            If True, adds nodes to the cluster (grows) to complete the job as quickly
            as possible. Adds up to 2560 cpus.
        max_parallelism (int, optional):
            The maximum number of `function_` instances allowed to be running at the same time.
            Defaults to the number of provided inputs.
        detach (bool, optional):
            If True, job will continue running on cluster, when canceled locally.
            Defaults to False.
        generator (bool, optional):
            If True, returns a generator that yields outputs as they are produced; otherwise,
            returns a list of outputs once all have been processed. Defaults to False.
        spinner (bool, optional):
            If set to False, disables the display of the status indicator/spinner. Defaults to True.

    Returns:
        List[Any] or Generator[Any, None, None]:
            A list containing the objects returned by `function_` in no particular order.
            If `generator=True`, returns a generator that yields results as they are produced.

    Raises:
        Any exception raised by `function_` on a worker is re-raised here on the
        client. The raised exception has ``exc.burla_input_index`` set to the
        index (in ``inputs``) of the item that triggered the failure, so you
        can identify which input broke without wrapping your UDF in try/except:

            try:
                results = remote_parallel_map(fn, inputs)
            except Exception as e:
                bad_input = inputs[e.burla_input_index]

    See Also:
        For more info see our overview: https://docs.burla.dev/overview
        or API-Reference: https://docs.burla.dev/api-reference
    """
    start_time = time()
    udf_error_event = Event()

    inputs = [(i,) if not isinstance(i, tuple) else i for i in inputs]
    if not inputs:
        return iter([]) if generator else []

    if grow and image is None:
        image = f"python:3.{sys.version_info.minor}"

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
    pkg_module_mapping = _pkg_module_mapping()
    packages = {}
    for module_name in package_module_names:
        # some of these are unnecessary since we get all that map to the base module
        # example google.cloud.storage -> google -> every installed google package
        # for now we just install more packages than we need to, it's fast enough
        if not pkg_module_mapping.get(module_name):
            continue
        for package_name in pkg_module_mapping.get(module_name):
            packages[package_name] = metadata.version(package_name)

    # unnecessary / already installed / will break stuff
    for package in BANNED_PACKAGES:
        packages.pop(package, None)

    # not an official dep
    if packages.get("SQLAlchemy") and "psycopg2-binary" in pkg_module_mapping.get("psycopg2", []):
        packages["psycopg2-binary"] = metadata.version("psycopg2-binary")

    # manually check for extras until we can support automatic extra detection.
    if packages.get("geopandas"):
        if "geoalchemy2" in pkg_module_mapping and not ("geoalchemy2" in packages):
            packages["geoalchemy2"] = metadata.version("geoalchemy2")
        if "geopy" in pkg_module_mapping and not ("geopy" in packages):
            packages["geopy"] = metadata.version("geopy")
        if "matplotlib" in pkg_module_mapping and not ("matplotlib" in packages):
            packages["matplotlib"] = metadata.version("matplotlib")
        if "mapclassify" in pkg_module_mapping and not ("mapclassify" in packages):
            packages["mapclassify"] = metadata.version("mapclassify")
        if "xyzservices" in pkg_module_mapping and not ("xyzservices" in packages):
            packages["xyzservices"] = metadata.version("xyzservices")
        if "folium" in pkg_module_mapping and not ("folium" in packages):
            packages["folium"] = metadata.version("folium")
        if "pointpats" in pkg_module_mapping and not ("pointpats" in packages):
            packages["pointpats"] = metadata.version("pointpats")
        if "scipy" in pkg_module_mapping and not ("scipy" in packages):
            packages["scipy"] = metadata.version("scipy")
        if "pyarrow" in pkg_module_mapping and not ("pyarrow" in packages):
            packages["pyarrow"] = metadata.version("pyarrow")
        if "SQLAlchemy" in pkg_module_mapping and not ("SQLAlchemy" in packages):
            packages["SQLAlchemy"] = metadata.version("SQLAlchemy")

    if packages.get("mapclassify"):
        if "libpysal" in pkg_module_mapping and not ("libpysal" in packages):
            packages["libpysal"] = metadata.version("libpysal")
        if "shapely" in pkg_module_mapping and not ("shapely" in packages):
            packages["shapely"] = metadata.version("shapely")
        if "matplotlib" in pkg_module_mapping and not ("matplotlib" in packages):
            packages["matplotlib"] = metadata.version("matplotlib")
    # ------------------------------------------------

    max_parallelism = max_parallelism if max_parallelism else len(inputs)
    uid = base64.urlsafe_b64encode(uuid4().bytes[:9]).decode()
    job_id = f"{function_.__name__}-{uid}"

    return_queue = Queue()
    original_signal_handlers = None
    try:
        if spinner is True and not stdio_supports_unicode():
            spinner = False
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
                        image=image,
                        func_gpu=func_gpu,
                    )
                )
            except BaseException:
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
            spinner.ok("OK")

        return _output_generator() if generator else list(_output_generator())

    except BaseException as e:
        if spinner:
            spinner.stop()

        # Best-effort: record real failures on the job doc via main_service.
        # Lifecycle cancellations already wrote their terminal status.
        lifecycle_exception = isinstance(e, (ClusterRestarted, ClusterShutdown, JobCanceled))
        if not (isinstance(e, MainServiceTimeout) or background or lifecycle_exception):
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
