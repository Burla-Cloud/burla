import os
import sys
import signal
import subprocess
import types
from typing import Union
from threading import Event

from yaspin import Spinner

from burla import get_cluster_dashboard_url

POSIX_SIGNALS_TO_HANDLE = ["SIGINT", "SIGTERM", "SIGHUP", "SIGQUIT"]
NT_SIGNALS_TO_HANDLE = ["SIGINT", "SIGBREAK"]
_signal_names_to_handle = POSIX_SIGNALS_TO_HANDLE if os.name == "posix" else NT_SIGNALS_TO_HANDLE
SIGNALS_TO_HANDLE = [getattr(signal, s) for s in _signal_names_to_handle]


class GoogleLoginError(Exception):
    pass


def restore_signal_handlers(original_signal_handlers):
    for sig, original_handler in original_signal_handlers.items():
        signal.signal(sig, original_handler)


class VerboseCalledProcessError(Exception):
    """This exists to include stderr in the exception message, CalledProcessError does not"""

    def __init__(self, cmd: str, stderr: bytes):
        try:
            stderr = stderr.decode()
        except Exception:
            pass
        msg = "SubCommand failed with non-zero exit code!\n"
        msg += f'Command = "{cmd}"\n'
        msg += f"Command Stderr--------------------------------------------------------\n"
        msg += f"{stderr}\n"
        msg += f"--------------------------------------------------------\n"
        msg += f"If you're not sure what to do, please email jake@burla.dev!\n"
        msg += f"We take errors very seriously, and would really like to help you get Burla installed!\n"
        super().__init__(msg)


def run_command(command, raise_error=True):
    result = subprocess.run(command, shell=True, capture_output=True)

    if result.returncode != 0 and raise_error:
        print("")
        raise VerboseCalledProcessError(command, result.stderr)
    else:
        return result


def install_signal_handlers(
    job_id: str,
    background: bool,
    spinner: Union[Spinner, bool],
    terminal_cancel_event: Event,
    inputs_done_event: Event,
):
    # Lazy import: `_cluster_client` imports `_auth`, which imports `_helpers`
    # (for `run_command`). Deferring this import to signal-handler install
    # time sidesteps that cycle.
    from burla._cluster_client import ClusterClient

    def _signal_handler(signum, frame):
        if terminal_cancel_event.is_set():
            return
        terminal_cancel_event.set()

        inputs_still_uploading = not inputs_done_event.is_set()
        job_failed = (background and inputs_still_uploading) or not background

        if background and inputs_still_uploading:
            fail_reason = "Client canceled background job before inputs were finished uploading."
        elif not background:
            fail_reason = "Cancel signal from client."

        if job_failed:
            # Best effort - if main_service is unreachable the node will still
            # detect the disconnect via its own job-doc liveness check and the
            # client exits either way.
            ClusterClient.patch_job_sync(
                job_id,
                updates={"status": "CANCELED"},
                append_fail_reason=fail_reason,
            )

        if background and inputs_done_event.is_set():
            main_service_url = get_cluster_dashboard_url()
            job_url = f"{main_service_url}/jobs/{job_id}"
            msg = "Background mode is enabled.\n"
            msg += f"This job will continue running on the cluster, to monitor progress go to:"
            msg += f"\n\n    {job_url}\n"
            spinner.write(msg)
            spinner.text = "Detached successfully."
            spinner.ok("✔")
        else:
            spinner.text = "Job Canceled."
            spinner.fail("✘")

    original_signal_handlers = {s: signal.getsignal(s) for s in SIGNALS_TO_HANDLE}
    [signal.signal(sig, _signal_handler) for sig in SIGNALS_TO_HANDLE]
    return original_signal_handlers


# Cache of the last sys.modules walk, keyed on len(sys.modules). In fat notebook
# envs the walk itself is 50-200ms per call; repeat `remote_parallel_map`
# invocations without new imports can reuse the previous result.
# Layout: (modules_count, custom_module_names, package_module_names, has_custom_modules)
_sys_modules_scan_cache = None


def _scan_sys_modules():
    global _sys_modules_scan_cache
    modules_count = len(sys.modules)
    if _sys_modules_scan_cache and _sys_modules_scan_cache[0] == modules_count:
        return _sys_modules_scan_cache[1], _sys_modules_scan_cache[2], _sys_modules_scan_cache[3]

    has_custom_modules = False
    custom_module_names = set()
    package_module_names = set()
    for module_name, module in sys.modules.items():
        spec = getattr(module, "__spec__", None)
        origin = getattr(spec, "origin", None)
        if origin:
            is_package = (
                "site-packages" in origin
                or "dist-packages" in origin
                or r"\Lib" in origin
                # Worker containers install pip packages here, not site-packages.
                or "/worker_service_python_env/" in origin
            )
            is_builtin = (
                origin in ("built-in", "frozen") or "lib/python" in origin or r"\DLLs" in origin
            )
            is_burla = "burla" in origin  # <- make dev mode not always false positive
            is_custom = not (is_package or is_builtin or is_burla)
            if is_package:
                base_module_name = module_name.split(".")[0]
                package_module_names.add(base_module_name)
            elif is_custom:
                custom_module_names.add(module_name)
                has_custom_modules = True
    _sys_modules_scan_cache = (
        modules_count,
        custom_module_names,
        package_module_names,
        has_custom_modules,
    )
    return custom_module_names, package_module_names, has_custom_modules


def get_modules_required_on_remote(function_):
    """
    Returns all package modules if custom user-defined modules exist.
    (because I don't want to write code to inspect custom modules for required packages right now)
    Only returns modules defined in `function_` namespace if there are no user-defined modules.
    """
    cached_custom, cached_packages, has_custom_modules = _scan_sys_modules()
    # Caller mutates these (removing temp-fix misdetections, adding xarray deps),
    # so hand back fresh copies instead of the cached sets.
    custom_module_names = set(cached_custom)
    package_module_names = set(cached_packages)
    if not has_custom_modules:
        # If there are NO custom modules, we install only packages that are in the namespace
        # of the users function, because these are the only ones that might be used.
        function_module_names = set()
        for var in function_.__globals__.values():
            is_module = isinstance(var, types.ModuleType)
            has_module = getattr(var, "__module__", None)
            if is_module or has_module:
                module_name = var.__name__ if is_module else var.__module__
                function_module_names.add(module_name.split(".")[0])
        package_module_names = package_module_names.intersection(function_module_names)
    return custom_module_names, package_module_names
