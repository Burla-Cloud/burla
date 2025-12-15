import json
import os
import sys
import sysconfig
import signal
import requests
import subprocess
import textwrap
import logging
import types
from typing import Union
from threading import Event

import cloudpickle
from yaspin import Spinner
from google.cloud.firestore_v1 import AsyncClient

from burla import _BURLA_BACKEND_URL, CONFIG_PATH

TOKEN_URI = "https://oauth2.googleapis.com/token"

N_FOUR_STANDARD_CPU_TO_RAM = {1: 4, 2: 8, 4: 16, 8: 32, 16: 64, 32: 128, 48: 192, 64: 256, 80: 320}
POSIX_SIGNALS_TO_HANDLE = ["SIGINT", "SIGTERM", "SIGHUP", "SIGQUIT"]
NT_SIGNALS_TO_HANDLE = ["SIGINT", "SIGBREAK"]
_signal_names_to_handle = POSIX_SIGNALS_TO_HANDLE if os.name == "posix" else NT_SIGNALS_TO_HANDLE
SIGNALS_TO_HANDLE = [getattr(signal, s) for s in _signal_names_to_handle]

# throws some uncatchable, unimportant, warnings
logging.getLogger("google.api_core.bidi").setLevel(logging.ERROR)
# prevent some annoying grpc logs / warnings
os.environ["GRPC_VERBOSITY"] = "ERROR"  # only log ERROR/FATAL
os.environ["GLOG_minloglevel"] = "2"  # 0-INFO, 1-WARNING, 2-ERROR, 3-FATAL
os.environ["GRPC_ENABLE_FORK_SUPPORT"] = "1"  # avoid fork() handler warnings

# needs to be imported after ^
from google.cloud.firestore import Client
from google.cloud.firestore import ArrayUnion
from google.cloud.firestore_v1.async_client import AsyncClient
from google.oauth2 import service_account


class GoogleLoginError(Exception):
    pass


class SuppressNativeStderr:
    """Temporarily silence C/C++ library logs that write directly to stderr.

    Used to suppress one-time gRPC/ALTS noise during Firestore channel setup.
    """

    def __enter__(self):
        # Duplicate FD 2 (stderr) directly to tolerate environments without a true sys.stderr fd.
        self._stderr_fd_copy = os.dup(2)
        self._devnull = open(os.devnull, "w")
        os.dup2(self._devnull.fileno(), 2)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.dup2(self._stderr_fd_copy, 2)
        os.close(self._stderr_fd_copy)
        self._devnull.close()
        return False

    async def __aenter__(self):
        return self.__enter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self.__exit__(exc_type, exc_val, exc_tb)


async def run_in_subprocess(func, *args):
    # I do it like this so it works in google colab, multiprocesing doesn't
    code = textwrap.dedent(
        """
        import sys, cloudpickle
        func, args = cloudpickle.load(sys.stdin.buffer)
        func(*args)
        """
    )
    cmd = [sys.executable, "-u", "-c", code]
    with SuppressNativeStderr():
        process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    process.stdin.write(cloudpickle.dumps((func, args)))
    process.stdin.close()
    return process


def parallelism_capacity(machine_type: str, func_cpu: int, func_ram: int):
    # Max number of workers this machine_type can run a job with the given resource requirements?
    if machine_type.startswith("n4-standard") and machine_type.split("-")[-1].isdigit():
        vm_cpu = int(machine_type.split("-")[-1])
        vm_ram = N_FOUR_STANDARD_CPU_TO_RAM[vm_cpu]
        return min(vm_cpu // func_cpu, vm_ram // func_ram)
    elif machine_type.startswith("a") and machine_type.endswith("g"):
        return 1
    raise ValueError(f"machine_type must be: n4-standard-X, a3-highgpu-Xg, or a3-ultragpu-8g")


def restore_signal_handlers(original_signal_handlers):
    for sig, original_handler in original_signal_handlers.items():
        signal.signal(sig, original_handler)


def log_telemetry(message, severity="INFO", **kwargs):
    if not os.environ.get("DISABLE_BURLA_TELEMETRY") == "True":
        try:
            json_payload = {"message": message, **kwargs}
            url = f"{_BURLA_BACKEND_URL}/v1/telemetry/log/{severity}"
            response = requests.post(url, json=json_payload)
            response.raise_for_status()
        except Exception:
            pass


async def log_telemetry_async(message, session, severity="INFO", **kwargs):
    if not os.environ.get("DISABLE_BURLA_TELEMETRY") == "True":
        try:
            json_payload = {"message": message, **kwargs}
            url = f"{_BURLA_BACKEND_URL}/v1/telemetry/log/{severity}"
            async with session.post(url, json=json_payload) as response:
                await response.text()
                response.raise_for_status()
        except Exception:
            pass


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


def get_db_clients():
    config = json.loads(CONFIG_PATH.read_text())
    key = config["client_svc_account_key"]
    scopes = ["https://www.googleapis.com/auth/datastore"]
    credentials = service_account.Credentials.from_service_account_info(key, scopes=scopes)

    # Silence native gRPC setup logs (e.g., ALTS creds ignored) that can appear once per process.
    # this seems to actually work and is NOT a forgotten failed attempt.
    with SuppressNativeStderr():
        kwargs = dict(project=config["project_id"], credentials=credentials, database="burla")
        async_db = AsyncClient(**kwargs)
        sync_db = Client(**kwargs)
    return sync_db, async_db


def install_signal_handlers(
    job_id: str,
    background: bool,
    spinner: Union[Spinner, bool],
    job_canceled_event: Event,
    inputs_done_event: Event,
):
    def _signal_handler(signum, frame):
        if job_canceled_event.is_set():
            return
        job_canceled_event.set()

        inputs_still_uploading = not inputs_done_event.is_set()
        job_failed = (background and inputs_still_uploading) or not background

        if background and inputs_still_uploading:
            fail_reason = "Client canceled background job before inputs were finished uploading."
        elif not background:
            fail_reason = "Cancel signal from client."

        if job_failed:
            try:
                sync_db, _ = get_db_clients()
                job_doc = sync_db.collection("jobs").document(job_id)
                if job_doc.get().to_dict()["status"] != "CANCELED":
                    job_doc.update({"status": "FAILED", "fail_reason": ArrayUnion([fail_reason])})
            except Exception:
                pass

        if background and inputs_done_event.is_set():
            main_service_url = json.loads(CONFIG_PATH.read_text())["cluster_dashboard_url"]
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


def get_modules_required_on_remote(function_):
    """
    Returns all package modules if custom user-defined modules exist.
    (because I don't want to write code to inspect custom modules for required packages right now)
    Only returns modules defined in `function_` namespace if there are no user-defined modules.
    """
    has_custom_modules = False
    custom_module_names = set()
    package_module_names = set()
    for module_name, module in sys.modules.items():
        spec = getattr(module, "__spec__", None)
        origin = getattr(spec, "origin", None)
        if origin:
            is_package = "site-packages" in origin or "dist-packages" in origin or r"\Lib" in origin
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
