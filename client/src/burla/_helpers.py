import json
import os
import sys
import signal
import ast
import inspect
import requests
import subprocess
import textwrap
import logging
from typing import Union
from threading import Event

import cloudpickle
from yaspin import Spinner

from burla import _BURLA_BACKEND_URL, CONFIG_PATH

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

# needs to be imported after ^
from google.cloud.firestore import Client
from google.cloud.firestore_v1.async_client import AsyncClient
from google.oauth2 import service_account


class GoogleLoginError(Exception):
    pass


async def run_in_subprocess(func, *args):
    code = textwrap.dedent(
        """
        import sys, cloudpickle
        func, args = cloudpickle.load(sys.stdin.buffer)
        func(*args)
        """
    )
    cmd = [sys.executable, "-u", "-c", code]
    process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    process.stdin.write(cloudpickle.dumps((func, args)))
    process.stdin.close()
    return process


def has_explicit_return(fn):
    src = inspect.getsource(fn)
    tree = ast.parse(src)

    class ReturnVisitor(ast.NodeVisitor):
        def __init__(self):
            self.found = False

        def visit_Return(self, node):
            self.found = True

    visitor = ReturnVisitor()
    visitor.visit(tree)
    return visitor.found


def parallelism_capacity(machine_type: str, func_cpu: int, func_ram: int):
    # Max number of workers this machine_type can run a job with the given resource requirements?
    if machine_type.startswith("n4-standard") and machine_type.split("-")[-1].isdigit():
        vm_cpu = int(machine_type.split("-")[-1])
        vm_ram = N_FOUR_STANDARD_CPU_TO_RAM[vm_cpu]
        return min(vm_cpu // func_cpu, vm_ram // func_ram)
    elif machine_type.startswith("a") and machine_type.endswith("g"):
        return 1
    raise ValueError(f"machine_type must be: n4-standard-X, a3-highgpu-Xg, or a3-ultragpu-8g")


def get_db_clients():
    config = json.loads(CONFIG_PATH.read_text())
    key = config["client_svc_account_key"]
    scopes = ["https://www.googleapis.com/auth/datastore"]
    credentials = service_account.Credentials.from_service_account_info(key, scopes=scopes)
    async_db = AsyncClient(project=config["project_id"], credentials=credentials, database="burla")
    sync_db = Client(project=config["project_id"], credentials=credentials, database="burla")
    return sync_db, async_db


def install_signal_handlers(
    job_id: str, spinner: Union[Spinner, bool] = False, job_canceled_event: Event = None
):
    def _signal_handler(signum, frame):
        job_canceled_event.set()
        if spinner:
            spinner.stop()
        try:
            sync_db, _ = get_db_clients()
            sync_db.collection("jobs").document(job_id).update({"status": "FAILED"})
        except Exception:
            pass
        sys.exit(0)

    original_signal_handlers = {s: signal.getsignal(s) for s in SIGNALS_TO_HANDLE}
    [signal.signal(sig, _signal_handler) for sig in SIGNALS_TO_HANDLE]
    return original_signal_handlers


def restore_signal_handlers(original_signal_handlers):
    for sig, original_handler in original_signal_handlers.items():
        signal.signal(sig, original_handler)


def log_telemetry(message, severity="INFO", **kwargs):
    try:
        json = {"message": message, **kwargs}
        response = requests.post(f"{_BURLA_BACKEND_URL}/v1/telemetry/log/{severity}", json=json)
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
        msg += f"If you're not sure what to do, please email jake@burla.dev, or call me at 508-320-8778!\n"
        msg += f"We take errors very seriously, and would really like to help you get Burla installed!\n"
        super().__init__(msg)


def run_command(command, raise_error=True):
    result = subprocess.run(command, shell=True, capture_output=True)

    if result.returncode != 0 and raise_error:
        print("")
        raise VerboseCalledProcessError(command, result.stderr)
    else:
        return result
