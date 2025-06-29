import os
import sys
import signal
import ast
import inspect
import requests
import subprocess
import textwrap
from typing import Union
from threading import Event

import google.auth
from google.cloud.firestore import Client
from google.cloud.firestore_v1.async_client import AsyncClient
from google.auth.exceptions import DefaultCredentialsError
from yaspin import Spinner
import cloudpickle

from burla import _BURLA_BACKEND_URL

N_FOUR_STANDARD_CPU_TO_RAM = {1: 4, 2: 8, 4: 16, 8: 32, 16: 64, 32: 128, 48: 192, 64: 256, 80: 320}
POSIX_SIGNALS_TO_HANDLE = ["SIGINT", "SIGTERM", "SIGHUP", "SIGQUIT"]
NT_SIGNALS_TO_HANDLE = ["SIGINT", "SIGBREAK"]
_signal_names_to_handle = POSIX_SIGNALS_TO_HANDLE if os.name == "posix" else NT_SIGNALS_TO_HANDLE
SIGNALS_TO_HANDLE = [getattr(signal, s) for s in _signal_names_to_handle]


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
    try:
        credentials, project_id = google.auth.default()
        if project_id == "":
            raise GoogleLoginError(
                "No google cloud project found, please sign in to the google cloud CLI:\n"
                "  1. gcloud config set project <your-project-id>\n"
                "  2. gcloud auth application-default login\n"
            )
        async_db = AsyncClient(project=project_id, credentials=credentials, database="burla")
        sync_db = Client(project=project_id, credentials=credentials, database="burla")
        return sync_db, async_db
    except DefaultCredentialsError as e:
        raise Exception(
            "No Google Application Default Credentials found. "
            "Please run `gcloud auth application-default login`."
        ) from e


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
