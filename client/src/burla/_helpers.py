import os
import sys
import signal
from typing import Optional

import google.auth
from google.cloud.firestore import Client
from google.cloud.firestore_v1.async_client import AsyncClient
from google.auth.exceptions import DefaultCredentialsError
from yaspin import yaspin

from burla._auth import get_gcs_credentials, get_auth_headers


N_FOUR_STANDARD_CPU_TO_RAM = {1: 4, 2: 8, 4: 16, 8: 32, 16: 64, 32: 128, 48: 192, 64: 256, 80: 320}
POSIX_SIGNALS_TO_HANDLE = ["SIGINT", "SIGTERM", "SIGHUP", "SIGQUIT"]
NT_SIGNALS_TO_HANDLE = ["SIGINT", "SIGBREAK"]
_signal_names_to_handle = POSIX_SIGNALS_TO_HANDLE if os.name == "posix" else NT_SIGNALS_TO_HANDLE
SIGNALS_TO_HANDLE = [getattr(signal, s) for s in _signal_names_to_handle]


class GoogleLoginError(Exception):
    pass


def parallelism_capacity(machine_type: str, func_cpu: int, func_ram: int):
    # Max number of workers this machine_type can run a job with the given resource requirements?
    if machine_type.startswith("n4-standard") and machine_type.split("-")[-1].isdigit():
        vm_cpu = int(machine_type.split("-")[-1])
        vm_ram = N_FOUR_STANDARD_CPU_TO_RAM[vm_cpu]
        return min(vm_cpu // func_cpu, vm_ram // func_ram)
    raise ValueError(f"machine_type must be n4-standard-X")


def get_host():
    # not defined in init because users often change this post-import
    custom_host = os.environ.get("BURLA_API_URL")
    return custom_host or "https://cluster.burla.dev"


def using_demo_cluster():
    # not defined in init because users often change this post-import
    return not bool(os.environ.get("BURLA_API_URL"))


def get_db_clients(api_key: Optional[str]):
    if using_demo_cluster():
        credentials = get_gcs_credentials(api_key)
        async_db = AsyncClient(project="burla-prod", credentials=credentials, database="burla")
        sync_db = Client(project="burla-prod", credentials=credentials, database="burla")
        return sync_db, async_db
    else:
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


def spinner_with_signal_handlers():
    def _signal_handler(signum, frame, spinner):
        spinner.stop()
        sys.exit(0)

    return yaspin(sigmap={sig: _signal_handler for sig in SIGNALS_TO_HANDLE})
