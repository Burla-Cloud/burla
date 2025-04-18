import os
import sys
import signal
import traceback
from threading import Thread, Event

import google.auth
from google.cloud import firestore
from google.auth.exceptions import DefaultCredentialsError
from yaspin import yaspin

from burla._auth import get_gcs_credentials


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


def get_db_and_project_id(auth_headers: dict):
    if using_demo_cluster():
        credentials = get_gcs_credentials(auth_headers)
        db = firestore.Client(credentials=credentials, project="burla-prod", database="burla")
        return db, "burla-prod"
    else:
        # api_url_according_to_user = os.environ.get("BURLA_API_URL")
        # if api_url_according_to_user and api_url_according_to_user != main_service_url():
        #     raise Exception(
        #         f"You are pointing to the main service at {api_url_according_to_user}.\n"
        #         f"However, according to the current project set in gcloud, "
        #         f"the main_service is currently running at {main_service_url()}.\n"
        #         f"Please ensure your gcloud is pointing at the same project that your burla "
        #         "api is deployed in."
        #     )
        try:
            credentials, project_id = google.auth.default()
            if project_id == "":
                raise GoogleLoginError(
                    "No google cloud project found, please sign in to the google cloud CLI:\n"
                    "  1. gcloud config set project <your-project-id>\n"
                    "  2. gcloud auth application-default login\n"
                )
            db = firestore.Client(credentials=credentials, project=project_id, database="burla")
            return db, project_id
        except DefaultCredentialsError as e:
            raise Exception(
                "No Google Application Default Credentials found. "
                "Please run `gcloud auth application-default login`."
            ) from e


def prep_graceful_shutdown_with_spinner(stop_event: Event):
    def _signal_handler(signum, frame, spinner):
        spinner.stop()
        stop_event.set()
        sys.exit(0)

    return yaspin(sigmap={sig: _signal_handler for sig in SIGNALS_TO_HANDLE})


def prep_graceful_shutdown(stop_event: Event):
    def _signal_handler(signum, frame):
        stop_event.set()
        sys.exit(0)

    for sig in SIGNALS_TO_HANDLE:
        signal.signal(sig, _signal_handler)


class ThreadWithExc(Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.traceback_str = None

    def run(self):
        try:
            if self._target:
                self._target(*self._args, **self._kwargs)
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
            traceback_str = "".join(traceback_details)
            self.traceback_str = traceback_str
