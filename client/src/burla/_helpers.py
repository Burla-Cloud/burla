import os
import signal
import subprocess
from typing import Union
from threading import Event

from yaspin import Spinner

from burla import get_cluster_dashboard_url

POSIX_SIGNALS_TO_HANDLE = ["SIGINT", "SIGTERM", "SIGHUP", "SIGQUIT"]
NT_SIGNALS_TO_HANDLE = ["SIGINT", "SIGBREAK"]
_signal_names_to_handle = POSIX_SIGNALS_TO_HANDLE if os.name == "posix" else NT_SIGNALS_TO_HANDLE
SIGNALS_TO_HANDLE = [getattr(signal, s) for s in _signal_names_to_handle]

# On Windows, a console subprocess pops a visible empty console window whenever
# the parent has no console of its own (notebooks, GUI shells).
CREATE_NO_WINDOW = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0


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
    result = subprocess.run(
        command, shell=True, capture_output=True, creationflags=CREATE_NO_WINDOW
    )

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
    from burla._reporting import safe_print

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
            # Best effort - if main_service is unreachable the nodes will still
            # detect the disconnect via their direct-heartbeat quorum and the
            # client exits either way.
            ClusterClient.patch_job_sync(
                job_id,
                updates={"status": "CANCELED"},
                append_fail_reason=fail_reason,
            )

        # spinner is False when the caller disabled it; touching it then would
        # turn the JobCanceled a Ctrl-C should raise into an AttributeError.
        if background and inputs_done_event.is_set():
            main_service_url = get_cluster_dashboard_url()
            job_url = f"{main_service_url}/jobs/{job_id}"
            msg = "Background mode is enabled.\n"
            msg += f"This job will continue running on the cluster, to monitor progress go to:"
            msg += f"\n\n    {job_url}\n"
            if spinner:
                spinner.write(msg)
                spinner.text = "Detached successfully."
                spinner.ok("✔")
            else:
                safe_print(msg)
        elif spinner:
            spinner.text = "Job Canceled."
            spinner.fail("✘")

    original_signal_handlers = {s: signal.getsignal(s) for s in SIGNALS_TO_HANDLE}
    [signal.signal(sig, _signal_handler) for sig in SIGNALS_TO_HANDLE]
    return original_signal_handlers
