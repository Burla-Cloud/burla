import os
import json
import sys

import requests

from burla import CONFIG_PATH, _BURLA_BACKEND_URL


def _safe_for_stream(message: str, stream) -> str:
    encoding = getattr(stream, "encoding", None) or "utf-8"
    return message.encode(encoding, errors="replace").decode(encoding, errors="replace")


def safe_print(message: str):
    print(_safe_for_stream(message, sys.stdout))


def safe_spinner_write(spinner, message: str):
    stream = getattr(spinner, "_stream", sys.stdout)
    spinner.write(_safe_for_stream(message, stream))


def stdio_supports_unicode() -> bool:
    stdout_encoding = (getattr(sys.stdout, "encoding", None) or "").lower()
    stderr_encoding = (getattr(sys.stderr, "encoding", None) or "").lower()
    return "utf" in stdout_encoding and "utf" in stderr_encoding


def _get_project_id():
    try:
        project_id = json.loads(CONFIG_PATH.read_text()).get("project_id")
    except Exception:
        return "unknown"
    return project_id if project_id else "unknown"


def log_telemetry(message: str, severity: str = "INFO", **kwargs):
    if os.environ.get("DISABLE_BURLA_TELEMETRY") == "True":
        return
    try:
        json_payload = {"message": message, **kwargs}
        url = f"{_BURLA_BACKEND_URL}/v1/telemetry/log/{severity}"
        response = requests.post(url, json=json_payload)
        response.raise_for_status()
    except Exception:
        pass


def log_job_failure_telemetry(
    job_id: str,
    exception: Exception,
    traceback_str: str,
    chill_exception: bool,
):
    project_id = _get_project_id()
    telemetry_kwargs = dict(traceback=traceback_str, project_id=project_id, job_id=job_id)
    if chill_exception:
        message = f"Job {job_id} failed with: {str(exception)}"
        log_telemetry(message, severity="INFO", **telemetry_kwargs)
    else:
        message = f"Job {job_id} FAILED due to NON-UDF-ERROR:\n```{traceback_str}```"
        log_telemetry(message, severity="ERROR", **telemetry_kwargs)


class RemoteParallelMapReporter:
    @classmethod
    async def _log_telemetry_async(cls, message: str, session, severity: str = "INFO", **kwargs):
        if os.environ.get("DISABLE_BURLA_TELEMETRY") == "True":
            return
        try:
            json_payload = {"message": message, **kwargs}
            url = f"{_BURLA_BACKEND_URL}/v1/telemetry/log/{severity}"
            async with session.post(url, json=json_payload) as response:
                await response.text()
                response.raise_for_status()
        except Exception:
            pass

    def __init__(self, **kwargs):
        self.spinner = kwargs["spinner"]
        self.function_name = kwargs["function_"].__name__
        self.input_count = len(kwargs["inputs"])
        self.function_size_gb = kwargs.get("function_size_gb", 0.0)
        self.function_cpu = kwargs["func_cpu"]
        self.function_ram = kwargs["func_ram"]
        self.background = kwargs["background"]
        self.generator = kwargs["generator"]
        self.grow = kwargs["grow"]
        self.max_parallelism = kwargs["max_parallelism"]
        self.job_id = kwargs["job_id"]
        self.session = kwargs["session"]
        self.project_id = _get_project_id()
        self.spinner_enabled = bool(self.spinner)

    def _write_message(self, message: str):
        if self.spinner:
            safe_spinner_write(self.spinner, message)
        else:
            safe_print(message)

    def print_detach_mode_enabled_message(self):
        message = f"Calling `{self.function_name}` on {self.input_count} inputs with detach mode enabled!\n"
        message += "This job will continue running if canceled locally, "
        message += "and inputs have finished uploading.\n-"
        self._write_message(message)

    def set_booting_nodes_message(self, number_of_booting_nodes: int):
        if not self.spinner:
            return
        self.spinner.text = f"Booting {number_of_booting_nodes} additional nodes ..."

    async def log_job_start_telemetry(self, nodes: list, packages: dict):
        ready_nodes = [node for node in nodes if node.state == "READY"]
        number_of_nodes = len(ready_nodes) if ready_nodes else len(nodes)
        machine_type = ready_nodes[0].machine_type if ready_nodes else "unknown-machine"
        function_size_str = (
            f" ({self.function_size_gb:.3f}GB)" if self.function_size_gb > 0.001 else ""
        )
        message = (
            f"\nCalling function `{self.function_name}`{function_size_str} on {self.input_count} "
        )
        message += f"inputs with {number_of_nodes} {machine_type} nodes and "
        message += f"{self.function_cpu}vCPUs/{self.function_ram}GB RAM per function.\n"
        message += (
            f"background={self.background}, generator={self.generator}, "
            f"spinner={self.spinner_enabled}, grow={self.grow}, "
        )
        message += f"max_parallelism={self.max_parallelism}, job_id={self.job_id}"
        if packages:
            message += f"\n---\nRequested packages: {packages}"
        await self._log_telemetry_async(message, self.session, project_id=self.project_id)

    def set_uploading_function_message(self, nodes: list):
        if not self.spinner:
            return
        number_of_nodes = len([node for node in nodes if node.state == "READY"])
        function_size_megabytes = self.function_size_gb * 1024
        total_data_gb = self.function_size_gb * number_of_nodes
        message = f"Uploading function `{self.function_name}` to {number_of_nodes} nodes ..."
        if total_data_gb > 0.01:
            message = (
                f"Uploading function `{self.function_name}` ({function_size_megabytes:.2f}MB) "
            )
            message += f"to {number_of_nodes} nodes ({total_data_gb:.2f}GB) ..."
        self.spinner.text = message

    def set_installing_packages_message(self):
        if not self.spinner:
            return
        self.spinner.text = "Installing packages ..."

    def print_inputs_done_message(self):
        message = "\n------------------------------\n"
        message += "Done uploading inputs!\n"
        message += "Job will now continue running if canceled locally.\n"
        message += "------------------------------"
        self._write_message(message)

    def set_running_progress_message(
        self,
        completed_inputs: int,
        total_parallelism: int,
        booting_nodes: int = 0,
        dynamic_worker_reduction: dict | None = None,
    ):
        if not self.spinner:
            return
        # Due to status lag, remaining inputs can briefly be lower than reported parallelism.
        running_inputs = min(total_parallelism, self.input_count - completed_inputs)
        message = (
            f"Calling `{self.function_name}`: {completed_inputs}/{self.input_count} completed, "
            f"{running_inputs} running."
        )
        if booting_nodes > 0:
            message += f" Booting {booting_nodes} nodes ..."
        if dynamic_worker_reduction:
            original = dynamic_worker_reduction["original"]
            current = dynamic_worker_reduction["current"]
            message += f"\nMemory pressure: workers {original} -> {current}."
        self.spinner.text = message

    async def log_job_success_telemetry(self, total_runtime: float):
        message = f"Job {self.job_id} completed successfully, total_runtime={total_runtime:.2f}s."
        await self._log_telemetry_async(message, self.session, project_id=self.project_id)

    def set_preparing_message(self):
        if not self.spinner:
            return
        self.spinner.text = (
            f"Preparing to call `{self.function_name}` on {self.input_count} inputs ..."
        )

    def finish_spinner_success(self):
        if not self.spinner:
            return
        self.spinner.text = f"Done! {self.input_count} `{self.function_name}` calls completed."
        self.spinner.ok("OK")

    def get_background_cancel_before_upload_message(self) -> str:
        message = "\n\nBackground job canceled before all inputs finished uploading to the cluster!"
        message += '\nPlease wait until the message "Done uploading inputs!" '
        message += "appears before canceling.\n\n-"
        return message

    def log_job_failure_telemetry(
        self,
        exception: Exception,
        traceback_str: str,
        chill_exception: bool,
    ):
        log_job_failure_telemetry(
            job_id=self.job_id,
            exception=exception,
            traceback_str=traceback_str,
            chill_exception=chill_exception,
        )

    @classmethod
    async def log_user_function_error_async(cls, job_id: str, session):
        message = f"Job {job_id} failed due to user function error."
        await cls._log_telemetry_async(message, session, project_id=_get_project_id())
