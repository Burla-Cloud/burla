import asyncio
import json
import pickle
import random
import sys
from queue import Queue
from threading import Event
from pickle import UnpicklingError
from time import time
import aiohttp
import cloudpickle
from aiohttp import ClientConnectorError, ClientError, ClientOSError, ClientTimeout
from tblib import Traceback
from yaspin import Spinner

from burla import get_cluster_dashboard_url
from burla._auth import get_auth_headers
from burla._cluster_client import ClusterClient, _local_host_from
from burla._reporting import RemoteParallelMapReporter, safe_print, safe_spinner_write


NODE_SILENCE_TIMEOUT_SECONDS = 2 * 60
RESULT_POLL_SILENCE_TIMEOUT_SECONDS = 10 * 60
NODE_BOOT_DEADLINE_SEC = 10 * 60
LOGIN_TIMEOUT_SEC = 10
MAX_INPUT_SIZE_BYTES = 1_000_000 * 200  # 200MB
MAX_CHUNK_SIZE_BYTES = 1_000_000 * 2  # 2MB
NETWORK_RETRY_ATTEMPTS = 5
NETWORK_RETRY_DELAY_SECONDS = 1
NETWORK_ERROR_TYPES = (
    asyncio.TimeoutError,
    ClientConnectorError,
    ClientOSError,
    ClientError,
    OSError,
)


class InputTooBig(Exception):
    def __init__(self, index: int):
        message = f"\n\nInput at index {index} exceeds maximum size of 0.2GB.\n"
        message += "Please download large inputs from the internet once inside your function.\n"
        message += "We apologize for this temporary limitation! "
        message += "If this is confusing or blocking you, please tell us! (jake@burla.dev)\n\n"
        super().__init__(message)


class NoNodes(Exception):
    pass


class AllNodesBusy(Exception):
    def __init__(self):
        super().__init__("All nodes are busy, please try again later.")


class NoCompatibleNodes(Exception):
    def __init__(self, detail: dict | None = None):
        reason = detail.get("reason") if isinstance(detail, dict) else None
        if reason == "image_mismatch":
            requested = detail.get("requested_image")
            available = detail.get("available_images") or []
            message = f"\n\nNo ready nodes have the requested image `{requested}`.\n"
            if available:
                message += f"Images on currently-ready nodes: {available}.\n"
            message += "Pass `grow=True` to boot nodes with this image, "
            message += "or add it to your cluster config.\n"
        elif reason == "gpu_mismatch":
            requested = detail.get("requested_func_gpu")
            available = detail.get("available_machine_types") or []
            message = f"\n\nNo ready nodes match `func_gpu={requested!r}`.\n"
            if available:
                message += f"Machine types on currently-ready nodes: {available}.\n"
            message += "Pass `grow=True` to boot a GPU node, "
            message += "or add GPU machines to your cluster config.\n"
        else:
            message = "No compatible nodes available. Are the machines in your cluster large enough "
            message += "to support your `func_cpu` and `func_ram` arguments?"
        super().__init__(message)


class MainServiceTimeout(Exception):
    def __init__(self):
        message = (
            "\nTimeout talking to main_service.\n"
            "Please check that your cluster's dashboard URL is reachable, "
            "then run `burla login` and try again.\n"
        )
        super().__init__(message)


class NodeDisconnected(Exception):
    def __init__(self, node: "Node", message: str | None = None):
        self.node = node
        super().__init__(message or f"Node {node.instance_name} failed during job.")


class VersionMismatch(Exception):
    def __init__(self, lower_version: str, upper_version: str, current_version: str):
        msg = f"Incompatible cluster and client versions!\n"
        msg += f"This cluster supports clients v{lower_version} - v{upper_version}"
        msg += f", you have v{current_version}.\n"
        msg += f"To use Burla now, update using this command:\n\n"
        msg += f"    pip install burla=={upper_version}\n\n"
        msg += f"-------------------------------------------\n"
        super().__init__(msg)


class JobCanceled(Exception):
    pass


class JobStalled(Exception):
    pass


class ClusterRestarted(Exception):
    def __init__(self):
        message = "\n\nThe cluster was restarted. "
        message += "Your job was ended because the nodes it was running on were destroyed.\n"
        super().__init__(message)


class ClusterShutdown(Exception):
    def __init__(self):
        message = "\n\nThe cluster was shut down. "
        message += "Your job was ended because the nodes it was running on were destroyed.\n"
        super().__init__(message)


class UnauthorizedError(Exception):
    def __init__(self):
        super().__init__("Unauthorized! Please run `burla login` to authenticate.")


async def _post_with_retries(session, url, headers, data, max_retries=5):
    for attempt_index in range(max_retries):
        try:
            async with session.post(url, data=data, headers=headers) as response:
                return response.status, await response.text()
        except aiohttp.client_exceptions.ServerDisconnectedError:
            if attempt_index == max_retries - 1:
                raise
            await asyncio.sleep(0.5)


async def _run_network_request_with_retries(request_function, max_retries=NETWORK_RETRY_ATTEMPTS):
    last_error = None
    for attempt_index in range(max_retries):
        try:
            return await request_function()
        except NETWORK_ERROR_TYPES as error:
            last_error = error
            if attempt_index == max_retries - 1:
                raise last_error
            await asyncio.sleep(NETWORK_RETRY_DELAY_SECONDS)


async def _fetch_cluster_state(client: ClusterClient) -> dict:
    """One main_service call returns booting / running counts + ready-node docs."""
    try:
        return await asyncio.wait_for(client.get_cluster_state(), timeout=LOGIN_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        raise MainServiceTimeout()


async def wait_for_nodes_to_be_ready(
    client: ClusterClient,
    spinner: bool | Spinner,
) -> list[dict]:
    # `ready_nodes` from main_service is already filtered to unreserved -
    # see `cluster_state` in main_service/endpoints/client.py.
    state = await _fetch_cluster_state(client)
    n_booting_nodes = state["booting_count"]
    n_running_nodes = state["running_count"]
    ready_nodes = state["ready_nodes"]

    if n_running_nodes != 0:
        start_time = time()
        time_waiting = 0
        while n_running_nodes != 0:
            if spinner:
                msg = f"Waiting for {n_running_nodes} running nodes to become ready..."
                spinner.text = msg + f" (timeout in {4-time_waiting:.1f}s)"
            await asyncio.sleep(0.01)
            state = await _fetch_cluster_state(client)
            n_running_nodes = state["running_count"]
            ready_nodes = state["ready_nodes"]
            time_waiting = time() - start_time
            if time_waiting > 4:
                raise AllNodesBusy()
    elif n_booting_nodes != 0:
        while n_booting_nodes != 0:
            if spinner:
                msg = f"{len(ready_nodes)} Nodes are ready, waiting for remaining {n_booting_nodes}"
                spinner.text = msg + " to boot before starting ..."
            await asyncio.sleep(0.1)
            state = await _fetch_cluster_state(client)
            n_booting_nodes = state["booting_count"]
            ready_nodes = state["ready_nodes"]
        if not ready_nodes:
            main_service_url = get_cluster_dashboard_url()
            msg = "\n\nZero nodes are ready after Booting. Did they fail to boot?\n"
            msg += f"Check your clsuter dashboard at: {main_service_url}\n\n"
            raise NoNodes(msg)

    if n_booting_nodes == 0 and n_running_nodes == 0 and len(ready_nodes) == 0:
        main_service_url = get_cluster_dashboard_url()
        msg = "\n\nZero nodes are ready. Is your cluster turned on?\n"
        msg += f'Go to {main_service_url} and hit "⏻ Start" to turn it on!\n\n'
        raise NoNodes(msg)
    return ready_nodes


class Node:
    __init_token = object()

    def __init__(self, init_token, spinner, client: ClusterClient, session):
        if init_token is not Node.__init_token:
            raise RuntimeError("Use classmethods `from_ready` or `from_booting` to construct.")
        self.client = client
        self.session = session
        self.job_id = None
        self.udf_error_event = None
        self.current_parallelism = 0
        self.installing_packages = False
        self.result_count = 0
        self.last_reply_timestamp = time()
        self.last_result_poll_timestamp = None
        self.started_booting_at = time()
        self.auth_headers = get_auth_headers()
        self.removed_reason = ""
        self.spinner_compatible_print = (
            lambda msg: safe_spinner_write(spinner, msg) if spinner else safe_print(msg)
        )

    def _seconds_since_last_reply(self):
        return time() - self.last_reply_timestamp

    def _node_silence_timeout_exceeded(self):
        return self._seconds_since_last_reply() > NODE_SILENCE_TIMEOUT_SECONDS

    def _result_poll_silence_timeout_exceeded(self):
        return self._seconds_since_last_reply() > RESULT_POLL_SILENCE_TIMEOUT_SECONDS

    def _node_silence_timeout_message(self, action: str):
        if action == "returning results":
            timeout_minutes = RESULT_POLL_SILENCE_TIMEOUT_SECONDS // 60
        else:
            timeout_minutes = NODE_SILENCE_TIMEOUT_SECONDS // 60
        return f"Node {self.instance_name} has not replied for over {timeout_minutes} minutes while {action}.\n"

    def _diagnostic_summary(self) -> str:
        line = f"Node diagnostics: id={self.instance_name}, state={self.state}"
        line += f", result_count={self.result_count}, current_parallelism={self.current_parallelism}"
        line += f", seconds_since_last_reply={self._seconds_since_last_reply():.1f}"
        if self.host:
            line += f", host={self.host}"
        if self.last_result_poll_timestamp is not None:
            age = time() - self.last_result_poll_timestamp
            line += f", seconds_since_last_result_poll={age:.1f}"
        return line

    async def _failure_message(self, base_msg: str | None = None) -> str:
        base = base_msg or f"Node {self.instance_name} failed during job."
        reason = await self.client.get_node_fail_reason(self.instance_name)
        # Fall back to base so enrichment can never produce a worse error than before.
        if not reason:
            return base
        return f"{base}\n\nLast error reported by the node:\n{reason}"

    async def _stall_summary_line(self) -> str:
        line = f"  {self.instance_name}  state={self.state}  result_count={self.result_count}"
        if self.state == "FAILED":
            reason = await self.client.get_node_fail_reason(self.instance_name)
            if reason:
                line += f"\n    reason: {reason}"
        elif self.state == "REMOVED" and self.removed_reason:
            line += f"\n    reason: {self.removed_reason}"
        return line

    def _empty_node_results(self):
        return {
            "results": [],
            "current_parallelism": self.current_parallelism,
            "logs": [],
        }

    def _print_logs(self, log_documents: list):
        if self.udf_error_event is not None and self.udf_error_event.is_set():
            return
        for log_document in log_documents:
            for log in log_document.get("logs", []):
                message = log["message"].rstrip("\r\n")
                self.spinner_compatible_print(message)

    @classmethod
    def from_ready(
        cls,
        instance_name: str,
        host: str,
        machine_type: str,
        target_parallelism: int,
        session,
        client: ClusterClient,
        spinner: bool | Spinner,
    ):
        self = cls(Node.__init_token, spinner, client, session)
        self.state = "READY"
        self.instance_name = instance_name
        self.host = host
        self.machine_type = machine_type
        self.target_parallelism = target_parallelism
        return self

    @classmethod
    def from_booting(
        cls,
        instance_name: str,
        target_parallelism: int,
        session,
        client: ClusterClient,
        spinner: bool | Spinner,
    ):
        self = cls(Node.__init_token, spinner, client, session)
        self.state = "BOOTING"
        self.instance_name = instance_name
        self.host = None
        self.machine_type = None
        self.target_parallelism = target_parallelism
        return self

    async def _update_status(self):
        node_data = await self.client.get_node(self.instance_name)
        if node_data is None:
            # A 404 during BOOTING can race main_service's background write of
            # the initial firestore doc — the instance_name came back from
            # /v1/jobs/{id}/start before the doc landed. Not a real eviction.
            if self.state == "BOOTING":
                return
            self.state = "FAILED"
            return
        self.state = node_data["status"]
        if self.state == "READY":
            self.host = _local_host_from(node_data["host"])
            self.machine_type = node_data["machine_type"]

    async def _fail_and_delete(self, message: str):
        self.state = "FAILED"
        self.spinner_compatible_print(f"Marking Node {self.instance_name} as FAILED: {message}")
        try:
            await self.client.fail_node(self.instance_name, message)
        except Exception as error:
            msg = f"Failed to mark node {self.instance_name} as FAILED: {error}"
            self.spinner_compatible_print(msg)

    async def _assign_job(
        self,
        job_id: str,
        background: bool,
        n_inputs: int,
        packages: dict,
        func_ram: int | str,
        start_time: float,
        function_pkl: bytes,
        udf_error_event: Event,
        assigned_node_ids: list,
    ):
        request_json = {
            "parallelism": self.target_parallelism,
            "is_background_job": background,
            "user_python_version": f"3.{sys.version_info.minor}",
            "n_inputs": n_inputs,
            "packages": packages,
            "func_ram": func_ram,
            "start_time": start_time,
            "node_ids_expected": assigned_node_ids,
            "cluster_dashboard_url": self.client._url,
        }
        url = f"{self.host}/jobs/{job_id}"
        timeout = aiohttp.ClientTimeout(120)
        self.last_reply_timestamp = time()

        async def request_function():
            data = aiohttp.FormData()
            data.add_field("request_json", json.dumps(request_json))
            data.add_field("function_pkl", function_pkl)
            async with self.session.post(
                url,
                data=data,
                headers=self.auth_headers,
                timeout=timeout,
            ) as response:
                self.last_reply_timestamp = time()
                if response.status == 200:
                    self.job_id = job_id
                    self.udf_error_event = udf_error_event
                    self.state = "RUNNING"
                    return self
                elif response.status == 401:
                    raise UnauthorizedError()
                elif response.status == 409:
                    reason = (await response.text()).strip()
                    self.state = "REMOVED"
                    self.removed_reason = reason
                    msg = f"Node {self.instance_name} refused job assignment, removed from job."
                    if reason:
                        msg += f"\n  Reason from node: {reason}"
                    self.spinner_compatible_print(msg)
                    return
                elif response.status == 503:
                    self.state = "REMOVED"
                    msg = f"Node {self.instance_name} is shutting down, removed from job."
                    self.spinner_compatible_print(msg)
                    return
                else:
                    msg = f"Failed to assign {self.instance_name}: {response.status}"
                    raise Exception(msg)

        while True:
            try:
                return await _run_network_request_with_retries(request_function, max_retries=2)
            except NETWORK_ERROR_TYPES:
                if self._node_silence_timeout_exceeded():
                    await self._fail_and_delete(self._node_silence_timeout_message("assigning job"))
                    return

    async def _gather_results(self):
        url = f"{self.host}/jobs/{self.job_id}/results"
        self.last_result_poll_timestamp = time()

        async def request_function():
            return await self.session.get(
                url,
                headers=self.auth_headers,
                timeout=ClientTimeout(total=15),
            )

        try:
            response = await _run_network_request_with_retries(request_function)
            async with response:
                self.last_reply_timestamp = time()
                if response.status == 404:
                    self.state = "DONE"
                    return {
                        "results": [],
                        "current_parallelism": 0,
                        "logs": [],
                    }
                if response.status != 200:
                    raise Exception(f"Result-check failed for node: {self.instance_name}")
                try:
                    node_results = pickle.loads(await response.content.read())
                    self.last_reply_timestamp = time()
                except UnpicklingError as error:
                    if "Memo value not found at index" not in str(error):
                        raise error
                    job_doc = await self.client.get_job(self.job_id)
                    if job_doc and job_doc.get("status") == "CANCELED":
                        raise JobCanceled("Job canceled from dashboard.")
                    msg = f"Node {self.instance_name} disconnected while transmitting results.\n"
                    raise NodeDisconnected(self, await self._failure_message(msg))
        except NETWORK_ERROR_TYPES:
            if self._result_poll_silence_timeout_exceeded():
                msg = self._node_silence_timeout_message("returning results")
                raise NodeDisconnected(self, await self._failure_message(msg))
            return self._empty_node_results()

        if node_results.get("cluster_shutdown"):
            raise ClusterShutdown()
        if node_results.get("cluster_restarted"):
            raise ClusterRestarted()
        if node_results.get("dashboard_canceled"):
            raise JobCanceled("\n\nJob canceled from dashboard.\n")

        self._print_logs(node_results.get("logs", []))
        return node_results

    async def _upload_input_chunk(self, input_chunk: list):
        data = aiohttp.FormData()
        data.add_field("inputs_pkl_with_idx", pickle.dumps(input_chunk))
        status = 409
        retry_count = 0
        while status in [404, 409]:
            url = f"{self.host}/jobs/{self.job_id}/inputs"
            status, response_text = await _post_with_retries(
                session=self.session,
                url=url,
                headers=self.auth_headers,
                data=data,
            )
            if status in [404, 409]:
                retry_count += 1
                if retry_count > 60:
                    raise Exception(response_text)
                await asyncio.sleep(0.5)
            elif status >= 400:
                raise Exception(response_text)

    async def execute_job(
        self,
        job_id: str,
        background: bool,
        n_inputs: int,
        packages: dict,
        func_ram: int | str,
        start_time: float,
        function_pkl: bytes,
        udf_error_event: Event,
        inputs_with_indicies: list,
        return_queue: Queue,
        nodes: list["Node"],
        assigned_node_ids: list,
        first_chunk_barrier: asyncio.Barrier | None,
    ):
        was_initially_ready = self.state == "READY"
        # wait until ready
        if self.state != "READY":
            await asyncio.sleep(max(0, 30 - (time() - start_time)))
            while self.state == "BOOTING":
                await self._update_status()
                if self.state == "BOOTING":
                    if (time() - self.started_booting_at) > NODE_BOOT_DEADLINE_SEC:
                        self.state = "FAILED"
                        break
                    await asyncio.sleep(random.uniform(2, 6))
            if self.state != "READY":
                return

        if packages:
            self.installing_packages = True
        await self._assign_job(
            job_id,
            background,
            n_inputs,
            packages,
            func_ram,
            start_time,
            function_pkl,
            udf_error_event,
            assigned_node_ids,
        )
        self.installing_packages = False
        if self.state in ("FAILED", "REMOVED"):
            if was_initially_ready and first_chunk_barrier:
                await first_chunk_barrier.abort()
            return

        while True:
            total_parallelism = sum(
                node.target_parallelism for node in nodes if node.state in ("READY", "RUNNING")
            ) or 1
            input_chunksize = max(
                self.target_parallelism,
                (n_inputs * self.target_parallelism) // total_parallelism,
            )
            input_chunk = []
            chunk_size_bytes = 0
            while inputs_with_indicies and len(input_chunk) < input_chunksize:
                input_index, input_ = inputs_with_indicies.pop()
                input_pkl = cloudpickle.dumps(input_)
                if len(input_pkl) > MAX_INPUT_SIZE_BYTES:
                    raise InputTooBig(input_index)
                if input_chunk and chunk_size_bytes + len(input_pkl) > MAX_CHUNK_SIZE_BYTES:
                    inputs_with_indicies.append((input_index, input_))
                    break
                input_chunk.append((input_index, input_pkl))
                chunk_size_bytes += len(input_pkl)

            if input_chunk:
                await self._upload_input_chunk(input_chunk)

            if was_initially_ready and first_chunk_barrier:
                try:
                    await first_chunk_barrier.wait()
                except asyncio.BrokenBarrierError:
                    pass
                first_chunk_barrier = None

            node_results = await self._gather_results()
            return_values = []
            for input_index, is_error, result_pkl in node_results["results"]:
                if is_error:
                    error_info = pickle.loads(result_pkl)
                    if error_info.get("is_infrastructure_error"):
                        msg = f"Worker on node {self.instance_name} failed "
                        msg += f"while executing input index {input_index}:\n\n"
                        msg += error_info["traceback_str"]
                        raise NodeDisconnected(self, await self._failure_message(msg))
                    traceback = Traceback.from_dict(error_info["traceback_dict"]).as_traceback()
                    self.udf_error_event.set()
                    log_error = RemoteParallelMapReporter.log_user_function_error_async
                    await log_error(self.job_id, self.session)
                    exc = error_info["exception"].with_traceback(traceback)
                    # Preserve the failing input index on the exception so callers
                    # can identify the bad item in a large batch; add a 3.11+
                    # note so it is visible in the default traceback. Guarded
                    # because some exception types disallow attribute writes.
                    try:
                        exc.burla_input_index = input_index
                        if hasattr(exc, "add_note"):
                            exc.add_note(f"[burla] failed on input index {input_index}")
                    except Exception:
                        pass
                    raise exc
                else:
                    return_values.append(cloudpickle.loads(result_pkl))

            self.current_parallelism = node_results["current_parallelism"]

            for return_value in return_values:
                return_queue.put_nowait(return_value)
                self.result_count += 1
            if self.state == "DONE":
                return
            await asyncio.sleep(0.05)
