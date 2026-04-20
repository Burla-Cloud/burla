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
from google.cloud.firestore import FieldFilter
from google.cloud.firestore_v1.async_client import AsyncClient
from packaging.version import Version
from six import reraise
from tblib import Traceback
from yaspin import Spinner

from burla import CONFIG_PATH, __version__
from burla._auth import get_auth_headers
from burla._helpers import parallelism_capacity
from burla._reporting import RemoteParallelMapReporter


NODE_SILENCE_TIMEOUT_SECONDS = 2 * 60
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
    def __init__(self):
        message = "No compatible nodes available. Are the machines in your cluster large enough to "
        message += "support your `func_cpu` and `func_ram` arguments?"
        super().__init__(message)


class FirestoreTimeout(Exception):
    def __init__(self):
        message = "\nTimeout waiting for DB.\nPlease run `burla login` and try again.\n"
        super().__init__(message)


class NodeDisconnected(Exception):
    pass


class VersionMismatch(Exception):
    def __init__(self, lower_version: Version, upper_version: Version, current_version: Version):
        msg = f"Incompatible cluster and client versions!\n"
        msg += f"This cluster supports clients v{lower_version} - v{upper_version}"
        msg += f", you have v{current_version}.\n"
        msg += f"To use Burla now, update using this command:\n\n"
        msg += f"    pip install burla=={upper_version}\n\n"
        msg += f"-------------------------------------------\n"
        super().__init__(msg)


class JobCanceled(Exception):
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


async def num_booting_nodes(db: AsyncClient):
    filter_ = FieldFilter("status", "==", "BOOTING")
    nodes_snapshot = await db.collection("nodes").where(filter=filter_).get()
    return len(nodes_snapshot)


async def num_running_nodes(db: AsyncClient):
    filter_ = FieldFilter("status", "==", "RUNNING")
    nodes_snapshot = await db.collection("nodes").where(filter=filter_).get()
    return len(nodes_snapshot)


async def get_ready_nodes(db: AsyncClient) -> list[dict]:
    status_filter = FieldFilter("status", "==", "READY")
    ready_nodes_coroutine = db.collection("nodes").where(filter=status_filter).get()
    try:
        docs = await asyncio.wait_for(ready_nodes_coroutine, timeout=LOGIN_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        raise FirestoreTimeout()
    return [
        document.to_dict() for document in docs if not document.to_dict().get("reserved_for_job")
    ]


async def wait_for_nodes_to_be_ready(
    db: AsyncClient,
    spinner: bool | Spinner,
) -> list[dict]:
    n_booting_nodes = await num_booting_nodes(db)
    n_running_nodes = await num_running_nodes(db)

    if n_running_nodes != 0:
        start_time = time()
        time_waiting = 0
        while n_running_nodes != 0:
            if spinner:
                msg = f"Waiting for {n_running_nodes} running nodes to become ready..."
                spinner.text = msg + f" (timeout in {4-time_waiting:.1f}s)"
            await asyncio.sleep(0.01)
            n_running_nodes = await num_running_nodes(db)
            ready_nodes = await get_ready_nodes(db)
            time_waiting = time() - start_time
            if time_waiting > 4:
                raise AllNodesBusy()
    elif n_booting_nodes != 0:
        ready_nodes = await get_ready_nodes(db)
        while n_booting_nodes != 0:
            if spinner:
                msg = f"{len(ready_nodes)} Nodes are ready, waiting for remaining {n_booting_nodes}"
                spinner.text = msg + " to boot before starting ..."
            await asyncio.sleep(0.1)
            n_booting_nodes = await num_booting_nodes(db)
            ready_nodes = await get_ready_nodes(db)
        if not ready_nodes:
            main_service_url = json.loads(CONFIG_PATH.read_text())["cluster_dashboard_url"]
            msg = "\n\nZero nodes are ready after Booting. Did they fail to boot?\n"
            msg += f"Check your clsuter dashboard at: {main_service_url}\n\n"
            raise NoNodes(msg)

    ready_nodes = await get_ready_nodes(db)
    if n_booting_nodes == 0 and n_running_nodes == 0 and len(ready_nodes) == 0:
        main_service_url = json.loads(CONFIG_PATH.read_text())["cluster_dashboard_url"]
        msg = "\n\nZero nodes are ready. Is your cluster turned on?\n"
        msg += f'Go to {main_service_url} and hit "⏻ Start" to turn it on!\n\n'
        raise NoNodes(msg)
    return ready_nodes


async def select_nodes_to_assign_to_job(
    db: AsyncClient,
    max_parallelism: int,
    func_cpu: int,
    func_ram: int,
    spinner: bool | Spinner,
    session,
) -> tuple[list["Node"], int]:
    ready_nodes = await get_ready_nodes(db)

    if not ready_nodes:
        ready_nodes = await wait_for_nodes_to_be_ready(db=db, spinner=spinner)

    upper_version = Version(ready_nodes[0]["main_svc_version"])
    lower_version = Version(ready_nodes[0]["min_compatible_client_version"])
    current_version = Version(__version__)
    if not lower_version <= current_version <= upper_version:
        raise VersionMismatch(lower_version, upper_version, current_version)

    planned_initial_job_parallelism = 0
    nodes_to_assign = []
    for node_data in ready_nodes:
        parallelism_deficit = max_parallelism - planned_initial_job_parallelism
        max_node_parallelism = parallelism_capacity(node_data["machine_type"], func_cpu, func_ram)

        if max_node_parallelism > 0 and parallelism_deficit > 0:
            node_target_parallelism = max_node_parallelism
            planned_initial_job_parallelism += node_target_parallelism
            host = node_data["host"]
            if host.startswith("http://node_"):
                host = f"http://localhost:{host.split(':')[-1]}"
            node = Node.from_ready(
                instance_name=node_data["instance_name"],
                host=host,
                machine_type=node_data["machine_type"],
                target_parallelism=node_target_parallelism,
                session=session,
                async_db=db,
                spinner=spinner,
            )
            nodes_to_assign.append(node)

    if len(nodes_to_assign) == 0:
        raise NoCompatibleNodes()

    return nodes_to_assign, planned_initial_job_parallelism


class Node:
    __init_token = object()

    def __init__(self, init_token, spinner, async_db, session):
        if init_token is not Node.__init_token:
            raise RuntimeError("Use classmethods `from_ready` or `from_booting` to construct.")
        self.async_db = async_db
        self.session = session
        self.job_id = None
        self.udf_error_event = None
        self.current_parallelism = 0
        self.installing_packages = False
        self.result_count = 0
        self.last_reply_timestamp = time()
        self.auth_headers = get_auth_headers()
        self.spinner_compatible_print = lambda msg: spinner.write(msg) if spinner else print(msg)

    def _seconds_since_last_reply(self):
        return time() - self.last_reply_timestamp

    def _node_silence_timeout_exceeded(self):
        return self._seconds_since_last_reply() > NODE_SILENCE_TIMEOUT_SECONDS

    def _node_silence_timeout_message(self, action: str):
        return f"Node {self.instance_name} has not replied for over 2 minutes while {action}.\n"

    def _empty_node_results(self):
        return {
            "results": [],
            "current_parallelism": self.current_parallelism,
        }

    @classmethod
    def from_ready(
        cls,
        instance_name: str,
        host: str,
        machine_type: str,
        target_parallelism: int,
        session,
        async_db,
        spinner: bool | Spinner,
    ):
        self = cls(Node.__init_token, spinner, async_db, session)
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
        async_db,
        spinner: bool | Spinner,
    ):
        self = cls(Node.__init_token, spinner, async_db, session)
        self.state = "BOOTING"
        self.instance_name = instance_name
        self.host = None
        self.machine_type = None
        self.target_parallelism = target_parallelism
        return self

    async def _update_status(self):
        node_ref = self.async_db.collection("nodes").document(self.instance_name)
        node_data = (await node_ref.get()).to_dict()
        self.state = node_data["status"]
        if self.state == "READY":
            host = node_data["host"]
            if host.startswith("http://node_"):
                host = f"http://localhost:{host.split(':')[-1]}"
            self.host = host
            self.machine_type = node_data["machine_type"]

    async def _fail_and_delete(self, message: str):
        try:
            self.state = "FAILED"
            self.spinner_compatible_print(f"Marking Node {self.instance_name} as FAILED: {message}")
            node_doc = self.async_db.collection("nodes").document(self.instance_name)
            await node_doc.update({"status": "FAILED"})
            await node_doc.collection("logs").document().set({"msg": message, "ts": time()})
            main_service_url = json.loads(CONFIG_PATH.read_text())["cluster_dashboard_url"]
            url = f"{main_service_url}/v1/cluster/{self.instance_name}"
            async with self.session.delete(url, headers=self.auth_headers, timeout=1) as response:
                if response.status != 200:
                    msg = f"Failed to delete node {self.instance_name}."
                    self.spinner_compatible_print(msg + f" ignoring: {response.status}")
        except Exception:
            pass

    async def _assign_job(
        self,
        job_id: str,
        background: bool,
        n_inputs: int,
        packages: dict,
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
            "start_time": start_time,
            "node_ids_expected": assigned_node_ids,
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
                    self.state = "REMOVED"
                    msg = f"Node {self.instance_name} refused job assignment, removed from job."
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
                    }
                if response.status != 200:
                    raise Exception(f"Result-check failed for node: {self.instance_name}")
                try:
                    node_results = pickle.loads(await response.content.read())
                    self.last_reply_timestamp = time()
                except UnpicklingError as error:
                    if "Memo value not found at index" not in str(error):
                        raise error
                    job_ref = self.async_db.collection("jobs").document(self.job_id)
                    if (await job_ref.get()).to_dict()["status"] == "CANCELED":
                        raise JobCanceled("Job canceled from dashboard.")
                    msg = f"Node {self.instance_name} disconnected while transmitting results.\n"
                    raise NodeDisconnected(msg)
        except NETWORK_ERROR_TYPES:
            if self._node_silence_timeout_exceeded():
                raise NodeDisconnected(self._node_silence_timeout_message("returning results"))
            return self._empty_node_results()

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
            while self.state != "READY":
                await self._update_status()
                if self.state == "READY":
                    break
                await asyncio.sleep(random.uniform(2, 6))

        if packages:
            self.installing_packages = True
        await self._assign_job(
            job_id,
            background,
            n_inputs,
            packages,
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
            n_ready_nodes = sum(1 for node in nodes if node.state in ("READY", "RUNNING"))
            input_chunksize = max(self.target_parallelism, n_inputs // n_ready_nodes)
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
                        msg += "(the cluster may have been restarted):\n\n"
                        msg += error_info["traceback_str"]
                        raise NodeDisconnected(msg)
                    traceback = Traceback.from_dict(error_info["traceback_dict"]).as_traceback()
                    self.udf_error_event.set()
                    log_error = RemoteParallelMapReporter.log_user_function_error_async
                    await log_error(self.job_id, self.session)
                    reraise(tp=error_info["type"], value=error_info["exception"], tb=traceback)
                else:
                    return_values.append(cloudpickle.loads(result_pkl))

            self.current_parallelism = node_results["current_parallelism"]

            for return_value in return_values:
                return_queue.put_nowait(return_value)
                self.result_count += 1
            if self.state == "DONE":
                return
            await asyncio.sleep(0.05)
