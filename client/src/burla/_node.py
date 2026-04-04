import asyncio
import json
import pickle
import sys
from pickle import UnpicklingError
from time import time

import aiohttp
import cloudpickle
from aiohttp import ClientError, ClientOSError, ClientTimeout
from google.cloud.firestore import FieldFilter
from google.cloud.firestore_v1.async_client import AsyncClient
from packaging.version import Version
from six import reraise
from tblib import Traceback
from yaspin import Spinner

from burla import CONFIG_PATH, __version__
from burla._helpers import parallelism_capacity
from burla._reporting import RemoteParallelMapReporter


NODE_SILENCE_TIMEOUT_SECONDS = 10 * 60
LOGIN_TIMEOUT_SEC = 3


class NodeConflict(Exception):
    def __init__(self, instance_name: str, response_text: str):
        message = f"ERROR from {instance_name}: {response_text}"
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


class UnPickleableUserFunctionException(Exception):
    def __init__(self, traceback_str: str):
        message = "\nThis exception had to be sent to your machine as a string:\n\n"
        message += f"{traceback_str}\n"
        super().__init__(message)


class UnauthorizedError(Exception):
    def __init__(self):
        super().__init__("Unauthorized! Please run `burla login` to authenticate.")


async def get_with_retries(session, url: str, headers: dict, max_retries=5):
    try:
        return await session.get(url, headers=headers, timeout=ClientTimeout(total=60))
    except asyncio.TimeoutError:
        if max_retries <= 1:
            raise
        await asyncio.sleep(1)
        return await get_with_retries(session, url, headers, max_retries=max_retries - 1)
    except (ClientOSError, ClientError, OSError) as error:
        if max_retries <= 1 or "Protocol wrong type for socket" not in str(error):
            raise
        await asyncio.sleep(1)
        return await get_with_retries(session, url, headers, max_retries=max_retries - 1)


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
    return [document.to_dict() for document in docs]


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
    auth_headers: dict,
) -> tuple[list["Node"], int]:
    ready_nodes = await get_ready_nodes(db)
    if not ready_nodes:
        ready_nodes = await wait_for_nodes_to_be_ready(db=db, spinner=spinner)

    upper_version = Version(ready_nodes[0]["main_svc_version"])
    lower_version = Version(ready_nodes[0]["min_compatible_client_version"])
    current_version = Version(__version__)
    if not lower_version <= current_version <= upper_version:
        raise VersionMismatch(
            lower_version=lower_version,
            upper_version=upper_version,
            current_version=current_version,
        )

    planned_initial_job_parallelism = 0
    nodes_to_assign = []
    for node_data in ready_nodes:
        parallelism_deficit = max_parallelism - planned_initial_job_parallelism
        max_node_parallelism = parallelism_capacity(node_data["machine_type"], func_cpu, func_ram)

        if max_node_parallelism > 0 and parallelism_deficit > 0:
            node_target_parallelism = min(parallelism_deficit, max_node_parallelism)
            planned_initial_job_parallelism += node_target_parallelism
            host = node_data["host"]
            if host.startswith("http://node_"):
                host = f"http://localhost:{host.split(':')[-1]}"
            node = Node(
                instance_name=node_data["instance_name"],
                host=host,
                machine_type=node_data["machine_type"],
                target_parallelism=node_target_parallelism,
                session=session,
                auth_headers=auth_headers,
                async_db=db,
                spinner=spinner,
            )
            nodes_to_assign.append(node)

    if len(nodes_to_assign) == 0:
        raise NoCompatibleNodes()

    return nodes_to_assign, planned_initial_job_parallelism


class Node:
    def __init__(
        self,
        instance_name: str,
        host: str,
        machine_type: str,
        target_parallelism: int,
        session=None,
        auth_headers: dict | None = None,
        async_db=None,
        spinner: bool | Spinner = False,
    ):
        self.instance_name = instance_name
        self.host = host
        self.machine_type = machine_type
        self.target_parallelism = target_parallelism
        self.session = session
        self.auth_headers = auth_headers
        self.async_db = async_db
        self.spinner_compatible_print = lambda msg: spinner.write(msg) if spinner else print(msg)
        self.input_chunks = None

    async def assign(
        self,
        job_id: str,
        background: bool,
        n_inputs: int,
        packages: list,
        start_time: float,
        function_pkl: bytes,
    ) -> "Node | None":
        request_json = {
            "parallelism": self.target_parallelism,
            "is_background_job": background,
            "user_python_version": f"3.{sys.version_info.minor}",
            "n_inputs": n_inputs,
            "packages": packages,
            "start_time": start_time,
        }
        data = aiohttp.FormData()
        data.add_field("request_json", json.dumps(request_json))
        data.add_field("function_pkl", function_pkl)
        url = f"{self.host}/jobs/{job_id}"
        request = self.session.post(
            url,
            data=data,
            headers=self.auth_headers,
            timeout=aiohttp.ClientTimeout(300),
        )
        try:
            async with request as response:
                if response.status == 200:
                    return self
                elif response.status == 401:
                    raise UnauthorizedError()
                elif response.status == 409:
                    raise NodeConflict(
                        instance_name=self.instance_name,
                        response_text=await response.text(),
                    )
                else:
                    msg = f"Failed to assign {self.instance_name}! ignoring: {response.status}"
                    self.spinner_compatible_print(msg)
        except asyncio.TimeoutError:
            msg = f"Timeout assigning {self.instance_name} to job! Failing node ..."
            self.spinner_compatible_print(msg)
            try:
                node_doc = self.async_db.collection("nodes").document(self.instance_name)
                await node_doc.update({"status": "FAILED", "display_in_dashboard": True})
                msg = f"Failed! This node didn't respond (in<300s) to client request to assign job."
                await node_doc.collection("logs").document().set({"msg": msg, "ts": time()})
                main_service_url = json.loads(CONFIG_PATH.read_text())["cluster_dashboard_url"]
                url = f"{main_service_url}/v1/cluster/{self.instance_name}"
                url += "?hide_if_failed=false"
                async with self.session.delete(
                    url,
                    headers=self.auth_headers,
                    timeout=1,
                ) as response:
                    if response.status != 200:
                        msg = f"Failed to delete node {self.instance_name}."
                        self.spinner_compatible_print(msg + f" ignoring: {response.status}")
            except:
                pass

    async def _get_raw_results(
        self,
        job_id: str,
        nodes: list,
        node_last_reply_timestamp: dict,
        job_ref,
    ):
        try:
            url = f"{self.host}/jobs/{job_id}/results"
            args = (self.session, url, self.auth_headers)
            async with await get_with_retries(*args) as response:
                if response.status == 404:
                    if self in nodes:
                        nodes.remove(self)  # <- node is likely rebooting/failed/done
                    node_last_reply_timestamp.pop(self.instance_name, None)
                    return None
                if response.status != 200:
                    raise Exception(f"Result-check failed for node: {self.instance_name}")
                node_last_reply_timestamp[self.instance_name] = time()
                try:
                    node_status = pickle.loads(await response.content.read())
                    node_last_reply_timestamp[self.instance_name] = time()
                    return node_status
                except UnpicklingError as error:
                    if "Memo value not found at index" not in str(error):
                        raise error
                    if (await job_ref.get()).to_dict()["status"] == "CANCELED":
                        raise JobCanceled("Job canceled from dashboard.")
                    msg = f"Node {self.instance_name} disconnected while transmitting results.\n"
                    raise NodeDisconnected(msg)
        except asyncio.TimeoutError:
            last_reply_timestamp = node_last_reply_timestamp[self.instance_name]
            seconds_since_last_reply = time() - last_reply_timestamp
            if seconds_since_last_reply > NODE_SILENCE_TIMEOUT_SECONDS:
                msg = f"Node {self.instance_name} has not replied for over 10 minutes!\n"
                raise NodeDisconnected(msg)
            return None

    async def get_results(
        self,
        job_id: str,
        nodes: list,
        node_last_reply_timestamp: dict,
        job_ref,
        user_function_error_event,
        project_id: str,
    ):
        node_status = await self._get_raw_results(
            job_id=job_id,
            nodes=nodes,
            node_last_reply_timestamp=node_last_reply_timestamp,
            job_ref=job_ref,
        )
        if not node_status:
            return None

        return_values = []
        for input_index, is_error, result_pkl in node_status["results"]:
            if is_error:
                error_info = pickle.loads(result_pkl)
                if error_info.get("traceback_dict"):
                    traceback = Traceback.from_dict(error_info["traceback_dict"]).as_traceback()
                    user_function_error_event.set()
                    log_error = RemoteParallelMapReporter.log_user_function_error_async
                    await log_error(job_id, self.session, project_id)
                    reraise(tp=error_info["type"], value=error_info["exception"], tb=traceback)
                raise UnPickleableUserFunctionException(error_info["traceback_str"])
            else:
                return_values.append(cloudpickle.loads(result_pkl))

        return {
            "udf_start_latency": node_status.get("udf_start_latency"),
            "packages_to_install": node_status.get("packages_to_install"),
            "all_packages_installed": node_status.get("all_packages_installed"),
            "is_empty": node_status["is_empty"],
            "current_parallelism": node_status["current_parallelism"],
            "currently_installing_package": node_status["currently_installing_package"],
            "return_values": return_values,
        }
