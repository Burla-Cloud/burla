import asyncio
import os
import pickle
import signal
import socket
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import aiodocker
import psutil
from tblib import Traceback

from node_service import SELF, ASYNC_DB, INSTANCE_NAME, IN_LOCAL_DEV_MODE, NUM_GPUS, __version__

RESULTS_QUEUE_RAM_LIMIT_BYTES = int(psutil.virtual_memory().total * 0.5)

WORKER_INTERNAL_PORT = 8080
LOG_FLUSH_INTERVAL_SECONDS = 1
MAX_LOG_DOCUMENT_SIZE_BYTES = 100_000
TRUNCATED_LOG_SUFFIX = "<too-long--remaining-msg-truncated-due-to-length>"
LOG_START_MARKER_PREFIX = "__burla_input_start__:"
LOG_END_MARKER_PREFIX = "__burla_input_end__:"

# The first worker on a fresh VM downloads uv from GitHub and installs cloudpickle/tblib into
# /worker_service_python_env before opening its socket. Under any network slowness this can take
# well over 10 seconds; 10s was causing ~15% of initial boots to fail.
WORKER_BOOT_TIMEOUT_SECONDS = 20


class JobLogWriter:
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.lock = asyncio.Lock()
        self.stop_event = asyncio.Event()
        self.pending_flush_event = asyncio.Event()
        self.logs_collection = ASYNC_DB.collection("jobs").document(job_id).collection("logs")
        self.log_buffers = {}
        self.pending_documents = []
        self.active_input_index = None
        self.partial_container_output = ""
        self.flush_task = asyncio.create_task(self._flush_loop())

    def _get_log_buffer(self, input_index: int):
        if input_index not in self.log_buffers:
            self.log_buffers[input_index] = {"logs": [], "size_bytes": 0}
        return self.log_buffers[input_index]

    def _truncate_message(self, message: str):
        message_size = len(message.encode("utf-8")) + 180
        if message_size <= MAX_LOG_DOCUMENT_SIZE_BYTES:
            return message
        max_bytes = MAX_LOG_DOCUMENT_SIZE_BYTES - len(TRUNCATED_LOG_SUFFIX.encode("utf-8"))
        truncated_bytes = message.encode("utf-8")[:max_bytes]
        truncated_message = truncated_bytes.decode("utf-8", errors="ignore")
        return truncated_message + TRUNCATED_LOG_SUFFIX

    def _queue_document_locked(self, input_index: int, is_error: bool = False):
        log_buffer = self.log_buffers.get(input_index)
        if not log_buffer or not log_buffer["logs"]:
            return
        document = {
            "logs": log_buffer["logs"],
            "timestamp": datetime.now(timezone.utc),
            "input_index": input_index,
        }
        if is_error:
            document["is_error"] = True
        self.pending_documents.append(document)
        self.log_buffers[input_index] = {"logs": [], "size_bytes": 0}

    def _queue_all_buffers_locked(self):
        for input_index in list(self.log_buffers):
            self._queue_document_locked(input_index)

    def _write_locked(self, input_index: int, message: str, timestamp: datetime):
        if not message.strip():
            return
        message = self._truncate_message(message)
        message_size = len(message.encode("utf-8")) + 180
        log_buffer = self._get_log_buffer(input_index)
        if log_buffer["size_bytes"] and (
            log_buffer["size_bytes"] + message_size > MAX_LOG_DOCUMENT_SIZE_BYTES
        ):
            self._queue_document_locked(input_index)
            log_buffer = self._get_log_buffer(input_index)
        log_buffer["logs"].append({"timestamp": timestamp, "message": message})
        log_buffer["size_bytes"] += message_size
        if log_buffer["size_bytes"] >= MAX_LOG_DOCUMENT_SIZE_BYTES:
            self._queue_document_locked(input_index)
            self.pending_flush_event.set()

    def _parse_container_log_line(self, container_log_line: str):
        timestamp_string, _, message = container_log_line.partition(" ")
        timestamp = datetime.fromisoformat(timestamp_string.replace("Z", "+00:00"))
        return timestamp, message

    def _capture_container_log_line_locked(self, container_log_line: str):
        timestamp, message = self._parse_container_log_line(container_log_line)
        stripped_message = message.strip()
        if stripped_message.startswith(LOG_START_MARKER_PREFIX):
            self.active_input_index = int(stripped_message.removeprefix(LOG_START_MARKER_PREFIX))
            return
        if stripped_message.startswith(LOG_END_MARKER_PREFIX):
            input_index = int(stripped_message.removeprefix(LOG_END_MARKER_PREFIX))
            self._queue_document_locked(input_index)
            self.active_input_index = None
            self.pending_flush_event.set()
            return
        if self.active_input_index is None:
            return
        self._write_locked(self.active_input_index, message, timestamp)

    async def capture_container_output(self, container_output_chunk: str):
        async with self.lock:
            complete_output = self.partial_container_output + container_output_chunk
            output_lines = complete_output.splitlines(keepends=True)
            if output_lines and not output_lines[-1].endswith(("\n", "\r")):
                self.partial_container_output = output_lines.pop()
            else:
                self.partial_container_output = ""
            for output_line in output_lines:
                self._capture_container_log_line_locked(output_line)

    async def write_error(self, input_index: int, traceback_str: str):
        async with self.lock:
            self.pending_documents.append(
                {
                    "logs": [{"timestamp": datetime.now(timezone.utc), "message": traceback_str}],
                    "timestamp": datetime.now(timezone.utc),
                    "input_index": input_index,
                    "is_error": True,
                }
            )
            self.pending_flush_event.set()

    async def finish_input(self, input_index: int):
        async with self.lock:
            self._queue_document_locked(input_index)
            self.pending_flush_event.set()

    async def _flush_pending_documents(self):
        async with self.lock:
            self._queue_all_buffers_locked()
            if not self.pending_documents:
                return
            documents = self.pending_documents
            self.pending_documents = []

        for document in documents:
            if not document.get("is_error"):
                SELF["pending_logs"].append(document)

        batch = ASYNC_DB.batch()
        for document in documents:
            batch.set(self.logs_collection.document(), document)
        await batch.commit()

    async def _flush_loop(self):
        while True:
            try:
                await asyncio.wait_for(
                    self.pending_flush_event.wait(), timeout=LOG_FLUSH_INTERVAL_SECONDS
                )
            except asyncio.TimeoutError:
                pass
            self.pending_flush_event.clear()
            await self._flush_pending_documents()
            if self.stop_event.is_set():
                break

    async def stop(self):
        self.stop_event.set()
        self.pending_flush_event.set()
        await self.flush_task


def _worker_oom_error():
    return RuntimeError(
        "\n\nWorker container was killed by the Linux OOM killer.\n"
        "This usually means the submitted function used more memory than the container had available.\n"
        "Increase the container memory limit or reduce memory usage inside the function.\n"
    )


def _worker_boot_timeout_error(logs: str):
    message = f"\n\nWorker boot timed out after {WORKER_BOOT_TIMEOUT_SECONDS} seconds.\n"
    message += "The worker container never became ready to accept connections.\n"
    message += "\nBuffered worker logs:\n"
    message += "---------------------\n"
    message += f"{logs}\n"
    return RuntimeError(message)


class WorkerClient:
    def __init__(self, image: str):
        self.container_name = f"worker_{uuid4().hex[:8]}"
        self.container_name += f"--node_{INSTANCE_NAME[11:]}" if IN_LOCAL_DEV_MODE else ""
        self.port = None
        self.image = image
        self.docker = aiodocker.Docker()
        self.is_idle = True
        self.python_version = None
        self.container = None
        self.container_id = None
        self.logstream_task = None
        self.reader = None
        self.writer = None
        self.process_inputs_task = None
        self.log_writer = None
        self.worker_host_pid = None

    def _worker_server_host_path(self):
        if IN_LOCAL_DEV_MODE:
            return f"{os.environ['HOST_PWD']}/node_service/src/node_service/worker_server.py"
        return str(Path(__file__).resolve().parent / "worker_server.py")

    async def _start_container(self):
        binds = [f"{self._worker_server_host_path()}:/opt/burla/worker_server.py"]

        host_config = {
            "PortBindings": {f"{WORKER_INTERNAL_PORT}/tcp": [{"HostIp": "127.0.0.1"}]},
            "ShmSize": 16 * 1024**3,
        }

        # node_auth bind: see NODE_AUTH_DIR in node_service/__init__.py.
        if IN_LOCAL_DEV_MODE:
            host_pwd = os.environ["HOST_PWD"]
            host_home_dir = os.environ["HOST_HOME_DIR"]
            worker_python_environment_dir = f"{host_pwd}/_worker_service_python_env/{INSTANCE_NAME}"
            host_config["NetworkMode"] = "local-burla-cluster"
            binds.extend(
                [
                    f"{host_home_dir}/.config/gcloud:/root/.config/gcloud",
                    f"{host_pwd}/_shared_workspace:/workspace/shared",
                    f"{worker_python_environment_dir}:/worker_service_python_env",
                    f"{host_pwd}/_node_auth:/root/.config/burla",
                ]
            )
        else:
            if NUM_GPUS != 0:
                host_config["DeviceRequests"] = [{"Count": -1, "Capabilities": [["gpu"]]}]
                host_config["Runtime"] = "nvidia"
            binds.extend(
                [
                    "/worker_service_python_env:/worker_service_python_env",
                    "/workspace/shared:/workspace/shared",
                    "/opt/burla/node_auth:/root/.config/burla",
                ]
            )

        host_config["Binds"] = binds

        # Shell loop keeps PID 1 alive so os.killpg against worker_server.py's process group from
        # the host only restarts Python, not the whole container. sleep 0.1 guards a crash loop.
        command = [
            "sh",
            "-lc",
            (
                "export PYTHONUNBUFFERED=1; "
                "export PYTHONPATH=/worker_service_python_env; "
                'export PATH="/worker_service_python_env/bin:$PATH"; '
                f"while true; do python /opt/burla/worker_server.py {WORKER_INTERNAL_PORT} {__version__}; sleep 0.1; done"
            ),
        ]

        config = {
            "Image": self.image,
            "Cmd": command,
            "WorkingDir": "/workspace",
            "ExposedPorts": {f"{WORKER_INTERNAL_PORT}/tcp": {}},
            "HostConfig": host_config,
        }

        self.container = await self.docker.containers.run(config=config, name=self.container_name)
        self.container_id = self.container.id

    async def _get_host_port(self):
        for _ in range(20):
            port_info = await self.container.port(WORKER_INTERNAL_PORT)
            if port_info:
                return int(port_info[0]["HostPort"])
            await asyncio.sleep(0.5)
        raise RuntimeError(f"Failed to get port for container {self.container_name} in 10s")

    async def _get_worker_host_pid(self) -> int:
        # Docker's /top endpoint returns host PIDs of every process in the container.
        # aiodocker doesn't expose a wrapper for it so we call it via the internal client.
        data = await self.docker._query_json(f"containers/{self.container_id}/top", method="GET")
        for row in data.get("Processes", []):
            cmd = row[-1]
            # The shell wrapper's CMD also contains worker_server.py because the script text
            # embeds that path. Skip the wrapper and match only the actual python invocation.
            if "while true" in cmd:
                continue
            if "worker_server.py" in cmd:
                return int(row[1])
        raise RuntimeError(f"worker_server.py not found in {self.container_name}")

    async def _get_python_version(self):
        for _ in range(20):
            logs = await self._get_logs()
            if logs:
                return logs.splitlines()[0].strip()
            await asyncio.sleep(0.1)
        raise RuntimeError(f"Failed to get python version for {self.container_name}.")

    async def _handle_container_logs(self):
        async for log_line in self.container.log(
            stdout=True, stderr=True, follow=True, timestamps=True
        ):
            self._capture_container_output_chunk(log_line)

    def _capture_container_output_chunk(self, container_output_chunk: str):
        if self.log_writer is None:
            return
        asyncio.create_task(self.log_writer.capture_container_output(container_output_chunk))

    async def _ensure_log_writer(self):
        current_job = SELF["current_job"]
        if current_job is None:
            return None
        if self.log_writer is not None and self.log_writer.job_id == current_job:
            return self.log_writer
        if self.log_writer is not None:
            await self.log_writer.stop()
        self.log_writer = JobLogWriter(current_job)
        return self.log_writer

    def _traceback_string(self, error: Exception):
        error_info = getattr(error, "burla_error_info", None)
        if error_info and error_info.get("traceback_dict"):
            traceback_object = Traceback.from_dict(error_info["traceback_dict"]).as_traceback()
            return "".join(traceback.format_exception(type(error), error, traceback_object))
        return "".join(traceback.format_exception(type(error), error, error.__traceback__))

    async def boot(self):
        await self._start_container()
        self.python_version = await self._get_python_version()
        self.port = WORKER_INTERNAL_PORT if IN_LOCAL_DEV_MODE else await self._get_host_port()
        boot_started_at = time.perf_counter()
        while True:
            try:
                connection_host = self.container_name if IN_LOCAL_DEV_MODE else "127.0.0.1"
                connection_port = WORKER_INTERNAL_PORT if IN_LOCAL_DEV_MODE else self.port
                self.reader, self.writer = await asyncio.open_connection(
                    connection_host, connection_port
                )
                worker_socket = self.writer.get_extra_info("socket")
                worker_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self.writer.write(b"s")
                await self.writer.drain()
                await self.reader.readexactly(1)
                break
            except (ConnectionRefusedError, ConnectionResetError, asyncio.IncompleteReadError):
                if self.writer is not None:
                    self.writer.close()
                    self.writer = None
                container_info = await self.container.show()
                if not container_info["State"]["Running"]:
                    await self._log_container_failure()
                    raise RuntimeError(f"Container {self.container_name} stopped while booting.")
                if time.perf_counter() - boot_started_at > WORKER_BOOT_TIMEOUT_SECONDS:
                    raise _worker_boot_timeout_error(await self._get_logs())
                await asyncio.sleep(0.1)
        self.is_idle = True
        self.logstream_task = asyncio.create_task(self._handle_container_logs())
        if not IN_LOCAL_DEV_MODE:
            self.worker_host_pid = await self._get_worker_host_pid()

    async def _raise_if_worker_failed(self):
        for _ in range(10):
            container_info = await self.container.show()
            if container_info["State"]["OOMKilled"]:
                raise _worker_oom_error()
            if not container_info["State"]["Running"]:
                await self._log_container_failure()
                raise RuntimeError("\n\nWorker container stopped unexpectedly.\n")
            await asyncio.sleep(0.1)
        raise RuntimeError("\n\nWorker connection closed unexpectedly.\n")

    async def _read_response(self):
        try:
            status = await self.reader.readexactly(1)
        except (ConnectionResetError, asyncio.IncompleteReadError):
            await self._raise_if_worker_failed()
        if status == b"s":
            payload_size = int.from_bytes(await self.reader.readexactly(8), "big")
            payload = await self.reader.readexactly(payload_size)
            if payload:
                return payload
            return None
        if status == b"e":
            error_size = int.from_bytes(await self.reader.readexactly(8), "big")
            error_response = pickle.loads(await self.reader.readexactly(error_size))
            error_info = error_response["error_info"]
            exception = error_info["exception"]
            if error_info.get("traceback_dict"):
                traceback = Traceback.from_dict(error_info["traceback_dict"]).as_traceback()
                exception = exception.with_traceback(traceback)
            exception.burla_error_info = error_info
            raise exception
        raise Exception(f"unknown response status: {status}")

    def _serialize_error(self, error: Exception):
        # UDF errors carry `burla_error_info` (set in `_read_response`) and pickle cleanly because
        # their type comes from user code the client already has. Infrastructure errors (worker
        # container died, aiodocker failures during cluster shutdown, etc.) may reference modules
        # like `aiodocker` that the client does not have installed, so we send a traceback string
        # instead of the raw exception type.
        error_info = getattr(error, "burla_error_info", None)
        if error_info is not None:
            return pickle.dumps(error_info)
        traceback_str = "".join(traceback.format_exception(type(error), error, error.__traceback__))
        return pickle.dumps({"traceback_str": traceback_str, "is_infrastructure_error": True})

    async def _process_inputs(self):
        while True:
            self.is_idle = True
            while SELF["results_queue"].size_bytes > RESULTS_QUEUE_RAM_LIMIT_BYTES:
                await asyncio.sleep(0.1)
            input_index, input_pkl = await SELF["inputs_queue"].get()

            self.is_idle = False
            await self._ensure_log_writer()
            try:
                result_pkl = await self.call_function(input_index, input_pkl)
                result = (input_index, False, result_pkl)
            except asyncio.CancelledError:
                raise
            except BaseException as error:
                if self.log_writer is not None:
                    await self.log_writer.write_error(input_index, self._traceback_string(error))
                result = (input_index, True, self._serialize_error(error))
            finally:
                if self.log_writer is not None:
                    await self.log_writer.finish_input(input_index)

            await SELF["results_queue"].put(result, len(result[2]))
            SELF["num_results_received"] += 1

    async def install_packages(self, packages: dict):
        try:
            payload = pickle.dumps(packages)
            self.writer.write(b"i")
            self.writer.write(len(payload).to_bytes(8, "big"))
            self.writer.write(payload)
            await self.writer.drain()
            await self._read_response()
        except (BrokenPipeError, ConnectionResetError):
            await self._raise_if_worker_failed()

    async def load_function(self, function_bytes: bytes):
        try:
            self.writer.write(b"l")
            self.writer.write(len(function_bytes).to_bytes(8, "big"))
            self.writer.write(function_bytes)
            await self.writer.drain()
            await self._read_response()
            if self.process_inputs_task is None:
                self.process_inputs_task = asyncio.create_task(self._process_inputs())
        except (BrokenPipeError, ConnectionResetError):
            await self._raise_if_worker_failed()

    async def call_function(self, input_index: int, argument_bytes: bytes):
        try:
            payload = pickle.dumps({"input_index": input_index, "argument_bytes": argument_bytes})
            self.writer.write(b"c")
            self.writer.write(len(payload).to_bytes(8, "big"))
            self.writer.write(payload)
            await self.writer.drain()
            return await self._read_response()
        except (BrokenPipeError, ConnectionResetError):
            await self._raise_if_worker_failed()

    async def reset(self):
        if self.process_inputs_task is not None:
            self.process_inputs_task.cancel()
            try:
                await self.process_inputs_task
            except asyncio.CancelledError:
                pass
            self.process_inputs_task = None
        if not self.is_idle:
            # Worker is mid-UDF. The worker_server.py main thread is blocked inside the
            # user's function and can't service the 'r' byte over TCP until the call returns.
            # Waiting on the UDF can take arbitrarily long, so kill the container and
            # boot a fresh one instead.
            await self._restart_container()
            return
        if self.writer is not None:
            self.writer.write(b"r")
            self.writer.write((0).to_bytes(8, "big"))
            await self.writer.drain()
            await self._read_response()
        if self.log_writer is not None:
            await self.log_writer.stop()
            self.log_writer = None
        self.is_idle = True

    async def _reconnect(self):
        reconnect_started_at = time.perf_counter()
        while True:
            try:
                connection_host = self.container_name if IN_LOCAL_DEV_MODE else "127.0.0.1"
                connection_port = WORKER_INTERNAL_PORT if IN_LOCAL_DEV_MODE else self.port
                self.reader, self.writer = await asyncio.open_connection(
                    connection_host, connection_port
                )
                worker_socket = self.writer.get_extra_info("socket")
                worker_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self.writer.write(b"s")
                await self.writer.drain()
                await self.reader.readexactly(1)
                break
            except (ConnectionRefusedError, ConnectionResetError, asyncio.IncompleteReadError):
                if self.writer is not None:
                    self.writer.close()
                    self.writer = None
                if time.perf_counter() - reconnect_started_at > WORKER_BOOT_TIMEOUT_SECONDS:
                    raise _worker_boot_timeout_error(await self._get_logs())
                await asyncio.sleep(0.05)
        self.is_idle = True
        self.logstream_task = asyncio.create_task(self._handle_container_logs())

    async def _restart_container(self):
        if self.writer is not None:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
            self.writer = None
            self.reader = None
        if self.logstream_task is not None:
            self.logstream_task.cancel()
            try:
                await self.logstream_task
            except asyncio.CancelledError:
                pass
            self.logstream_task = None
        if self.log_writer is not None:
            await self.log_writer.stop()
            self.log_writer = None
        if IN_LOCAL_DEV_MODE:
            await self.container.restart(t=0)
        else:
            os.killpg(self.worker_host_pid, signal.SIGKILL)
        await self._reconnect()
        if not IN_LOCAL_DEV_MODE:
            self.worker_host_pid = await self._get_worker_host_pid()

    async def _container_exists(self):
        if not self.container_id:
            return False
        try:
            await self.container.show()
            return True
        except aiodocker.DockerError as e:
            if e.status == 404:
                return False
            raise

    async def _get_logs(self):
        log_lines = await self.container.log(stdout=True, stderr=True)
        return "".join(log_lines)

    async def _log_container_failure(self):
        if await self._container_exists():
            print(await self._get_logs(), end="")
