import asyncio
import os
import pickle
import time
from pathlib import Path
from queue import Empty
from uuid import uuid4

import docker
from docker.types import DeviceRequest
from tblib import Traceback

from node_service import SELF, INSTANCE_NAME, IN_LOCAL_DEV_MODE, NUM_GPUS

WORKER_INTERNAL_PORT = 8080


class WorkerContainerOutOfMemoryError(RuntimeError):
    def __init__(self):
        super().__init__(
            "\n\nWorker container was killed by the Linux OOM killer.\n"
            "This usually means the submitted function used more memory than the container had available.\n"
            "Increase the container memory limit or reduce memory usage inside the function.\n"
        )


class WorkerBootTimeoutError(RuntimeError):
    def __init__(self, logs: str):
        message = "\n\nWorker boot timed out after 10 seconds.\n"
        message += "The worker container never became ready to accept connections.\n"
        message += "\nBuffered worker logs:\n"
        message += "---------------------\n"
        message += f"{logs}\n"
        super().__init__(message)


class WorkerClient:
    def __init__(self, image: str):
        self.container_name = f"worker_{uuid4().hex[:8]}"
        self.container_name += f"--node_{INSTANCE_NAME[11:]}" if IN_LOCAL_DEV_MODE else ""
        self.port = None
        self.image = image
        self.docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
        self.is_idle = True
        self.is_empty = True
        self.currently_installing_package = None
        self.python_version = None
        self.container_id = None
        self.logstream_task = None
        self.reader = None
        self.writer = None
        self.process_inputs_task = None

    def _worker_server_host_path(self):
        if IN_LOCAL_DEV_MODE:
            return f"{os.environ['HOST_PWD']}/node_service/src/node_service/worker_server.py"
        return str(Path(__file__).resolve().parent / "worker_server.py")

    def _start_container_sync(self):
        binds = {self._worker_server_host_path(): "/opt/burla/worker_server.py"}
        if IN_LOCAL_DEV_MODE:
            host_pwd = os.environ["HOST_PWD"]
            host_home_dir = os.environ["HOST_HOME_DIR"]
            worker_python_environment_dir = f"{host_pwd}/_worker_service_python_env/{INSTANCE_NAME}"
            host_config = self.docker_client.create_host_config(
                port_bindings={WORKER_INTERNAL_PORT: ("127.0.0.1", None)},
                network_mode="local-burla-cluster",
                binds={
                    **binds,
                    f"{host_home_dir}/.config/gcloud": "/root/.config/gcloud",
                    f"{host_pwd}/_shared_workspace": "/workspace/shared",
                    worker_python_environment_dir: "/worker_service_python_env",
                },
                shm_size="16g",
            )
        else:
            device_requests = (
                [DeviceRequest(count=-1, capabilities=[["gpu"]])] if NUM_GPUS != 0 else []
            )
            host_config = self.docker_client.create_host_config(
                port_bindings={WORKER_INTERNAL_PORT: ("127.0.0.1", None)},
                device_requests=device_requests,
                binds={
                    **binds,
                    "/worker_service_python_env": "/worker_service_python_env",
                    "/workspace/shared": "/workspace/shared",
                },
                shm_size="16g",
            )

        command = [
            "sh",
            "-lc",
            (
                "export PYTHONPATH=/worker_service_python_env; "
                'export PATH="/worker_service_python_env/bin:$PATH"; '
                f"exec python /opt/burla/worker_server.py {WORKER_INTERNAL_PORT}"
            ),
        ]

        container = self.docker_client.create_container(
            image=self.image,
            command=command,
            name=self.container_name,
            ports=[WORKER_INTERNAL_PORT],
            host_config=host_config,
            detach=True,
            runtime="nvidia" if NUM_GPUS != 0 else None,
        )
        self.docker_client.start(container=container["Id"])
        self.container_id = container["Id"]

    async def _get_host_port(self):
        for _ in range(20):
            container_info = await asyncio.to_thread(self._inspect_container_sync)
            port_info = container_info["NetworkSettings"]["Ports"].get(
                f"{WORKER_INTERNAL_PORT}/tcp"
            )
            if port_info:
                return int(port_info[0]["HostPort"])
            await asyncio.sleep(0.5)
        raise RuntimeError(f"Failed to get port for container {self.container_name} in 10s")

    async def _get_python_version(self):
        for _ in range(20):
            logs = await asyncio.to_thread(self._get_logs)
            if logs:
                return logs.splitlines()[0].strip()
            await asyncio.sleep(0.1)
        raise RuntimeError(f"Failed to get python version for {self.container_name}.")

    async def _handle_container_logs(self):
        def stream_logs():
            log_generator = self.docker_client.logs(
                container=self.container_id, stream=True, follow=True, stdout=True, stderr=True
            )
            for log_line in log_generator:
                print(log_line.decode("utf-8", errors="ignore"), end="")

        await asyncio.to_thread(stream_logs)

    async def boot(self):
        await asyncio.to_thread(self._start_container_sync)
        self.python_version = await self._get_python_version()
        self.port = WORKER_INTERNAL_PORT if IN_LOCAL_DEV_MODE else await self._get_host_port()
        await asyncio.sleep(0.5)
        boot_started_at = time.perf_counter()
        while True:
            try:
                connection_host = self.container_name if IN_LOCAL_DEV_MODE else "127.0.0.1"
                connection_port = WORKER_INTERNAL_PORT if IN_LOCAL_DEV_MODE else self.port
                self.reader, self.writer = await asyncio.open_connection(
                    connection_host, connection_port
                )
                self.writer.write(b"s")
                await self.writer.drain()
                await self.reader.readexactly(1)
                break
            except (ConnectionRefusedError, ConnectionResetError, asyncio.IncompleteReadError):
                if self.writer is not None:
                    self.writer.close()
                    self.writer = None
                container_info = await asyncio.to_thread(self._inspect_container_sync)
                if not container_info["State"]["Running"]:
                    self._log_container_failure()
                    raise RuntimeError(f"Container {self.container_name} stopped while booting.")
                if time.perf_counter() - boot_started_at > 10:
                    raise WorkerBootTimeoutError(await asyncio.to_thread(self._get_logs))
                await asyncio.sleep(0.1)
        self.is_idle = True
        self.is_empty = True
        self.logstream_task = asyncio.create_task(self._handle_container_logs())

    async def _raise_if_worker_failed(self):
        for _ in range(10):
            container_info = await asyncio.to_thread(self._inspect_container_sync)
            if container_info["State"]["OOMKilled"]:
                raise WorkerContainerOutOfMemoryError()
            if not container_info["State"]["Running"]:
                self._log_container_failure()
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
                # return cloudpickle.loads(payload)
            return None
        if status == b"e":
            error_size = int.from_bytes(await self.reader.readexactly(8), "big")
            error_info = pickle.loads(await self.reader.readexactly(error_size))
            exception = error_info["exception"]
            if error_info.get("traceback_dict"):
                traceback = Traceback.from_dict(error_info["traceback_dict"]).as_traceback()
                exception = exception.with_traceback(traceback)
            exception.burla_error_info = error_info
            raise exception
        raise Exception(f"unknown response status: {status}")

    def _serialize_error(self, error: Exception):
        error_info = getattr(error, "burla_error_info", None)
        if error_info is not None:
            return pickle.dumps(error_info)
        error_info = {"type": type(error), "exception": error}
        if error.__traceback__ is not None:
            error_info["traceback_dict"] = Traceback(error.__traceback__).to_dict()
        return pickle.dumps(error_info)

    async def _process_inputs(self):
        while True:
            try:
                input_index, input_pkl = SELF["inputs_queue"].get_nowait()
            except Empty:
                self.is_idle = True
                self.is_empty = True
                await asyncio.sleep(0.05)
                continue

            self.is_idle = False
            self.is_empty = False
            try:
                result_pkl = await self.call_function(input_pkl)
                result = (input_index, False, result_pkl)
            except Exception as error:
                result = (input_index, True, self._serialize_error(error))

            SELF["results_queue"].put(result, len(result[2]))
            SELF["num_results_received"] += 1

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

    async def call_function(self, argument_bytes: bytes):
        try:
            self.writer.write(b"c")
            self.writer.write(len(argument_bytes).to_bytes(8, "big"))
            self.writer.write(argument_bytes)
            await self.writer.drain()
            return await self._read_response()
        except (BrokenPipeError, ConnectionResetError):
            await self._raise_if_worker_failed()

    async def stop(self):
        try:
            if self.process_inputs_task is not None:
                self.process_inputs_task.cancel()
                try:
                    await self.process_inputs_task
                except asyncio.CancelledError:
                    pass
            if self.writer is not None:
                self.writer.close()
                await self.writer.wait_closed()
            await asyncio.to_thread(
                self.docker_client.remove_container, self.container_id, force=True
            )
            if self.logstream_task is not None:
                await self.logstream_task
        finally:
            await asyncio.to_thread(self.docker_client.close)

    def _container_exists(self):
        if not self.container_id:
            return False
        try:
            self._inspect_container_sync()
            return True
        except docker.errors.NotFound:
            return False

    def _inspect_container_sync(self):
        return self.docker_client.inspect_container(self.container_id)

    def _get_logs(self):
        return self.docker_client.logs(self.container_id).decode("utf-8", errors="ignore")

    def _log_container_failure(self):
        if self._container_exists():
            print(self._get_logs(), end="")
