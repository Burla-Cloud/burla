import os
import sys
import json
import requests
from uuid import uuid4
from time import sleep
import threading

import docker
from docker.errors import APIError
from google.cloud import logging
from google.auth.transport.requests import Request
from docker.types import DeviceRequest

from node_service import PROJECT_ID, INSTANCE_NAME, IN_LOCAL_DEV_MODE, CREDENTIALS, GPU

LOGGER = logging.Client().logger("node_service")
WORKER_INTERNAL_PORT = 8080


class Worker:
    """An instance of this = a running container with a running `worker_service` instance."""

    def __init__(
        self,
        python_version: str,
        image: str,
        docker_client: docker.APIClient,
        send_logs_to_gcl: bool = False,
        stream_logs: bool = None,
    ):
        self.is_idle = False
        self.is_empty = False
        self.container = None
        self.container_id = None
        self.container_name = f"worker_{uuid4().hex[:8]}--node_{INSTANCE_NAME[11:]}"
        self.url = None
        self.host_port = None
        self.docker_client = docker_client
        self.python_version = python_version

        try:
            for line in docker_client.pull(image, stream=True, decode=True):
                print(f"{line['id'][:12]}: {line['status']} {line.get("progress", "")}")
        except APIError as e:
            if e.response.status_code == 401:
                CREDENTIALS.refresh(Request())
                auth_config = {"username": "oauth2accesstoken", "password": CREDENTIALS.token}
                logs = docker_client.pull(image, auth_config=auth_config, stream=True, decode=True)
                for line in logs:
                    print(f"{line['id'][:12]}: {line['status']} {line.get("progress", "")}")
            else:
                raise

        try:
            # ODDLY, if docker_client.pull fails to pull the image, it will NOT throw any error >:(
            # check here that the image was actually pulled and exists on disk,
            docker_client.inspect_image(image)
        except docker.errors.ImageNotFound:
            msg = f"Image {image} not found after pulling!\nDid vm run out of disk space?"
            raise Exception(msg)

        cmd_script = f"""    
            # Find python version:
            python_cmd=""
            for py in python{self.python_version} python3 python; do
                is_executable=$(command -v $py >/dev/null 2>&1 && echo true || echo false)
                version_matches=$($py --version 2>&1 | grep -q "{self.python_version}" && echo true || echo false)
                if [ "$is_executable" = true ] && [ "$version_matches" = true ]; then
                    echo "Found correct python version: $py"
                    python_cmd=$py
                    break
                fi
            done

            # If python version not found, exit
            if [ -z "$python_cmd" ]; then
                echo "Python {self.python_version} not found"
                exit 1
            fi

            # Ensure git is installed
            if ! command -v git >/dev/null 2>&1; then
                echo "git not found, installing..."
                apt-get update && apt-get install -y git
            fi

            # Install worker_service if missing
            $python_cmd -c "import worker_service" 2>/dev/null || (
                echo "Installing worker_service..."
                git clone --depth 1 https://github.com/Burla-Cloud/burla.git --no-checkout
                cd burla
                git sparse-checkout init --cone
                git sparse-checkout set worker_service
                git checkout main
                cd worker_service
                $python_cmd -m pip install --break-system-packages .
            )

            # If local dev mode, run in reload mode
            reload_flag=""
            if [ "{IN_LOCAL_DEV_MODE}" = "True" ]; then
                reload_flag="--reload"
            fi

            # Start the worker service
            exec $python_cmd -m uvicorn worker_service:app --host 0.0.0.0 \
                --port {WORKER_INTERNAL_PORT} --workers 1 \
                --timeout-keep-alive 30 $reload_flag
        """.strip()
        cmd = ["bash", "-c", cmd_script]
        if IN_LOCAL_DEV_MODE:
            host_config = docker_client.create_host_config(
                port_bindings={WORKER_INTERNAL_PORT: ("127.0.0.1", None)},
                network_mode="local-burla-cluster",
                binds={
                    f"{os.environ['HOST_HOME_DIR']}/.config/gcloud": "/root/.config/gcloud",
                    f"{os.environ['HOST_PWD']}/worker_service": "/burla/worker_service",
                },
            )
        else:
            device_requests = [DeviceRequest(count=-1, capabilities=[["gpu"]])] if GPU else []
            host_config = docker_client.create_host_config(
                port_bindings={WORKER_INTERNAL_PORT: ("127.0.0.1", None)},
                ipc_mode="host",
                device_requests=device_requests,
            )

        # start container
        self.container = docker_client.create_container(
            image=image,
            command=cmd,
            name=self.container_name,
            ports=[WORKER_INTERNAL_PORT],
            host_config=host_config,
            environment={
                "GOOGLE_CLOUD_PROJECT": PROJECT_ID,
                "IN_LOCAL_DEV_MODE": IN_LOCAL_DEV_MODE,
                "WORKER_NAME": self.container_name,
                "SEND_LOGS_TO_GCL": send_logs_to_gcl,
            },
            detach=True,
            runtime="nvidia" if GPU else None,
        )
        self.container_id = self.container.get("Id")
        docker_client.start(container=self.container_id)

        # wait for port to be assigned to the container
        def _get_host_port(attempt: int = 0):
            info = docker_client.inspect_container(self.container_id)
            host_port_info = info["NetworkSettings"]["Ports"].get(f"{WORKER_INTERNAL_PORT}/tcp")
            if host_port_info:
                return int(host_port_info[0]["HostPort"])
            elif attempt > 20:
                raise RuntimeError(f"Failed to get port for container {self.container_name} in 10s")
            sleep(0.5)
            return _get_host_port(attempt + 1)

        self.url = f"http://127.0.0.1:{_get_host_port()}"
        if IN_LOCAL_DEV_MODE:
            self.url = f"http://{self.container_name}:{WORKER_INTERNAL_PORT}"

        should_stream = stream_logs if stream_logs is not None else False
        should_stream = True
        if should_stream:
            self._start_log_streaming()

        # wait until READY
        if self.status() != "READY":
            raise Exception(f"Worker {self.container_name} failed to become READY.")

    def _start_log_streaming(self):
        def stream_logs():
            try:
                for log_line in self.docker_client.logs(
                    self.container_id, stream=True, follow=True, stdout=True, stderr=True
                ):
                    print(
                        f"[{self.container_name}] {log_line.decode('utf-8', errors='ignore').rstrip()}"
                    )
            except Exception as e:
                print(f"Log streaming stopped for {self.container_name}: {e}")

        log_thread = threading.Thread(target=stream_logs, daemon=True)
        log_thread.start()

    def exists(self):
        if not self.container_id:
            return False
        try:
            self.docker_client.inspect_container(self.container_id)
            return True
        except docker.errors.NotFound:
            return False

    def logs(self):
        if self.exists():
            return self.docker_client.logs(self.container_id).decode("utf-8", errors="ignore")
        raise Exception("This worker no longer exists.")

    def remove(self):
        pass
        if self.exists():
            try:
                self.docker_client.remove_container(self.container_id, force=True)
            except docker.errors.APIError as e:
                if not "409 Client Error" in str(e):
                    raise e

    def log_debug_info(self):
        logs = self.logs() if self.exists() else "Unable to retrieve container logs."
        logs = f"\nERROR INSIDE CONTAINER:\n{logs}\n"
        info = self.docker_client.containers(all=True)
        info = json.loads(json.dumps(info, default=lambda thing: str(thing)))
        struct = {"severity": "ERROR", "LOGS_FROM_FAILED_CONTAINER": logs, "CONTAINERS INFO": info}
        LOGGER.log_struct(struct)
        if IN_LOCAL_DEV_MODE:
            print(logs, file=sys.stderr)  # <- make local debugging easier

    def status(self, attempt: int = 0):
        # A worker can also be "IDLE" (waiting for inputs) but that is not returned by this endpoint
        # "IDLE" is not a possible return value here because it is only returned/assigned to `self`
        # when checking results (for efficiency reasons).
        try:
            response = requests.get(f"{self.url}/")
            response.raise_for_status()
            status = response.json()["status"]  # will be one of: READY, BUSY
        except requests.exceptions.ConnectionError as e:
            if attempt < 10:
                sleep(3)
                return self.status(attempt + 1)
            else:
                status = "FAILED"

        if status == "FAILED":
            self.log_debug_info()
            self.remove()

        return status
