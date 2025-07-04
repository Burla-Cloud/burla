import os
import sys
import json
import requests
from uuid import uuid4
from time import sleep
import threading

import docker
from google.cloud import logging
from docker.types import DeviceRequest

from node_service import PROJECT_ID, INSTANCE_NAME, IN_LOCAL_DEV_MODE, NUM_GPUS, __version__


WORKER_INTERNAL_PORT = 8080


class Worker:
    """An instance of this = a running container with a running `worker_service` instance."""

    def __init__(
        self,
        python_version: str,
        image: str,
        send_logs_to_gcl: bool = False,
    ):
        self.is_idle = False
        self.is_empty = False
        self.container = None
        self.container_id = None
        self.container_name = f"worker_{uuid4().hex[:8]}--node_{INSTANCE_NAME[11:]}"
        self.url = None
        self.host_port = None
        self.python_version = python_version

        # dont assign to self because must be closed after use or causes issues :(
        docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")

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

            # TODO: update worker service if version is out of sync with this nodes version!

            # Install worker_service if missing
            $python_cmd -c "import worker_service" 2>/dev/null || (
                echo "Installing worker_service..."
                git clone --depth 1 --branch {__version__} https://github.com/Burla-Cloud/burla.git --no-checkout
                cd burla
                git sparse-checkout init --cone
                git sparse-checkout set worker_service
                git checkout {__version__}
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
        cmd = ["-c", cmd_script]
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
            gpu_device_requests = [DeviceRequest(count=-1, capabilities=[["gpu"]])]
            device_requests = gpu_device_requests if NUM_GPUS != 0 else []
            host_config = docker_client.create_host_config(
                port_bindings={WORKER_INTERNAL_PORT: ("127.0.0.1", None)},
                ipc_mode="host",
                device_requests=device_requests,
            )

        # start container
        self.container = docker_client.create_container(
            image=image,
            command=cmd,
            entrypoint=["bash"],
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
            runtime="nvidia" if NUM_GPUS != 0 else None,
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

        docker_client.close()

        if send_logs_to_gcl:
            self._start_log_streaming()

        # wait until READY
        if self.status() != "READY":
            raise Exception(f"Worker {self.container_name} failed to become READY.")

    def _start_log_streaming(self):
        def stream_logs():
            try:
                docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
                for log_line in docker_client.logs(
                    self.container_id, stream=True, follow=True, stdout=True, stderr=True
                ):
                    print(
                        f"[{self.container_name}] {log_line.decode('utf-8', errors='ignore').rstrip()}"
                    )
            except Exception as e:
                print(f"Log streaming stopped for {self.container_name}: {e}")
            finally:
                docker_client.close()

        log_thread = threading.Thread(target=stream_logs, daemon=True)
        log_thread.start()

    def exists(self):
        if not self.container_id:
            return False
        try:
            docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
            docker_client.inspect_container(self.container_id)
            return True
        except docker.errors.NotFound:
            return False
        finally:
            docker_client.close()

    def logs(self):
        if self.exists():
            docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
            logs = docker_client.logs(self.container_id).decode("utf-8", errors="ignore")
            docker_client.close()
            return logs
        raise Exception("This worker no longer exists.")

    def remove(self):
        pass
        if self.exists():
            try:
                docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
                docker_client.remove_container(self.container_id, force=True)
            except docker.errors.APIError as e:
                if not "409 Client Error" in str(e):
                    raise e
            finally:
                docker_client.close()

    def log_debug_info(self):
        try:
            logs = self.logs() if self.exists() else "Unable to retrieve container logs."
            logs = f"\nERROR INSIDE CONTAINER:\n{logs}\n"
            docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
            info = docker_client.containers(all=True)
            info = json.loads(json.dumps(info, default=lambda thing: str(thing)))
            struct = {
                "severity": "ERROR",
                "LOGS_FROM_FAILED_CONTAINER": logs,
                "CONTAINERS INFO": info,
            }
            logging.Client().logger("node_service").log_struct(struct)
        finally:
            docker_client.close()

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
