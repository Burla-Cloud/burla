import os
import sys
import requests
from uuid import uuid4
from time import sleep, time
import traceback
import threading

import docker
from docker.types import DeviceRequest
from google.cloud import logging, firestore
from google.auth.transport.requests import Request

from node_service import (
    PROJECT_ID,
    CREDENTIALS,
    INSTANCE_NAME,
    IN_LOCAL_DEV_MODE,
    NUM_GPUS,
    __version__,
)

CREDENTIALS.refresh(Request())
WORKER_INTERNAL_PORT = 8080


class Worker:
    """An instance of this = a running container with a running `worker_service` instance."""

    def __init__(
        self,
        python_version: str,
        image: str,
        install_worker: bool = False,
        boot_timeout_sec: int = 120,
    ):
        self.is_idle = False
        self.is_empty = False
        self.container = None
        self.container_id = None
        if IN_LOCAL_DEV_MODE:
            self.container_name = f"worker_{uuid4().hex[:8]}--node_{INSTANCE_NAME[11:]}"
        else:
            self.container_name = f"worker_{uuid4().hex[:8]}"
        self.url = None
        self.host_port = None
        self.python_version = python_version
        self.boot_timeout_sec = boot_timeout_sec

        # dont assign to self because must be closed after use or causes issues :(
        docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")

        cmd_script = f"""
            # worker service is installed here and mounted to all other containers
            export PYTHONPATH=/worker_service_python_env
            DB_BASE_URL="https://firestore.googleapis.com/v1/projects/{PROJECT_ID}/databases/burla/documents"

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

            # Install worker_service if missing
            if [ "{install_worker}" = "True" ] && ! $python_cmd -c "import worker_service" 2>/dev/null; then

                MSG="Installing Burla worker-service inside container image: {image} ..."
                TS=$(date +%s)
                payload='{{"fields":{{"msg":{{"stringValue":"'"$MSG"'"}}, "ts":{{"integerValue":"'"$TS"'"}}}}}}'
                curl -sS -o /dev/null -X POST "$DB_BASE_URL/nodes/{INSTANCE_NAME}/logs" \\
                    -H "Authorization: Bearer {CREDENTIALS.token}" \\
                    -H "Content-Type: application/json" \\
                    -d "$payload"
                echo "$MSG"

                # install local worker_service in edit mode or use version on github if not in DEV
                if [ "{IN_LOCAL_DEV_MODE}" = "True" ]; then
                    echo "Installing local dev version ..."
                    cd /burla/worker_service
                    $python_cmd -m pip install . --break-system-packages --no-cache-dir \
                        --only-binary=:all: --target /worker_service_python_env
                else
                    # try with tarball first because faster
                    if curl -Ls -o burla.tar.gz https://github.com/Burla-Cloud/burla/archive/{__version__}.tar.gz; then
                        echo "Installing from tarball ..."
                        tar -xzf burla.tar.gz
                        cd burla-{__version__}/worker_service
                    else
                        echo "Tarball not found, falling back to git..."
                        # Ensure git is installed
                        if ! command -v git >/dev/null 2>&1; then
                            echo "git not found, installing..."
                            apt-get update && apt-get install -y git
                        fi
                        git clone --depth 1 --filter=blob:none --sparse --branch {__version__} https://github.com/Burla-Cloud/burla.git
                        cd burla
                        git sparse-checkout set worker_service
                        cd worker_service
                    fi
                    $python_cmd -m pip install --break-system-packages --no-cache-dir \
                        --only-binary=:all: --target /worker_service_python_env .
                fi

                MSG="Successfully installed worker-service."
                TS=$(date +%s)
                payload='{{"fields":{{"msg":{{"stringValue":"'"$MSG"'"}}, "ts":{{"integerValue":"'"$TS"'"}}}}}}'
                curl -sS -o /dev/null -X POST "$DB_BASE_URL/nodes/{INSTANCE_NAME}/logs" \\
                    -H "Authorization: Bearer {CREDENTIALS.token}" \\
                    -H "Content-Type: application/json" \\
                    -d "$payload"
                echo "$MSG"
            fi

            # Wait for worker_service to become importable when not installing
            if [ "{install_worker}" != "True" ]; then
                start_time=$(date +%s)
                until $python_cmd -c "import worker_service" 2>/dev/null; do
                    now=$(date +%s)
                    if [ $((now - start_time)) -ge {self.boot_timeout_sec} ]; then
                        echo "Timeout waiting for worker_service to become importable after {self.boot_timeout_sec} seconds"
                        exit 1
                    fi
                    sleep 1
                done
            fi

            # Start the worker service,
            # Restart automatically if it dies (IMPORTANT!):
            # Because it kills itself intentionally when it needs to cancel a running job.
            while true; do
                $python_cmd -m uvicorn worker_service:app --host 0.0.0.0 \
                    --port {WORKER_INTERNAL_PORT} --workers 1 \
                    --timeout-keep-alive 30
                echo "Restarting worker service..."
            done
        """.strip()
        cmd = ["-c", cmd_script]
        if IN_LOCAL_DEV_MODE:
            host_config = docker_client.create_host_config(
                port_bindings={WORKER_INTERNAL_PORT: ("127.0.0.1", None)},
                network_mode="local-burla-cluster",
                binds={
                    f"{os.environ['HOST_HOME_DIR']}/.config/gcloud": "/root/.config/gcloud",
                    f"{os.environ['HOST_PWD']}/worker_service_python_env": "/worker_service_python_env",
                    f"{os.environ['HOST_PWD']}/worker_service/src/worker_service": "/worker_service_python_env/worker_service",
                    f"{os.environ['HOST_PWD']}/.temp_token.txt": "/burla/.temp_token.txt",
                },
            )
        else:
            gpu_device_requests = [DeviceRequest(count=-1, capabilities=[["gpu"]])]
            device_requests = gpu_device_requests if NUM_GPUS != 0 else []
            host_config = docker_client.create_host_config(
                port_bindings={WORKER_INTERNAL_PORT: ("127.0.0.1", None)},
                ipc_mode="host",
                device_requests=device_requests,
                binds={"/worker_service_python_env": "/worker_service_python_env"},
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

        if install_worker:
            self._start_log_streaming()

        # wait until READY
        if self.status() != "READY":
            raise Exception(f"Worker {self.container_name} failed to become READY.")

    def _start_log_streaming(self):
        def stream_logs():
            try:
                docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
                log_generator = docker_client.logs(
                    container=self.container_id, stream=True, follow=True, stdout=True, stderr=True
                )
                for log_line in log_generator:
                    msg = log_line.decode("utf-8", errors="ignore").rstrip()
                    print(f"[{self.container_name}] {msg}")
            except Exception as e:
                print(f"Log streaming stopped for {self.container_name}: {traceback.format_exc()}")
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
        logs = self.logs() if self.exists() else "Unable to retrieve container logs."
        struct = {"severity": "ERROR", "LOGS_FROM_FAILED_CONTAINER": logs}
        logging.Client().logger("node_service").log_struct(struct)

        error_title = f"Container {self.container_name} has FAILED! Logs from container:\n"
        log = {"msg": f"{error_title}\n{logs.strip()}", "ts": time()}
        firestore_client = firestore.Client(project=PROJECT_ID, database="burla")
        node_ref = firestore_client.collection("nodes").document(INSTANCE_NAME)
        node_ref.collection("logs").document().set(log)

        if IN_LOCAL_DEV_MODE:
            print(logs, file=sys.stderr)  # <- make local debugging easier

    def status(self, attempt: int = 0):
        # Check if Docker container is running; fail if not
        docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
        try:
            info = docker_client.inspect_container(self.container_id)
            if not info.get("State", {}).get("Running"):
                self.log_debug_info()
                self.remove()
                return "FAILED"
        except docker.errors.NotFound:
            self.log_debug_info()
            self.remove()
            return "FAILED"
        finally:
            docker_client.close()

        # A worker can also be "IDLE" (waiting for inputs) but that is not returned by this endpoint
        # "IDLE" is not a possible return value here because it is only returned/assigned to `self`
        # when checking results (for efficiency reasons).
        try:
            response = requests.get(f"{self.url}/")
            response.raise_for_status()
            status = response.json()["status"]  # will be one of: READY, BUSY
        except requests.exceptions.ConnectionError as e:
            if attempt < self.boot_timeout_sec:
                sleep(1)
                return self.status(attempt + 1)
            else:
                status = "FAILED"

        if status == "FAILED":
            self.log_debug_info()
            self.remove()

        return status
