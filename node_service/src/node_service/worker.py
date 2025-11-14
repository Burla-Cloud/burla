import os
import random
import sys
import requests
from uuid import uuid4
from time import sleep, time
import traceback
import threading
from pathlib import Path

import docker
from docker.types import DeviceRequest, Ulimit
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
        image: str,
        latest_burla_version: str,
        elected_installer: bool = False,
        boot_timeout_sec: int = 120,
    ):
        #
        # WORKERS CLASS INSTANCES ARE PERSERVED ACROSS JOBS
        # THUS, THEY NEED TO BE RESET PER JOB
        #
        boot_start_time = time()
        self.elected_installer = elected_installer
        self.is_idle = False
        self.is_empty = False
        self.packages_to_install = None
        self.container = None
        self.container_id = None
        if IN_LOCAL_DEV_MODE:
            self.container_name = f"worker_{uuid4().hex[:8]}--node_{INSTANCE_NAME[11:]}"
        else:
            self.container_name = f"worker_{uuid4().hex[:8]}"
        self.url = None
        self.host_port = None
        self.python_version = None  # <- only assigned when container starts
        self.boot_timeout_sec = boot_timeout_sec

        # dont assign to self because must be closed after use or causes issues :(
        docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")

        python_command = "python"  # <- is also hardcoded in `_install_packages` in udf_executor
        cmd_script = f"""
            # worker service is installed here and mounted to all other containers
            export PYTHONPATH=/worker_service_python_env
            export PATH="/worker_service_python_env/bin:$PATH"
            DB_BASE_URL="https://firestore.googleapis.com/v1/projects/{PROJECT_ID}/databases/burla/documents"

            # install curl if missing
            if ! command -v curl >/dev/null 2>&1; then
                MSG="curl not found inside container image, installing ..."
                TS=$(date +%s)
                payload='{{"fields":{{"msg":{{"stringValue":"'"$MSG"'"}}, "ts":{{"integerValue":"'"$TS"'"}}}}}}'
                curl -sS -o /dev/null -X POST "$DB_BASE_URL/nodes/{INSTANCE_NAME}/logs" \\
                    -H "Authorization: Bearer {CREDENTIALS.token}" \\
                    -H "Content-Type: application/json" \\
                    -d "$payload"
                echo "$MSG"
                apt-get update && apt-get install -y curl
            fi

            # install uv if missing
            if [ "{self.elected_installer}" = "True" ] && ! command -v uv >/dev/null 2>&1; then
                # needed even if we have worker service already to install packages
                curl -LsSf https://astral.sh/uv/install.sh | sh
                export PATH="$HOME/.cargo/bin:$PATH"
                export PATH="$HOME/.local/bin:$PATH"
            fi

            # Install worker_service
            if [ "{self.elected_installer}" = "True" ]; then

                # Check that the python command is available and print its version
                if ! command -v {python_command} >/dev/null 2>&1; then
                    echo '-'
                    echo 'ERROR:'
                    echo 'The command `{python_command}` does not point to any valid python executable inside this Docker image!'
                    echo 'Currently using image: `{image}`'
                    echo 'Please ensure the command `{python_command}` points to a valid python executable inside this image.'
                    echo 'Ask jake (jake@burla.dev) if you need help with this!'
                    echo '-'
                    set -e
                    exit 1;
                    set +e
                fi
                
                # save version to file to report it to node service
                {python_command} -c 'import sys; print(f"{{sys.version_info.major}}.{{sys.version_info.minor}}")' > "/python_version_marker/python_version"

                MSG="Command $(echo '`{python_command}`') is pointing to python version $(cat '/python_version_marker/python_version'), using python$(cat '/python_version_marker/python_version')!";
                MSG="$MSG\n(please ensure you're running this same version locally when you call $(echo '`remote_parallel_map`'))"
                TS=$(date +%s)
                payload='{{"fields":{{"msg":{{"stringValue":"'"$MSG"'"}}, "ts":{{"integerValue":"'"$TS"'"}}}}}}'
                curl -sS -o /dev/null -X POST "$DB_BASE_URL/nodes/{INSTANCE_NAME}/logs" \\
                    -H "Authorization: Bearer {CREDENTIALS.token}" \\
                    -H "Content-Type: application/json" \\
                    -d "$payload"
                echo "$MSG"

                MSG="Installing Burla worker-service inside container ..."
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
                    mkdir -p /worker_service_python_env
                    mkdir -p /burla/worker_service
                    # del everything in /worker_service_python_env except `worker_service` (mounted)
                    find /worker_service_python_env -mindepth 1 -maxdepth 1 ! -name worker_service -exec rm -rf {{}} +
                    cd /burla/worker_service
                    set -e
                    uv pip install --python {python_command} --target /worker_service_python_env . || {{ 
                        echo "ERROR: Failed to install local worker_service with uv. Exiting."; 
                        exit 1;
                    }}
                    set +e
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
                    # can only do this with linux host, breaks in dev using macos host :(
                    export UV_CACHE_DIR=/worker_service_python_env/.uv-cache
                    mkdir -p "$UV_CACHE_DIR" /worker_service_python_env
                    set -e
                    uv pip install --python {python_command} --target /worker_service_python_env . || {{ 
                        echo "ERROR: Failed to install local worker_service with uv. Exiting."; 
                        exit 1; 
                    }}
                    set +e
                fi

                # Install burla so it is not automatically installed when users run the quickstart
                # this saves a sec or two off quickstart runtime.
                # don't simply add as a worker_svc dependency cause it's hard to make it always use latest pipy version.
                set -e
                uv pip install --python {python_command} --target /worker_service_python_env burla=={latest_burla_version} || {{ 
                    echo "ERROR: Failed to install burla client into worker. Exiting."; 
                    exit 1; 
                }}
                set +e
                # mark that worker_svc installed in shared dir so other containers can continue
                touch /worker_service_python_env/.WORKER_SVC_INSTALLED

                MSG="Successfully installed worker-service."
                TS=$(date +%s)
                payload='{{"fields":{{"msg":{{"stringValue":"'"$MSG"'"}}, "ts":{{"integerValue":"'"$TS"'"}}}}}}'
                curl -sS -o /dev/null -X POST "$DB_BASE_URL/nodes/{INSTANCE_NAME}/logs" \\
                    -H "Authorization: Bearer {CREDENTIALS.token}" \\
                    -H "Content-Type: application/json" \\
                    -d "$payload"
                echo "$MSG"
            fi

            # Wait until installer container is done installing worker svc
            if [ "{self.elected_installer}" != "True" ]; then
                start_time=$(date +%s)
                until [ -f /worker_service_python_env/.WORKER_SVC_INSTALLED ]; do
                    now=$(date +%s)
                    if [ $((now - start_time)) -ge {self.boot_timeout_sec} ]; then
                        echo "Timeout waiting for worker_service install completion after {self.boot_timeout_sec} seconds"
                        set -e
                        exit 1
                        set +e
                    fi
                    sleep 1
                done
            fi

            mkdir -p /workspace/shared
            
            # Start the worker service,
            # Restart automatically if it dies (IMPORTANT!):
            # Because it kills itself intentionally when it needs to cancel a running job.
            while true; do
                cd /workspace
                {python_command} -m uvicorn worker_service:app --host 0.0.0.0 \
                    --port {WORKER_INTERNAL_PORT} --workers 1 \
                    --timeout-keep-alive 30 \
                    || true  # <- intentionally ignore errors so script dosen't exit.
            done
        """.strip()
        cmd = ["-c", cmd_script]
        if IN_LOCAL_DEV_MODE:
            host_config = docker_client.create_host_config(
                port_bindings={WORKER_INTERNAL_PORT: ("127.0.0.1", None)},
                network_mode="local-burla-cluster",
                binds={
                    f"{os.environ['HOST_HOME_DIR']}/.config/gcloud": "/root/.config/gcloud",
                    f"{os.environ['HOST_PWD']}/worker_service": "/burla/worker_service",
                    f"{os.environ['HOST_PWD']}/worker_service/src/worker_service": "/worker_service_python_env/worker_service",
                    f"{os.environ['HOST_PWD']}/_shared_workspace": "/workspace/shared",
                    f"{os.environ['HOST_PWD']}/_worker_service_python_env": "/worker_service_python_env",
                    f"{os.environ['HOST_PWD']}/_python_version_marker": "/python_version_marker",
                    f"{os.environ['HOST_PWD']}/.temp_token.txt": "/burla/.temp_token.txt",
                },
                ipc_mode="host",
                oom_kill_disable=True,
                memswap_limit=-1,
                shm_size="16g",
                ulimits=[
                    Ulimit(name="memlock", soft=-1, hard=-1),
                    Ulimit(name="nofile", soft=1048576, hard=1048576),
                ],
            )
        else:
            gpu_device_requests = [DeviceRequest(count=-1, capabilities=[["gpu"]])]
            device_requests = gpu_device_requests if NUM_GPUS != 0 else []
            host_config = docker_client.create_host_config(
                port_bindings={WORKER_INTERNAL_PORT: ("127.0.0.1", None)},
                device_requests=device_requests,
                binds={
                    "/python_version_marker": "/python_version_marker",
                    "/worker_service_python_env": "/worker_service_python_env",
                    "/workspace/shared": "/workspace/shared",
                },
                ipc_mode="host",
                oom_kill_disable=True,
                memswap_limit=-1,
                shm_size="16g",
                ulimits=[
                    Ulimit(name="memlock", soft=-1, hard=-1),
                    Ulimit(name="nofile", soft=1048576, hard=1048576),
                ],
            )

        # start container
        attempt = 0
        while True:
            attempt += 1
            # sometimes docker's backend just stops responding, retry in this case.
            try:
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
                        "ELECTED_INSTALLER": self.elected_installer,
                        "INSTANCE_NAME": INSTANCE_NAME,
                    },
                    detach=True,
                    runtime="nvidia" if NUM_GPUS != 0 else None,
                )
                self.container_id = self.container.get("Id")
                docker_client.start(container=self.container_id)
            except (requests.exceptions.ReadTimeout, docker.errors.APIError) as e:
                if attempt > 5:
                    raise e

                msg = f"\nError starting container {self.container_name}:\n"
                msg += "```\n"
                msg += traceback.format_exc()
                msg += "\n```\n"
                msg += f"Retrying in {random.uniform(1, 5)} seconds...\n\n\n"
                # print(msg)

                struct = {"severity": "ERROR", "MESSAGE": msg}
                logging.Client().logger("node_service").log_struct(struct)

                firestore_client = firestore.Client(project=PROJECT_ID, database="burla")
                node_ref = firestore_client.collection("nodes").document(INSTANCE_NAME)
                node_ref.collection("logs").document().set({"msg": msg, "ts": time()})

                sleep(random.uniform(1, 5))  # <- avoid theoretical thundering herd
                try:
                    docker_client.remove_container(self.container_name, force=True)
                except Exception:
                    pass
                # idk if recreating client actually helps
                # it might because random docker api errors can be due to client issues ??
                docker_client.close()
                docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
            else:
                break

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

        if self.elected_installer:
            self._start_log_streaming()

        ready = False
        start = time()
        while not ready:

            try:
                info = docker_client.inspect_container(self.container_id)
                container_is_running = info.get("State", {}).get("Running", False)
            except docker.errors.NotFound:
                container_is_running = False

            if not container_is_running:
                self.log_debug_info()
                raise Exception(f"Container: {self.container_name} not running while booting?")

            timed_out = time() - start > self.boot_timeout_sec
            if timed_out:
                self.log_debug_info()
                msg = f"Worker {self.container_name} boot timed out after "
                raise Exception(f"{msg} {self.boot_timeout_sec} seconds.")

            try:
                response = requests.get(f"{self.url}/")
                response.raise_for_status()
                status = response.json()["status"]  # can only be one of: READY, BUSY
                if status != "READY":
                    raise Exception(f"Worker {self.container_name} has status {status} after boot?")
                ready = True
            except requests.exceptions.ConnectionError:
                sleep(1)

        self.python_version = Path("/python_version_marker/python_version").read_text().strip()

        boot_duration = time() - boot_start_time
        # print(f"Worker {self.container_name} booted after: {boot_duration:.2f} seconds")
        docker_client.close()

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
