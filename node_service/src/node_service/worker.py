import os
import sys
import json
import requests
from uuid import uuid4
from time import sleep

import docker
from google.cloud import logging
from google.auth.transport.requests import Request

from node_service import PROJECT_ID, INSTANCE_NAME, IN_LOCAL_DEV_MODE, CREDENTIALS

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

        # this is a signifigant assumption! (it's in the tooltip so users should be aware?)
        self.python_executable = f"python{self.python_version}"

        # pull image
        is_private_image = f"docker.pkg.dev/{PROJECT_ID}" in image
        is_private_image = is_private_image or f"gcr.io/{PROJECT_ID}" in image

        if is_private_image:
            # use current GCP vm's credentials to pull the image
            CREDENTIALS.refresh(Request())
            auth_config = {"username": "oauth2accesstoken", "password": CREDENTIALS.token}
            docker_client.pull(image, auth_config=auth_config)
        else:
            docker_client.pull(image)

        try:
            # ODDLY, if docker_client.pull fails to pull the image, it will NOT throw any error >:(
            # check here that the image was actually pulled and exists on disk,
            docker_client.inspect_image(image)
        except docker.errors.ImageNotFound:
            msg = f"Image {image} not found after pulling!\nDid vm run out of disk space?"
            raise Exception(msg)

        # setup host_config
        host_arg = ["--host", "0.0.0.0"]
        port_arg = ["--port", str(WORKER_INTERNAL_PORT)]
        workers_arg = ["--workers", "1"]
        timeout_arg = ["--timeout-keep-alive", "30"]
        uvicorn_cmd_args = [*host_arg, *port_arg, *workers_arg, *timeout_arg]
        cmd = [self.python_executable, "-m", "uvicorn", "worker_service:app", *uvicorn_cmd_args]
        if IN_LOCAL_DEV_MODE:
            cmd.append("--reload")
            host_config = docker_client.create_host_config(
                port_bindings={WORKER_INTERNAL_PORT: ("127.0.0.1", None)},
                network_mode="local-burla-cluster",
                binds={
                    f"{os.environ['HOST_HOME_DIR']}/.config/gcloud": "/root/.config/gcloud",
                    f"{os.environ['HOST_PWD']}/worker_service": "/burla/worker_service",
                },
            )
        else:
            port_bindings = {WORKER_INTERNAL_PORT: ("127.0.0.1", None)}
            host_config = docker_client.create_host_config(port_bindings=port_bindings)

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

        # wait until READY
        if self.status() != "READY":
            raise Exception(f"Worker {self.container_name} failed to become READY.")

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
