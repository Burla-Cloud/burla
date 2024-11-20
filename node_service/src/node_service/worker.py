import os
import sys
import json
import requests
import traceback
from uuid import uuid4
from time import sleep

import docker
from google.cloud import logging

from node_service import PROJECT_ID, IN_DEV, ACCESS_TOKEN, INSTANCE_NAME, IN_LOCAL_DEV_MODE
from node_service.helpers import next_free_port

LOGGER = logging.Client().logger("node_service")


class Worker:
    """An instance of this = a running container with a running `worker_service` instance."""

    def __init__(
        self,
        python_version: str,
        python_executable: str,
        image: str,
        docker_client: docker.APIClient,
    ):
        self.container = None
        attempt = 0

        # Use the provided APIClient directly
        auth_config = {"username": "oauth2accesstoken", "password": ACCESS_TOKEN}
        docker_client.pull(image, auth_config=auth_config)

        # ODDLY, if `docker_client.pull` fails to pull the image, it will NOT throw an error...
        # check here that the image was actually pulled and exists on disk,
        try:
            docker_client.inspect_image(image)
        except docker.errors.ImageNotFound:
            msg = f"Image {image} not found after pulling!\nDid vm run out of disk space?"
            raise Exception(msg)

        while self.container is None:
            port = next_free_port()
            gunicorn_command = f"gunicorn -t 60 -b 0.0.0.0:{port} worker_service:app"

            if IN_LOCAL_DEV_MODE:
                host_config = docker_client.create_host_config(
                    port_bindings={port: port},
                    network_mode="local-burla-cluster",
                    binds={
                        f"{os.environ['HOST_HOME_DIR']}/.config/gcloud": "/root/.config/gcloud",
                        f"{os.environ['HOST_PWD']}/worker_service": "/burla",
                    },
                )
            else:
                host_config = docker_client.create_host_config(port_bindings={port: port})

            try:
                container_name = f"worker_{uuid4().hex[:4]}--node_{INSTANCE_NAME[11:]}"
                container = docker_client.create_container(
                    image=image,
                    command=["/bin/sh", "-c", f"{python_executable} -m {gunicorn_command}"],
                    name=container_name,
                    ports=[port],
                    host_config=host_config,
                    environment={
                        "GOOGLE_CLOUD_PROJECT": PROJECT_ID,
                        "PROJECT_ID": PROJECT_ID,
                        "IN_LOCAL_DEV_MODE": IN_LOCAL_DEV_MODE,
                    },
                    detach=True,
                )
                docker_client.start(container=container.get("Id"))
                self.container = container
                self.container_name = container_name
            except docker.errors.APIError as e:
                if ("address already in use" in str(e)) or ("port is already allocated" in str(e)):
                    # This leaves an extra container in the "Created" state.
                    containers_status = [c["State"] for c in docker_client.containers(all=True)]
                    LOGGER.log_struct(
                        {
                            "severity": "WARNING",
                            "message": f"PORT ALREADY IN USE, TRYING AGAIN.",
                            "containers_status": containers_status,
                        }
                    )
                else:
                    raise e
            except requests.exceptions.ConnectionError as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
                traceback_str = "".join(traceback_details)
                msg = "error thrown on `docker run` after returning."
                log = {"message": msg, "exception": str(e), "traceback": traceback_str}
                LOGGER.log_struct(dict(severity="WARNING", **log))
                pass  # Thrown by containers.run long after it has already returned ??
            else:
                # Sometimes the container doesn't start and also doesn't throw an error ??
                # This is the case when calling containers.run() and container.start()
                attempt = 0
                sleep(1)
                container_info = docker_client.inspect_container(self.container.get("Id"))
                while container_info["State"]["Status"] == "created":
                    docker_client.start(container=self.container.get("Id"))
                    attempt += 1
                    if attempt == 10:
                        raise Exception("Unable to start node.")
                    sleep(1)
                    container_info = docker_client.inspect_container(self.container.get("Id"))

                if attempt > 1:
                    LOGGER.log_struct(
                        {
                            "severity": "INFO",
                            "message": f"CONTAINER STARTED! after {attempt+1} attempt(s)",
                            "state": container_info["State"]["Status"],
                            "name": container_info["Name"],
                        }
                    )
            attempt += 1
            if attempt == 10:
                raise Exception("Unable to start container.")

        self.docker_client = docker_client
        self.python_version = python_version
        self.host = f"http://{container_name}:{port}" if IN_DEV else f"http://127.0.0.1:{port}"

        if self.status() != "READY":
            raise Exception("Worker failed to start.")

    def exists(self):
        try:
            self.docker_client.inspect_container(self.container.get("Id"))
            return True
        except docker.errors.NotFound:
            return False

    def logs(self):
        if self.exists():
            return self.docker_client.logs(self.container.get("Id")).decode("utf-8")
        raise Exception("This worker no longer exists.")

    def remove(self):
        if self.exists():
            try:
                self.docker_client.remove_container(
                    self.container.get("Id"), force=True
                )  # The "force" arg kills it if it's not stopped
            except docker.errors.APIError as e:
                if not "409 Client Error" in str(e):
                    raise e

    def log_debug_info(self):
        container_logs = self.logs() if self.exists() else "Unable to retrieve container logs."
        container_logs = f"\nERROR INSIDE CONTAINER:\n{container_logs}\n"
        containers_info = self.docker_client.containers(all=True)
        containers_info = json.loads(json.dumps(containers_info, default=lambda thing: str(thing)))
        logger = logging.Client().logger("node_service")
        logger.log_struct(
            {
                "severity": "ERROR",
                "LOGS_FROM_FAILED_CONTAINER": container_logs,
                "CONTAINERS INFO": containers_info,
            }
        )

        if os.environ.get("IN_DEV"):  # <- to make debugging easier
            print(container_logs, file=sys.stderr)

    def status(self, attempt: int = 0):
        try:
            response = requests.get(f"{self.host}/")
            response.raise_for_status()
            status = response.json()["status"]  # will be one of: READY, RUNNING, FAILED, DONE
        except requests.exceptions.ConnectionError:
            if attempt <= 30:
                sleep(3)
                return self.status(attempt + 1)
            else:
                status = "FAILED"

        if status == "FAILED":
            self.log_debug_info()
            self.remove()

        return status
