import os
import json

import docker

from main_service import (
    BURLA_BACKEND_URL,
    CLUSTER_ID_TOKEN,
    MAIN_SERVICE_URL_FOR_NODES,
    PROJECT_ID,
)


class LocalDockerProvider:
    """local-dev: nodes are docker containers on the `local-burla-cluster`
    network, mounted so node/worker code hot-reloads on save."""

    def create_node_container(
        self,
        instance_name: str,
        port: int,
        containers: list[dict],
        inactivity_shutdown_time_sec,
        reserved_for_job,
    ) -> str:
        image = f"us-docker.pkg.dev/{PROJECT_ID}/burla-node-service/burla-node-service:latest"
        docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
        host_config = docker_client.create_host_config(
            port_bindings={port: ("127.0.0.1", port)},
            network_mode="local-burla-cluster",
            binds={
                f"{os.environ['HOST_HOME_DIR']}/.config/gcloud": "/root/.config/gcloud",
                f"{os.environ['HOST_PWD']}/node_service": "/opt/burla/node_service",
                f"{os.environ['HOST_PWD']}/_shared_workspace": "/workspace/shared",
                f"{os.environ['HOST_PWD']}/_worker_service_python_env": "/worker_service_python_env",
                f"{os.environ['HOST_PWD']}/_python_version_marker": "/python_version_marker",
                # node_auth bind: see NODE_AUTH_DIR in node_service/__init__.py.
                f"{os.environ['HOST_PWD']}/_node_auth": "/opt/burla/node_auth",
                "/var/run/docker.sock": "/var/run/docker.sock",
            },
        )

        try:
            docker_client.pull(image)
        except docker.errors.APIError as error:
            if "Unauthenticated request" in str(error):
                import google.auth
                from google.auth.transport.requests import Request

                credentials, _ = google.auth.default()
                credentials.refresh(Request())
                auth_config = {
                    "username": "oauth2accesstoken",
                    "password": credentials.token,
                }
                docker_client.pull(image, auth_config=auth_config)
            else:
                raise

        cmd_script = f"""
            cd /opt/burla/node_service
            uv run -m uvicorn node_service:app --host 0.0.0.0 --port {port} --workers 1 \
                --timeout-keep-alive 600 --reload
        """.strip()
        container_name = f"node_{instance_name[11:]}"
        container = docker_client.create_container(
            image=image,
            command=["-c", cmd_script],
            entrypoint=["bash"],
            name=container_name,
            ports=[port],
            host_config=host_config,
            environment={
                "PROJECT_ID": PROJECT_ID,
                "BURLA_BACKEND_URL": BURLA_BACKEND_URL,
                "IN_LOCAL_DEV_MODE": "True",
                "HOST_HOME_DIR": os.environ["HOST_HOME_DIR"],
                "HOST_PWD": os.environ["HOST_PWD"],
                "INSTANCE_NAME": instance_name,
                "CONTAINERS": json.dumps(containers),
                "INACTIVITY_SHUTDOWN_TIME_SEC": inactivity_shutdown_time_sec,
                "RESERVED_FOR_JOB": reserved_for_job or "",
                "NUM_GPUS": 0,
                "MAIN_SERVICE_URL": MAIN_SERVICE_URL_FOR_NODES,
                "CLUSTER_ID_TOKEN": CLUSTER_ID_TOKEN,
                "BURLA_BACKEND_URL": BURLA_BACKEND_URL,
            },
            detach=True,
        )
        docker_client.start(container=container.get("Id"))
        return f"http://{container_name}:{port}"

    def delete_instance(self, instance_name: str, zone: str | None = None):
        docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
        container_name = f"node_{instance_name[11:]}"
        for container in docker_client.containers(all=True):
            if any(name == f"/{container_name}" for name in container["Names"]):
                docker_client.remove_container(container["Id"], force=True)
