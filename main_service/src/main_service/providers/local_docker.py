import os
import json

import docker

from main_service import (
    BURLA_BACKEND_URL,
    CLUSTER_ID_TOKEN,
    CLUSTER_NAME,
    LOCAL_DEV_NETWORK,
    MAIN_SERVICE_URL_FOR_NODES,
    PROJECT_ID,
)


def _ensure_node_image(docker_client, image: str):
    """`make local-dev` builds this image locally, so it is normally already here.
    Only reach for a registry when it isn't, which is how a custom
    BURLA_NODE_IMAGE pointing at a real registry keeps working."""
    try:
        docker_client.inspect_image(image)
    except docker.errors.ImageNotFound:
        docker_client.pull(image)


class LocalDockerProvider:
    """local-dev: nodes are docker containers on this cluster's own docker
    network, mounted so node/worker code hot-reloads on save. Every container
    carries a `burla-cluster` label so teardown never touches another
    checkout's cluster running on the same docker daemon."""

    def create_node_container(
        self,
        instance_name: str,
        port: int,
        containers: list[dict],
        inactivity_shutdown_time_sec,
        reserved_for_job,
    ) -> str:
        image = os.environ.get("BURLA_NODE_IMAGE", "burla-node-service:local-dev")
        docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
        host_config = docker_client.create_host_config(
            port_bindings={port: ("127.0.0.1", port)},
            network_mode=LOCAL_DEV_NETWORK,
            # The head runs on the docker host, so nodes reach it (MAIN_SERVICE_URL
            # below) at host.docker.internal. Explicit mapping so this also works
            # on Linux, where Docker Desktop's automatic alias is absent.
            extra_hosts={"host.docker.internal": "host-gateway"},
            binds={
                f"{os.environ['HOST_PWD']}/node_service": "/opt/burla/node_service",
                f"{os.environ['HOST_PWD']}/_shared_workspace": "/workspace/shared",
                f"{os.environ['HOST_PWD']}/_worker_service_python_env": "/worker_service_python_env",
                # node_auth bind: see NODE_AUTH_DIR in node_service/__init__.py.
                f"{os.environ['HOST_PWD']}/_node_auth": "/opt/burla/node_auth",
                "/var/run/docker.sock": "/var/run/docker.sock",
            },
        )

        _ensure_node_image(docker_client, image)

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
            # `burla-cluster` marks everything belonging to this cluster (what
            # `make stop` removes). `burla-cluster-member` marks only the
            # nodes/workers, so the head tearing the cluster down cannot delete
            # itself.
            labels={
                "burla-cluster": CLUSTER_NAME,
                "burla-cluster-member": CLUSTER_NAME,
            },
            environment={
                # Without this python buffers stdout, so `docker logs node_*`
                # lags far behind the node and looks stuck mid-boot.
                "PYTHONUNBUFFERED": "1",
                "PROJECT_ID": PROJECT_ID,
                "IN_LOCAL_DEV_MODE": "True",
                "BURLA_CLUSTER_NAME": CLUSTER_NAME,
                "LOCAL_DEV_NETWORK": LOCAL_DEV_NETWORK,
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
