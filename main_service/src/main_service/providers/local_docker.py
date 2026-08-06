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
    """local-dev: each node is a privileged container acting as a fake VM. It
    runs its own docker daemon (see node_service/local_dev_entrypoint.sh), so
    workers are inner containers owned by the node, exactly like on a real VM,
    and node_service runs the same code path in every mode. Node containers
    carry a `burla-cluster` label so teardown never touches another checkout's
    cluster; workers live inside the node and die with it."""

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
            # Privileged so the node can run its own dockerd for its workers.
            privileged=True,
            port_bindings={port: ("127.0.0.1", port)},
            network_mode=LOCAL_DEV_NETWORK,
            # The head runs on the docker host, so nodes reach it (MAIN_SERVICE_URL
            # below) at host.docker.internal. Explicit mapping so this also works
            # on Linux, where Docker Desktop's automatic alias is absent.
            extra_hosts={"host.docker.internal": "host-gateway"},
            binds={
                f"{os.environ['HOST_PWD']}/node_service": "/opt/burla/node_service",
                f"{os.environ['HOST_PWD']}/_shared_workspace": "/workspace/shared",
                # node_auth bind: see NODE_AUTH_DIR in node_service/__init__.py.
                f"{os.environ['HOST_PWD']}/_node_auth": "/opt/burla/node_auth",
                # Real VMs carry the client checkout at this path; workers
                # install burla from it when their python env is empty.
                f"{os.environ['HOST_PWD']}/client": {
                    "bind": "/opt/burla/client",
                    "mode": "ro",
                },
                f"{os.environ['HOST_PWD']}/_image_seed": {
                    "bind": "/opt/burla/image-seed",
                    "mode": "ro",
                },
            },
        )

        _ensure_node_image(docker_client, image)

        container_name = f"node_{instance_name[11:]}"
        container = docker_client.create_container(
            image=image,
            command=["/opt/burla/node_service/local_dev_entrypoint.sh"],
            entrypoint=["bash"],
            name=container_name,
            ports=[port],
            host_config=host_config,
            # The inner daemon's image/layer store. Declared as an anonymous
            # volume because overlayfs cannot stack on the node's own overlayfs
            # root; without it dockerd falls back to the crawling `vfs` driver.
            # `make stop` removes these volumes together with the containers.
            volumes=["/var/lib/docker"],
            # `burla-cluster` marks everything belonging to this cluster (what
            # `make stop` removes). `burla-cluster-member` marks only the
            # nodes, so the head tearing the cluster down cannot delete itself.
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
                "NODE_PORT": port,
                "INSTANCE_NAME": instance_name,
                "CONTAINERS": json.dumps(containers),
                "INACTIVITY_SHUTDOWN_TIME_SEC": inactivity_shutdown_time_sec,
                "RESERVED_FOR_JOB": reserved_for_job or "",
                "NUM_GPUS": 0,
                "MAIN_SERVICE_URL": MAIN_SERVICE_URL_FOR_NODES,
                "CLUSTER_ID_TOKEN": CLUSTER_ID_TOKEN,
                "BURLA_BACKEND_URL": BURLA_BACKEND_URL,
                # Pass the telemetry kill switch down so a cluster started
                # with it set is silent end to end (nodes forward it to
                # their workers, where nested rpm clients also send telemetry).
                "DISABLE_BURLA_TELEMETRY": os.environ.get("DISABLE_BURLA_TELEMETRY", ""),
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
                # v=True removes the node's anonymous /var/lib/docker volume,
                # which is ~1GB of inner images per node otherwise stranded.
                docker_client.remove_container(container["Id"], force=True, v=True)
