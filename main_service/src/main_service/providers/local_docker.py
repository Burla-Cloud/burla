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


def remove_container(docker_client, container_id: str):
    """The head removes node containers from several places on purpose (each
    node's delete, plus label sweeps on shutdown and after boots), and the test
    suite restarts the cluster back to back, so two removals of one container
    overlap. Docker answers the loser with a 409 ("removal already in
    progress"). The container is going away either way, so treat that, and an
    already-gone container, as success: raising here aborts a whole cluster
    teardown and leaves the cluster with no nodes at all."""
    try:
        docker_client.remove_container(container_id, force=True)
    except docker.errors.NotFound:
        pass
    except docker.errors.APIError as e:
        if "already in progress" not in str(e):
            raise


def _cluster_volume(docker_client, name: str) -> str:
    """A docker volume tagged for this cluster, so `make stop` reclaims it."""
    full_name = f"burla-{CLUSTER_NAME}-{name}"
    docker_client.create_volume(name=full_name, labels={"burla-cluster": CLUSTER_NAME})
    return full_name


def _shared_uv_cache_volume(docker_client) -> str:
    """One uv cache for every burla cluster on this machine (uv is built for
    concurrent use). Labeled with its own key so `make stop` leaves it alone:
    emptying the cache is what made the first post-stop test run re-download
    every wheel at once, which is exactly the load spike that wedged nodes.
    `make stop-all` reclaims it."""
    docker_client.create_volume(name="burla-uv-cache", labels={"burla-uv-cache": "1"})
    return "burla-uv-cache"


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
        # Everything heavy a node builds for itself lives in volumes rather than
        # in the node, so a replacement node inherits it instead of rebuilding
        # it: the inner image store (~1GB unpacked) and the python env rpm
        # replicates from the client (gigabytes, installed inside the client's
        # 2min assign-job budget). The test suite replaces nodes constantly.
        # Keyed by port, this cluster's node slot: ports are handed out from a
        # fixed base, so a replacement reuses the store of the node it replaces
        # and two live nodes never share one.
        # These are volumes and not host binds because a host bind on macOS is
        # virtiofs, and two nodes installing multi-GB envs across it at once
        # stall each other long enough to look dead to the head.
        inner_docker_store = _cluster_volume(docker_client, f"docker-{port}")
        worker_python_env = _cluster_volume(docker_client, f"worker-env-{port}")
        uv_cache = _shared_uv_cache_volume(docker_client)
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
                uv_cache: "/uv_cache",
                worker_python_env: "/worker_service_python_env",
                # /var/lib/docker also cannot be a directory in the node's own
                # overlayfs root, which cannot stack another overlayfs: the
                # inner dockerd would fall back to the crawling `vfs` driver.
                inner_docker_store: "/var/lib/docker",
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
                remove_container(docker_client, container["Id"])
