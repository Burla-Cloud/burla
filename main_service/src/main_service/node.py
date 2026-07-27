import sys
import json
import requests
import textwrap
import traceback
from dataclasses import dataclass, asdict
from requests.exceptions import ConnectionError, ConnectTimeout, HTTPError, Timeout
from time import sleep, time
from uuid import uuid4
from typing import Optional

from main_service import (
    PROJECT_ID,
    IN_LOCAL_DEV_MODE,
    CURRENT_BURLA_VERSION,
    MAIN_SERVICE_URL_FOR_NODES,
    CLUSTER_ID_TOKEN,
)
from main_service import cluster_state
from main_service.helpers import Logger, format_traceback
from main_service.providers import get_provider, InstanceDeletedMidBoot
from main_service.providers.catalog import machine_spec


@dataclass
class Container:
    image: str

    @classmethod
    def from_dict(cls, _dict: dict):
        return cls(
            image=_dict["image"],
        )

    def to_dict(self):
        return asdict(self)


NODE_BOOT_TIMEOUT = 60 * 10
NODE_SERVICE_RESERVED_MEMORY_GB = 4


class Node:

    def __init__(self):
        # Prevents instantiation of nodes that do not exist.
        raise NotImplementedError("Please use `Node.start`, or `Node.from_state`")

    @classmethod
    def from_state(
        cls,
        logger: Logger,
        node_dict: dict,
        auth_headers: dict,
        provider=None,
    ):
        self = cls.__new__(cls)
        self.logger = logger
        self.instance_name = node_dict["instance_name"]
        self.machine_type = node_dict.get("machine_type")
        self.containers = [Container.from_dict(c) for c in node_dict.get("containers") or []]
        self.started_booting_at = node_dict.get("started_booting_at")
        self.inactivity_shutdown_time_sec = node_dict.get("inactivity_shutdown_time_sec")
        self.host = node_dict.get("host")
        self.zone = node_dict.get("zone")
        self.current_job = node_dict.get("current_job")
        self.is_booting = node_dict.get("status") == "BOOTING"
        self.auth_headers = auth_headers
        self.provider = provider or get_provider()
        return self

    @classmethod
    def start(
        cls,
        logger: Logger,
        machine_type: str,
        region: str,
        containers: list[Container],
        auth_headers: dict,
        spot: bool = False,
        service_port: int = 8080,  # <- this needs to be open in your cloud firewall!
        sync_bucket_name: Optional[str] = None,  # <- not a uri, just the name
        provider=None,
        inactivity_shutdown_time_sec: Optional[int] = None,
        disk_size: Optional[int] = None,
        instance_name: Optional[str] = None,
        reserved_for_job: Optional[str] = None,
    ):
        self = cls.__new__(cls)
        self.logger = logger
        self.region = region
        self.machine_type = machine_type
        self.containers = containers
        self.auth_headers = auth_headers
        self.spot = spot
        self.port = service_port
        self.sync_bucket_name = sync_bucket_name
        self.inactivity_shutdown_time_sec = inactivity_shutdown_time_sec
        self.reserved_for_job = reserved_for_job
        self.disk_size = disk_size if disk_size else 20  # minimum is 10 due to disk image
        self.provider = provider or get_provider()

        self.instance_name = instance_name if instance_name else f"burla-node-{uuid4().hex[:8]}"
        self.started_booting_at = time()
        self.is_booting = True
        self.host = None
        self.zone = None
        self.current_job = None
        self.num_gpus = machine_spec(machine_type)["gpus"] if not IN_LOCAL_DEV_MODE else 0

        cluster_state.update_node(
            self.instance_name,
            {
                "instance_name": self.instance_name,
                "status": "BOOTING",
                "machine_type": machine_type,
                "gcp_region": region,
                "containers": [container.to_dict() for container in containers],
                "started_booting_at": self.started_booting_at,
                "inactivity_shutdown_time_sec": inactivity_shutdown_time_sec,
                "disk_size": self.disk_size,
                "num_gpus": self.num_gpus,
                "spot": spot,
                "port": service_port,
                "sync_gcs_bucket_name": sync_bucket_name,
                "host": None,
                "zone": None,
                "current_job": None,
                "reserved_for_job": reserved_for_job,
            },
        )

        try:
            if IN_LOCAL_DEV_MODE:
                self.host = self.provider.create_node_container(
                    instance_name=self.instance_name,
                    port=self.port,
                    containers=[c.to_dict() for c in containers],
                    inactivity_shutdown_time_sec=inactivity_shutdown_time_sec,
                    reserved_for_job=reserved_for_job,
                )
            else:
                self.host, self.zone = self.provider.create_instance(
                    instance_name=self.instance_name,
                    machine_type=machine_type,
                    region=region,
                    disk_size=self.disk_size,
                    spot=spot,
                    num_gpus=self.num_gpus,
                    port=self.port,
                    startup_script=self.__get_startup_script(),
                    shutdown_script=self.__get_shutdown_script(),
                    on_log=lambda msg: cluster_state.add_node_log(self.instance_name, msg),
                )
                self.host = f"http://{self.host}:{self.port}"

            # The node polls its state-push responses for `host` and won't mark
            # itself READY until it appears.
            cluster_state.update_node(self.instance_name, {"host": self.host, "zone": self.zone})

            start = time()
            status = self.status()
            while status not in ("READY", "RUNNING"):
                sleep(1)
                booting_too_long = (time() - start) > NODE_BOOT_TIMEOUT
                status = self.status()

                # Startup-script trap reports FAILED over HTTP, bypassing the
                # node_service path self.status() checks.
                if status == "BOOTING":
                    state = cluster_state.get_node(self.instance_name)
                    if state and state.get("status") == "FAILED":
                        status = "FAILED"

                if status == "FAILED" or booting_too_long:
                    msg = f"Node {self.instance_name} Failed to start! (timeout={booting_too_long})"
                    raise Exception(msg)
        except InstanceDeletedMidBoot:
            raise
        except Exception as e:
            cluster_state.update_node(self.instance_name, {"status": "FAILED"})
            cluster_state.add_node_log(self.instance_name, traceback.format_exc())
            self.delete()
            raise e

        self.is_booting = False
        return self

    def delete(self):
        # FAILED nodes keep their status so the doc remains visible for debugging.
        cluster_state.update_node(self.instance_name, {"status": "DELETED", "ended_at": time()})
        self.provider.delete_instance(self.instance_name, self.zone)

    def status(self):
        """Returns one of: `BOOTING`, `RUNNING`, `READY`, `FAILED`"""

        if self.host is not None:
            try:
                response = requests.get(f"{self.host}/", timeout=2, headers=self.auth_headers)
                response.raise_for_status()
                return response.json()["status"]
            except (ConnectionError, ConnectTimeout, Timeout, HTTPError):
                # Transient error responses during boot (e.g. a 500 while the
                # service warms up) must not insta-fail the node; the boot
                # timeout still bounds how long we wait.
                if self.is_booting:
                    return "BOOTING"
                else:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
                    traceback_str = format_traceback(tb_details)
                    msg = f"Node {self.instance_name} has FAILED (no response after 2 sec)."
                    self.logger.log(msg, severity="ERROR", traceback=traceback_str)
                    return "FAILED"
        elif self.is_booting:
            return "BOOTING"
        else:
            raise Exception("Node not booting but also has no hostname?")

    def __get_startup_script(self):
        mount_script = ""
        if self.sync_bucket_name and self.sync_bucket_name != "None":
            mount_script = self.provider.mount_shared_workspace_script(self.sync_bucket_name)

        # cloud-init (EC2 user-data) only executes scripts whose shebang is at
        # byte 0, so the indented template must be dedented + stripped. GCE's
        # guest agent tolerates either form.
        script = f"""
        #! /bin/bash
        set -Eeuo pipefail

        HEAD_URL="{MAIN_SERVICE_URL_FOR_NODES}"
        AUTH_HEADER="Authorization: Bearer {CLUSTER_ID_TOKEN}"
        NODE_NAME="{self.instance_name}"

        report_log() {{
            payload=$(jq -n --arg msg "$1" --arg ts "$(date +%s)" \\
                '{{"logs":[{{"msg":$msg,"ts":($ts|tonumber)}}]}}')
            curl -sS -o /dev/null -X POST "$HEAD_URL/v1/nodes/$NODE_NAME/logs:batch" \\
                -H "$AUTH_HEADER" -H "Content-Type: application/json" -d "$payload" || true
        }}

        handle_error() {{
            MSG="Startup script failed! Deleting VM $NODE_NAME ... "
            echo "$MSG"
            report_log "$MSG"
            status_payload=$(jq -n --arg ts "$(date +%s)" \\
                '{{"status":"FAILED","ended_at":($ts|tonumber)}}')
            curl -sS -o /dev/null -X PUT "$HEAD_URL/v1/nodes/$NODE_NAME/state" \\
                -H "$AUTH_HEADER" -H "Content-Type: application/json" -d "$status_payload" || true
            curl -sS -o /dev/null -X POST "$HEAD_URL/v1/nodes/$NODE_NAME/self_delete" \\
                -H "$AUTH_HEADER" || true
            exit 1
        }}
        trap 'handle_error' ERR

        # make docker pull faster, this seems to actually do nothing at all.
        # TODO: figure out why/if this doesn't work.
        mkdir -p /etc/docker
        jq '. + {{"max-concurrent-downloads": 32}}' /etc/docker/daemon.json 2>/dev/null || echo '{{}}' | jq '. + {{"max-concurrent-downloads": 32}}' > /etc/docker/daemon.json
        killall -HUP dockerd || true

        # mount shared workspace bucket at /workspace/shared
        cd /
        mkdir -p /workspace/shared
        {mount_script}

        # make uv work, this is an oopsie from when building the disk image:
        export PATH="/root/.cargo/bin:$PATH"
        export PATH="/root/.local/bin:$PATH"

        report_log "Installing Burla node service v{CURRENT_BURLA_VERSION} ..."

        export NUM_GPUS="{self.num_gpus}"
        export INSTANCE_NAME="$NODE_NAME"
        export PROJECT_ID="{PROJECT_ID}"
        export CONTAINERS='{json.dumps([c.to_dict() for c in self.containers])}'
        export INACTIVITY_SHUTDOWN_TIME_SEC="{self.inactivity_shutdown_time_sec}"
        export RESERVED_FOR_JOB="{self.reserved_for_job or ''}"
        export MAIN_SERVICE_URL="$HEAD_URL"
        export CLUSTER_ID_TOKEN="{CLUSTER_ID_TOKEN}"

        cd /opt/burla
        git fetch --depth=1 origin "{CURRENT_BURLA_VERSION}"
        git reset --hard FETCH_HEAD

        # Node images ship a pre-warmed /opt/burla/.venv; newer uv refuses to
        # overwrite an existing venv unless told to (older uv, as baked into
        # the GCP images, ignores this env var and overwrites regardless).
        export UV_VENV_CLEAR=1
        uv venv --python 3.13 --seed
        uv pip install ./node_service

        # Pre-populate the shared /worker_service_python_env so worker[0]'s boot doesn't
        # have to download uv from GitHub and `uv pip install burla` over PyPI inside the
        # container. We detect the python version from the first user container image
        # because cp311/cp312/cp313 C-extension wheels (cryptography, aiohttp, …) are
        # ABI-incompatible across cpython tags. Subshell makes the whole block best-effort:
        # if the image is missing python or anything else trips, fall through and let
        # worker_server.py run its own download path rather than killing the VM via the
        # outer trap.
        (
            FIRST_IMAGE=$(echo "$CONTAINERS" | python3 -c \\
                'import json,sys; d=json.load(sys.stdin); print(d[0]["image"] if d else "")')
            [ -n "$FIRST_IMAGE" ] || exit 0
            docker pull "$FIRST_IMAGE" >/dev/null
            PY_VERSION=$(docker run --rm --entrypoint python "$FIRST_IMAGE" -c \\
                'import sys; print(f"{{sys.version_info.major}}.{{sys.version_info.minor}}")')
            mkdir -p /worker_service_python_env/bin
            cp "$(command -v uv)" /worker_service_python_env/bin/uv
            uv pip install \\
                --python-version "$PY_VERSION" \\
                --python-platform x86_64-manylinux2014 \\
                --target /worker_service_python_env \\
                "burla=={CURRENT_BURLA_VERSION}"
        ) || true

        total_memory_kb=$(awk '/MemTotal/ {{print $2}}' /proc/meminfo)
        worker_memory_kb=$((total_memory_kb - {NODE_SERVICE_RESERVED_MEMORY_GB} * 1024 * 1024))
        if [ "$worker_memory_kb" -lt $((1024 * 1024)) ]; then
            worker_memory_kb=$((1024 * 1024))
        fi

        printf '[Slice]\\nMemoryMin={NODE_SERVICE_RESERVED_MEMORY_GB}G\\nCPUWeight=1000\\n' \\
            >/etc/systemd/system/burla-node-service.slice
        printf '[Slice]\\nMemoryMax=%sK\\nCPUWeight=80\\n' "$worker_memory_kb" \\
            >/etc/systemd/system/burla-workers.slice

        systemctl daemon-reload
        systemctl start burla-node-service.slice burla-workers.slice

        systemd-run \\
            --unit=burla-node-service \\
            --slice=burla-node-service.slice \\
            --property=MemoryMin={NODE_SERVICE_RESERVED_MEMORY_GB}G \\
            --property=CPUWeight=1000 \\
            --property=OOMScoreAdjust=-900 \\
            --setenv=NUM_GPUS="$NUM_GPUS" \\
            --setenv=INSTANCE_NAME="$INSTANCE_NAME" \\
            --setenv=PROJECT_ID="$PROJECT_ID" \\
            --setenv=CONTAINERS="$CONTAINERS" \\
            --setenv=INACTIVITY_SHUTDOWN_TIME_SEC="$INACTIVITY_SHUTDOWN_TIME_SEC" \\
            --setenv=RESERVED_FOR_JOB="$RESERVED_FOR_JOB" \\
            --setenv=MAIN_SERVICE_URL="$MAIN_SERVICE_URL" \\
            --setenv=CLUSTER_ID_TOKEN="$CLUSTER_ID_TOKEN" \\
            --collect \\
            /opt/burla/.venv/bin/python -m uvicorn node_service:app --host 0.0.0.0 --port {self.port} --workers 1 --timeout-keep-alive 600

        journalctl -fu burla-node-service
        """
        return textwrap.dedent(script).strip() + "\n"

    def __get_shutdown_script(self):
        script = f"""
        #! /bin/bash
        # Tell the node_service this VM is being shutdown so it can reassign inputs and stuff.
        curl -X POST "http://localhost:{self.port}/shutdown"
        """
        return textwrap.dedent(script).strip() + "\n"
