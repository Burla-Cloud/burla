import sys
import json
import base64
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
    BURLA_BACKEND_URL,
    IN_LOCAL_DEV_MODE,
    IN_CLIENT_HOSTED_MODE,
    CURRENT_BURLA_VERSION,
    NODE_SOURCE_REF,
    MAIN_SERVICE_URL_FOR_NODES,
    CLUSTER_ID_TOKEN,
    SELF_DELETE_GUEST_ATTRIBUTE,
    BURLA_RELAY_SERVER_ADDR,
    BURLA_RELAY_SERVER_PORT,
    FRP_VERSION,
    relay_fqdn,
)
from main_service import cluster_state
from main_service.helpers import Logger, format_traceback
from main_service.providers import get_provider, InstanceDeletedMidBoot
from main_service.providers.catalog import machine_spec
from main_service.transport_tls import CA_CERT_PATH, cluster_ca_pem


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
        provider=None,
    ):
        self = cls.__new__(cls)
        self.logger = logger
        self.instance_name = node_dict["instance_name"]
        self.machine_type = node_dict.get("machine_type")
        self.containers = [
            Container.from_dict(c) for c in node_dict.get("containers") or []
        ]
        self.started_booting_at = node_dict.get("started_booting_at")
        self.inactivity_shutdown_time_sec = node_dict.get(
            "inactivity_shutdown_time_sec"
        )
        self.host = node_dict.get("host")
        self.peer_host = node_dict.get("peer_host")
        self.public_ip = node_dict.get("public_ip")
        self.private_ip = node_dict.get("private_ip")
        self.zone = node_dict.get("zone")
        self.current_job = node_dict.get("current_job")
        self.is_booting = node_dict.get("status") == "BOOTING"
        self.provider = provider or get_provider()
        return self

    @classmethod
    def start(
        cls,
        logger: Logger,
        machine_type: str,
        region: str,
        containers: list[Container],
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
        self.spot = spot
        self.port = service_port
        self.sync_bucket_name = sync_bucket_name
        self.inactivity_shutdown_time_sec = inactivity_shutdown_time_sec
        self.reserved_for_job = reserved_for_job
        self.disk_size = (
            disk_size if disk_size else 20
        )  # minimum is 10 due to disk image
        self.provider = provider or get_provider()

        self.instance_name = (
            instance_name if instance_name else f"burla-node-{uuid4().hex[:8]}"
        )
        self.started_booting_at = time()
        self.is_booting = True
        self.host = None
        self.peer_host = None
        self.public_ip = None
        self.private_ip = None
        self.zone = None
        self.current_job = None
        self.num_gpus = (
            machine_spec(machine_type)["gpus"] if not IN_LOCAL_DEV_MODE else 0
        )

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
                "peer_host": None,
                "public_ip": None,
                "private_ip": None,
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
                self.peer_host = self.host
            else:
                self.public_ip, self.private_ip, self.zone = (
                    self.provider.create_instance(
                        instance_name=self.instance_name,
                        machine_type=machine_type,
                        region=region,
                        disk_size=self.disk_size,
                        spot=spot,
                        num_gpus=self.num_gpus,
                        port=self.port,
                        startup_script=self.__get_startup_script(),
                        shutdown_script=self.__get_shutdown_script(),
                        on_log=lambda msg: cluster_state.add_node_log(
                            self.instance_name, msg
                        ),
                        # Only the shared-workspace mount needs cloud
                        # credentials on the VM.
                        needs_cloud_credentials=self._filesystem_enabled(),
                    )
                )
                # Clients reach the node through the relay on 443; nodes and
                # the head still talk to each other directly over the VPC.
                self.host = f"https://{relay_fqdn(self.instance_name)}"
                self.peer_host = f"https://{self.private_ip}:{self.port}"

            # The node polls its state-push responses for `host` and won't mark
            # itself READY until it appears.
            cluster_state.update_node(
                self.instance_name,
                {
                    "host": self.host,
                    "peer_host": self.peer_host,
                    "public_ip": self.public_ip,
                    "private_ip": self.private_ip,
                    "zone": self.zone,
                },
            )

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
        cluster_state.update_node(
            self.instance_name, {"status": "DELETED", "ended_at": time()}
        )
        self.provider.delete_instance(self.instance_name, self.zone)

    def _filesystem_enabled(self) -> bool:
        return bool(self.sync_bucket_name) and self.sync_bucket_name != "None"

    def status(self):
        """Returns one of: `BOOTING`, `RUNNING`, `READY`, `FAILED`"""

        # `host` points at the relay. A head VM shares a VPC with the node so
        # it polls the private IP directly; a client-hosted head is outside
        # the VPC and must go through the relay like any other client.
        if IN_CLIENT_HOSTED_MODE:
            poll_host = self.host or self.peer_host
        else:
            poll_host = self.peer_host or self.host

        # In local-dev the head runs on the docker host, not on the cluster
        # network, so it can't resolve a node's container name. Node ports are
        # published on 127.0.0.1, so poll there instead.
        if IN_LOCAL_DEV_MODE and poll_host:
            poll_host = f"http://127.0.0.1:{poll_host.rsplit(':', 1)[-1]}"

        if poll_host is not None:
            try:
                verify = str(CA_CERT_PATH) if poll_host.startswith("https://") else True
                response = requests.get(
                    f"{poll_host}/",
                    timeout=2,
                    headers={"Authorization": f"Bearer {CLUSTER_ID_TOKEN}"},
                    verify=verify,
                )
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
                    tb_details = traceback.format_exception(
                        exc_type, exc_value, exc_traceback
                    )
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
        if self._filesystem_enabled():
            mount_script = self.provider.mount_shared_workspace_script(
                self.sync_bucket_name
            )

        subdomain = f"{self.instance_name}--{PROJECT_ID}"
        frp_dir = f"frp_{FRP_VERSION}_linux_amd64"
        frp_url = (
            "https://github.com/fatedier/frp/releases/download/"
            f"v{FRP_VERSION}/{frp_dir}.tar.gz"
        )
        relay_tunnel_script = f"""
        report_log "Connecting relay tunnel {subdomain} ..."
        curl -fsSL -o /tmp/frp.tgz {frp_url}
        tar -xzf /tmp/frp.tgz -C /tmp
        cp /tmp/{frp_dir}/frpc /usr/local/bin/frpc
        cat > /etc/burla/frpc.toml <<FRPC_EOF
        serverAddr = "{BURLA_RELAY_SERVER_ADDR}"
        serverPort = {BURLA_RELAY_SERVER_PORT}
        loginFailExit = false
        user = "{PROJECT_ID}"
        metadatas.token = "{CLUSTER_ID_TOKEN}"
        transport.poolCount = 4

        [[proxies]]
        name = "{subdomain}"
        type = "https"
        localIP = "127.0.0.1"
        localPort = {self.port}
        subdomain = "{subdomain}"
        FRPC_EOF
        chmod 600 /etc/burla/frpc.toml
        systemd-run --unit=burla-frpc --property=Restart=always \\
            /usr/local/bin/frpc -c /etc/burla/frpc.toml"""

        ca_pem_b64 = base64.b64encode(cluster_ca_pem().encode()).decode()
        caddy_config = f""":{self.port} {{
    tls /etc/caddy/node.pem /etc/caddy/node.key
    reverse_proxy 127.0.0.1:8081
}}
"""
        caddy_config_b64 = base64.b64encode(caddy_config.encode()).decode()
        containers_b64 = base64.b64encode(
            json.dumps([container.to_dict() for container in self.containers]).encode()
        ).decode()

        # cloud-init (EC2 user-data) only executes scripts whose shebang is at
        # byte 0, so the indented template must be dedented + stripped. GCE's
        # guest agent tolerates either form.
        script = f"""
        #! /bin/bash
        set -Eeuo pipefail

        HEAD_URL="{MAIN_SERVICE_URL_FOR_NODES}"
        AUTH_HEADER="Authorization: Bearer {CLUSTER_ID_TOKEN}"
        NODE_NAME="{self.instance_name}"
        TLS_DIR="/etc/burla/tls"
        mkdir -p "$TLS_DIR" /etc/burla/caddy
        echo "{ca_pem_b64}" | base64 -d > "$TLS_DIR/ca.pem"
        cat /etc/ssl/certs/ca-certificates.crt "$TLS_DIR/ca.pem" > "$TLS_DIR/ca-bundle.pem"

        report_log() {{
            payload=$(jq -n --arg msg "$1" --arg ts "$(date +%s)" \\
                '{{"logs":[{{"msg":$msg,"ts":($ts|tonumber)}}]}}')
            curl -sS --cacert "$TLS_DIR/ca.pem" -o /dev/null \\
                -X POST "$HEAD_URL/v1/nodes/$NODE_NAME/logs:batch" \\
                -H "$AUTH_HEADER" -H "Content-Type: application/json" -d "$payload" || true
        }}

        handle_error() {{
            MSG="Startup script failed at line $1 with exit code $2! Deleting VM $NODE_NAME ... "
            echo "$MSG"
            report_log "$MSG"
            status_payload=$(jq -n --arg ts "$(date +%s)" \\
                '{{"status":"FAILED","ended_at":($ts|tonumber)}}')
            curl -sS --cacert "$TLS_DIR/ca.pem" -o /dev/null \\
                -X PUT "$HEAD_URL/v1/nodes/$NODE_NAME/state" \\
                -H "$AUTH_HEADER" -H "Content-Type: application/json" -d "$status_payload" || true
            curl -sS --cacert "$TLS_DIR/ca.pem" -o /dev/null \\
                -X POST "$HEAD_URL/v1/nodes/$NODE_NAME/self_delete" \\
                -H "$AUTH_HEADER" || true
            exit 1
        }}
        trap 'handle_error "$LINENO" "$?"' ERR

        openssl ecparam -name prime256v1 -genkey -noout -out "$TLS_DIR/node.key"
        chmod 600 "$TLS_DIR/node.key"
        openssl req -new -key "$TLS_DIR/node.key" -subj "/CN=$NODE_NAME" \\
            -out "$TLS_DIR/node.csr"
        csr_payload=$(jq -n --rawfile csr "$TLS_DIR/node.csr" '{{"csr":$csr}}')
        until cert_response=$(curl --fail --silent --show-error \\
            --cacert "$TLS_DIR/ca.pem" \\
            -X POST "$HEAD_URL/v1/nodes/$NODE_NAME/certificate" \\
            -H "$AUTH_HEADER" -H "Content-Type: application/json" \\
            -d "$csr_payload"); do
            sleep 1
        done
        echo "$cert_response" | jq -r .certificate > "$TLS_DIR/node.pem"

        rm -rf /etc/burla/caddy/Caddyfile
        echo "{caddy_config_b64}" | base64 -d > /etc/burla/caddy/Caddyfile

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
        export CONTAINERS=$(echo "{containers_b64}" | base64 -d)
        export INACTIVITY_SHUTDOWN_TIME_SEC="{self.inactivity_shutdown_time_sec}"
        export RESERVED_FOR_JOB="{self.reserved_for_job or ''}"
        export MAIN_SERVICE_URL="$HEAD_URL"
        export CLUSTER_ID_TOKEN="{CLUSTER_ID_TOKEN}"
        export BURLA_BACKEND_URL="{BURLA_BACKEND_URL}"

        cd /opt/burla
        # main_service is needed because building the client from source
        # vendors it into the wheel (client-hosted mode).
        git sparse-checkout set node_service client main_service
        git fetch --depth=1 origin "{NODE_SOURCE_REF}"
        git reset --hard FETCH_HEAD

        # Node images ship a pre-warmed /opt/burla/.venv; newer uv refuses to
        # overwrite an existing venv unless told to (older uv, as baked into
        # the GCP images, ignores this env var and overwrites regardless).
        export UV_VENV_CLEAR=1
        uv venv --python 3.13 --seed
        uv pip install ./node_service

        # Pre-populate the shared /worker_service_python_env so worker[0]'s boot doesn't
        # have to download uv from GitHub and install Burla inside the
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
                ./client
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
        docker rm -f burla-node-caddy || true
        docker pull caddy:2.10.2-alpine
        if ! CADDY_OUTPUT=$(docker run -d --restart=always --network=host \\
            --name=burla-node-caddy \\
            -v /etc/burla/tls/node.pem:/etc/caddy/node.pem:ro \\
            -v /etc/burla/tls/node.key:/etc/caddy/node.key:ro \\
            -v /etc/burla/caddy/Caddyfile:/etc/caddy/Caddyfile:ro \\
            caddy:2.10.2-alpine caddy run \\
            --config /etc/caddy/Caddyfile --adapter caddyfile 2>&1); then
            report_log "Node Caddy failed: $CADDY_OUTPUT"
            handle_error "$LINENO" 1
        fi
{relay_tunnel_script}

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
            --setenv=BURLA_BACKEND_URL="$BURLA_BACKEND_URL" \\
            --setenv=CLUSTER_CA_PATH="$TLS_DIR/ca.pem" \\
            --setenv=NODE_TLS_KEY_PATH="$TLS_DIR/node.key" \\
            --setenv=NODE_TLS_CERT_PATH="$TLS_DIR/node.pem" \\
            --collect \\
            /opt/burla/.venv/bin/python -m uvicorn node_service:app --host 127.0.0.1 --port 8081 --workers 1 --timeout-keep-alive 600

        journalctl -fu burla-node-service
        """
        return textwrap.dedent(script).strip() + "\n"

    def __get_shutdown_script(self):
        # GCP only (AWS ignores shutdown_script and terminates on poweroff
        # instead). The guest attribute records that Burla wanted this VM gone,
        # so `delete_stopped_instances` can finish deleting it without touching
        # a VM someone stopped on purpose. Written here as well as in the node
        # service, so it is set even when the stop wasn't node-initiated.
        script = f"""
        #! /bin/bash
        # Tell the node_service this VM is being shutdown so it can reassign inputs and stuff.
        curl -X POST "http://localhost:8081/shutdown"
        curl -X PUT --data "true" -H "Metadata-Flavor: Google" \\
            "http://metadata.google.internal/computeMetadata/v1/instance/guest-attributes/{SELF_DELETE_GUEST_ATTRIBUTE}" \\
            || true
        """
        return textwrap.dedent(script).strip() + "\n"
