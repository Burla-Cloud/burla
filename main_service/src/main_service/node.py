import os
import sys
import json
import requests
import traceback
from dataclasses import dataclass, asdict
from requests.exceptions import ConnectionError, ConnectTimeout, Timeout
from time import sleep, time
from uuid import uuid4
from typing import Optional

import docker
from docker.errors import APIError
from google.cloud import resourcemanager_v3
from google.auth.transport.requests import Request
from google.api_core.exceptions import NotFound, ServiceUnavailable, Conflict
from google.cloud.compute_v1 import MachineTypesClient, AggregatedListMachineTypesRequest
from google.cloud import firestore
from google.cloud.firestore import DocumentSnapshot
from google.cloud.compute_v1 import (
    AttachedDisk,
    NetworkInterface,
    AttachedDiskInitializeParams,
    Metadata,
    Items,
    AccessConfig,
    ServiceAccount,
    Tags,
    InstancesClient,
    Instance,
    Scheduling,
)

from main_service import PROJECT_ID, CREDENTIALS, IN_LOCAL_DEV_MODE, CURRENT_BURLA_VERSION
from main_service.helpers import Logger, format_traceback


@dataclass
class Container:
    image: int
    python_version: str

    @classmethod
    def from_dict(cls, _dict: dict):
        return cls(
            image=_dict["image"],
            python_version=_dict["python_version"],
        )

    def to_dict(self):
        return asdict(self)


# This is 100% guessed, is used for unimportant estimates / ranking
TOTAL_BOOT_TIME = 60
TOTAL_REBOOT_TIME = 30

client = resourcemanager_v3.ProjectsClient(credentials=CREDENTIALS)
project = client.get_project(name=f"projects/{PROJECT_ID}")
GCE_DEFAULT_SVC = f"{project.name.split('/')[-1]}-compute@developer.gserviceaccount.com"

NODE_BOOT_TIMEOUT = 60 * 10


def zones_supporting_machine_type(region_name: str, machine_type_name: str):
    name_filter = f"name={machine_type_name}"
    request = AggregatedListMachineTypesRequest(project=PROJECT_ID, filter=name_filter)
    zone_generator = MachineTypesClient().aggregated_list(request=request)
    for zone, matches in zone_generator:
        if matches.machine_types and zone.startswith(f"zones/{region_name}"):
            yield zone.split("/")[1]


class Node:

    def __init__(self):
        # Prevents instantiation of nodes that do not exist.
        raise NotImplementedError("Please use `Node.start`, or `Node.from_snapshot`")

    @classmethod
    def from_snapshot(
        cls,
        db: firestore.Client,
        logger: Logger,
        node_snapshot: DocumentSnapshot,
        auth_headers: dict,
        instance_client: Optional[InstancesClient] = None,
    ):
        node_doc = node_snapshot.to_dict()
        self = cls.__new__(cls)
        self.node_ref = node_snapshot.reference
        self.db = db
        self.logger = logger
        self.instance_name = node_doc["instance_name"]
        self.machine_type = node_doc["machine_type"]
        self.containers = [Container.from_dict(c) for c in node_doc["containers"]]
        self.started_booting_at = node_doc["started_booting_at"]
        self.inactivity_shutdown_time_sec = node_doc["inactivity_shutdown_time_sec"]
        self.host = node_doc["host"]
        self.zone = node_doc["zone"]
        self.current_job = node_doc["current_job"]
        self.is_booting = node_doc["status"] == "BOOTING"
        self.instance_client = instance_client
        self.auth_headers = auth_headers
        return self

    @classmethod
    def start(
        cls,
        db: firestore.Client,
        logger: Logger,
        machine_type: str,
        gcp_region: str,
        containers: list[Container],
        auth_headers: dict,
        spot: bool = False,
        service_port: int = 8080,  # <- this needs to be open in your cloud firewall!
        as_local_container: bool = False,
        sync_gcs_bucket_name: Optional[str] = None,  # <- not a uri, just the name
        instance_client: Optional[InstancesClient] = None,
        inactivity_shutdown_time_sec: Optional[int] = None,
        disk_size: Optional[int] = None,
    ):
        self = cls.__new__(cls)
        self.db = db
        self.logger = logger
        self.gcp_region = gcp_region
        self.machine_type = machine_type
        self.containers = containers
        self.auth_headers = auth_headers
        self.spot = spot
        self.port = service_port
        self.sync_gcs_bucket_name = sync_gcs_bucket_name
        self.inactivity_shutdown_time_sec = inactivity_shutdown_time_sec
        self.disk_size = disk_size if disk_size else 20  # minimum is 10 due to disk image
        self.instance_client = instance_client if instance_client else InstancesClient()

        self.instance_name = f"burla-node-{uuid4().hex[:8]}"
        self.started_booting_at = time()
        self.is_booting = True
        self.host = None
        self.zone = None
        self.current_job = None
        self.node_ref = self.db.collection("nodes").document(self.instance_name)

        self.num_gpus = 0
        if machine_type.startswith("a"):
            self.num_gpus = int(machine_type.split("-")[-1][:-1])

        if machine_type.startswith("n4"):
            self.disk_image = "projects/burla-prod/global/images/burla-node-nogpu-2"
        elif machine_type.startswith("a2") or machine_type.startswith("a3"):
            self.disk_image = "projects/burla-prod/global/images/burla-node-gpu-2"
        else:
            raise ValueError(f"Invalid machine type: {machine_type}")

        current_state = dict(self.__dict__)  # <- create copy to modify / save
        current_state["status"] = "BOOTING"
        current_state["main_svc_version"] = CURRENT_BURLA_VERSION
        current_state["display_in_dashboard"] = True
        current_state["containers"] = [container.to_dict() for container in containers]
        attrs_to_not_save = ["db", "logger", "instance_client", "node_ref", "auth_headers"]
        current_state = {k: v for k, v in current_state.items() if k not in attrs_to_not_save}
        self.node_ref.set(current_state)

        log = {"msg": f"Adding node {self.instance_name} ({self.machine_type}) ...", "ts": time()}
        self.node_ref.collection("logs").document().set(log)

        try:
            if as_local_container:
                self.__start_svc_in_local_container()
            else:
                self.__start_svc_in_vm(disk_image=self.disk_image, disk_size=self.disk_size)

            start = time()
            status = self.status()
            while status != "READY":
                sleep(1)
                booting_too_long = (time() - start) > NODE_BOOT_TIMEOUT
                status = self.status()

                if status == "FAILED" or booting_too_long:
                    msg = f"Node {self.instance_name} Failed to start! (timeout={booting_too_long})"
                    raise Exception(msg)
        except Exception as e:
            self.node_ref.update({"status": "FAILED"})
            log = {"msg": traceback.format_exc(), "ts": time()}
            self.node_ref.collection("logs").document().set(log)
            self.delete()
            raise e

        self.node_ref.update(dict(host=self.host, zone=self.zone))  # node svc marks itself as ready
        self.is_booting = False
        return self

    def delete(self, hide_if_failed: bool = False):
        """
        hide_if_failed: should I hide this node from the dashboard if it's state is failed?
        be default, no, so the user can inspect the logs of a failed node, then remove it later.
        """
        node_snapshot = self.node_ref.get()
        node_is_failed = node_snapshot.exists and node_snapshot.to_dict().get("status") == "FAILED"
        display_if_failed = not hide_if_failed

        if node_is_failed:
            self.node_ref.update(dict(status="FAILED", display_in_dashboard=display_if_failed))
        else:
            self.node_ref.update(dict(status="DELETED", display_in_dashboard=False))

        if not self.instance_client:
            self.instance_client = InstancesClient()
        try:
            kwargs = dict(project=PROJECT_ID, zone=self.zone, instance=self.instance_name)
            self.instance_client.delete(**kwargs)
        except (NotFound, ValueError):
            pass  # these errors mean it was already deleted.

    def status(self):
        """Returns one of: `BOOTING`, `RUNNING`, `READY`, `FAILED`"""

        if self.host is not None:
            try:
                response = requests.get(f"{self.host}/", timeout=2, headers=self.auth_headers)
                response.raise_for_status()
                return response.json()["status"]
            except (ConnectionError, ConnectTimeout, Timeout):
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

    def __start_svc_in_local_container(self):
        image = f"us-docker.pkg.dev/{PROJECT_ID}/burla-node-service/burla-node-service:latest"
        docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
        host_config = docker_client.create_host_config(
            port_bindings={self.port: self.port},
            network_mode="local-burla-cluster",
            binds={
                f"{os.environ['HOST_HOME_DIR']}/.config/gcloud": "/root/.config/gcloud",
                f"{os.environ['HOST_PWD']}/node_service": "/opt/burla/node_service",
                f"{os.environ['HOST_PWD']}/_shared_workspace": "/shared_workspace",
                f"{os.environ['HOST_PWD']}/_worker_service_python_env": "/worker_service_python_env",
                "/var/run/docker.sock": "/var/run/docker.sock",
                "/usr/local/bin/docker": "/usr/bin/docker",
            },
        )

        try:
            docker_client.pull(image)
        except APIError as e:
            if "Unauthenticated request" in str(e):
                CREDENTIALS.refresh(Request())
                auth_config = {"username": "oauth2accesstoken", "password": CREDENTIALS.token}
                docker_client.pull(image, auth_config=auth_config)
            else:
                raise

        cmd_script = f"""
            # We can't run gcsfuse in dev mode (without some very complicated hacks)
            # It's not compatible with macos and is very annoying to setup in docker with volumes.
            # 
            # mkdir -p /shared_workspace /var/cache/gcsfuse
            # gcsfuse \
            #     --client-protocol=http2 \
            #     --only-dir=shared_workspace \
            #     --metadata-cache-ttl-secs=1 \
            #     --cache-dir=/var/cache/gcsfuse \
            #     {self.sync_gcs_bucket_name} /shared_workspace
            cd /opt/burla/node_service
            uv run -m uvicorn node_service:app --host 0.0.0.0 --port {self.port} --workers 1 \
                --timeout-keep-alive 600 --reload
        """.strip()

        container_name = f"node_{self.instance_name[11:]}"
        container = docker_client.create_container(
            image=image,
            command=["-c", cmd_script],
            entrypoint=["bash"],
            name=container_name,
            ports=[self.port],
            host_config=host_config,
            environment={
                "GOOGLE_CLOUD_PROJECT": PROJECT_ID,
                "IN_LOCAL_DEV_MODE": IN_LOCAL_DEV_MODE,
                "HOST_HOME_DIR": os.environ["HOST_HOME_DIR"],
                "HOST_PWD": os.environ["HOST_PWD"],
                "INSTANCE_NAME": self.instance_name,
                "CONTAINERS": json.dumps([c.to_dict() for c in self.containers]),
                "INACTIVITY_SHUTDOWN_TIME_SEC": self.inactivity_shutdown_time_sec,
                "NUM_GPUS": 0,
            },
            detach=True,
        )
        docker_client.start(container=container.get("Id"))
        self.host = f"http://{container_name}:{self.port}"
        self.node_ref.update(dict(host=self.host))

    def __start_svc_in_vm(self, disk_image: str, disk_size: int):
        disk_params = AttachedDiskInitializeParams(source_image=disk_image, disk_size_gb=disk_size)
        disk = AttachedDisk(auto_delete=True, boot=True, initialize_params=disk_params)

        network_name = "global/networks/default"
        access_config = AccessConfig(name="External NAT", type="ONE_TO_ONE_NAT")
        network_interface = NetworkInterface(name=network_name, access_configs=[access_config])

        if self.spot:
            scheduling = Scheduling(
                provisioning_model="SPOT",
                instance_termination_action="DELETE",
                on_host_maintenance="TERMINATE",
                automatic_restart=False,
            )
        else:
            scheduling = Scheduling(
                provisioning_model="STANDARD",
                on_host_maintenance="TERMINATE",
                automatic_restart=False,
            )

        access_anything_scope = "https://www.googleapis.com/auth/cloud-platform"
        service_account = ServiceAccount(email=GCE_DEFAULT_SVC, scopes=[access_anything_scope])

        startup_script = self.__get_startup_script()
        shutdown_script = self.__get_shutdown_script()
        startup_script_metadata = Items(key="startup-script", value=startup_script)
        shutdown_script_metadata = Items(key="shutdown-script", value=shutdown_script)
        exhausted_zones = []
        zones = list(zones_supporting_machine_type(self.gcp_region, self.machine_type))
        if not zones:
            msg = f"None of the zones in region {self.gcp_region} "
            raise Exception(msg + f"support the machine type {self.machine_type}.")

        for zone in zones:
            msg = f"Attempting to provision {self.machine_type} in zone: {zone}"
            self.node_ref.collection("logs").document().set({"msg": msg, "ts": time()})
            try:
                instance = Instance(
                    name=self.instance_name,
                    machine_type=f"zones/{zone}/machineTypes/{self.machine_type}",
                    disks=[disk],
                    network_interfaces=[network_interface],
                    service_accounts=[service_account],
                    metadata=Metadata(items=[startup_script_metadata, shutdown_script_metadata]),
                    tags=Tags(items=["burla-cluster-node"]),
                    scheduling=scheduling,
                )
                kw = dict(project=PROJECT_ID, zone=zone, instance_resource=instance)
                self.instance_client.insert(**kw).result()
                instance_created = True
                break
            except ServiceUnavailable:  # not enough instances in this zone, try next zone.
                exhausted_zones.append(zone)
                instance_created = False
                msg = f"No available capacity for {self.machine_type} in zone: {zone}"
                self.node_ref.collection("logs").document().set({"msg": msg, "ts": time()})
            except Conflict:
                raise Exception(f"Node {self.instance_name} deleted while starting.")

        if not instance_created and exhausted_zones:
            msg = f"ZONE_RESOURCE_POOL_EXHAUSTED: {exhausted_zones} currently have no "
            msg += f"available capacity for VM {self.machine_type}\n"
            raise Exception(msg)

        kw = dict(project=PROJECT_ID, zone=zone, instance=self.instance_name)
        external_ip = self.instance_client.get(**kw).network_interfaces[0].access_configs[0].nat_i_p

        self.host = f"http://{external_ip}:{self.port}"
        self.zone = zone
        self.node_ref.update(dict(host=self.host, zone=self.zone))
        msg = f"Successfully provisioned {self.machine_type} in zone: {zone}"
        self.node_ref.collection("logs").document().set({"msg": msg, "ts": time()})

    def __get_startup_script(self):
        return f"""
        #! /bin/bash        
        set -Eeuo pipefail
        handle_error() {{
        	ACCESS_TOKEN=$(curl -s -H "Metadata-Flavor: Google" \
        	"http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" \
        	| jq -r .access_token)
        
        	MSG="Startup script failed! See Google Cloud Logging. Deleting VM {self.instance_name} ... "
            echo "$MSG"
        	DB_BASE_URL="https://firestore.googleapis.com/v1/projects/{PROJECT_ID}/databases/burla/documents"
        	payload=$(jq -n --arg msg "$MSG" --arg ts "$(date +%s)" '{{"fields":{{"msg":{{"stringValue":$msg}},"ts":{{"integerValue":$ts}},}}}}')
        	curl -sS -o /dev/null -X POST "$DB_BASE_URL/nodes/{self.instance_name}/logs" \
                -H "Authorization: Bearer $ACCESS_TOKEN" \
                -H "Content-Type: application/json" \
                -d "$payload" || true

            # set status as FAILED
            status_payload=$(jq -n '{{"fields":{{"status":{{"stringValue":"FAILED"}},"display_in_dashboard":{{"booleanValue":true}}}}}}')
            curl -sS -o /dev/null -X PATCH "$DB_BASE_URL/nodes/{self.instance_name}?updateMask.fieldPaths=status&updateMask.fieldPaths=display_in_dashboard" \
                -H "Authorization: Bearer $ACCESS_TOKEN" \
                -H "Content-Type: application/json" \
                -d "$status_payload" || true

            # delete vm
        	INSTANCE_NAME=$(curl -s -H "Metadata-Flavor: Google" \
            "http://metadata.google.internal/computeMetadata/v1/instance/name")
        	ZONE=$(curl -s -H "Metadata-Flavor: Google" \
            "http://metadata.google.internal/computeMetadata/v1/instance/zone" | awk -F/ '{{print $NF}}')
        	curl -sS -X DELETE \
            -H "Authorization: Bearer $ACCESS_TOKEN" \
            "https://compute.googleapis.com/compute/v1/projects/{PROJECT_ID}/zones/$ZONE/instances/$INSTANCE_NAME" || true
        	exit 1
        }}
        trap 'handle_error' ERR

        ACCESS_TOKEN=$(curl -s -H "Metadata-Flavor: Google" \
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" \
        | jq -r .access_token)

        MSG="Installing Burla node service v{CURRENT_BURLA_VERSION} ..."
        echo "$MSG"
        DB_BASE_URL="https://firestore.googleapis.com/v1/projects/{PROJECT_ID}/databases/burla/documents"
        payload=$(jq -n --arg msg "$MSG" --arg ts "$(date +%s)" '{{"fields":{{"msg":{{"stringValue":$msg}},"ts":{{"integerValue":$ts}}}}}}')
        curl -sS -o /dev/null -X POST "$DB_BASE_URL/nodes/{self.instance_name}/logs" \
            -H "Authorization: Bearer $ACCESS_TOKEN" \
            -H "Content-Type: application/json" \
            -d "$payload"

        # make uv work, this is an oopsie from when building the disk image:
        export PATH="/root/.cargo/bin:$PATH"
        export PATH="/root/.local/bin:$PATH"

        cd /opt
        # git clone --depth 1 --branch {CURRENT_BURLA_VERSION} https://github.com/Burla-Cloud/burla.git  --no-checkout
        cd burla
        git fetch --depth=1 origin "{CURRENT_BURLA_VERSION}" || git fetch --depth=1 origin "tag {CURRENT_BURLA_VERSION}"
        git reset --hard FETCH_HEAD
        cd node_service
        uv pip install .

        MSG="Successfully installed node service."
        echo "$MSG"
        payload=$(jq -n --arg msg "$MSG" --arg ts "$(date +%s)" '{{"fields":{{"msg":{{"stringValue":$msg}},"ts":{{"integerValue":$ts}}}}}}')
        curl -sS -o /dev/null -X POST "$DB_BASE_URL/nodes/{self.instance_name}/logs" \
            -H "Authorization: Bearer $ACCESS_TOKEN" \
            -H "Content-Type: application/json" \
            -d "$payload"

        # start gcsfuse to sync working dirs with GCS bucket if specified
        cd /
        mkdir -p /shared_workspace
        if [ "{self.sync_gcs_bucket_name}" != "None" ]; then
            mkdir -p /var/cache/gcsfuse
            gcsfuse \
                --client-protocol=http2 \
                --only-dir=shared_workspace \
                --metadata-cache-ttl-secs=1 \
                --cache-dir=/var/cache/gcsfuse \
                {self.sync_gcs_bucket_name} /shared_workspace
            cd /opt/burla/node_service

            MSG="Started GCSFuse: syncing /shared_workspace with gs://{self.sync_gcs_bucket_name}"
            echo "$MSG"
            payload=$(jq -n --arg msg "$MSG" --arg ts "$(date +%s)" '{{"fields":{{"msg":{{"stringValue":$msg}},"ts":{{"integerValue":$ts}}}}}}')
            curl -sS -o /dev/null -X POST "$DB_BASE_URL/nodes/{self.instance_name}/logs" \
                -H "Authorization: Bearer $ACCESS_TOKEN" \
                -H "Content-Type: application/json" \
                -d "$payload"
        fi

        # authenticate docker:
        echo "$ACCESS_TOKEN" | docker login -u oauth2accesstoken --password-stdin https://us-docker.pkg.dev

        export NUM_GPUS="{self.num_gpus}"
        export INSTANCE_NAME="{self.instance_name}"
        export PROJECT_ID="{PROJECT_ID}"
        export CONTAINERS='{json.dumps([c.to_dict() for c in self.containers])}'
        export INACTIVITY_SHUTDOWN_TIME_SEC="{self.inactivity_shutdown_time_sec}"
        uv run -m uvicorn node_service:app --host 0.0.0.0 --port {self.port} --workers 1 --timeout-keep-alive 600
        """

    def __get_shutdown_script(self):
        return f"""
        #! /bin/bash
        # Tell the node_service this VM is being shutdown so it can reassign inputs and stuff.
        curl -X POST "http://localhost:{self.port}/shutdown"
        """
