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
from google.cloud import resourcemanager_v3
from google.auth.transport.requests import Request
from google.api_core.exceptions import NotFound, ServiceUnavailable, Conflict
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

from main_service import PROJECT_ID, CREDENTIALS, IN_LOCAL_DEV_MODE
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

NODE_BOOT_TIMEOUT = 60 * 3
ACCEPTABLE_ZONES = ["us-central1-b", "us-central1-c", "us-central1-f", "us-central1-a"]
NODE_SVC_VERSION = "1.0.11"  # <- this maps to a git tag/release or branch


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
        containers: list[Container],
        auth_headers: dict,
        spot: bool = False,
        service_port: int = 8080,  # <- this needs to be open in your cloud firewall!
        as_local_container: bool = False,
        instance_client: Optional[InstancesClient] = None,
        inactivity_shutdown_time_sec: Optional[int] = None,
        disk_size: Optional[int] = None,
        verbose=False,
        disk_image: str = "projects/burla-prod/global/images/burla-cluster-node-image-6",
    ):
        self = cls.__new__(cls)
        self.db = db
        self.logger = logger
        self.machine_type = machine_type
        self.containers = containers
        self.auth_headers = auth_headers
        self.spot = spot
        self.port = service_port
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

        if verbose:
            self.logger.log(f"Adding node {self.instance_name} ..")

        current_state = dict(self.__dict__)  # <- create copy to modify / save
        current_state["status"] = "BOOTING"
        current_state["containers"] = [container.to_dict() for container in containers]
        attrs_to_not_save = ["db", "logger", "instance_client", "node_ref"]
        current_state = {k: v for k, v in current_state.items() if k not in attrs_to_not_save}
        self.node_ref.set(current_state)

        if as_local_container:
            self.__start_svc_in_local_container()
        else:
            self.__start_svc_in_vm(disk_image=disk_image, disk_size=self.disk_size)

        start = time()
        status = self.status()
        while status != "READY":
            sleep(1)
            booting_too_long = (time() - start) > NODE_BOOT_TIMEOUT
            status = self.status()

            if status == "FAILED" or booting_too_long:
                self.delete()
                msg = f"Node {self.instance_name} Failed to start! (timeout={booting_too_long})"
                raise Exception(msg)

        self.node_ref.update(dict(host=self.host, zone=self.zone))  # node svc marks itself as ready
        self.is_booting = False
        return self

    def delete(self):
        """
        An `instance_client.delete` request creates an `operation` that runs in the background.
        """
        if not self.instance_client:
            self.instance_client = InstancesClient()

        try:
            kwargs = dict(project=PROJECT_ID, zone=self.zone, instance=self.instance_name)
            self.instance_client.delete(**kwargs)
        except (NotFound, ValueError):
            pass  # these errors mean it was already deleted.
        self.node_ref.delete()  # Delete the document instead of marking as deleted

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
        command = f"uvicorn node_service:app --host 0.0.0.0 --port {self.port} --workers 1 "
        command += "--timeout-keep-alive 600 --reload"

        docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
        host_config = docker_client.create_host_config(
            port_bindings={self.port: self.port},
            network_mode="local-burla-cluster",
            binds={
                f"{os.environ['HOST_HOME_DIR']}/.config/gcloud": "/root/.config/gcloud",
                f"{os.environ['HOST_PWD']}/node_service": "/burla/node_service",
                "/var/run/docker.sock": "/var/run/docker.sock",
            },
        )

        image_stored_in_gcp = "docker.pkg.dev" in image or "gcr.io" in image
        if image_stored_in_gcp:
            CREDENTIALS.refresh(Request())
            auth_config = {"username": "oauth2accesstoken", "password": CREDENTIALS.token}
            docker_client.pull(image, auth_config=auth_config)
        else:
            docker_client.pull(image)

        container_name = f"node_{self.instance_name[11:]}"
        container = docker_client.create_container(
            image=image,
            command=["bash", "-c", f"python3.11 -m {command}"],
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
                "BOOTING_FOR_FIRST_TIME": "True",
            },
            detach=True,
        )
        docker_client.start(container=container.get("Id"))
        self.host = f"http://{container_name}:{self.port}"

    def __start_svc_in_vm(self, disk_image: str, disk_size: int):
        disk_params = AttachedDiskInitializeParams(source_image=disk_image, disk_size_gb=disk_size)
        disk = AttachedDisk(auto_delete=True, boot=True, initialize_params=disk_params)

        network_name = "global/networks/default"
        access_config = AccessConfig(name="External NAT", type="ONE_TO_ONE_NAT")
        network_interface = NetworkInterface(name=network_name, access_configs=[access_config])

        if self.spot:
            scheduling = Scheduling(provisioning_model="SPOT", instance_termination_action="DELETE")
        else:
            scheduling = Scheduling(provisioning_model="STANDARD")

        access_anything_scope = "https://www.googleapis.com/auth/cloud-platform"
        service_account = ServiceAccount(email=GCE_DEFAULT_SVC, scopes=[access_anything_scope])

        startup_script = self.__get_startup_script()
        shutdown_script = self.__get_shutdown_script()
        startup_script_metadata = Items(key="startup-script", value=startup_script)
        shutdown_script_metadata = Items(key="shutdown-script", value=shutdown_script)
        for zone in ACCEPTABLE_ZONES:
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
                self.instance_client.insert(
                    project=PROJECT_ID, zone=zone, instance_resource=instance
                ).result()
                instance_created = True
                break

            except ServiceUnavailable:  # <- not enough instances in this zone, try next zone.
                instance_created = False
            except Conflict:
                raise Exception(f"Node {self.instance_name} deleted while starting.")

        if not instance_created:
            raise Exception(f"Unable to provision {instance} in any of: {ACCEPTABLE_ZONES}")

        kw = dict(project=PROJECT_ID, zone=zone, instance=self.instance_name)
        external_ip = self.instance_client.get(**kw).network_interfaces[0].access_configs[0].nat_i_p

        self.host = f"http://{external_ip}:{self.port}"
        self.zone = zone

    def __get_startup_script(self):
        return f"""
        #! /bin/bash
        # This script installs and starts the node service
        
        # Increases max num open files so we can have more connections open.
        # ulimit -n 4096 # baked into image when I built `burla-cluster-node-image-5`

        gcloud config set account {GCE_DEFAULT_SVC}

        echo "DOWNLOADING BURLA NODE SERVICE V{NODE_SVC_VERSION}"
        git clone --depth 1 --branch {NODE_SVC_VERSION} https://github.com/Burla-Cloud/burla.git  --no-checkout
        cd burla
        git sparse-checkout init --cone
        git sparse-checkout set node_service
        git checkout {NODE_SVC_VERSION}
        cd node_service
        python3.11 -m pip install --break-system-packages .
        echo "Done installing packages."

        export INSTANCE_NAME="{self.instance_name}"
        export PROJECT_ID="{PROJECT_ID}"
        export CONTAINERS='{json.dumps([c.to_dict() for c in self.containers])}'
        export INACTIVITY_SHUTDOWN_TIME_SEC="{self.inactivity_shutdown_time_sec}"

        python3.11 -m uvicorn node_service:app --host 0.0.0.0 --port {self.port} --workers 1 --timeout-keep-alive 600
        """

    def __get_shutdown_script(self):
        return f"""
        #! /bin/bash
        # Tell the node_service this VM is being shutdown so it can reassign inputs and stuff.
        curl -X POST "http://localhost:{self.port}/shutdown"
        """
