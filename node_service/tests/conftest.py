import os
import json
import subprocess
from time import sleep
from uuid import uuid4
import requests
import threading

import docker
import uvicorn
import pytest
from google.cloud import firestore


os.environ["GRPC_VERBOSITY"] = "ERROR"
os.environ["GLOG_minloglevel"] = "2"

PORT = 5000
HOSTNAME = f"http://127.0.0.1:{PORT}"
cmd = ["gcloud", "config", "get-value", "project"]
PROJECT_ID = subprocess.run(cmd, capture_output=True, text=True).stdout.strip()

CONTAINERS = [
    # {
    #     "image": f"us-docker.pkg.dev/{PROJECT_ID}/burla-job-containers/default/image-nogpu:latest",
    #     "python_executable": "/.pyenv/versions/3.10.*/bin/python3.10",
    #     "python_version": "3.10",
    # },
    {
        "image": f"us-docker.pkg.dev/{PROJECT_ID}/burla-job-containers/default/image-nogpu:latest",
        "python_executable": "/.pyenv/versions/3.11.*/bin/python3.11",
        "python_version": "3.11",
    },
    # {
    #     "image": f"us-docker.pkg.dev/{PROJECT_ID}/burla-job-containers/default/image-nogpu:latest",
    #     "python_executable": "/.pyenv/versions/3.12.*/bin/python3.12",
    #     "python_version": "3.12",
    # },
]


def delete_containers():
    client = docker.from_env()
    containers = client.containers.list(all=True)
    for container in containers:
        if container.name.startswith("image-nogpu"):
            print(f"REMOVING: {container.name}")
            container.remove(force=True)


def start_server(app):
    uvicorn.run(app, host="0.0.0.0", port=PORT)


@pytest.fixture(scope="module")
def hostname():
    print("\n")
    delete_containers()

    INSTANCE_NAME = "test-node-" + str(uuid4())
    os.environ["INSTANCE_NAME"] = INSTANCE_NAME
    db = firestore.Client(project=PROJECT_ID)
    node_doc = db.collection("nodes").document(INSTANCE_NAME)
    node_doc.set({})

    from node_service import app

    os.environ["CONTAINERS"] = json.dumps(CONTAINERS)
    server_thread = threading.Thread(target=start_server, args=(app,), daemon=True)
    server_thread.start()
    sleep(3)

    # Wait until node service has started all workers
    attempt = 0
    while True:
        try:
            response = requests.get(f"{HOSTNAME}/")
            response.raise_for_status()
            status = response.json()["status"]
        except requests.exceptions.ConnectionError:
            status = None

        if status == "FAILED":
            raise Exception("Node service entered state: FAILED")
        if status == "READY":
            break

        sleep(2)
        attempt += 1
        if attempt > 10:
            msg = "TIMEOUT! Node Service not ready after 20 seconds?\n"
            msg += "(build a new container recently? could just be talking a while to download ...)"
            raise Exception(msg)

    print("\nNODE SERVICE STARTED\n")

    try:
        yield HOSTNAME
    finally:
        node_doc.delete()
