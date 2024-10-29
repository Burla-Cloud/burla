import os
import sys
import json
import pickle
import pytest
import requests
import subprocess
from uuid import uuid4
from time import sleep
from six import reraise
from queue import Queue
from typing import Optional
from threading import Thread, Event
from concurrent.futures import ThreadPoolExecutor

import cloudpickle
import docker
from tblib import Traceback
from google.cloud import firestore
from google.cloud.firestore import DocumentReference
from google.cloud.storage import Client, Blob

"""
If node_service is imported anywhere here the containers will be started 
then deleted by `delete_containers` in conftest before testing starts!!
DO NOT import node_service here.
"""

GCS_CLIENT = Client()
cmd = ["gcloud", "config", "get-value", "project"]
PROJECT_ID = subprocess.run(cmd, capture_output=True, text=True).stdout.strip()


def print_logs_from_db(job_doc_ref: DocumentReference, stop_event: Event):

    def on_snapshot(collection_snapshot, changes, read_time):
        for change in changes:
            if change.type.name == "ADDED":
                print(change.document.to_dict()["msg"])

    collection_ref = job_doc_ref.collection("logs")
    query_watch = collection_ref.on_snapshot(on_snapshot)

    while not stop_event.is_set():
        stop_event.wait(0.5)  # this does not block the processing of new documents
    query_watch.unsubscribe()


def enqueue_results_from_db(job_doc_ref: DocumentReference, stop_event: Event, queue: Queue):

    def on_snapshot(collection_snapshot, changes, read_time):
        for change in changes:
            if change.type.name == "ADDED":
                input_index = change.document.id
                result_doc = change.document.to_dict()
                result_tuple = (input_index, result_doc["is_error"], result_doc["result_pkl"])
                queue.put(result_tuple)
                print(f"Got result #{input_index}")

    collection_ref = job_doc_ref.collection("results")
    query_watch = collection_ref.on_snapshot(on_snapshot)

    while not stop_event.is_set():
        stop_event.wait(0.5)  # this does not block the processing of new documents
    query_watch.unsubscribe()


def upload_inputs(DB: firestore.Client, inputs_id: str, inputs: list):
    """
    Uploads inputs into a separate collection not connected to the job
    so that uploading can start before the job document is created.
    """
    batch_size = 100
    inputs_parent_doc = DB.collection("inputs").document(inputs_id)

    n_docs_in_firestore_batch = 0
    firestore_batch = DB.batch()

    for batch_min_index in range(0, len(inputs), batch_size):
        batch_max_index = batch_min_index + batch_size
        input_batch = inputs[batch_min_index:batch_max_index]
        subcollection = inputs_parent_doc.collection(f"{batch_min_index}-{batch_max_index}")

        for local_input_index, input_ in enumerate(input_batch):
            input_index = local_input_index + batch_min_index
            input_pkl = cloudpickle.dumps(input_)
            input_too_big = len(input_pkl) > 1_048_376  # 1MB size limit

            if input_too_big:
                msg = f"Input at index {input_index} is greater than 1MB in size.\n"
                msg += "Inputs greater than 1MB are unfortunately not yet supported."
                raise Exception(msg)
            else:
                doc_ref = subcollection.document(str(input_index))
                firestore_batch.set(doc_ref, {"input": input_pkl, "claimed": False})
                n_docs_in_firestore_batch += 1

            # max num documents per firestore batch is 500, push batch when this is reached.
            if n_docs_in_firestore_batch >= 500:
                firestore_batch.commit()
                firestore_batch = DB.batch()
                n_docs_in_firestore_batch = 0

    firestore_batch.commit()


def periodiocally_healthcheck_job(
    job_id: str,
    healthcheck_frequency_sec: int,
    node_svc_hostname: str,
    stop_event: Event,
    error_event: Event,
):
    while not stop_event.is_set():
        response = requests.get(f"{node_svc_hostname}/jobs/{job_id}")
        if response.status_code == 200:
            stop_event.wait(healthcheck_frequency_sec)
            continue

        if response.status_code == 404:
            # this thread often runs for a bit after the job has ended, causing 404s
            # for now, just ignore these.
            pass
        else:
            error_event.set()
        return


def _create_job_document_in_database(
    job_id, inputs_id, image, dependencies, n_inputs, faux_python_version: Optional[str] = None
):
    python_version = faux_python_version if faux_python_version else f"3.{sys.version_info.minor}"
    db = firestore.Client(project=PROJECT_ID)
    job_ref = db.collection("jobs").document(job_id)
    job_ref.set(
        {
            "test": True,
            "inputs_id": inputs_id,
            "n_inputs": n_inputs,
            "planned_future_job_parallelism": 1,
            "function_uri": f"gs://burla-jobs/12345/{job_id}/function.pkl",
            "user_python_version": python_version,
        }
    )


def _wait_until_node_svc_not_busy(node_svc_hostname, attempt=0):
    response = requests.get(node_svc_hostname, timeout=60)
    response.raise_for_status()

    if response.json()["status"] != "READY":
        sleep(5)
        print(f"Waiting for not to be READY, current status={response.json()['status']}")
        return _wait_until_node_svc_not_busy(node_svc_hostname, attempt=attempt + 1)
    elif attempt == 30:
        raise Exception("node should have rebooted by now ?")


def _assert_node_service_left_proper_containers_running():
    from node_service import INSTANCE_N_CPUS  # <- see note near import statements at top.

    db = firestore.Client(project=PROJECT_ID)
    config = db.collection("cluster_config").document("cluster_config").get().to_dict()

    client = docker.from_env()
    attempts = 0
    in_standby = False
    while not in_standby:
        # ignore `main_service` container so that in local testing I can use the `main_service`
        # container while I am running the `node_service` tests.
        containers = [c for c in client.containers.list(all=True) if c.name != "main_service"]

        # all container svc running ?
        for container in containers:
            port = int(list(container.attrs["NetworkSettings"]["Ports"].values())[0][0]["HostPort"])
            response = requests.get(f"http://127.0.0.1:{port}")
            response.raise_for_status()
            assert response.json()["status"] == "READY"

        # correct num containers ?
        machine_type = "n4-standard-2"
        for node in config["Nodes"]:
            if node["machine_type"] == machine_type:
                break
        in_standby = len(containers) == len(node["containers"]) * INSTANCE_N_CPUS

        sleep(2)
        if attempts == 10:
            raise Exception("standby containers not started ??")
        attempts += 1


def _execute_job(
    node_svc_hostname,
    my_function,
    my_inputs,
    my_packages,
    my_image,
    send_inputs_through_gcs=False,
    faux_python_version=None,
):
    db = firestore.Client()
    JOB_ID = str(uuid4()) + "-test"
    INPUTS_ID = str(uuid4()) + "-test"
    DEFAULT_IMAGE = (
        f"us-docker.pkg.dev/{PROJECT_ID}/burla-job-containers/default/image-nogpu:latest"
    )
    image = my_image if my_image else DEFAULT_IMAGE

    # in separate thread start uploading inputs:
    input_uploader_thread = Thread(
        target=upload_inputs,
        args=(db, INPUTS_ID, my_inputs),
        daemon=True,
    )
    input_uploader_thread.start()

    _create_job_document_in_database(
        JOB_ID, INPUTS_ID, image, my_packages, len(my_inputs), faux_python_version
    )

    # request job execution
    payload = {"parallelism": 1, "starting_index": 0}
    if send_inputs_through_gcs:
        response = requests.post(f"{node_svc_hostname}/jobs/{JOB_ID}", json=payload)
    else:
        function_pkl = cloudpickle.dumps(my_function)
        files = dict(function_pkl=function_pkl)
        data = dict(request_json=json.dumps(payload))
        response = requests.post(f"{node_svc_hostname}/jobs/{JOB_ID}", files=files, data=data)

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if ("500" in str(e)) and response.text:
            print(response.text)
        raise e

    stop_event = Event()
    job_doc_ref = db.collection("jobs").document(JOB_ID)

    # Start collecting logs generated by this job using a separate thread.
    args = (job_doc_ref, stop_event)
    log_thread = Thread(target=print_logs_from_db, args=args, daemon=True)
    log_thread.start()

    # Start collecting outputs generated by this job using a separate thread.
    result_queue = Queue()
    args = (job_doc_ref, stop_event, result_queue)
    result_thread = Thread(target=enqueue_results_from_db, args=args, daemon=True)
    result_thread.start()

    # Run periodic healthchecks on the job/cluster from a separate thread.
    error_event = Event()
    healthcheck_freq_sec = 5
    args = (JOB_ID, healthcheck_freq_sec, node_svc_hostname, stop_event, error_event)
    healthchecker_thread = Thread(target=periodiocally_healthcheck_job, args=args, daemon=True)
    healthchecker_thread.start()

    # loop until job is done
    attempts = 0
    outputs = []
    while len(outputs) < len(my_inputs):
        sleep(1)

        if error_event.is_set():
            raise Exception("Cluster Error. (healthcheck failed)")

        while not result_queue.empty():
            input_index, is_error, result_pkl = result_queue.get()
            if is_error:
                exc_info = pickle.loads(result_pkl)
                traceback = Traceback.from_dict(exc_info["traceback_dict"]).as_traceback()
                reraise(tp=exc_info["type"], value=exc_info["exception"], tb=traceback)
            else:
                outputs.append(cloudpickle.loads(result_pkl))

        attempts += 1
        if attempts >= 60 * 3:
            raise Exception("TIMEOUT: Job took > 3 minutes to finish?")

    stop_event.set()
    db.collection("jobs").document(JOB_ID).delete()
    return outputs


def test_healthcheck(hostname):
    response = requests.get(f"{hostname}/")
    response.raise_for_status()
    assert response.json() == {"status": "READY"}


def test_everything_simple(hostname):
    my_image = None
    my_inputs = list(range(10))
    my_packages = []

    def my_function(my_input):
        print(f"Processing input: {my_input}")
        return my_input * 2

    return_values = _execute_job(hostname, my_function, my_inputs, my_packages, my_image)

    # collect expected returns with no stdout
    sys.stdout = open(os.devnull, "w")
    expected_return_values = [my_function(input_) for input_ in my_inputs]
    sys.stdout = sys.__stdout__

    assert return_values == expected_return_values

    # _wait_until_node_svc_not_busy(hostname)
    # _assert_node_service_left_proper_containers_running()


def test_UDF_error(hostname):
    my_image = None
    my_inputs = ["hi", "hi"]
    my_packages = []

    def my_function(my_input):
        print(1 / 0)
        return my_input * 2

    with pytest.raises(ZeroDivisionError):
        _execute_job(hostname, my_function, my_inputs, my_packages, my_image)

    # _wait_until_node_svc_not_busy(hostname)
    # _assert_node_service_left_proper_containers_running()


def test_incompatible_containers_error(hostname):
    my_image = None
    my_inputs = ["hi", "hi"]
    my_packages = []

    def my_function(my_input):
        return my_input * 2

    with pytest.raises(requests.exceptions.HTTPError):
        _execute_job(
            hostname, my_function, my_inputs, my_packages, my_image, faux_python_version="3.9"
        )
