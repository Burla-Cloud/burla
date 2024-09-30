import io
import logging
import requests
from queue import Queue
from threading import Event
from concurrent.futures import ThreadPoolExecutor

import cloudpickle
from google.cloud import firestore
from google.cloud.firestore import DocumentReference

from burla import _BURLA_SERVICE_URL

# throws some uncatchable, unimportant, warnings
logging.getLogger("google.api_core.bidi").setLevel(logging.ERROR)


def periodiocally_healthcheck_job(
    job_id: str,
    healthcheck_frequency_sec: int,
    auth_headers: dict,
    stop_event: Event,
    cluster_error_occurred: Event,
    auth_error_occurred: Event,
):
    while not stop_event.is_set():
        response = requests.get(f"{_BURLA_SERVICE_URL}/v1/jobs/{job_id}", headers=auth_headers)
        if response.status_code == 200:
            stop_event.wait(healthcheck_frequency_sec)
            continue

        if response.status_code == 401:
            auth_error_occurred.set()
        else:
            cluster_error_occurred.set()
        return


def print_logs_from_db(
    job_doc_ref: DocumentReference, stop_event: Event, log_msg_stdout: io.TextIOWrapper
):

    def on_snapshot(collection_snapshot, changes, read_time):
        for change in changes:
            if change.type.name == "ADDED":
                log_msg_stdout.write(change.document.to_dict()["msg"])

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

    collection_ref = job_doc_ref.collection("results")
    query_watch = collection_ref.on_snapshot(on_snapshot)

    while not stop_event.is_set():
        stop_event.wait(0.5)  # this does not block the processing of new documents
    query_watch.unsubscribe()


def _upload_input(inputs_collection, input_index, input_):
    input_pkl = cloudpickle.dumps(input_)
    input_too_big = len(input_pkl) > 1_048_376

    if input_too_big:
        msg = f"Input at index {input_index} is greater than 1MB in size.\n"
        msg += "Inputs greater than 1MB are unfortunately not yet supported."
        raise Exception(msg)
    else:
        doc = {"input": input_pkl, "claimed": False}
        inputs_collection.document(str(input_index)).set(doc)


def upload_inputs(DB: firestore.Client, inputs_id: str, inputs: list):
    """
    Uploads inputs into a separate collection not connected to the job
    so that uploading can start before the job document is created.
    """
    inputs_collection = DB.collection("inputs").document(inputs_id).collection("inputs")

    futures = []
    with ThreadPoolExecutor(max_workers=32) as executor:
        for input_index, input_ in enumerate(inputs):
            future = executor.submit(_upload_input, inputs_collection, input_index, input_)
            futures.append(future)

        for future in futures:
            future.result()  # This will raise exceptions if any occurred in the threads
