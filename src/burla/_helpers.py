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

from time import time


def periodiocally_healthcheck_job(
    job_id: str,
    healthcheck_frequency_sec: int,
    auth_headers: dict,
    stop_event: Event,
    cluster_error_event: Event,
    auth_error_event: Event,
):
    while not stop_event.is_set():
        response = requests.get(f"{_BURLA_SERVICE_URL}/v1/jobs/{job_id}", headers=auth_headers)
        stop_event.wait(healthcheck_frequency_sec)
        # if response.status_code == 200:
        #     stop_event.wait(healthcheck_frequency_sec)
        #     continue

        # if response.status_code == 401:
        #     auth_error_event.set()
        # else:
        #     cluster_error_event.set()
        # return


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
                result = change.document.to_dict()
                result_tuple = (result["index"], result["is_error"], result["result_pkl"])
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
        doc = {"index": input_index, "input": input_pkl, "claimed": False}
        inputs_collection.document().set(doc)
        return time()


def upload_inputs(DB: firestore.Client, inputs_id: str, inputs: list):
    """
    Uploads inputs into a separate collection not connected to the job
    so that uploading can start before the job document is created.
    """
    batch_size = 100
    inputs_parent_doc = DB.collection("inputs").document(inputs_id)

    upload_times = []

    futures = []
    with ThreadPoolExecutor(max_workers=32) as executor:

        # upload into separate subcollections each containing <batch_size> inputs.
        # placing in separtate collections spreads out load when attempting to read inputs quickly.
        for batch_min_index in range(0, len(inputs), batch_size):
            batch_max_index = batch_min_index + batch_size
            input_batch = inputs[batch_min_index:batch_max_index]
            subcollection = inputs_parent_doc.collection(f"{batch_min_index}-{batch_max_index}")

            # schedule upload of current batch
            for local_input_index, input_ in enumerate(input_batch):
                input_index = local_input_index + batch_min_index
                future = executor.submit(_upload_input, subcollection, input_index, input_)
                futures.append(future)

        for future in futures:
            # This will raise exceptions if any occurred in the threads
            upload_time = future.result()
            upload_times.append(upload_time)

    print(upload_times)
