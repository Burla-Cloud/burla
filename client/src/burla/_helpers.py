import io
import logging
import requests
from queue import Queue
from threading import Event

from google.cloud import firestore
from google.cloud.firestore import DocumentReference
from google.api_core.retry import Retry, if_exception_type
from google.api_core.exceptions import Unknown

from burla import _BURLA_SERVICE_URL

# throws some uncatchable, unimportant, warnings
logging.getLogger("google.api_core.bidi").setLevel(logging.ERROR)

from time import time


class InputTooBig(Exception):
    pass


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
        if response.status_code == 200:
            stop_event.wait(healthcheck_frequency_sec)
            continue

        if response.status_code == 401:
            auth_error_event.set()
        elif response.status_code == 404:
            # this thread often runs for a bit after the job has ended, causing 404s
            # for now, just ignore these.
            pass
        else:
            cluster_error_event.set()
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
                result = change.document.to_dict()
                result_tuple = (change.document.id, result["is_error"], result["result_pkl"])
                queue.put(result_tuple)

    collection_ref = job_doc_ref.collection("results")
    query_watch = collection_ref.on_snapshot(on_snapshot)

    while not stop_event.is_set():
        stop_event.wait(0.5)  # this does not block the processing of new documents
    query_watch.unsubscribe()


def upload_inputs(DB: firestore.Client, inputs_id: str, inputs_pkl: list[bytes], stop_event: Event):
    """
    Uploads inputs into a separate collection not connected to the job
    so that uploading can start before the job document is created.
    """
    batch_size = 100
    inputs_parent_doc = DB.collection("inputs").document(inputs_id)

    firestore_commit_retry_policy = Retry(
        initial=5.0,
        maximum=120.0,
        multiplier=2.0,
        deadline=900.0,
        predicate=if_exception_type(Unknown),
        reraise=True,
    )

    total_n_bytes_firestore_batch = 0
    firestore_batch = DB.batch()

    for batch_min_index in range(0, len(inputs_pkl), batch_size):
        batch_max_index = batch_min_index + batch_size
        input_batch = inputs_pkl[batch_min_index:batch_max_index]
        subcollection = inputs_parent_doc.collection(f"{batch_min_index}-{batch_max_index}")

        for local_input_index, input_pkl in enumerate(input_batch):
            input_index = local_input_index + batch_min_index
            input_too_big = len(input_pkl) > 1_000_000  # 1MB size limit per firestore doc

            if stop_event.is_set():
                return

            # if batch will contain too much data (10MB), push it before adding input to next batch.
            if total_n_bytes_firestore_batch + len(input_pkl) > 10_000_000:
                firestore_batch.commit(retry=firestore_commit_retry_policy)
                firestore_batch = DB.batch()
                total_n_bytes_firestore_batch = 0

            if input_too_big:
                msg = f"Input at index {input_index} is greater than 1MB in size.\n"
                msg += "Individual inputs greater than 1MB in size are currently not supported."
                raise InputTooBig(msg)
            else:
                doc_ref = subcollection.document(str(input_index))
                firestore_batch.set(doc_ref, {"input": input_pkl, "claimed": False})
                total_n_bytes_firestore_batch += len(input_pkl)

    firestore_batch.commit(retry=firestore_commit_retry_policy)
