from queue import Queue
from threading import Event
from concurrent.futures import ThreadPoolExecutor

import cloudpickle
from yaspin import Spinner
from google.cloud import firestore
from google.cloud.firestore import DocumentReference

import logging

# throws some uncatchable, unimportant, warnings
logging.getLogger("google.api_core.bidi").setLevel(logging.ERROR)


class StatusMessage:
    function_name = None
    total_cpus = None
    total_gpus = None
    n_inputs = None

    uploading_inputs = "Uploading Inputs ..."
    uploading_function = "Uploading Function ..."
    downloading = "Downloading Outputs ..."

    @classmethod
    def preparing(cls):
        msg = f"Preparing to run {cls.n_inputs} inputs through `{cls.function_name}` with "
        if cls.total_gpus > 0:
            msg += f"{cls.total_cpus} CPUs, and {cls.total_gpus} GPUs."
        else:
            msg += f"{cls.total_cpus} CPUs."
        return msg

    @classmethod
    def running(cls):
        msg = f"Running {cls.n_inputs} inputs through `{cls.function_name}` with {cls.total_cpus} "
        msg += f"CPUs, and {cls.total_gpus} GPUs." if cls.total_gpus > 0 else "CPUs."
        return msg


class JobTimeoutError(Exception):
    def __init__(self, job_id, timeout):
        super().__init__(f"Burla job with id: '{job_id}' timed out after {timeout} seconds.")


class InstallError(Exception):
    def __init__(self, stdout: str):
        super().__init__(
            f"The following error occurred attempting to pip install packages:\n{stdout}"
        )


class ServerError(Exception):
    def __init__(self):
        super().__init__(
            (
                "An unknown error occurred in Burla's cloud, this is not an error with your code. "
                "Someone has been notified, please try again later."
            )
        )


def nopath_warning(message, category, filename, lineno, line=None):
    return f"{category.__name__}: {message}\n"


def print_logs_from_db(job_doc_ref: DocumentReference, stop_event: Event, spinner: Spinner):

    def on_snapshot(collection_snapshot, changes, read_time):
        for change in changes:
            if change.type.name == "ADDED":
                spinner.write(change.document.to_dict()["msg"])

    collection_ref = job_doc_ref.collection("logs")
    query_watch = collection_ref.on_snapshot(on_snapshot)

    try:
        while not stop_event.is_set():
            stop_event.wait(0.5)  # this does not block the processing of new documents
    finally:
        query_watch.unsubscribe()


def enqueue_outputs_from_db(job_doc_ref: DocumentReference, stop_event: Event, output_queue: Queue):

    def on_snapshot(collection_snapshot, changes, read_time):
        for change in changes:
            if change.type.name == "ADDED":
                output_pkl = change.document.to_dict()["output"]
                output_queue.put(cloudpickle.loads(output_pkl))

    collection_ref = job_doc_ref.collection("outputs")
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
    with ThreadPoolExecutor() as executor:
        for input_index, input_ in enumerate(inputs):
            future = executor.submit(_upload_input, inputs_collection, input_index, input_)
            futures.append(future)

        for future in futures:
            future.result()  # This will raise exceptions if any occurred in the threads
