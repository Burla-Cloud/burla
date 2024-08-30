import os
import logging
from queue import Queue
from threading import Event
from concurrent.futures import TimeoutError
from concurrent.futures import ThreadPoolExecutor

import cloudpickle
from yaspin import Spinner
from google.cloud import pubsub
from google.cloud import firestore

from burla import OUTPUTS_SUBSCRIPTION_PATH, LOGS_SUBSCRIPTION_PATH


# was getting the exact same uncatchable, unimportant, error:
# https://stackoverflow.com/questions/77138981/how-to-handle-acknowledging-a-pubsub-streampull-subscription-message
logging.getLogger("google.cloud.pubsub_v1").setLevel(logging.ERROR)

# gRPC streams from pubsub will throw some unblockable annoying warnings
os.environ["GRPC_VERBOSITY"] = "ERROR"


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


def print_logs_from_stream(
    subscriber: pubsub.SubscriberClient, stop_event: Event, spinner: Spinner
):

    def callback(message):
        message.ack()
        try:
            spinner.write(message.data.decode())
        except:
            # ignore messages that cannot be unpickled (are not pickled)
            # ack these messages anyway so they don't loop through this subsctiption
            print(f"ERROR: data instance: {type(message.data)}, data: {message.data}")
            pass

    future = subscriber.subscribe(LOGS_SUBSCRIPTION_PATH, callback=callback)
    while not stop_event.is_set():
        try:
            future.result(timeout=0.1)
        except TimeoutError:
            continue


def enqueue_outputs_from_stream(
    subscriber: pubsub.SubscriberClient, stop_event: Event, output_queue: Queue
):

    def callback(message):
        message.ack()
        try:
            output_queue.put(cloudpickle.loads(message.data))
        except:
            # ignore messages that cannot be unpickled (are not pickled)
            # ack these messages anyway so they don't loop through this subsctiption
            pass

    future = subscriber.subscribe(OUTPUTS_SUBSCRIPTION_PATH, callback=callback)
    while not stop_event.is_set():
        try:
            future.result(timeout=0.1)
        except TimeoutError:
            continue


def _upload_input(inputs_collection, input_index, input_):
    input_pkl = cloudpickle.dumps(input_)
    input_too_big = len(input_pkl) > 1_048_576

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
