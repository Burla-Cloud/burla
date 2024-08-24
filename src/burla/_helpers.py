import os
import logging
from queue import Queue
from threading import Event
from concurrent.futures import TimeoutError

import cloudpickle
from yaspin import Spinner
from google.cloud import pubsub
from google.cloud.pubsub_v1.types import BatchSettings

from burla import INPUTS_TOPIC_PATH, OUTPUTS_SUBSCRIPTION_PATH, LOGS_SUBSCRIPTION_PATH


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


def upload_inputs(inputs: list):
    batch_settings = BatchSettings(max_bytes=10000000, max_latency=0.01, max_messages=1000)
    publisher = pubsub.PublisherClient(batch_settings=batch_settings)

    if not (0 <= len(inputs) <= 4294967295):
        raise ValueError("too many inputs: ID does not fit in 4 bytes.")

    for input_index, input_ in enumerate(inputs):
        packed_data = input_index.to_bytes(length=4, byteorder="big") + cloudpickle.dumps(input_)
        publisher.publish(topic=INPUTS_TOPIC_PATH, data=packed_data)
