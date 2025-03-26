import os
import io
import logging
import requests
from queue import Queue
from threading import Event
import asyncio
import aiohttp
import pickle

import cloudpickle
import google.auth
from google.cloud import firestore
from google.cloud.firestore import DocumentReference
from google.auth.exceptions import DefaultCredentialsError

from burla._auth import AuthException, get_gcs_credentials
from burla._install import main_service_url

# throws some uncatchable, unimportant, warnings
logging.getLogger("google.api_core.bidi").setLevel(logging.ERROR)


class GoogleLoginError(Exception):
    pass


class InputTooBig(Exception):
    pass


class UnknownClusterError(Exception):
    def __init__(self):
        msg = "\nAn unknown error occurred inside your Burla cluster, "
        msg += "this is not an error with your code, but with the Burla.\n"
        msg += "If this issue is urgent please don't hesitate to call me (Jake) directly"
        msg += " at 508-320-8778, or email me at jake@burla.dev."
        super().__init__(msg)


def get_host():
    # not defined in init because users often change this post-import
    custom_host = os.environ.get("BURLA_API_URL")
    return custom_host or "https://cluster.burla.dev"


def using_demo_cluster():
    # not defined in init because users often change this post-import
    return not bool(os.environ.get("BURLA_API_URL"))


def get_db(auth_headers: dict):
    if using_demo_cluster():
        credentials = get_gcs_credentials(auth_headers)
        return firestore.Client(credentials=credentials, project="burla-prod", database="burla")
    else:
        api_url_according_to_user = os.environ.get("BURLA_API_URL")

        # if api_url_according_to_user and api_url_according_to_user != main_service_url():
        #     raise Exception(
        #         f"You are pointing to the main service at {api_url_according_to_user}.\n"
        #         f"However, according to the current project set in gcloud, "
        #         f"the main_service is currently running at {main_service_url()}.\n"
        #         f"Please ensure your gcloud is pointing at the same project that your burla "
        #         "api is deployed in."
        #     )
        try:
            credentials, project = google.auth.default()
            if project == "":
                raise GoogleLoginError(
                    "No google cloud project found, please sign in to the google cloud CLI:\n"
                    "  1. gcloud config set project <your-project-id>\n"
                    "  2. gcloud auth application-default login\n"
                )
            return firestore.Client(credentials=credentials, project=project, database="burla")
        except DefaultCredentialsError as e:
            raise Exception(
                "No Google Application Default Credentials found. "
                "Please run `gcloud auth application-default login`."
            ) from e


def healthcheck_job(job_id: str, auth_headers: dict):
    response = requests.get(f"{get_host()}/v1/jobs/{job_id}", headers=auth_headers)

    if response.status_code == 200:
        return
    elif response.status_code == 401:
        raise AuthException()
    elif response.status_code == 404:
        # this thread often runs for a bit after the job has ended, causing 404s
        # for now, just ignore these.
        return
    else:
        raise UnknownClusterError()


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


def upload_inputs(job_id: str, nodes: list[dict], inputs: list, stop_event: Event):

    def _chunk_inputs_by_size(
        inputs_pkl_with_idx: list,
        min_chunk_size: int = 1_048_576,  # 1MB
        max_chunk_size: int = 1_048_576 * 256,  # 256MB
    ):
        chunks = []
        current_chunk = []
        current_chunk_size = 0

        for input_pkl_with_idx in inputs_pkl_with_idx:

            input_size = len(input_pkl_with_idx[1])
            if input_size > max_chunk_size:
                # This exists to prevent theoretical (never demonstrated) memory issues
                raise InputTooBig(f"Input of size {input_size} exceeds maximum size of 1GB.")

            next_chunk_too_small = current_chunk_size + input_size < min_chunk_size
            next_chunk_too_big = current_chunk_size + input_size > max_chunk_size
            next_chunk_size_is_acceptable = not next_chunk_too_small and not next_chunk_too_big

            if next_chunk_too_small:
                # add input to the current chunk
                current_chunk.append(input_pkl_with_idx)
                current_chunk_size += input_size
            elif next_chunk_size_is_acceptable:
                # add input to the current chunk AND yield the chunk
                current_chunk.append(input_pkl_with_idx)
                chunks.append(current_chunk)
                current_chunk = []
                current_chunk_size = 0
            elif next_chunk_too_big:
                # yield the chunk, add current input to next chunk
                chunks.append(current_chunk)
                current_chunk = [input_pkl_with_idx]
                current_chunk_size = input_size

        # Add the last chunk if it's not empty
        if current_chunk:
            chunks.append(current_chunk)

        if chunks:
            return chunks
        else:
            return [current_chunk]

    async def upload_input_chunk(session, url, inputs_chunk):
        data = aiohttp.FormData()
        data.add_field("inputs_pkl_with_idx", pickle.dumps(inputs_chunk))
        async with session.post(f"{url}/jobs/{job_id}/inputs", data=data) as response:
            response.raise_for_status()

    async def upload_all():
        async with aiohttp.ClientSession() as session:
            # assume every node has the same target parallelism (number of workers/config)
            # TODO: `nodes` contains the parallelism per node, which could be different!
            # this algorithim should send less stuff to nodes with less parallelism / etc.

            # attach original index to each input so we can tell user which input failed
            inputs_pkl_with_idx = [(i, cloudpickle.dumps(input)) for i, input in enumerate(inputs)]

            # Divide inputs into one chunk for each node.
            # Within chunks assigned to a node, chunk further so we don't send too much data at once
            size = len(inputs_pkl_with_idx) // len(nodes)
            extra = len(inputs_pkl_with_idx) % len(nodes)
            start = 0
            for i, node in enumerate(nodes):
                end = start + size + (1 if i < extra else 0)

                inputs_for_current_node = _chunk_inputs_by_size(inputs_pkl_with_idx[start:end])

                node["input_chunks"] = inputs_for_current_node
                start = end

            for node in nodes:
                chunk_sizes = [len(chunk) for chunk in node["input_chunks"]]
                n_chunks = len(node["input_chunks"])
                print(f"uploading {n_chunks} chunks with sizes {chunk_sizes} to {node['host']}")

            # cuncurrently, for each node, upload the n'th chunk of inputs
            nodes_with_input_chunks = [n for n in nodes if n["input_chunks"]]

            while nodes_with_input_chunks:
                tasks = []
                for node in nodes_with_input_chunks:
                    inputs_chunk = node["input_chunks"].pop(0)
                    tasks.append(upload_input_chunk(session, node["host"], inputs_chunk))
                await asyncio.gather(*tasks)  # <- wait for the n'th chunk of every node to upload.
                nodes_with_input_chunks = [n for n in nodes if n["input_chunks"]]

    try:
        asyncio.run(upload_all())
    except Exception as e:
        stop_event.set()
        raise Exception(f"Failed to upload inputs: {str(e)}") from e
