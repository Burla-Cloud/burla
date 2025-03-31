import io
import logging
import asyncio
import aiohttp
import pickle
from time import time
from datetime import datetime
from queue import Queue
from threading import Event

import cloudpickle
from google.cloud.firestore import DocumentReference

# throws some uncatchable, unimportant, warnings
logging.getLogger("google.api_core.bidi").setLevel(logging.ERROR)


JOB_HEALTHCHECK_FREQUENCY_SEC = 6


class InputTooBig(Exception):
    pass


def send_job_healthchecks(
    job_id: str, stop_event: Event, nodes: list[dict], log_msg_stdout: io.TextIOWrapper
):
    async def _healthcheck_single_node(session, node):
        async with session.get(f"{node['host']}/jobs/{job_id}") as response:
            return node, response.status

    async def _healthcheck_all_nodes(nodes):
        async with aiohttp.ClientSession() as session:
            tasks = [_healthcheck_single_node(session, node) for node in nodes]
            return await asyncio.gather(*tasks, return_exceptions=True)

    try:
        while not stop_event.is_set():
            stop_event.wait(JOB_HEALTHCHECK_FREQUENCY_SEC)
            start = time()

            time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            log_msg_stdout.write(f"Sending healthchecks to all nodes... ({time_str} EDT)")
            results = asyncio.run(_healthcheck_all_nodes(nodes))
            log_msg_stdout.write(f"Received all healthcheck responses ({time() - start:.2f}s)")

            # Check if any node returned a non-200 status
            failed_nodes = [f"{n['host']}: {status}" for n, status in results if status != 200]
            if failed_nodes:
                log_msg_stdout.write(f"Healthcheck failed for nodes: {', '.join(failed_nodes)}")
                # TODO: if a node fails, check what results it returned and send remainder of inputs to other nodes
                return
    except Exception:
        stop_event.set()
        raise


def print_logs_from_db(
    job_doc_ref: DocumentReference, stop_event: Event, log_msg_stdout: io.TextIOWrapper
):
    def _on_snapshot(collection_snapshot, changes, read_time):
        for change in changes:
            if change.type.name == "ADDED":
                log_msg_stdout.write(change.document.to_dict()["msg"])

    try:
        collection_ref = job_doc_ref.collection("logs")
        query_watch = collection_ref.on_snapshot(_on_snapshot)
        while not stop_event.is_set():
            stop_event.wait(0.5)  # this does not block the processing of new documents
    except Exception as e:
        # Because logs are non-essential, don't kill the job if it breaks.
        msg = f"ERROR: Logstream failed with {e}\nContinuing Job execution without logs..."
        log_msg_stdout.write(msg)
    finally:
        query_watch.unsubscribe()


def enqueue_results_from_db(job_doc_ref: DocumentReference, stop_event: Event, queue: Queue):
    def _on_snapshot(collection_snapshot, changes, read_time):
        for change in changes:
            if change.type.name == "ADDED":
                result = change.document.to_dict()
                result_tuple = (change.document.id, result["is_error"], result["result_pkl"])
                queue.put(result_tuple)

    try:
        collection_ref = job_doc_ref.collection("results")
        query_watch = collection_ref.on_snapshot(_on_snapshot)
        while not stop_event.is_set():
            stop_event.wait(0.5)  # this does not block the processing of new documents
    except Exception:
        stop_event.set()
        raise
    finally:
        query_watch.unsubscribe()


def upload_inputs(
    job_id: str,
    nodes: list[dict],
    inputs: list,
    stop_event: Event,
    log_msg_stdout: io.TextIOWrapper,
):

    def _chunk_inputs_by_size(
        inputs_pkl_with_idx: list,
        min_chunk_size: int = 1_048_576 * 0.5,  # 0.5MB
        max_chunk_size: int = 1_048_576 * 1000,  # 1GB
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

                sum_of_chunk_of_chunks = sum([len(c) for c in inputs_for_current_node])
                len_of_chunk = len(inputs_pkl_with_idx[start:end])
                assert sum_of_chunk_of_chunks == len_of_chunk
                msg = f"sum_of_chunk_of_chunks:{sum_of_chunk_of_chunks} == len_of_chunk:{len_of_chunk}"
                log_msg_stdout.write(msg)

                node["input_chunks"] = inputs_for_current_node
                start = end

            for node in nodes:
                chunk_sizes = [len(chunk) for chunk in node["input_chunks"]]
                n_chunks = len(node["input_chunks"])
                msg = f"Uploading {n_chunks} chunks with {chunk_sizes} inputs to {node['host']}"
                log_msg_stdout.write(msg)

            sum_of_inputs = sum(sum(len(chunk) for chunk in node["input_chunks"]) for node in nodes)
            log_msg_stdout.write(f"Sum of chunks:{sum_of_inputs} == n_inputs:{len(inputs)}")
            assert sum_of_inputs == len(inputs)

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
    except Exception:
        stop_event.set()
        raise
