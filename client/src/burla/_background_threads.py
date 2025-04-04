import io
import logging
import asyncio
import aiohttp
import pickle
from queue import Queue
from threading import Event

import cloudpickle
from google.cloud.firestore import DocumentReference

# throws some uncatchable, unimportant, warnings
logging.getLogger("google.api_core.bidi").setLevel(logging.ERROR)
RESULT_CHECK_FREQUENCY_SEC = 0.4


class InputTooBig(Exception):
    pass


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
        msg = f"ERROR: stdout-stream failed with {e}\nContinuing Job execution without stdout..."
        log_msg_stdout.write(msg)
    finally:
        query_watch.unsubscribe()


def enqueue_results(
    job_id: str,
    stop_event: Event,
    nodes: list[dict],
    queue: Queue,
    log_msg_stdout: io.TextIOWrapper,
):
    async def _result_check_single_node(session, node):
        async with session.get(f"{node['host']}/jobs/{job_id}/results") as response:
            if response.status != 200:
                return node, response.status

            response_pkl = b"".join([c async for c in response.content.iter_chunked(8192)])
            response = pickle.loads(response_pkl)
            # msg = f"Received {len(response['results'])} results from {node['instance_name']} "
            # log_msg_stdout.write(msg + f"({len(response_pkl)} bytes)")
            [queue.put(result) for result in response["results"]]
            node["current_parallelism"] = response["current_node_parallelism"]
            return node, response.status

    async def _result_check_all_nodes(nodes):
        async with aiohttp.ClientSession() as session:
            tasks = [_result_check_single_node(session, node) for node in nodes]
            return await asyncio.gather(*tasks)

    try:
        while not stop_event.is_set():
            stop_event.wait(RESULT_CHECK_FREQUENCY_SEC)

            # start = time()
            # log_msg_stdout.write(f"Checking results from all nodes...")
            results = asyncio.run(_result_check_all_nodes(nodes))
            # log_msg_stdout.write(f"Received all result check responses ({time() - start:.2f}s)")

            # Check if any node returned a non-200 status
            failed_nodes = [f"{n['host']}: {status}" for n, status in results if status != 200]
            if failed_nodes:
                # log_msg_stdout.write(f"result-check failed for nodes: {', '.join(failed_nodes)}")
                # TODO: if a node fails, send its assigned unfinished inputs to other nodes
                return
    except Exception:
        stop_event.set()
        raise


def upload_inputs(
    job_id: str,
    nodes: list[dict],
    inputs: list,
    stop_event: Event,
    log_msg_stdout: io.TextIOWrapper,
):

    def _chunk_inputs_by_size(
        inputs_pkl_with_idx: list,
        min_chunk_size: int = 1_048_576 * 1,  # 1MB
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
        return chunks

    async def upload_inputs_single_node(session, node):
        url = node["host"]
        while len(node["input_chunks"]) > 0:
            input_chunk = node["input_chunks"].pop(0)
            data = aiohttp.FormData()
            data.add_field("inputs_pkl_with_idx", pickle.dumps(input_chunk))
            async with session.post(f"{url}/jobs/{job_id}/inputs", data=data) as response:
                response.raise_for_status()
        # tell node all the inputs have been uploaded:
        async with session.post(f"{url}/jobs/{job_id}/inputs/done") as response:
            response.raise_for_status()

    async def upload_all():
        async with aiohttp.ClientSession() as session:
            # assume every node has the same target parallelism (number of workers/config)
            # TODO: `nodes` contains the parallelism per node, which could be different!
            # this algorithim should send less stuff to nodes with less parallelism / etc.

            # TODO: These functions are super slow, not the upload but the divisions!

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

                node["input_chunks"] = inputs_for_current_node
                start = end

            for node in nodes:
                chunk_sizes = [len(chunk) for chunk in node["input_chunks"]]
                n_chunks = len(node["input_chunks"])
                msg = f"Uploading {n_chunks} chunks with {chunk_sizes} inputs to {node['host']}"
                log_msg_stdout.write(msg)

            sum_of_inputs = sum(sum(len(chunk) for chunk in node["input_chunks"]) for node in nodes)
            assert sum_of_inputs == len(inputs)

            tasks = [upload_inputs_single_node(session, node) for node in nodes]
            await asyncio.gather(*tasks)

    try:
        asyncio.run(upload_all())
        log_msg_stdout.write("Uploaded all inputs.")
    except Exception:
        stop_event.set()
        raise
