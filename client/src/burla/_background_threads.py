import io
import logging
import asyncio
import aiohttp
import pickle
from queue import Queue
from threading import Event
from time import time

import psutil
import cloudpickle
from google.cloud.firestore import DocumentReference

# throws some uncatchable, unimportant, warnings
logging.getLogger("google.api_core.bidi").setLevel(logging.ERROR)


class InputTooBig(Exception):
    pass


def send_alive_pings(job_doc_ref: DocumentReference, stop_event: Event):
    while not stop_event.is_set():
        stop_event.wait(2)
        job_doc_ref.update({"last_ping_from_client": time()})


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
            stop_event.wait(0.3)  # this does not block the processing of new documents
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
        start = time()

        async with session.get(f"{node['host']}/jobs/{job_id}/results") as response:
            if response.status == 200:
                job_results_pkl = b"".join([c async for c in response.content.iter_chunked(8192)])
                job_results = pickle.loads(job_results_pkl)

                msg = f"received {len(job_results['results'])} results in {time() - start:.2f}s"
                log_msg_stdout.write(msg + f" from {node['instance_name']}")

                [queue.put(result) for result in job_results["results"]]
                node["current_parallelism"] = job_results["current_parallelism"]
            else:
                msg = f"result-check failed for: {node['instance_name']} status: {response.status}"
                log_msg_stdout.write(msg)
            return node, response.status

    async def _main_loop():
        async with aiohttp.ClientSession() as session:
            while not stop_event.is_set():
                await asyncio.sleep(2)

                tasks = [_result_check_single_node(session, node) for node in nodes]
                results = await asyncio.gather(*tasks)

                for node, status in results:
                    if status == 404:
                        nodes.remove(node)
                    elif status != 200:
                        raise Exception(f"Result-check failed for node: {node['instance_name']}")

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_main_loop())
    except Exception:
        stop_event.set()
        raise
    finally:
        loop.close()


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
                yield current_chunk
                current_chunk = []
                current_chunk_size = 0
            elif next_chunk_too_big:
                # yield the chunk, add current input to next chunk
                yield current_chunk
                current_chunk = [input_pkl_with_idx]
                current_chunk_size = input_size
        # yield the last chunk if it's not empty
        if current_chunk:
            yield current_chunk

    async def _upload_inputs_single_node(session, node):
        url = node["host"]
        for input_chunk in node["input_chunks"]:
            data = aiohttp.FormData()
            data.add_field("inputs_pkl_with_idx", pickle.dumps(input_chunk))
            async with session.post(f"{url}/jobs/{job_id}/inputs", data=data) as response:
                response.raise_for_status()

        async with session.post(f"{url}/jobs/{job_id}/inputs/done") as response:
            response.raise_for_status()

    async def upload_all():
        async with aiohttp.ClientSession() as session:
            # attach original index to each input so we can tell user which input failed
            inputs_pkl_with_idx = [(i, cloudpickle.dumps(input)) for i, input in enumerate(inputs)]
            n_inputs = len(inputs_pkl_with_idx)

            # Divide inputs into even chunks for each node (even #inputs not #bytes).
            # Within chunks assigned to a node, chunk further so we don't send too much data at once
            # ( ^ chunk by #bytes not by #inputs).
            size = len(inputs_pkl_with_idx) // len(nodes)
            extra = len(inputs_pkl_with_idx) % len(nodes)
            start = 0
            for i, node in enumerate(nodes):
                end = start + size + (1 if i < extra else 0)
                inputs_for_current_node = _chunk_inputs_by_size(inputs_pkl_with_idx[start:end])
                node["input_chunks"] = inputs_for_current_node
                start = end

            bytes_sent_before = psutil.net_io_counters().bytes_sent
            await asyncio.gather(*[_upload_inputs_single_node(session, node) for node in nodes])
            bytes_sent_after = psutil.net_io_counters().bytes_sent

            MB_sent = (bytes_sent_after - bytes_sent_before) / (1024 * 1024)
            msg = f"Uploaded {n_inputs} inputs ({MB_sent:.2f} MB) at {(MB_sent * 8):.2f} Mbps"
            log_msg_stdout.write(msg)

    try:
        asyncio.run(upload_all())
    except Exception:
        stop_event.set()
        raise
