import io
import logging
import asyncio
import aiohttp
import pickle
from asyncio import Queue
from threading import Event
from time import time
from six import reraise
import os

import cloudpickle
from tblib import Traceback
from google.cloud.firestore import DocumentReference
from google.cloud import firestore


# throws some uncatchable, unimportant, warnings
logging.getLogger("google.api_core.bidi").setLevel(logging.ERROR)
# prevent some annoying grpc logs / warnings
os.environ["GRPC_VERBOSITY"] = "ERROR"  # only log ERROR/FATAL
os.environ["GLOG_minloglevel"] = "2"  # 0-INFO, 1-WARNING, 2-ERROR, 3-FATAL


class InputTooBig(Exception):
    pass


async def send_alive_pings(job_doc_ref: DocumentReference, log_msg_stdout):
    log_msg_stdout.write("Sending alive pings...")
    while True:
        await asyncio.sleep(1.5)
        await job_doc_ref.update({"last_ping_from_client": time()})
        log_msg_stdout.write(".")


async def upload_inputs(
    job_id: str,
    nodes: list[dict],
    inputs: list,
    session: aiohttp.ClientSession,
):

    def _chunk_inputs_by_size(
        inputs_pkl_with_idx: list,
        min_chunk_size: int = 1_048_576 * 0.2,  # 0.5MB
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

    # attach original index to each input so we can tell user which input failed
    inputs_pkl_with_idx = [(i, cloudpickle.dumps(input)) for i, input in enumerate(inputs)]

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

    await asyncio.gather(*[_upload_inputs_single_node(session, node) for node in nodes])
