import os
import logging
import asyncio
import aiohttp
import pickle
from time import time, sleep
from threading import Event

import cloudpickle

from burla._helpers import get_db_clients

# throws some uncatchable, unimportant, warnings
logging.getLogger("google.api_core.bidi").setLevel(logging.ERROR)
# prevent some annoying grpc logs / warnings
os.environ["GRPC_VERBOSITY"] = "ERROR"  # only log ERROR/FATAL
os.environ["GLOG_minloglevel"] = "2"  # 0-INFO, 1-WARNING, 2-ERROR, 3-FATAL


class InputTooBig(Exception):
    pass


def send_alive_pings(job_id: str):
    """
    Constantly update `last_ping_from_client` in Firestore to tell nodes client is still listening.
    This must run in a separate process.
    Otherwise, at when a lots of stuff (high parallelism, large inputs/outputs, or both) is
    happening the thread or async task (tried both) are starved, causing Nodes to believe the
    client disconnected (which it didn't) and restart, causing the job to fail.
    """
    sync_db, _ = get_db_clients()
    job_doc = sync_db.collection("jobs").document(job_id)
    last_update_time = 0
    while True:
        now = time()
        if now - last_update_time > 1:
            job_doc.update({"last_ping_from_client": now})
            last_update_time = now
        sleep(0.1)


async def upload_inputs(
    job_id: str,
    nodes: list[dict],
    inputs: list,
    session: aiohttp.ClientSession,
    auth_headers: dict,
    job_canceled_event: Event,
):

    async def _upload_inputs_single_node(session, node):
        async for input_chunk in node["input_chunks"]:  # <- actual pickling/chunking happens here
            data = aiohttp.FormData()
            inputs_pkl_with_idx = pickle.dumps(input_chunk)
            data.add_field("inputs_pkl_with_idx", inputs_pkl_with_idx)

            url = f"{node['host']}/jobs/{job_id}/inputs"
            async with session.post(url, data=data, headers=auth_headers) as response:
                response.raise_for_status()

        url = f"{node['host']}/jobs/{job_id}/inputs/done"
        async with session.post(url, headers=auth_headers) as response:
            response.raise_for_status()

    async def _chunk_inputs_by_size_generator(
        inputs: list,
        start_index: int,
        min_chunk_size: int = 1_000_000 * 6,  # 6MB
        max_chunk_size: int = 1_000_000 * 1000,  # 1GB
    ):
        current_chunk = []
        current_chunk_size = 0
        total_bytes = 0

        for index_within_chunk, input in enumerate(inputs):
            index = start_index + index_within_chunk
            input_pkl_with_idx = (index, cloudpickle.dumps(input))
            input_size = len(input_pkl_with_idx[1])

            total_bytes += input_size
            if total_bytes > 1000:
                await asyncio.sleep(0)
                total_bytes = 0

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

    # Divide inputs into even chunks for each node (even #inputs not #bytes).
    # Within chunks assigned to a node, chunk further so we don't send too much data at once
    # ( ^ chunk by #bytes not by #inputs).
    size = len(inputs) // len(nodes)
    extra = len(inputs) % len(nodes)
    start = 0
    for i, node in enumerate(nodes):
        end = start + size + (1 if i < extra else 0)
        inputs_for_node = inputs[start:end]
        node["input_chunks"] = _chunk_inputs_by_size_generator(inputs_for_node, start_index=start)
        start = end

    try:
        await asyncio.gather(*[_upload_inputs_single_node(session, node) for node in nodes])
    except aiohttp.client_exceptions.ServerDisconnectedError as e:
        if not job_canceled_event.is_set():
            raise e
