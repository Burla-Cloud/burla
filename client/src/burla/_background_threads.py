import logging
import asyncio
import aiohttp
import pickle
from time import time, sleep
import os

import cloudpickle
from google.cloud.firestore import Client


# throws some uncatchable, unimportant, warnings
logging.getLogger("google.api_core.bidi").setLevel(logging.ERROR)
# prevent some annoying grpc logs / warnings
os.environ["GRPC_VERBOSITY"] = "ERROR"  # only log ERROR/FATAL
os.environ["GLOG_minloglevel"] = "2"  # 0-INFO, 1-WARNING, 2-ERROR, 3-FATAL


class InputTooBig(Exception):
    pass


def send_alive_pings(sync_db: Client, job_id: str):
    job_doc = sync_db.collection("jobs").document(job_id)
    last_ping_time = time()
    starve_time = 2
    while True:
        sleep(1)
        current_time = time()
        job_doc.update({"last_ping_from_client": current_time})
        last_ping_time = current_time

        if current_time - last_ping_time > starve_time:
            starve_time = current_time - last_ping_time
            print(f"\nWARNING: ping thread starve increased! ({starve_time}s)")


async def upload_inputs(
    job_id: str,
    nodes: list[dict],
    inputs: list,
    session: aiohttp.ClientSession,
):

    async def _upload_inputs_single_node(session, node):
        url = node["host"]
        async for input_chunk in node["input_chunks"]:  # <- actual pickling/chunking happens here
            data = aiohttp.FormData()
            inputs_pkl_with_idx = await asyncio.to_thread(pickle.dumps, input_chunk)
            data.add_field("inputs_pkl_with_idx", inputs_pkl_with_idx)
            async with session.post(f"{url}/jobs/{job_id}/inputs", data=data) as response:
                response.raise_for_status()

        async with session.post(f"{url}/jobs/{job_id}/inputs/done") as response:
            response.raise_for_status()

    async def _chunk_inputs_by_size_generator(
        inputs: list,
        start_index: int,
        min_chunk_size: int = 1_000_000 * 1,  # 1MB
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

    await asyncio.gather(*[_upload_inputs_single_node(session, node) for node in nodes])
