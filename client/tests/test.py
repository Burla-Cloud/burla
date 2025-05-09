"""
The tests here assume the cluster is running in "local-dev-mode".
"""

import heapq
import math
import random
from time import time, sleep

from burla import remote_parallel_map


def _normally_distributed_random_numbers(quantity):

    def clamp(x, lower=0, upper=60):
        return max(lower, min(x, upper))

    def box_muller():
        u1 = random.random()
        u2 = random.random()
        z = math.sqrt(-2 * math.log(u1)) * math.cos(2 * math.pi * u2)
        return z

    mean = -30
    std_dev = 20
    samples = []

    for _ in range(quantity):
        val = clamp(mean + box_muller() * std_dev)
        val = val * 4
        samples.append(val)

    return samples


def _min_total_runtime(sleep_times, num_workers):
    workers = [0] * num_workers
    heapq.heapify(workers)
    for t in sorted(sleep_times, reverse=True):
        earliest = heapq.heappop(workers)
        heapq.heappush(workers, earliest + t)
    return max(workers)


def _max_total_runtime(sleep_times, num_workers):
    chunk_size = len(sleep_times) // num_workers
    remainder = len(sleep_times) % num_workers

    chunks = []
    start = 0
    for i in range(num_workers):
        end = start + chunk_size + (1 if i < remainder else 0)
        chunks.append(sum(sleep_times[start:end]))
        start = end

    return max(chunks)


def test_base():

    my_inputs = list(range(100))

    # my_inputs = [1 for _ in range(15_000)]
    # my_inputs[4321] = 90

    # N_WORKERS = 160
    # my_inputs = _normally_distributed_random_numbers(1_000)
    # print(f"\nsum of all sleeps: {sum(my_inputs)}")
    # print(f"lowest possible runtime: {_min_total_runtime(my_inputs, N_WORKERS)}")
    # print(f"highest possible runtime: {_max_total_runtime(my_inputs, N_WORKERS)}")
    # print("")

    # make inputs bigger
    # INPUT_SIZE = 1_000_000
    INPUT_SIZE = 10
    # my_inputs = [{"sleep_time": my_input, "blob": bytes(INPUT_SIZE)} for my_input in my_inputs]

    start = time()

    def simple_test_function(test_input):

        # print(f"sleeping for {test_input} seconds")
        # if test_input == 100_000:
        #     sleep(90)

        # blob_size_mb = len(test_input["blob"]) / 1_000_000
        # print(f"Sleeping for {test_input['sleep_time']}s, blob size: {blob_size_mb:.2f} MB")
        sleep(1)

        return test_input

    # results = remote_parallel_map(simple_test_function, my_inputs)
    results = remote_parallel_map(simple_test_function, my_inputs, background=True)

    e2e_runtime = time() - start

    print(f"e2e_runtime: {e2e_runtime}")
