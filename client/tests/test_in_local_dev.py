"""
The tests here assume the cluster is running in "local-dev-mode".
"""

import heapq
import math
import os
import random
from time import time, sleep

import docker

from burla import remote_parallel_map


# call the locally running instance of Burla!
os.environ["BURLA_API_URL"] = "http://localhost:5001"


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
        val = val * 0.3
        samples.append(val)

    return samples


def _min_total_runtime(sleep_times, num_workers):
    workers = [0] * num_workers
    heapq.heapify(workers)
    for t in sorted(sleep_times, reverse=True):
        earliest = heapq.heappop(workers)
        heapq.heappush(workers, earliest + t)
    return max(workers)


def in_remote_dev_mode():
    docker_client = docker.from_env()
    main_svc_container = docker_client.containers.get("main_service")
    env_vars = {e.split("=")[0]: e.split("=")[1] for e in main_svc_container.attrs["Config"]["Env"]}
    return env_vars.get("IN_LOCAL_DEV_MODE") != "True"


def run_simple_test_job():

    my_inputs = list(range(10_000_000))

    # N_WORKERS = 5
    # my_inputs = _normally_distributed_random_numbers(500)
    # print(f"\nsum of all sleeps: {sum(my_inputs)}")
    # print(f"lowest possible runtime: {_min_total_runtime(my_inputs, N_WORKERS)}\n")

    # # make inputs bigger
    # INPUT_SIZE = 1_000_000
    # my_inputs = [{"sleep_time": my_input, "blob": bytes(INPUT_SIZE)} for my_input in my_inputs]

    # stdout = StringIO()
    # sys.stdout = stdouts
    start = time()

    def simple_test_function(test_input):
        # print(test_input)
        # print(f"STARTING input #{test_input}")
        # print(f"sleeping for {test_input} seconds")
        # if test_input == 100_000:
        #     sleep(90)

        # blob_size_mb = len(test_input["blob"]) / 1_000_000
        # print(f"Sleeping for {test_input['sleep_time']}s, blob size: {blob_size_mb:.2f} MB")
        # sleep(test_input["sleep_time"])

        # print(f"FINISHED input #{test_input}")
        return test_input  # f"Waited 1 seconds for input {test_input}!"

    results = remote_parallel_map(simple_test_function, my_inputs)  ##, generator=True)

    # for result in results:
    #     print(result)

    e2e_runtime = time() - start
    # sys.stdout = sys.__stdout__
    # stdout = stdout.getvalue()

    # assert e2e_runtime < 5
    print(f"e2e_runtime: {e2e_runtime}")
    # assert all([result in test_inputs for result in results])
    # if not len(results) == len(test_inputs):
    #     print(results)

    # for i in range(n_inputs):
    #     assert str(i) in stdout


def test_base():
    # assert in_remote_dev_mode()

    run_simple_test_job()
