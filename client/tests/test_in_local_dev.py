"""
The tests here assume the cluster is running in "remote-dev-mode".
"""

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
        samples.append(val)

    return samples


def in_remote_dev_mode():
    docker_client = docker.from_env()
    main_svc_container = docker_client.containers.get("main_service")
    env_vars = {e.split("=")[0]: e.split("=")[1] for e in main_svc_container.attrs["Config"]["Env"]}
    return env_vars.get("IN_LOCAL_DEV_MODE") != "True"


def run_simple_test_job():

    my_inputs = list(range(100_000_000))
    # my_inputs = _normally_distributed_random_numbers(1_000_000)
    # print(f"\nsum of all sleeps: {sum(my_inputs)}")
    # print(f"lowest possible runtime: {sum(my_inputs) / 10}\n")
    # stdout = StringIO()
    # sys.stdout = stdouts
    start = time()

    def simple_test_function(test_input):
        # print(test_input)
        # print(f"STARTING input #{test_input}")
        # print(f"sleeping for {test_input} seconds")
        # sleep(0.01)
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
    assert len(results) == len(my_inputs)
    # for i in range(n_inputs):
    #     assert str(i) in stdout


def test_base():
    # assert in_remote_dev_mode()

    run_simple_test_job()
