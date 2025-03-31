"""
The tests here assume the cluster is running in "remote-dev-mode".
"""

import os
import sys
from io import StringIO
from time import time, sleep

import docker

from burla import remote_parallel_map


# call the locally running instance of Burla!
os.environ["BURLA_API_URL"] = "http://localhost:5001"


def in_remote_dev_mode():
    docker_client = docker.from_env()
    main_svc_container = docker_client.containers.get("main_service")
    env_vars = {e.split("=")[0]: e.split("=")[1] for e in main_svc_container.attrs["Config"]["Env"]}
    return env_vars.get("IN_LOCAL_DEV_MODE") != "True"


def run_simple_test_job(n_inputs=5):

    test_inputs = list(range(n_inputs))
    # stdout = StringIO()
    # sys.stdout = stdout
    start = time()

    def simple_test_function(test_input):
        # print(test_input)
        print(f"STARTING input #{test_input}")
        sleep(10)
        print(f"FINISHED input #{test_input}")
        return test_input * 2

    results = remote_parallel_map(simple_test_function, test_inputs)

    e2e_runtime = time() - start
    # sys.stdout = sys.__stdout__
    # stdout = stdout.getvalue()

    # assert e2e_runtime < 5
    print(f"e2e_runtime: {e2e_runtime}")
    # assert all([result in test_inputs for result in results])
    assert len(results) == len(test_inputs)
    # for i in range(n_inputs):
    #     assert str(i) in stdout


def test_base():
    assert in_remote_dev_mode()

    run_simple_test_job(n_inputs=100_000)
