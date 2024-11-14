"""
The tests here assume the cluster is running in "local-dev-mode".
"""

import sys
from io import StringIO
from time import time, sleep

import docker
from burla import remote_parallel_map


# ALL ASSUMPTIONS REGARDING STANDBY STATE ARE HERE:
# (defined in LOCAL_DEV_CONFIG in main_service init and `INSTANCE_N_CPUS` in node_service init)
N_STANDBY_MAIN_SVC_CONTAINERS = 1
N_STANDBY_NODE_SVC_CONTAINERS = 2
N_STANDBY_WORKER_CONTAINERS = 4


def local_cluster_in_standby():
    docker_client = docker.from_env()
    containers = docker_client.containers.list()
    main_svc_containers = [c for c in containers if c.name == "main_service"]
    node_svc_containers = [c for c in containers if c.name.startswith("node_service")]
    container_svc_containers = [c for c in containers if c.name.startswith("container_service")]
    in_standby = True
    in_standby = len(main_svc_containers) == N_STANDBY_MAIN_SVC_CONTAINERS
    in_standby = len(node_svc_containers) == N_STANDBY_NODE_SVC_CONTAINERS
    in_standby = len(container_svc_containers) == N_STANDBY_WORKER_CONTAINERS
    return in_standby


def run_simple_test_job(n_inputs=5):
    test_inputs = list(range(n_inputs))
    stdout = StringIO()
    sys.stdout = stdout
    start = time()

    def simple_test_function(test_input):
        print(test_input)
        return test_input

    results = list(remote_parallel_map(simple_test_function, test_inputs))

    e2e_runtime = time() - start
    sys.stdout = sys.__stdout__
    stdout = stdout.getvalue()

    assert e2e_runtime < 10
    assert all([result in test_inputs for result in results])
    assert len(results) == len(test_inputs)
    for i in range(n_inputs):
        assert str(i) in stdout


def test_base():
    docker_client = docker.from_env()

    if not local_cluster_in_standby():
        raise Exception("Local cluster not in standby.")

    containers = docker_client.containers.list()
    pre_job_worker_names = set([c.name for c in containers if c.name.startswith("container")])

    run_simple_test_job()

    # ensure workers reboot within 10s
    start = time()
    reboot_timeout_seconds = 10
    all_workers_rebooted = False

    while not all_workers_rebooted:
        containers = docker_client.containers.list()
        post_job_worker_names = set([c.name for c in containers if c.name.startswith("container")])

        num_workers_removed = len(pre_job_worker_names - post_job_worker_names)
        all_pre_job_workers_removed = num_workers_removed == N_STANDBY_WORKER_CONTAINERS
        correct_num_post_job_workers = len(post_job_worker_names) == N_STANDBY_WORKER_CONTAINERS
        all_workers_rebooted = all_pre_job_workers_removed and correct_num_post_job_workers

        if reboot_timeout_seconds < time() - start:
            raise Exception(f"workers not rebooted after {reboot_timeout_seconds}s")
        else:
            sleep(0.1)

    assert local_cluster_in_standby()
