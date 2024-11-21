"""
The tests here assume the cluster is running in "local-dev-mode".
"""

import os
import sys
from io import StringIO
from time import time, sleep

import pytest
import docker
from google.cloud import firestore

from burla import remote_parallel_map

# hide annpying firestore log messages
os.environ["GRPC_VERBOSITY"] = "ERROR"


# ALL ASSUMPTIONS REGARDING STANDBY STATE ARE HERE:
# (defined in LOCAL_DEV_CONFIG in main_service init and `INSTANCE_N_CPUS` in node_service init)
N_STANDBY_MAIN_SVC_CONTAINERS = 1
N_STANDBY_NODE_SVC_CONTAINERS = 2
N_STANDBY_WORKER_CONTAINERS = 4

DOCKER_CLIENT = docker.from_env()


def local_cluster_in_standby():
    containers = DOCKER_CLIENT.containers.list()
    main_svc_containers = [c for c in containers if c.name == "main_service"]
    node_svc_containers = [c for c in containers if c.name.startswith("node")]
    worker_svc_containers = [c for c in containers if c.name.startswith("worker")]
    in_standby = True
    in_standby = len(main_svc_containers) == N_STANDBY_MAIN_SVC_CONTAINERS
    in_standby = len(node_svc_containers) == N_STANDBY_NODE_SVC_CONTAINERS
    in_standby = len(worker_svc_containers) == N_STANDBY_WORKER_CONTAINERS

    # if good so far, assert both nodes are in state "ready":
    if in_standby:
        nodes_collection = firestore.Client().collection("nodes")
        for node_container in node_svc_containers:
            node_doc = nodes_collection.document(f"burla-node-{node_container.name[-8:]}")
            in_standby = node_doc.get().to_dict()["status"] == "READY"

    return in_standby


def rpm_assert_restart(*a, **kw):
    """
    asserts cluster is in standby and restarts itself correctly before/after calling rpm.
    returns any errors thrown by rpm, still asserts cluster restarted correctly.
    """

    if not local_cluster_in_standby():
        raise Exception("Local cluster not in standby.")

    containers = DOCKER_CLIENT.containers.list()
    pre_job_worker_names = set([c.name for c in containers if c.name.startswith("worker")])

    stdout = StringIO()
    sys.stdout = stdout
    start = time()

    rpm_exception = None
    try:
        results = list(remote_parallel_map(*a, **kw))
    except Exception as e:
        rpm_exception = e
        results = None

    runtime = time() - start
    sys.stdout = sys.__stdout__
    stdout = stdout.getvalue()

    # ensure workers reboot
    start = time()
    reboot_timeout_seconds = 10
    all_workers_rebooted = False
    cluster_in_standby = False

    while not (all_workers_rebooted and cluster_in_standby):
        containers = DOCKER_CLIENT.containers.list()
        post_job_worker_names = set([c.name for c in containers if c.name.startswith("worker")])

        num_workers_removed = len(pre_job_worker_names - post_job_worker_names)
        all_pre_job_workers_removed = num_workers_removed == N_STANDBY_WORKER_CONTAINERS
        correct_num_post_job_workers = len(post_job_worker_names) == N_STANDBY_WORKER_CONTAINERS
        all_workers_rebooted = all_pre_job_workers_removed and correct_num_post_job_workers

        cluster_in_standby = local_cluster_in_standby()

        if reboot_timeout_seconds < time() - start:
            raise Exception(f"workers not rebooted after {reboot_timeout_seconds}s")
        else:
            sleep(0.1)

    return results, stdout, runtime, rpm_exception


def test_base():

    my_inputs = list(range(5))

    def my_function(test_input):
        print(test_input)
        return test_input

    results, stdout, runtime, rpm_exception = rpm_assert_restart(my_function, my_inputs)

    if rpm_exception:
        raise rpm_exception

    print(f"E2E remote_parallel_map runtime: {runtime}")
    assert runtime < 10
    assert all([result in my_inputs for result in results])
    assert len(results) == len(my_inputs)
    for i in range(len(my_inputs)):
        assert str(i) in stdout


def test_udf_error():
    """
    Ensure the error is re-raised.
    Also ensure that other nodes quickly stop once one throws an error.
    """

    def my_function(test_input):
        if test_input == 2:
            print(1 / 0)
        else:
            sleep(60)
        return test_input

    _, _, runtime, rpm_exception = rpm_assert_restart(my_function, list(range(5)))

    assert isinstance(rpm_exception, ZeroDivisionError)
    assert runtime < 10  # <- IMPORTANT! asserts the other nodes restarted before udf finished
