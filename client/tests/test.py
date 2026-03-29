"""
The tests here assume the cluster is running in "local-dev-mode".
"""

from time import sleep, perf_counter
import pytest
from google.cloud.firestore import FieldFilter
from burla import remote_parallel_map
from burla._helpers import get_db_clients


N_INPUTS = 100
MAX_RUNTIME_SECONDS_WHEN_READY = 3
READY_WAIT_TIMEOUT_SECONDS = 6
READY_WAIT_POLL_INTERVAL_SECONDS = 0.25
MIN_READY_NODES = 1


def _count_nodes_with_status(sync_db, status):
    status_filter = FieldFilter("status", "==", status)
    nodes = sync_db.collection("nodes").where(filter=status_filter).get()
    return len(nodes)


def _nodes_are_ready(sync_db):
    number_of_ready_nodes = _count_nodes_with_status(sync_db, "READY")
    number_of_running_nodes = _count_nodes_with_status(sync_db, "RUNNING")
    number_of_booting_nodes = _count_nodes_with_status(sync_db, "BOOTING")
    enough_ready_nodes = number_of_ready_nodes >= MIN_READY_NODES
    no_startup_transition = number_of_running_nodes == 0 and number_of_booting_nodes == 0
    return enough_ready_nodes and no_startup_transition


def _wait_for_ready_nodes(sync_db, timeout_seconds):
    wait_deadline = perf_counter() + timeout_seconds
    while perf_counter() < wait_deadline:
        if _nodes_are_ready(sync_db):
            return True
        sleep(READY_WAIT_POLL_INTERVAL_SECONDS)
    return _nodes_are_ready(sync_db)


def test_base():
    sync_db, _ = get_db_clients()
    nodes_are_ready = _wait_for_ready_nodes(sync_db, READY_WAIT_TIMEOUT_SECONDS)
    if not nodes_are_ready:
        pytest.skip(f"nodes did not become ready within {READY_WAIT_TIMEOUT_SECONDS}s")

    def test_function(test_input):
        return test_input

    start_time_seconds = perf_counter()
    results = remote_parallel_map(test_function, list(range(N_INPUTS)))
    runtime_seconds = perf_counter() - start_time_seconds

    assert runtime_seconds <= MAX_RUNTIME_SECONDS_WHEN_READY, (
        f"test_base runtime was {runtime_seconds:.2f}s with ready nodes, "
        f"which exceeds {MAX_RUNTIME_SECONDS_WHEN_READY}s."
    )


def _test_big_function():

    big_object = bytes(107 * 1_000_000)

    def test_function(test_input):
        return test_input

    results = remote_parallel_map(test_function, list(range(N_INPUTS)))


def _test_big_inputs():

    INPUT_SIZE = 10 * 1_000_000
    my_inputs = [bytes(INPUT_SIZE) for _ in range(1_000)]

    def test_function(test_input):
        sleep(1)
        return 1

    results = remote_parallel_map(test_function, my_inputs)


def _test_big_returns():

    RETURN_SIZE = 100 * 1_000_000

    def test_function(test_input):
        return bytes(RETURN_SIZE)

    results = remote_parallel_map(test_function, list(range(100)))
