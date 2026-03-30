"""
The tests here assume the cluster is running in "local-dev-mode".
"""

from time import sleep, perf_counter
import signal
import io
import contextlib
import pytest
from google.cloud.firestore import FieldFilter
from burla import remote_parallel_map
from burla._helpers import get_db_clients


N_INPUTS = 10
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


def _run_with_timeout(function_to_run, timeout_seconds):
    def timeout_handler(signal_number, current_stack_frame):
        raise TimeoutError

    original_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
    try:
        return function_to_run()
    except TimeoutError:
        pytest.fail(f"test did not finish within {timeout_seconds}s")
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, original_handler)


def test_base():
    sync_db, _ = get_db_clients()
    nodes_are_ready = _wait_for_ready_nodes(sync_db, READY_WAIT_TIMEOUT_SECONDS)
    if not nodes_are_ready:
        pytest.skip(f"nodes did not become ready within {READY_WAIT_TIMEOUT_SECONDS}s")

    function_namespace = {}
    exec(
        "def test_function(test_input):\n" "    print('hi')\n" "    return test_input\n",
        {},
        function_namespace,
    )
    test_function = function_namespace["test_function"]

    stdout_buffer = io.StringIO()
    with contextlib.redirect_stdout(stdout_buffer):
        _run_with_timeout(
            lambda: remote_parallel_map(test_function, list(range(N_INPUTS)), spinner=False),
            MAX_RUNTIME_SECONDS_WHEN_READY,
        )
    stdout_lines = [line.strip() for line in stdout_buffer.getvalue().splitlines()]
    hi_count = sum(1 for line in stdout_lines if line == "hi")
    assert hi_count == N_INPUTS, f"expected {N_INPUTS} 'hi' logs, got {hi_count}"


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
