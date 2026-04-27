"""
DEV VM ONLY. These tests drive a live local-dev cluster over HTTP and
assume the full Burla stack is running inside a dev VM (Docker-in-Docker,
Firestore SA access, /srv/burla bind mounts). Do not run on a laptop.
See client/tests/README.md for the workflow.
"""

from time import sleep
import multiprocessing as mp
import os
import queue
import io
import contextlib
import traceback
import pytest
from burla import remote_parallel_map


N_INPUTS = 100
MAX_RUNTIME_SECONDS_WHEN_READY = 30


def _run_rpm_in_subprocess(result_queue, function_source, inputs):
    function_namespace = {}
    exec(function_source, {}, function_namespace)
    test_function = function_namespace["test_function"]

    stdout_buffer = io.StringIO()
    try:
        os.environ["BURLA_CLUSTER_DASHBOARD_URL"] = "http://localhost:5001"
        with contextlib.redirect_stdout(stdout_buffer):
            outputs = remote_parallel_map(test_function, inputs, spinner=False, grow=True)
        result_queue.put({"ok": True, "stdout": stdout_buffer.getvalue(), "outputs": outputs})
    except Exception:
        result_queue.put({"ok": False, "traceback": traceback.format_exc()})


def _run_with_timeout(function_source, inputs, timeout_seconds):
    context = mp.get_context("spawn")
    result_queue = context.Queue()
    process = context.Process(
        target=_run_rpm_in_subprocess, args=(result_queue, function_source, inputs)
    )
    process.start()
    process.join(timeout_seconds)

    if process.is_alive():
        process.terminate()
        process.join()
        pytest.fail(f"test did not finish within {timeout_seconds}s")

    if process.exitcode != 0:
        pytest.fail(f"test process exited with code {process.exitcode}")

    try:
        result = result_queue.get(timeout=1)
    except queue.Empty:
        pytest.fail("test subprocess ended without returning a result")

    if not result["ok"]:
        pytest.fail(result["traceback"])

    return result


def test_base():
    function_source = "def test_function(test_input):\n    print('hi')\n    return test_input\n"
    result = _run_with_timeout(function_source, list(range(N_INPUTS)), MAX_RUNTIME_SECONDS_WHEN_READY)
    stdout_lines = [line.strip() for line in result["stdout"].splitlines()]
    hi_count = sum(1 for line in stdout_lines if line == "hi")
    assert len(result["outputs"]) == N_INPUTS
    assert set(result["outputs"]) == set(range(N_INPUTS))
    assert hi_count == N_INPUTS


def test_cwd_is_workspace():
    function_source = "def test_function(_):\n    import os\n    return os.getcwd()\n"
    result = _run_with_timeout(function_source, [None], MAX_RUNTIME_SECONDS_WHEN_READY)
    assert result["outputs"] == ["/workspace"]


def test_nested_rpm():
    function_source = (
        "def test_function(x):\n"
        "    from burla import remote_parallel_map\n"
        "    return remote_parallel_map(lambda n: n + 100, [x], spinner=False)[0]\n"
    )
    result = _run_with_timeout(function_source, [1], MAX_RUNTIME_SECONDS_WHEN_READY * 4)
    assert result["outputs"] == [101]


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
