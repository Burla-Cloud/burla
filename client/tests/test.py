"""
The tests here assume the cluster is running in "local-dev-mode".
"""

from pathlib import Path
from time import sleep
import multiprocessing as mp
import queue
import io
import contextlib
import traceback
import json
import pytest
from burla import remote_parallel_map
from burla import _remote_parallel_map
from burla import _auth
from burla import _helpers


N_INPUTS = 100
MAX_RUNTIME_SECONDS_WHEN_READY = 30


def _run_test_base_in_subprocess(result_queue):
    function_namespace = {}
    exec(
        "def test_function(test_input):\n" "    print('hi')\n" "    return test_input\n",
        {},
        function_namespace,
    )
    test_function = function_namespace["test_function"]

    stdout_buffer = io.StringIO()
    try:
        config_path = _remote_parallel_map.CONFIG_PATH
        config_json = json.loads(config_path.read_text())
        local_dev_config = {**config_json, "cluster_dashboard_url": "http://localhost:5001"}
        temp_config_path = Path("/tmp/burla_local_dev_test_credentials.json")
        temp_config_path.write_text(json.dumps(local_dev_config))
        _remote_parallel_map.CONFIG_PATH = temp_config_path
        _auth.CONFIG_PATH = temp_config_path
        _helpers.CONFIG_PATH = temp_config_path

        with contextlib.redirect_stdout(stdout_buffer):
            outputs = remote_parallel_map(
                test_function, list(range(N_INPUTS)), spinner=False, grow=True
            )
        result_queue.put({"ok": True, "stdout": stdout_buffer.getvalue(), "outputs": outputs})
    except Exception:
        result_queue.put({"ok": False, "traceback": traceback.format_exc()})


def _run_with_timeout(timeout_seconds):
    context = mp.get_context("spawn")
    result_queue = context.Queue()
    process = context.Process(target=_run_test_base_in_subprocess, args=(result_queue,))
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
    result = _run_with_timeout(MAX_RUNTIME_SECONDS_WHEN_READY)
    stdout_lines = [line.strip() for line in result["stdout"].splitlines()]
    hi_count = sum(1 for line in stdout_lines if line == "hi")
    assert len(result["outputs"]) == N_INPUTS
    assert set(result["outputs"]) == set(range(N_INPUTS))
    assert hi_count == N_INPUTS


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
