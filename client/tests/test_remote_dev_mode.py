"""
Remote-dev validation test.

Run this only in remote-dev mode (main service local, nodes on cloud VMs).
"""

from time import perf_counter
import multiprocessing as mp
import queue
import io
import contextlib
import traceback
import pytest

from burla import remote_parallel_map
from burla._helpers import get_db_clients


N_INPUTS = 1000
MAX_RUNTIME_SECONDS_WHEN_READY = 3
REQUIRED_READY_NODES = 13
REQUIRED_MACHINE_TYPE = "n4-standard-80"


def _get_ready_80cpu_nodes():
    sync_db, _ = get_db_clients()
    nodes = [doc.to_dict() for doc in sync_db.collection("nodes").stream()]
    return [
        node
        for node in nodes
        if node.get("status") == "READY" and node.get("machine_type") == REQUIRED_MACHINE_TYPE
    ]


def _run_test_in_subprocess(result_queue):
    function_namespace = {}
    exec(
        "def test_function(test_input):\n" "    print('hi')\n" "    return test_input\n",
        {},
        function_namespace,
    )
    test_function = function_namespace["test_function"]

    stdout_buffer = io.StringIO()
    try:
        start = perf_counter()
        with contextlib.redirect_stdout(stdout_buffer):
            outputs = remote_parallel_map(test_function, list(range(N_INPUTS)), spinner=False)
        runtime_seconds = perf_counter() - start
        result_queue.put(
            {
                "ok": True,
                "stdout": stdout_buffer.getvalue(),
                "runtime_seconds": runtime_seconds,
                "outputs": outputs,
            }
        )
    except Exception:
        result_queue.put({"ok": False, "traceback": traceback.format_exc()})


def _run_with_timeout(timeout_seconds):
    context = mp.get_context("spawn")
    result_queue = context.Queue()
    process = context.Process(target=_run_test_in_subprocess, args=(result_queue,))
    process.start()
    process.join(timeout_seconds + 30)

    if process.is_alive():
        process.terminate()
        process.join()
        pytest.fail(f"test did not finish within {timeout_seconds + 30}s")

    if process.exitcode != 0:
        pytest.fail(f"test process exited with code {process.exitcode}")

    try:
        result = result_queue.get(timeout=1)
    except queue.Empty:
        pytest.fail("test subprocess ended without returning a result")

    if not result["ok"]:
        pytest.fail(result["traceback"])

    return result


def test_remote_dev_1000_inputs_under_3_seconds():
    ready_80cpu_nodes = _get_ready_80cpu_nodes()
    n_ready = len(ready_80cpu_nodes)
    assert (
        n_ready >= REQUIRED_READY_NODES
    ), f"Need at least {REQUIRED_READY_NODES} READY {REQUIRED_MACHINE_TYPE} nodes, found {n_ready}."

    result = _run_with_timeout(MAX_RUNTIME_SECONDS_WHEN_READY)
    stdout_lines = [line.strip() for line in result["stdout"].splitlines()]
    hi_count = sum(1 for line in stdout_lines if line == "hi")

    assert len(result["outputs"]) == N_INPUTS
    assert set(result["outputs"]) == set(range(N_INPUTS))
    assert hi_count > 0, "expected at least one forwarded 'hi' log"
    assert (
        result["runtime_seconds"] < MAX_RUNTIME_SECONDS_WHEN_READY
    ), f"expected runtime < {MAX_RUNTIME_SECONDS_WHEN_READY}s, got {result['runtime_seconds']:.2f}s"
