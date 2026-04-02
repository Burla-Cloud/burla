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
import json
from pathlib import Path
import time
import requests
import pytest

from burla import remote_parallel_map
from burla import _remote_parallel_map
from burla import _auth
from burla import _helpers
from burla._helpers import get_db_clients


N_INPUTS = 1000
MAX_RUNTIME_SECONDS_WHEN_OFF = 300
REQUIRED_MACHINE_TYPE = "n4-standard-80"


def _get_ready_80cpu_nodes():
    sync_db, _ = get_db_clients()
    nodes = [doc.to_dict() for doc in sync_db.collection("nodes").stream()]
    return [
        node
        for node in nodes
        if node.get("status") == "READY" and node.get("machine_type") == REQUIRED_MACHINE_TYPE
    ]


def _get_active_80cpu_nodes():
    sync_db, _ = get_db_clients()
    nodes = [doc.to_dict() for doc in sync_db.collection("nodes").stream()]
    return [
        node
        for node in nodes
        if node.get("status") in {"READY", "BOOTING", "RUNNING"}
        and node.get("machine_type") == REQUIRED_MACHINE_TYPE
    ]


def _cluster_auth_headers():
    config = json.loads(_remote_parallel_map.CONFIG_PATH.read_text())
    return {
        "Authorization": f"Bearer {config['auth_token']}",
        "X-User-Email": config["email"],
    }


def _shutdown_cluster_and_wait_until_off():
    headers = _cluster_auth_headers()
    response = requests.post("http://localhost:5001/v1/cluster/shutdown", headers=headers, timeout=30)
    response.raise_for_status()

    deadline = time.time() + 300
    while time.time() < deadline:
        if len(_get_active_80cpu_nodes()) == 0:
            return
        time.sleep(5)
    pytest.fail("Cluster did not fully shut down within timeout.")


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
        config_path = _remote_parallel_map.CONFIG_PATH
        config_json = json.loads(config_path.read_text())
        remote_dev_config = {**config_json, "cluster_dashboard_url": "http://localhost:5001"}
        temp_config_path = Path("/tmp/burla_remote_dev_test_credentials.json")
        temp_config_path.write_text(json.dumps(remote_dev_config))
        _remote_parallel_map.CONFIG_PATH = temp_config_path
        _auth.CONFIG_PATH = temp_config_path
        _helpers.CONFIG_PATH = temp_config_path

        start_timestamp = time.time()
        start = perf_counter()
        with contextlib.redirect_stdout(stdout_buffer):
            outputs = remote_parallel_map(
                test_function, list(range(N_INPUTS)), spinner=False, grow=True
            )
        runtime_seconds = perf_counter() - start
        sync_db, _ = get_db_clients()
        active_nodes_after = _get_active_80cpu_nodes()
        recent_jobs = []
        for job_doc in sync_db.collection("jobs").stream():
            job = job_doc.to_dict() or {}
            if job.get("function_name") != "test_function":
                continue
            if job.get("n_inputs") != N_INPUTS:
                continue
            if float(job.get("started_at", 0)) >= start_timestamp - 5:
                recent_jobs.append(job)
        if not recent_jobs:
            raise Exception("No matching job document found for remote-dev grow test.")
        latest_job = max(recent_jobs, key=lambda job: float(job.get("started_at", 0)))
        result_queue.put(
            {
                "ok": True,
                "stdout": stdout_buffer.getvalue(),
                "runtime_seconds": runtime_seconds,
                "outputs": outputs,
                "job_target_parallelism": latest_job.get("target_parallelism"),
                "n_active_80cpu_nodes_after": len(active_nodes_after),
                "active_machine_types_after": sorted(
                    list({node.get("machine_type") for node in active_nodes_after})
                ),
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


def test_remote_dev_grow_from_cluster_off_and_execute():
    _shutdown_cluster_and_wait_until_off()
    result = _run_with_timeout(MAX_RUNTIME_SECONDS_WHEN_OFF)

    assert len(result["outputs"]) == N_INPUTS
    assert set(result["outputs"]) == set(range(N_INPUTS))
    assert result["job_target_parallelism"] == N_INPUTS
    assert result["n_active_80cpu_nodes_after"] >= 13
    assert result["active_machine_types_after"] == [REQUIRED_MACHINE_TYPE]
