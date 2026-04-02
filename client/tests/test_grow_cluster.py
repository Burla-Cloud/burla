from pathlib import Path
from time import perf_counter, time, sleep
import multiprocessing as mp
import queue
import io
import contextlib
import traceback
import json
import pytest
import requests

from burla import remote_parallel_map
from burla._helpers import get_db_clients
from burla import _remote_parallel_map
from burla import _auth
from burla import _helpers

N_INPUTS = 20
EXPECTED_LOCAL_DEV_CPUS = 4
EXPECTED_LOCAL_DEV_MAX_NODES = 2
EXPECTED_LOCAL_DEV_MACHINE_TYPE = "n4-standard-2"
MAX_RUNTIME_SECONDS_WHEN_READY = 120


def _active_local_dev_nodes():
    sync_db, _ = get_db_clients()
    nodes = [doc.to_dict() for doc in sync_db.collection("nodes").stream()]
    return [
        node
        for node in nodes
        if node.get("status") in {"READY", "BOOTING", "RUNNING"}
        and node.get("machine_type") == EXPECTED_LOCAL_DEV_MACHINE_TYPE
    ]


def _cluster_auth_headers():
    config = json.loads(_remote_parallel_map.CONFIG_PATH.read_text())
    return {
        "Authorization": f"Bearer {config['auth_token']}",
        "X-User-Email": config["email"],
    }


def _shutdown_local_cluster_and_wait_until_off():
    headers = _cluster_auth_headers()
    response = requests.post("http://localhost:5001/v1/cluster/shutdown", headers=headers, timeout=30)
    response.raise_for_status()

    deadline = time() + 120
    while time() < deadline:
        if len(_active_local_dev_nodes()) == 0:
            return
        sleep(2)
    pytest.fail("Local cluster did not fully shut down within timeout.")


def _cpu_count_from_nodes(nodes):
    total_cpus = 0
    for node in nodes:
        machine_type = str(node.get("machine_type") or "")
        if machine_type.startswith("n4-standard-") and machine_type.split("-")[-1].isdigit():
            total_cpus += int(machine_type.split("-")[-1])
    return total_cpus


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
        local_dev_config = {**config_json, "cluster_dashboard_url": "http://localhost:5001"}
        temp_config_path = Path("/tmp/burla_local_dev_test_credentials.json")
        temp_config_path.write_text(json.dumps(local_dev_config))
        _remote_parallel_map.CONFIG_PATH = temp_config_path
        _auth.CONFIG_PATH = temp_config_path
        _helpers.CONFIG_PATH = temp_config_path

        nodes_before = _active_local_dev_nodes()
        cpus_before = _cpu_count_from_nodes(nodes_before)

        start = perf_counter()
        start_timestamp = time()
        with contextlib.redirect_stdout(stdout_buffer):
            outputs = remote_parallel_map(
                test_function,
                list(range(N_INPUTS)),
                grow=True,
                spinner=False,
            )
        runtime_seconds = perf_counter() - start

        nodes_after = _active_local_dev_nodes()
        cpus_after = _cpu_count_from_nodes(nodes_after)
        sync_db, _ = get_db_clients()
        nodes_started_for_run = []
        for node_doc in sync_db.collection("nodes").stream():
            node = node_doc.to_dict() or {}
            if node.get("machine_type") != EXPECTED_LOCAL_DEV_MACHINE_TYPE:
                continue
            if float(node.get("started_booting_at", 0)) >= start_timestamp - 5:
                nodes_started_for_run.append(node)
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
            raise Exception("No matching local-dev job document found for grow integration test.")
        latest_job = max(recent_jobs, key=lambda job: float(job.get("started_at", 0)))
        result_queue.put(
            {
                "ok": True,
                "stdout": stdout_buffer.getvalue(),
                "runtime_seconds": runtime_seconds,
                "outputs": outputs,
                "cpus_before": cpus_before,
                "cpus_after": cpus_after,
                "nodes_after": len(nodes_after),
                "nodes_started_for_run": len(nodes_started_for_run),
                "job_target_parallelism": latest_job.get("target_parallelism"),
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


def test_local_dev_grow_cluster_and_execute_job():
    required_cpus_for_high_ram = _remote_parallel_map._required_cluster_cpus(
        n_inputs=2, max_parallelism=2, func_cpu=1, func_ram=320
    )
    assert required_cpus_for_high_ram == 160

    _shutdown_local_cluster_and_wait_until_off()
    result = _run_with_timeout(MAX_RUNTIME_SECONDS_WHEN_READY)

    assert len(result["outputs"]) == N_INPUTS
    assert set(result["outputs"]) == set(range(N_INPUTS))
    assert result["nodes_started_for_run"] >= EXPECTED_LOCAL_DEV_MAX_NODES
    assert result["nodes_after"] <= EXPECTED_LOCAL_DEV_MAX_NODES
    assert result["job_target_parallelism"] == EXPECTED_LOCAL_DEV_CPUS

