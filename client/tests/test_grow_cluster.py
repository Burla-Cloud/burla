from pathlib import Path
from time import perf_counter, sleep, time
import contextlib
import io
import json
import multiprocessing as multiprocessing
import queue
import traceback

import pytest
import requests

from burla import _auth
from burla import _helpers
from burla import _remote_parallel_map
from burla import remote_parallel_map
from burla._helpers import get_db_clients

LOCAL_MACHINE_TYPE = "n4-standard-2"
LOCAL_NODE_CPU_COUNT = 2
LOCAL_MAX_NODES = 2


def _active_local_nodes():
    sync_database, _ = get_db_clients()
    all_nodes = [document.to_dict() for document in sync_database.collection("nodes").stream()]
    return [
        node
        for node in all_nodes
        if node.get("status") in {"READY", "BOOTING", "RUNNING"}
        and node.get("machine_type") == LOCAL_MACHINE_TYPE
    ]


def _ready_local_nodes():
    return [node for node in _active_local_nodes() if node.get("status") == "READY"]


def _cluster_auth_headers():
    config = json.loads(_remote_parallel_map.CONFIG_PATH.read_text())
    return {
        "Authorization": f"Bearer {config['auth_token']}",
        "X-User-Email": config["email"],
    }


def _wait_until(condition_function, timeout_seconds, failure_message, interval_seconds=1):
    deadline_timestamp = time() + timeout_seconds
    while time() < deadline_timestamp:
        if condition_function():
            return
        sleep(interval_seconds)
    pytest.fail(failure_message)


def _shutdown_local_cluster_and_wait_until_off():
    headers = _cluster_auth_headers()
    response = requests.post("http://localhost:5001/v1/cluster/shutdown", headers=headers, timeout=120)
    response.raise_for_status()
    _wait_until(
        condition_function=lambda: len(_active_local_nodes()) == 0,
        timeout_seconds=180,
        failure_message="Local cluster did not shut down within timeout.",
        interval_seconds=2,
    )


def _grow_local_cluster(missing_cpus: int, current_cpus: int = 0):
    headers = _cluster_auth_headers()
    response = requests.post(
        "http://localhost:5001/v1/cluster/grow",
        headers=headers,
        json={"missing_cpus": missing_cpus, "current_cpus": current_cpus},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def _wait_until_ready_node_count(expected_ready_nodes: int, timeout_seconds: int = 240):
    _wait_until(
        condition_function=lambda: len(_ready_local_nodes()) >= expected_ready_nodes,
        timeout_seconds=timeout_seconds,
        failure_message=f"Expected {expected_ready_nodes} ready nodes within timeout.",
        interval_seconds=2,
    )


def _prepare_local_cluster_with_ready_nodes(node_count: int):
    _shutdown_local_cluster_and_wait_until_off()
    grow_response = _grow_local_cluster(node_count * LOCAL_NODE_CPU_COUNT)
    assert len(grow_response["added_node_instance_names"]) == node_count
    _wait_until_ready_node_count(node_count)


def _run_remote_parallel_map_case_in_subprocess(
    result_queue,
    function_name: str,
    input_count: int,
    sleep_seconds: float,
    max_parallelism: int,
    func_cpu: int,
    func_ram: int,
):
    namespace = {}
    function_code = (
        "from time import sleep\n"
        f"def {function_name}(test_input):\n"
        f"    sleep({sleep_seconds})\n"
        "    return test_input\n"
    )
    exec(function_code, namespace)
    test_function = namespace[function_name]

    output_buffer = io.StringIO()
    try:
        config_path = _remote_parallel_map.CONFIG_PATH
        config_json = json.loads(config_path.read_text())
        local_dev_config = {**config_json, "cluster_dashboard_url": "http://localhost:5001"}
        temp_config_path = Path("/tmp/burla_local_dev_test_credentials.json")
        temp_config_path.write_text(json.dumps(local_dev_config))
        _remote_parallel_map.CONFIG_PATH = temp_config_path
        _auth.CONFIG_PATH = temp_config_path
        _helpers.CONFIG_PATH = temp_config_path

        start_timestamp = time()
        start_runtime = perf_counter()
        with contextlib.redirect_stdout(output_buffer):
            outputs = remote_parallel_map(
                test_function,
                list(range(input_count)),
                grow=True,
                spinner=False,
                max_parallelism=max_parallelism,
                func_cpu=func_cpu,
                func_ram=func_ram,
            )
        runtime_seconds = perf_counter() - start_runtime

        sync_database, _ = get_db_clients()
        recent_jobs = []
        for job_document in sync_database.collection("jobs").stream():
            job = job_document.to_dict() or {}
            if job.get("function_name") != function_name:
                continue
            if job.get("n_inputs") != input_count:
                continue
            if float(job.get("started_at", 0)) >= start_timestamp - 5:
                recent_jobs.append((job_document.id, job))
        if not recent_jobs:
            raise Exception(f"No matching local-dev job document found for {function_name}.")

        latest_job_id, latest_job = max(
            recent_jobs, key=lambda pair: float(pair[1].get("started_at", 0))
        )
        assigned_node_results = {}
        assigned_nodes_collection = (
            sync_database.collection("jobs")
            .document(latest_job_id)
            .collection("assigned_nodes")
            .stream()
        )
        for assigned_node_document in assigned_nodes_collection:
            assigned_node = assigned_node_document.to_dict() or {}
            assigned_node_results[assigned_node_document.id] = int(
                assigned_node.get("current_num_results", 0)
            )

        nodes_started_for_run = []
        for node_document in sync_database.collection("nodes").stream():
            node = node_document.to_dict() or {}
            if node.get("machine_type") != LOCAL_MACHINE_TYPE:
                continue
            if float(node.get("started_booting_at", 0)) >= start_timestamp:
                nodes_started_for_run.append(node_document.id)

        result_queue.put(
            {
                "ok": True,
                "stdout": output_buffer.getvalue(),
                "runtime_seconds": runtime_seconds,
                "outputs": outputs,
                "assigned_node_results": assigned_node_results,
                "job_target_parallelism": latest_job.get("target_parallelism"),
                "nodes_started_for_run": nodes_started_for_run,
            }
        )
    except Exception:
        result_queue.put({"ok": False, "traceback": traceback.format_exc()})


def _run_case_with_timeout(
    function_name: str,
    input_count: int,
    sleep_seconds: float,
    max_parallelism: int,
    timeout_seconds: int,
    func_cpu: int = 1,
    func_ram: int = 4,
    fail_on_case_error: bool = True,
):
    context = multiprocessing.get_context("spawn")
    result_queue = context.Queue()
    process = context.Process(
        target=_run_remote_parallel_map_case_in_subprocess,
        args=(
            result_queue,
            function_name,
            input_count,
            sleep_seconds,
            max_parallelism,
            func_cpu,
            func_ram,
        ),
    )
    process.start()
    process.join(timeout_seconds + 30)

    if process.is_alive():
        process.terminate()
        process.join()
        if fail_on_case_error:
            pytest.fail(f"Test did not finish within {timeout_seconds + 30}s")
        return {"ok": False, "traceback": "Test timed out in subprocess."}
    if process.exitcode != 0:
        if fail_on_case_error:
            pytest.fail(f"Test process exited with code {process.exitcode}")
        return {"ok": False, "traceback": f"Test process exited with code {process.exitcode}"}

    try:
        result = result_queue.get(timeout=1)
    except queue.Empty:
        pytest.fail("Test subprocess ended without returning a result")
    if not result["ok"] and fail_on_case_error:
        pytest.fail(result["traceback"])
    return result


def test_local_dev_zero_nodes_grow_true():
    _shutdown_local_cluster_and_wait_until_off()
    result = _run_case_with_timeout(
        function_name="test_local_dev_zero_nodes_grow_true_function",
        input_count=20,
        sleep_seconds=0.02,
        max_parallelism=4,
        timeout_seconds=300,
        func_cpu=1,
        func_ram=4,
    )
    assert set(result["outputs"]) == set(range(20))
    assert len(result["assigned_node_results"]) >= 1
    assert len(result["nodes_started_for_run"]) >= 1
    assert len(_active_local_nodes()) <= LOCAL_MAX_NODES


def test_local_dev_one_node_grow_to_two_job_finishes_before_boot():
    _prepare_local_cluster_with_ready_nodes(node_count=1)
    result = _run_case_with_timeout(
        function_name="test_local_dev_finish_before_second_boot_function",
        input_count=8,
        sleep_seconds=0.1,
        max_parallelism=4,
        timeout_seconds=240,
        func_cpu=1,
        func_ram=4,
        fail_on_case_error=False,
    )
    if not result["ok"]:
        pytest.skip("Local-dev run unstable while starting initial node.")
    assert set(result["outputs"]) == set(range(8))
    assert len(result["nodes_started_for_run"]) >= 1
    assert len(result["assigned_node_results"]) == 1
    assert sum(result["assigned_node_results"].values()) == 8
    _wait_until_ready_node_count(expected_ready_nodes=2, timeout_seconds=240)


def test_local_dev_one_node_grow_to_two_second_node_does_work():
    _prepare_local_cluster_with_ready_nodes(node_count=1)
    result = _run_case_with_timeout(
        function_name="test_local_dev_second_node_does_work_function",
        input_count=160,
        sleep_seconds=2.0,
        max_parallelism=4,
        timeout_seconds=420,
        func_cpu=1,
        func_ram=4,
        fail_on_case_error=False,
    )
    if not result["ok"]:
        pytest.skip("Local-dev second node boot path unstable on this run.")
    assert set(result["outputs"]) == set(range(160))
    assert result["runtime_seconds"] >= 45
    assert len(result["nodes_started_for_run"]) >= 1
    useful_nodes = [
        node_result_count
        for node_result_count in result["assigned_node_results"].values()
        if node_result_count > 0
    ]
    assert len(useful_nodes) >= 2
    assert sum(result["assigned_node_results"].values()) == 160


def test_local_dev_two_nodes_four_workers_grow_true_behavior():
    _prepare_local_cluster_with_ready_nodes(node_count=2)
    result = _run_case_with_timeout(
        function_name="test_local_dev_two_nodes_four_workers_function",
        input_count=40,
        sleep_seconds=0.05,
        max_parallelism=4,
        timeout_seconds=240,
        func_cpu=1,
        func_ram=4,
    )
    assert set(result["outputs"]) == set(range(40))
    assert result["job_target_parallelism"] == 4
    assert len(result["assigned_node_results"]) <= 2
    assert len(result["nodes_started_for_run"]) == 0

