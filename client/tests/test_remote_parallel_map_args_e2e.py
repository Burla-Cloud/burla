"""
End-to-end integration coverage for remote_parallel_map arguments.
"""

from pathlib import Path
from time import sleep, time
import contextlib
import io
import json

import pytest
import requests

from burla import _auth
from burla import _helpers
from burla import _remote_parallel_map
from burla import remote_parallel_map
from burla._helpers import get_db_clients

LOCAL_MACHINE_TYPE = "n4-standard-2"


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
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def _ensure_local_ready_node():
    if len(_ready_local_nodes()) >= 1:
        return
    _grow_local_cluster(missing_cpus=2, current_cpus=len(_active_local_nodes()) * 2)
    _wait_until(
        condition_function=lambda: len(_ready_local_nodes()) >= 1,
        timeout_seconds=240,
        failure_message="Expected at least one ready local node within timeout.",
        interval_seconds=2,
    )


def _latest_job_for(function_name: str, expected_input_count: int, started_after: float):
    sync_database, _ = get_db_clients()
    matching_jobs = []
    for job_document in sync_database.collection("jobs").stream():
        job = job_document.to_dict() or {}
        if job.get("function_name") != function_name:
            continue
        if job.get("n_inputs") != expected_input_count:
            continue
        if float(job.get("started_at", 0)) >= started_after - 5:
            matching_jobs.append(job)
    if not matching_jobs:
        raise Exception(f"No matching job found for {function_name}.")
    return max(matching_jobs, key=lambda job: float(job.get("started_at", 0)))


def test_remote_parallel_map_arguments_e2e():
    config_path = _remote_parallel_map.CONFIG_PATH
    config_json = json.loads(config_path.read_text())
    local_dev_config = {**config_json, "cluster_dashboard_url": "http://localhost:5001"}
    temp_config_path = Path("/tmp/burla_local_dev_test_credentials.json")
    temp_config_path.write_text(json.dumps(local_dev_config))
    _remote_parallel_map.CONFIG_PATH = temp_config_path
    _auth.CONFIG_PATH = temp_config_path
    _helpers.CONFIG_PATH = temp_config_path

    _ensure_local_ready_node()

    # function_ + inputs + spinner=False
    namespace = {}
    exec(
        "def test_function(value):\n"
        "    print('integration-stdout-token')\n"
        "    return value\n",
        {},
        namespace,
    )
    function_inputs_case = namespace["test_function"]
    input_values = list(range(10))
    stdout_buffer = io.StringIO()
    started_at = time()
    with contextlib.redirect_stdout(stdout_buffer):
        outputs_one = remote_parallel_map(
            function_inputs_case, input_values, spinner=False, grow=False, max_parallelism=1
        )
    first_job = _latest_job_for(function_inputs_case.__name__, len(input_values), started_at)

    # generator=True
    exec(
        "def rpm_args_generator_case(value):\n"
        "    return value * 2\n",
        {},
        namespace,
    )
    generator_case = namespace["rpm_args_generator_case"]
    generator_inputs = list(range(8))
    started_at = time()
    generator_result = remote_parallel_map(
        generator_case, generator_inputs, spinner=False, generator=True, grow=False, max_parallelism=1
    )
    outputs_two = list(generator_result)
    second_job = _latest_job_for(generator_case.__name__, len(generator_inputs), started_at)

    # func_cpu + func_ram + max_parallelism
    exec(
        "def rpm_args_resources_case(value):\n"
        "    return value\n",
        {},
        namespace,
    )
    resources_case = namespace["rpm_args_resources_case"]
    resources_inputs = list(range(10))
    started_at = time()
    outputs_three = remote_parallel_map(
        resources_case,
        resources_inputs,
        spinner=False,
        grow=False,
        func_cpu=1,
        func_ram=4,
        max_parallelism=1,
    )
    third_job = _latest_job_for(resources_case.__name__, len(resources_inputs), started_at)

    # detach=True
    exec(
        "def rpm_args_detach_case(value):\n"
        "    return value\n",
        {},
        namespace,
    )
    detach_case = namespace["rpm_args_detach_case"]
    detach_inputs = [1]
    started_at = time()
    outputs_four = remote_parallel_map(
        detach_case,
        detach_inputs,
        spinner=False,
        grow=True,
        max_parallelism=1,
        detach=True,
    )
    fourth_job = _latest_job_for(detach_case.__name__, len(detach_inputs), started_at)

    # grow=True from zero active nodes
    _shutdown_local_cluster_and_wait_until_off()
    exec(
        "def rpm_args_grow_case(value):\n"
        "    return value\n",
        {},
        namespace,
    )
    grow_case = namespace["rpm_args_grow_case"]
    grow_inputs = list(range(10))
    started_at = time()
    outputs_five = remote_parallel_map(
        grow_case, grow_inputs, spinner=False, grow=True, max_parallelism=2
    )
    fifth_job = _latest_job_for(grow_case.__name__, len(grow_inputs), started_at)
    active_nodes_after_grow = len(_active_local_nodes())

    assert set(outputs_one) == set(range(10))
    assert "Preparing to call" not in stdout_buffer.getvalue()
    assert first_job.get("is_background_job") is False

    assert set(outputs_two) == set([value * 2 for value in range(8)])
    assert second_job.get("target_parallelism") == 1

    assert set(outputs_three) == set(range(10))
    assert third_job.get("func_cpu") == 1
    assert third_job.get("func_ram") == 4
    assert third_job.get("target_parallelism") == 1
    assert third_job.get("is_background_job") is False

    assert outputs_four == [1]
    assert fourth_job.get("is_background_job") is True

    assert set(outputs_five) == set(range(10))
    assert fifth_job.get("is_background_job") is False
    assert active_nodes_after_grow >= 1
