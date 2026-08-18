"""
Scenario: a node stops answering but is not dead.

A node pushed hard enough by the user's own workload can starve node_service
until it stops answering the head and the client, while the workload it is
running is fine. Nothing in the cluster may conclude "dead" from silence alone,
because the cost of being wrong is killing a healthy job.

Two shapes of the same thing: node_service frozen outright, and a node buried
under a workload that uses everything it has. Both must finish with every
result. The opposite case, a node that really is gone, is in
`test_node_lost_mid_job.py` and is allowed the full 3-minute silence budget.
"""

from __future__ import annotations

import subprocess
import threading
import time

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.slow]

# Comfortably past the head's NODE_FRESHNESS_SEC (15s), so the head has given
# up on the node's state and the client has polled it unreachable many times.
FREEZE_SECONDS = 60


def _container_name(instance_name: str) -> str:
    return instance_name.replace("burla-node-", "node_")


def _node_service_pids(container: str) -> list[str]:
    # The node image has no `ps`; /proc has the same answer everywhere.
    list_processes = (
        'for d in /proc/[0-9]*; do echo "${d#/proc/} $(cat $d/comm 2>/dev/null)"; done'
    )
    listing = subprocess.run(
        ["docker", "exec", container, "sh", "-c", list_processes],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    pids = []
    for line in listing:
        pid, _, command = line.strip().partition(" ")
        if command.strip() in ("uv", "python3"):
            pids.append(pid)
    return pids


def _signal_node_service(container: str, signal: str, pids: list[str]):
    # No `kill` binary in the node image; the shell builtin is always there.
    subprocess.run(
        ["docker", "exec", container, "sh", "-c", f"kill -{signal} {' '.join(pids)}"],
        capture_output=True,
        text=True,
        check=True,
    )


def _busy_node(main_http_client):
    for node in main_http_client.get("/v1/cluster/nodes").json()["nodes"]:
        if node.get("status") == "RUNNING" and node.get("current_job"):
            return node
    return None


@pytest.mark.local_dev
@pytest.mark.timeout(600)
def test_frozen_node_service_does_not_fail_the_job(
    rpm_subprocess, local_dev_cluster, main_http_client, wait_for_fixture
):
    """node_service is stopped outright while its workers keep running: exactly
    what a starved node looks like from the outside."""
    source = "import time\ndef test_function(x):\n    time.sleep(3)\n    return x * 10\n"
    inputs = list(range(6))
    result_box: dict = {}

    def _run():
        result_box["result"] = rpm_subprocess(
            source, inputs, timeout_seconds=480, grow=False
        )

    rpm_thread = threading.Thread(target=_run, daemon=True)
    rpm_thread.start()

    node = wait_for_fixture(
        lambda: _busy_node(main_http_client),
        timeout=120,
        message="no node ever claimed the job",
    )
    container = _container_name(node["instance_name"])
    pids = _node_service_pids(container)
    assert pids, f"found no node_service processes in {container}"

    _signal_node_service(container, "STOP", pids)
    try:
        time.sleep(FREEZE_SECONDS)
        # The head must not have concluded anything: the container is still
        # there, so the node is still the client's node.
        nodes = {
            n["instance_name"]: n
            for n in main_http_client.get("/v1/cluster/nodes").json()["nodes"]
        }
        still_there = nodes.get(node["instance_name"])
        assert still_there, "head dropped a node whose VM still exists"
        assert still_there["status"] != "FAILED", "head failed a node that was only silent"
        assert rpm_thread.is_alive(), "client gave up on a node that still exists"
    finally:
        _signal_node_service(container, "CONT", pids)

    rpm_thread.join(timeout=300)
    assert not rpm_thread.is_alive(), "client never finished after the node came back"
    result = result_box["result"]
    assert result["ok"], result.get("traceback")
    assert sorted(result["outputs"]) == [x * 10 for x in inputs]


@pytest.mark.timeout(900)
def test_node_buried_under_its_workload_finishes_the_job(
    rpm_subprocess, local_dev_cluster, main_http_client, wait_for_fixture
):
    """Every worker pegs a core and holds memory for the whole job, which is
    the state that starves node_service in the first place."""
    source = (
        "import time\n"
        "def test_function(x):\n"
        "    ballast = bytearray(150 * 1024 * 1024)\n"
        "    deadline = time.time() + 40\n"
        "    total = 0\n"
        "    while time.time() < deadline:\n"
        "        total += sum(range(10_000))\n"
        "    return (x, len(ballast), total > 0)\n"
    )
    inputs = list(range(8))
    result = rpm_subprocess(source, inputs, timeout_seconds=720, grow=False)

    assert result["ok"], result.get("traceback")
    assert sorted(output[0] for output in result["outputs"]) == inputs
    assert all(output[2] for output in result["outputs"])

    job_id = None
    for summary in main_http_client.get("/v1/jobs?page=0").json()["jobs"]:
        if summary.get("function_name") == "test_function":
            job_id = summary["jobId"]
            break
    assert job_id, "job never appeared on the head"

    def _terminal_status():
        job = main_http_client.get(f"/v1/jobs/{job_id}").json()
        return job if job["status"] != "RUNNING" else None

    # The head learns the job is over from the node's next push, just after the
    # client already has its results.
    job = wait_for_fixture(_terminal_status, timeout=30)
    assert job["status"] == "COMPLETED", f"head recorded {job['status']}: {job.get('fail_reason')}"
