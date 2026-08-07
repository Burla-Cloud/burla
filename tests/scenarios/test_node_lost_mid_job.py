"""
Scenario: a node disappears mid-job.

Spot preemption and plain VM loss are advertised as survivable, and the client
reports them as `NodeDisconnected`. The existing worker-crash test kills a
process inside a healthy node; this kills the node itself.

The client should not sit through its 10-minute result-poll silence budget for
this: the head confirms with the cloud that the VM is gone, so the client is
expected to say so within seconds. The companion case, a node that goes silent
but still exists, is in `test_unresponsive_node_mid_job.py` and must NOT fail.
"""

from __future__ import annotations

import shutil
import subprocess
import threading

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.slow]

# The head needs NODE_FRESHNESS_SEC (15s) to consider the node gone; the rest
# is room for the client's next poll and its retries.
CLIENT_EXIT_BUDGET_SEC = 90


def _container_name(instance_name: str) -> str:
    return instance_name.replace("burla-node-", "node_")


@pytest.mark.timeout(CLIENT_EXIT_BUDGET_SEC + 180)
def test_node_lost_mid_job_surfaces_NodeDisconnected(
    rpm_subprocess, local_dev_cluster, main_http_client, wait_for_fixture
):
    if shutil.which("docker") is None:
        pytest.skip("no docker; nodes are real VMs and cannot be killed this way")

    source = "import time\ndef test_function(x):\n    time.sleep(20)\n    return x\n"
    result_box: dict = {}

    def _run():
        result_box["result"] = rpm_subprocess(
            source,
            list(range(4)),
            timeout_seconds=CLIENT_EXIT_BUDGET_SEC,
            grow=False,
        )

    rpm_thread = threading.Thread(target=_run, daemon=True)
    rpm_thread.start()
    try:

        def _busy_node():
            for node in main_http_client.get("/v1/cluster/nodes").json()["nodes"]:
                if node.get("current_job"):
                    return node
            return None

        node = wait_for_fixture(
            _busy_node, timeout=60, message="no node ever claimed the job"
        )
        # By exact container name: never by `node_*` prefix, which would take
        # out other checkouts' clusters.
        killed = subprocess.run(
            ["docker", "rm", "-f", _container_name(node["instance_name"])],
            capture_output=True,
            text=True,
        )
        if killed.returncode != 0:
            pytest.skip(f"node is not a local container: {killed.stderr.strip()}")
    finally:
        rpm_thread.join(timeout=CLIENT_EXIT_BUDGET_SEC + 60)

    assert not rpm_thread.is_alive(), "client hung after its node was killed"
    result = result_box["result"]
    assert not result["ok"], "client returned results from a node that was killed"
    assert result["exception_type"] == "NodeDisconnected", (
        f"expected NodeDisconnected, got {result['exception_type']}: "
        f"{result['exception_message']}"
    )
