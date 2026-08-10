"""
Scenario: the cluster is shut down mid-job.

The restart path is covered; shutdown is the other half of the same lifecycle
branch and reaches the client as `ClusterShutdown`. Only the head's
`cluster_shutdown` flag was tested before.

Shutdown deletes the node before it can attach the `cluster_shutdown` signal
to a /results response, so when the client's polls start failing it asks the
head for the job doc and raises `ClusterShutdown` promptly. The generous
budget below is headroom, not an expectation.
"""

from __future__ import annotations

import threading
import time

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.slow]

CLIENT_EXIT_BUDGET_SEC = 840


@pytest.mark.timeout(CLIENT_EXIT_BUDGET_SEC + 180)
def test_cluster_shutdown_mid_job_surfaces_ClusterShutdown(
    rpm_subprocess, local_dev_cluster, main_http_client, wait_for_fixture
):
    source = "import time\ndef test_function(x):\n    time.sleep(15)\n    return x\n"
    result_box: dict = {}

    def _run():
        result_box["result"] = rpm_subprocess(
            source,
            list(range(4)),
            timeout_seconds=CLIENT_EXIT_BUDGET_SEC,
            grow=True,
        )

    rpm_thread = threading.Thread(target=_run, daemon=True)
    rpm_thread.start()
    time.sleep(6)

    # Shutdown deletes every VM synchronously and can 500 partway through; the
    # job's terminal state is written before that work starts.
    main_http_client.post("/v1/cluster/shutdown", timeout=600)

    rpm_thread.join(timeout=CLIENT_EXIT_BUDGET_SEC + 60)
    assert not rpm_thread.is_alive(), "client never exited after cluster shutdown"

    result = result_box["result"]
    assert not result["ok"], "client succeeded after the cluster was shut down"
    assert result["exception_type"] == "ClusterShutdown", (
        f"expected ClusterShutdown, got {result['exception_type']}: "
        f"{result['exception_message']}"
    )

    def _shutdown_job():
        for summary in main_http_client.get("/v1/jobs?page=0").json()["jobs"]:
            if summary.get("function_name") != "test_function":
                continue
            job = main_http_client.get(f"/v1/jobs/{summary['jobId']}").json()
            if job.get("cluster_shutdown"):
                return job
        return None

    job = wait_for_fixture(_shutdown_job, timeout=15)
    assert job["status"] == "CANCELED"
