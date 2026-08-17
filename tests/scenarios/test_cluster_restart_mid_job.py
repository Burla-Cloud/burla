"""
Scenario 2: cluster restart mid-job.

Submits a slow job, restarts the cluster while it runs, verifies the client
raises `ClusterRestarted` and the job doc has `cluster_restarted=True` with
`status=CANCELED`. Then runs a second job after the restart to prove the
cluster recovers.
"""

from __future__ import annotations

import threading
import time

import pytest

# Restart is `docker rm` locally but a real terminate-and-reboot on VMs, and
# the client-visible behavior that matters is the latter. Recovery means a
# full EC2 node boot (~3-4 min), which doesn't fit the default 120s timeout.
pytestmark = [
    pytest.mark.e2e,
    pytest.mark.slow,
    pytest.mark.remote_dev,
    pytest.mark.timeout(600),
]


def test_cluster_restart_mid_job(
    rpm_subprocess,
    local_dev_cluster,
    main_http_client,
    wait_for_fixture,
):
    # Start a deliberately slow job in a background thread so this test can
    # trigger a restart while it runs.
    slow_source = (
        "import time\n"
        "def test_function(x):\n"
        "    time.sleep(15)\n"
        "    return x\n"
    )

    rpm_result_box: dict = {}

    def _run_slow():
        rpm_result_box["result"] = rpm_subprocess(
            slow_source, list(range(4)), timeout_seconds=300, grow=True
        )

    started_after = time.time()
    slow_thread = threading.Thread(target=_run_slow, daemon=True)
    slow_thread.start()

    def _running_job_with_inputs_uploaded():
        jobs = main_http_client.get("/v1/jobs?page=0").json()["jobs"]
        for summary in jobs:
            if summary.get("function_name") != "test_function":
                continue
            if summary.get("started_at", 0) < started_after:
                continue
            job = main_http_client.get(f"/v1/jobs/{summary['jobId']}").json()
            if job.get("status") == "RUNNING" and job.get("all_inputs_uploaded"):
                return summary["jobId"]
        return None

    job_id = wait_for_fixture(
        _running_job_with_inputs_uploaded,
        timeout=240,
        message="slow job never reached active execution before restart",
    )
    restart_resp = main_http_client.post("/v1/cluster/restart")
    assert restart_resp.status_code in (200, 204)

    slow_thread.join(timeout=60)
    assert not slow_thread.is_alive(), "client never exited after cluster restart"
    assert "result" in rpm_result_box

    result = rpm_result_box["result"]
    assert not result["ok"], f"client succeeded after cluster restart: {result['outputs']}"
    assert result["exception_type"] == "ClusterRestarted", result["exception_message"]

    def _restarted_job():
        job = main_http_client.get(f"/v1/jobs/{job_id}").json()
        return job if job.get("cluster_restarted") is True else None

    job = wait_for_fixture(_restarted_job, timeout=15)
    assert job["cluster_restarted"] is True
    assert job["status"] == "CANCELED"

    # After the restart settles, a fresh rpm must succeed. Poll for a READY
    # node first so we don't race the restart.
    def _ready():
        state = main_http_client.get("/v1/cluster/state").json()
        return state["ready_nodes"] if state.get("ready_nodes") else None

    wait_for_fixture(_ready, timeout=300, message="cluster never recovered after restart")

    recover_source = "def test_function(x):\n    return x + 1\n"
    recover_result = rpm_subprocess(recover_source, [1, 2, 3], timeout_seconds=60, grow=True)
    assert recover_result["ok"], recover_result.get("traceback")
    assert sorted(recover_result["outputs"]) == [2, 3, 4]
