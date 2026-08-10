"""
Scenario: the two ways a running job gets canceled.

Ctrl-C in the terminal and Stop on the dashboard both have to end the client
call with `JobCanceled` and leave the job CANCELED on the head. Neither path
was covered end to end: the suite only checked the head's `dashboard_canceled`
flag from a service test.
"""

from __future__ import annotations

import threading

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.slow]

SLOW_UDF = "import time\ndef test_function(x):\n    time.sleep(20)\n    return x\n"


def _canceled_job(main_http_client, fail_reason_fragment: str):
    for summary in main_http_client.get("/v1/jobs?page=0").json()["jobs"]:
        if summary.get("function_name") != "test_function":
            continue
        job = main_http_client.get(f"/v1/jobs/{summary['jobId']}").json()
        if job.get("status") != "CANCELED":
            continue
        if any(fail_reason_fragment in reason for reason in job.get("fail_reason") or []):
            return job
    return None


def test_ctrl_c_cancels_the_running_job(
    ctrl_c_after, local_dev_cluster, main_http_client, wait_for_fixture
):
    result = ctrl_c_after(SLOW_UDF, list(range(4)), delay_s=8, grow=False)

    assert not result["ok"], "Ctrl-C did not stop the job"
    assert result["exception_type"] == "JobCanceled"

    wait_for_fixture(
        lambda: _canceled_job(main_http_client, "Cancel signal from client"),
        timeout=15,
        message="head never recorded the client's cancellation",
    )


def test_dashboard_stop_cancels_the_clients_job(
    rpm_subprocess, local_dev_cluster, main_http_client, wait_for_fixture
):
    result_box: dict = {}

    def _run():
        result_box["result"] = rpm_subprocess(
            SLOW_UDF, list(range(4)), timeout_seconds=120, grow=False
        )

    rpm_thread = threading.Thread(target=_run, daemon=True)
    rpm_thread.start()
    try:

        def _running_job_id():
            nodes = main_http_client.get("/v1/cluster/nodes").json()["nodes"]
            for node in nodes:
                if node.get("current_job"):
                    return node["current_job"]
            return None

        job_id = wait_for_fixture(
            _running_job_id, timeout=60, message="no node ever claimed the job"
        )
        stop = main_http_client.post(f"/v1/jobs/{job_id}/stop")
        assert stop.status_code in (200, 204)
    finally:
        rpm_thread.join(timeout=120)

    assert not rpm_thread.is_alive(), "client never exited after the job was stopped"
    result = result_box["result"]
    assert not result["ok"], "client returned results for a job stopped on the dashboard"
    assert result["exception_type"] == "JobCanceled"
    assert "dashboard" in result["exception_message"].lower()
