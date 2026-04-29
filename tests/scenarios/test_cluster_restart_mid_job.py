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

pytestmark = [pytest.mark.chaos, pytest.mark.slow]


def test_cluster_restart_mid_job(
    rpm_subprocess,
    local_dev_cluster,
    firestore_db,
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
            slow_source, list(range(4)), timeout_seconds=120, grow=True
        )

    slow_thread = threading.Thread(target=_run_slow, daemon=True)
    slow_thread.start()

    # Give the client ~4s to actually start uploading inputs, then restart.
    time.sleep(4)
    restart_resp = main_http_client.post("/v1/cluster/restart")
    assert restart_resp.status_code in (200, 204)

    # Client should see the restart and either raise ClusterRestarted or
    # exit with a related domain exception. Wait up to 30s.
    slow_thread.join(timeout=60)
    assert not slow_thread.is_alive(), "client never exited after cluster restart"
    assert "result" in rpm_result_box

    result = rpm_result_box["result"]
    assert not result["ok"], f"client succeeded after cluster restart: {result['outputs']}"
    assert result["exception_type"] in (
        "ClusterRestarted",
        "NodeDisconnected",
        "JobStalled",
    ), f"unexpected exception {result['exception_type']}: {result['exception_message']}"

    # Firestore: at least one test_function job should have cluster_restarted=True.
    from google.cloud.firestore_v1.base_query import FieldFilter

    def _restarted_job():
        docs = (
            firestore_db.collection("jobs")
            .where(filter=FieldFilter("function_name", "==", "test_function"))
            .stream()
        )
        most_recent = None
        for doc in docs:
            data = doc.to_dict()
            if data.get("cluster_restarted") is True:
                if most_recent is None or data.get("started_at", 0) > most_recent.get("started_at", 0):
                    most_recent = data
        return most_recent

    job = wait_for_fixture(_restarted_job, timeout=15)
    assert job["cluster_restarted"] is True
    assert job["status"] == "CANCELED"

    # After the restart settles, a fresh rpm must succeed. Poll for a READY
    # node first so we don't race the restart.
    def _ready():
        state = main_http_client.get("/v1/cluster/state").json()
        return state["ready_nodes"] if state.get("ready_nodes") else None

    wait_for_fixture(_ready, timeout=180, message="cluster never recovered after restart")

    recover_source = "def test_function(x):\n    return x + 1\n"
    recover_result = rpm_subprocess(recover_source, [1, 2, 3], timeout_seconds=60, grow=True)
    assert recover_result["ok"], recover_result.get("traceback")
    assert sorted(recover_result["outputs"]) == [2, 3, 4]
