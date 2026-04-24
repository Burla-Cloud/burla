"""
Section 18 of the test plan: POST /v1/cluster/restart and /v1/cluster/shutdown.

These mutate the cluster — run only when you're OK with the cluster being
reset. Gated on the `chaos` marker.
"""

from __future__ import annotations

import time

import pytest


pytestmark = [pytest.mark.chaos, pytest.mark.slow]


def test_restart_marks_running_jobs_cluster_restarted(
    main_http_client, local_dev_cluster, firestore_db, isolated_job_id, cleanup_job,
    wait_for_fixture,
):
    """Seed a RUNNING job, hit /restart, verify the flag gets set synchronously
    before the restart returns."""
    job_id = cleanup_job(isolated_job_id())
    firestore_db.collection("jobs").document(job_id).set({
        "function_name": "test_function",
        "n_inputs": 1,
        "status": "RUNNING",
        "started_at": time.time(),
        "fail_reason": [],
    })

    resp = main_http_client.post("/v1/cluster/restart")
    assert resp.status_code in (200, 204)

    # Immediately after the response, the flag must already be in firestore.
    def _has_flag():
        d = firestore_db.collection("jobs").document(job_id).get()
        return d.to_dict() if d.exists else None

    doc = wait_for_fixture(_has_flag, timeout=3)
    assert doc.get("cluster_restarted") is True
    assert doc.get("status") == "CANCELED"


def test_shutdown_marks_running_jobs_cluster_shutdown(
    main_http_client, local_dev_cluster, firestore_db, isolated_job_id, cleanup_job,
    wait_for_fixture,
):
    job_id = cleanup_job(isolated_job_id())
    firestore_db.collection("jobs").document(job_id).set({
        "function_name": "test_function",
        "n_inputs": 1,
        "status": "RUNNING",
        "started_at": time.time(),
        "fail_reason": [],
    })

    resp = main_http_client.post("/v1/cluster/shutdown")
    # Shutdown runs synchronously and may 500 if GCE returns errors during VM
    # teardown, but the `_mark_running_jobs_with_lifecycle_event` write must
    # have landed before the VM calls.
    assert resp.status_code in (200, 204, 500)

    def _flag():
        d = firestore_db.collection("jobs").document(job_id).get()
        return d.to_dict() if d.exists else None

    doc = wait_for_fixture(_flag, timeout=5)
    assert doc.get("cluster_shutdown") is True
    assert doc.get("status") == "CANCELED"
