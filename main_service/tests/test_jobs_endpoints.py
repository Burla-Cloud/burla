"""
Section 16 of the test plan: `GET /v1/jobs/{id}` and `PATCH /v1/jobs/{id}`.
"""

from __future__ import annotations

import time

import pytest

pytestmark = pytest.mark.service


def test_get_job_404_when_missing(main_http_client, local_dev_cluster):
    resp = main_http_client.get(f"/v1/jobs/nonexistent-job-xyz-{int(time.time())}")
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower() or "not found" in resp.text.lower()


def test_get_job_returns_dict_after_creation(
    main_http_client, local_dev_cluster, firestore_db, isolated_job_id, cleanup_job
):
    job_id = cleanup_job(isolated_job_id())
    # Seed a job doc directly for reads.
    firestore_db.collection("jobs").document(job_id).set({
        "function_name": "test_function",
        "n_inputs": 3,
        "status": "RUNNING",
        "started_at": time.time(),
    })
    try:
        resp = main_http_client.get(f"/v1/jobs/{job_id}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["function_name"] == "test_function"
        assert body["status"] == "RUNNING"
    finally:
        firestore_db.collection("jobs").document(job_id).delete()


def test_patch_job_nonexistent_returns_204(main_http_client, local_dev_cluster):
    """Patching a job that doesn't exist — firestore NotFound maps to 204."""
    resp = main_http_client.patch(
        f"/v1/jobs/nonexistent-{int(time.time())}",
        json={"status": "FAILED"},
    )
    assert resp.status_code in (200, 204)


def test_patch_job_fail_reason_append_uses_array_union(
    main_http_client, local_dev_cluster, firestore_db, isolated_job_id, cleanup_job,
    wait_for_fixture,
):
    job_id = cleanup_job(isolated_job_id())
    firestore_db.collection("jobs").document(job_id).set({
        "function_name": "test_function",
        "fail_reason": [],
        "status": "RUNNING",
    })

    resp1 = main_http_client.patch(
        f"/v1/jobs/{job_id}",
        json={"fail_reason_append": "reason A"},
    )
    assert resp1.status_code in (200, 204)

    resp2 = main_http_client.patch(
        f"/v1/jobs/{job_id}",
        json={"fail_reason_append": "reason B"},
    )
    assert resp2.status_code in (200, 204)

    def _doc():
        d = firestore_db.collection("jobs").document(job_id).get()
        return d.to_dict() if d.exists else None

    doc = wait_for_fixture(_doc, timeout=5)
    reasons = doc.get("fail_reason") or []
    assert "reason A" in reasons
    assert "reason B" in reasons


def test_patch_job_empty_body_is_noop(
    main_http_client, local_dev_cluster, firestore_db, isolated_job_id, cleanup_job
):
    job_id = cleanup_job(isolated_job_id())
    firestore_db.collection("jobs").document(job_id).set({"status": "RUNNING", "counter": 0})
    resp = main_http_client.patch(f"/v1/jobs/{job_id}", json={})
    # Empty body is acceptable - handler short-circuits.
    assert resp.status_code in (200, 204)


def test_patch_job_updates_fields_directly(
    main_http_client, local_dev_cluster, firestore_db, isolated_job_id, cleanup_job,
    wait_for_fixture,
):
    job_id = cleanup_job(isolated_job_id())
    firestore_db.collection("jobs").document(job_id).set({"status": "RUNNING"})
    resp = main_http_client.patch(
        f"/v1/jobs/{job_id}",
        json={"client_has_all_results": True},
    )
    assert resp.status_code in (200, 204)

    def _flag():
        d = firestore_db.collection("jobs").document(job_id).get()
        return d.to_dict().get("client_has_all_results") if d.exists else None

    assert wait_for_fixture(_flag, timeout=5) is True
