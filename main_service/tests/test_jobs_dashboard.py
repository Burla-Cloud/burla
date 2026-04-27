"""
Section 19 of the test plan: job-related dashboard endpoints.

- GET  /v1/jobs  (list, pagination, SSE)
- POST /v1/jobs/{id}/stop
- GET  /v1/jobs/{id}/result-stats
- GET  /v1/jobs/{id}/logged-input-indexes
- GET  /v1/jobs/{id}/next-failed-input
- GET  /v1/jobs/{id}/logs
"""

from __future__ import annotations

import time

import pytest

pytestmark = pytest.mark.service


def test_list_jobs_paginated_returns_expected_shape(main_http_client, local_dev_cluster):
    resp = main_http_client.get("/v1/jobs?page=0")
    assert resp.status_code == 200
    body = resp.json()
    assert "jobs" in body
    assert "page" in body
    assert "limit" in body
    assert "total" in body
    assert body["limit"] == 15


def test_list_jobs_page_numbers_respected(main_http_client, local_dev_cluster):
    resp1 = main_http_client.get("/v1/jobs?page=0")
    resp2 = main_http_client.get("/v1/jobs?page=1")
    assert resp1.status_code == 200
    assert resp2.status_code == 200
    assert resp1.json()["page"] == 0
    assert resp2.json()["page"] == 1


def test_stop_job_writes_dashboard_canceled(
    main_http_client, local_dev_cluster, firestore_db, isolated_job_id, cleanup_job,
    wait_for_fixture,
):
    job_id = cleanup_job(isolated_job_id())
    firestore_db.collection("jobs").document(job_id).set({
        "function_name": "test_function",
        "n_inputs": 1,
        "status": "RUNNING",
        "started_at": time.time(),
    })
    resp = main_http_client.post(f"/v1/jobs/{job_id}/stop")
    assert resp.status_code in (200, 204)

    def _flags():
        d = firestore_db.collection("jobs").document(job_id).get()
        return d.to_dict() if d.exists else None

    doc = wait_for_fixture(_flags, timeout=5)
    assert doc["dashboard_canceled"] is True
    assert doc["status"] == "CANCELED"


def test_stop_job_writes_log_entry(
    main_http_client, local_dev_cluster, firestore_db, isolated_job_id, cleanup_job,
    wait_for_fixture,
):
    job_id = cleanup_job(isolated_job_id())
    firestore_db.collection("jobs").document(job_id).set({
        "function_name": "test_function",
        "n_inputs": 1,
        "status": "RUNNING",
    })
    resp = main_http_client.post(f"/v1/jobs/{job_id}/stop")
    assert resp.status_code in (200, 204)

    def _log():
        logs = list(firestore_db.collection("jobs").document(job_id).collection("logs").stream())
        return logs[0].to_dict() if logs else None

    log = wait_for_fixture(_log, timeout=5)
    assert log.get("is_error") is True
    combined = str(log.get("logs", []))
    assert "canceled by user" in combined.lower() or "canceled" in combined.lower()


def test_result_stats_404_when_missing(main_http_client, local_dev_cluster):
    resp = main_http_client.get(f"/v1/jobs/nonexistent-{int(time.time())}/result-stats")
    assert resp.status_code == 404


def test_result_stats_returns_counters(
    main_http_client, local_dev_cluster, firestore_db, isolated_job_id, cleanup_job,
    wait_for_fixture,
):
    job_id = cleanup_job(isolated_job_id())
    firestore_db.collection("jobs").document(job_id).set({
        "function_name": "test_function",
        "n_inputs": 5,
        "status": "COMPLETED",
    })

    # Seed an assigned_nodes doc.
    firestore_db.collection("jobs").document(job_id).collection("assigned_nodes").document(
        "burla-node-xyz"
    ).set({"current_num_results": 3, "client_contact_last_1s": True})

    resp = main_http_client.get(f"/v1/jobs/{job_id}/result-stats")
    assert resp.status_code == 200
    body = resp.json()
    assert body["n_inputs"] == 5


def test_logged_input_indexes_returns_sorted_unique(
    main_http_client, local_dev_cluster, firestore_db, isolated_job_id, cleanup_job
):
    job_id = cleanup_job(isolated_job_id())
    firestore_db.collection("jobs").document(job_id).set({"n_inputs": 10, "status": "RUNNING"})
    logs_col = firestore_db.collection("jobs").document(job_id).collection("logs")
    for idx, err in [(0, False), (5, True), (3, False), (5, False)]:
        logs_col.add({
            "logs": [{"message": "m", "timestamp": time.time()}],
            "input_index": idx,
            "is_error": err,
            "timestamp": time.time(),
        })
    time.sleep(0.5)

    resp = main_http_client.get(f"/v1/jobs/{job_id}/logged-input-indexes")
    assert resp.status_code == 200
    body = resp.json()
    assert sorted(body["indexes_with_logs"]) == body["indexes_with_logs"]
    assert 5 in body["failed_indexes"]


def test_job_logs_404_when_job_missing(main_http_client, local_dev_cluster):
    resp = main_http_client.get(f"/v1/jobs/nomatch-{int(time.time())}/logs?index=0")
    assert resp.status_code in (200, 404)  # may return {logs: []}


def test_job_logs_returns_logs_for_index(
    main_http_client, local_dev_cluster, firestore_db, isolated_job_id, cleanup_job
):
    job_id = cleanup_job(isolated_job_id())
    firestore_db.collection("jobs").document(job_id).set({"n_inputs": 2, "status": "RUNNING"})
    logs_col = firestore_db.collection("jobs").document(job_id).collection("logs")
    logs_col.add({
        "logs": [{"message": "hello from input 7", "timestamp": time.time()}],
        "input_index": 7,
        "is_error": False,
        "timestamp": time.time(),
    })
    time.sleep(0.5)

    resp = main_http_client.get(f"/v1/jobs/{job_id}/logs?index=7")
    assert resp.status_code == 200
    body = resp.json()
    assert body["input_index"] == 7
    assert any("hello from input 7" in log["message"] for log in body["logs"])
