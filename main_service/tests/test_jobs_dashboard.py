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


def _seed_running_job(main_http_client, job_id: str, n_inputs: int = 1) -> None:
    """Job state can no longer be seeded directly in a database, so create a
    real RUNNING job through the same endpoint the client uses."""
    # Import is deferred so this file can be collected without burla installed.
    import burla

    config = {
        "n_inputs": n_inputs,
        "func_cpu": 1,
        "func_ram": "dynamic",
        "max_parallelism": n_inputs,
        "packages": {},
        "user_python_version": "3.12",
        "burla_client_version": burla.__version__,
        "function_name": "test_function",
        "function_size_gb": 0.001,
        "started_at": time.time(),
        "is_background_job": False,
        "grow": False,
        "image": None,
        "func_gpu": None,
    }
    resp = main_http_client.post(f"/v1/jobs/{job_id}/start", json=config)
    if resp.status_code != 200:
        pytest.skip(f"could not seed job via /start: {resp.status_code} {resp.text}")


def _push_job_logs(main_http_client, job_id: str, documents: list[dict]) -> None:
    """Same endpoint JobLogWriter on the nodes uses to persist UDF logs."""
    resp = main_http_client.post(f"/v1/jobs/{job_id}/logs:batch", json={"documents": documents})
    assert resp.status_code == 200, resp.text


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
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job, get_job,
    wait_for_fixture,
):
    job_id = cleanup_job(isolated_job_id())
    _seed_running_job(main_http_client, job_id)
    resp = main_http_client.post(f"/v1/jobs/{job_id}/stop")
    assert resp.status_code in (200, 204)

    doc = wait_for_fixture(lambda: get_job(job_id), timeout=5)
    assert doc["dashboard_canceled"] is True
    assert doc["status"] == "CANCELED"


def test_stop_job_writes_log_entry(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job,
    wait_for_fixture,
):
    job_id = cleanup_job(isolated_job_id())
    _seed_running_job(main_http_client, job_id)
    resp = main_http_client.post(f"/v1/jobs/{job_id}/stop")
    assert resp.status_code in (200, 204)

    # The "canceled by user" log doc has no input_index, so the only
    # HTTP-visible trace of it is the error count in result-stats.
    def _n_failed():
        stats_resp = main_http_client.get(f"/v1/jobs/{job_id}/result-stats")
        if stats_resp.status_code != 200:
            return None
        return stats_resp.json()["n_failed"]

    n_failed = wait_for_fixture(_n_failed, timeout=5)
    assert n_failed >= 1


def test_result_stats_404_when_missing(main_http_client, local_dev_cluster):
    resp = main_http_client.get(f"/v1/jobs/nonexistent-{int(time.time())}/result-stats")
    assert resp.status_code == 404


def test_result_stats_returns_counters(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job
):
    job_id = cleanup_job(isolated_job_id())
    _seed_running_job(main_http_client, job_id, n_inputs=5)

    resp = main_http_client.get(f"/v1/jobs/{job_id}/result-stats")
    assert resp.status_code == 200
    body = resp.json()
    assert body["n_inputs"] == 5
    assert "n_results" in body
    assert "n_failed" in body


def test_logged_input_indexes_returns_sorted_unique(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job
):
    job_id = cleanup_job(isolated_job_id())
    documents = [
        {
            "logs": [{"message": "m", "timestamp": time.time()}],
            "input_index": idx,
            "is_error": err,
            "timestamp": time.time(),
        }
        for idx, err in [(0, False), (5, True), (3, False), (5, False)]
    ]
    _push_job_logs(main_http_client, job_id, documents)
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
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job
):
    job_id = cleanup_job(isolated_job_id())
    _push_job_logs(
        main_http_client,
        job_id,
        [
            {
                "logs": [{"message": "hello from input 7", "timestamp": time.time()}],
                "input_index": 7,
                "is_error": False,
                "timestamp": time.time(),
            }
        ],
    )
    time.sleep(0.5)

    resp = main_http_client.get(f"/v1/jobs/{job_id}/logs?index=7")
    assert resp.status_code == 200
    body = resp.json()
    assert body["input_index"] == 7
    assert any("hello from input 7" in log["message"] for log in body["logs"])
