"""
Job-endpoint contracts that need precisely seeded state: the dashboard-stop
cancellation signal and event, per-call logs, and the 404 boundary the
client's pollers depend on. Happy-path rendering of the jobs pages is covered
by the browser tier in tests/dashboard/.
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
    assert resp.status_code == 200, resp.text


def _push_job_logs(main_http_client, job_id: str, documents: list[dict]) -> None:
    """Same endpoint JobLogWriter on the nodes uses to persist UDF logs."""
    resp = main_http_client.post(
        f"/v1/jobs/{job_id}/logs:batch", json={"documents": documents}
    )
    assert resp.status_code == 200, resp.text


def test_stop_job_writes_dashboard_canceled(
    main_http_client,
    local_dev_cluster,
    isolated_job_id,
    cleanup_job,
    get_job,
    wait_for_fixture,
):
    job_id = cleanup_job(isolated_job_id())
    _seed_running_job(main_http_client, job_id)
    resp = main_http_client.post(f"/v1/jobs/{job_id}/stop")
    assert resp.status_code in (200, 204)

    doc = wait_for_fixture(lambda: get_job(job_id), timeout=5)
    assert doc["dashboard_canceled"] is True
    assert doc["status"] == "CANCELED"


def test_stop_job_writes_event(
    main_http_client,
    local_dev_cluster,
    isolated_job_id,
    cleanup_job,
):
    job_id = cleanup_job(isolated_job_id())
    _seed_running_job(main_http_client, job_id)
    resp = main_http_client.post(f"/v1/jobs/{job_id}/stop")
    assert resp.status_code in (200, 204)

    events_resp = main_http_client.get(f"/v1/jobs/{job_id}/events")
    assert events_resp.status_code == 200
    messages = [event["message"] for event in events_resp.json()["events"]]
    assert any(message.startswith("Job canceled by user:") for message in messages)


def test_result_stats_404_when_missing(main_http_client, local_dev_cluster):
    resp = main_http_client.get(f"/v1/jobs/nonexistent-{int(time.time())}/result-stats")
    assert resp.status_code == 404


def test_job_logs_returns_logs_for_index(
    main_http_client,
    node_push_client,
    local_dev_cluster,
    isolated_job_id,
    cleanup_job,
):
    job_id = cleanup_job(isolated_job_id())
    _push_job_logs(
        node_push_client,
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
