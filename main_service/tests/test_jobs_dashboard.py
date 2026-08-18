"""
Job-endpoint contracts that need precisely seeded state: the dashboard-stop
cancellation signal the client reacts to, log-index semantics, and the 404
boundary the client's pollers depend on. Happy-path rendering of the jobs
pages is covered by the browser tier in tests/dashboard/.
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
    resp = main_http_client.post(
        f"/v1/jobs/{job_id}/logs:batch", json={"documents": documents}
    )
    assert resp.status_code == 200, resp.text


def test_management_usage_accepts_cluster_token(node_push_client, local_dev_cluster):
    response = node_push_client.get("/v1/management/usage")
    assert response.status_code == 200, response.text
    assert "days" in response.json()


def test_management_settings_exposes_and_validates_machine_regions(
    main_http_client,
    local_dev_cluster,
):
    settings = main_http_client.get("/v1/management/settings").json()
    all_regions = set(settings["options"]["regions"])
    gpu_machines = [
        machine
        for machine in settings["options"]["machine_types"]
        if machine["gpu_count"]
    ]
    assert gpu_machines
    assert all(set(machine["regions"]) <= all_regions for machine in gpu_machines)
    assert any(set(machine["regions"]) < all_regions for machine in gpu_machines)

    invalid = main_http_client.patch(
        "/v1/management/settings",
        json={"machine_type": settings["machine_type"], "region": "not-a-region"},
    )
    assert invalid.status_code == 422, invalid.text
    assert "region is not available" in invalid.json()["error"]["message"]


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


def test_stop_job_writes_job_notice(
    main_http_client,
    local_dev_cluster,
    isolated_job_id,
    cleanup_job,
    wait_for_fixture,
):
    job_id = cleanup_job(isolated_job_id())
    _seed_running_job(main_http_client, job_id)
    resp = main_http_client.post(f"/v1/jobs/{job_id}/stop")
    assert resp.status_code in (200, 204)

    def _notice():
        response = main_http_client.get(f"/v1/management/jobs/{job_id}")
        if response.status_code != 200:
            return None
        body = response.json()
        notices = body["notices"]
        if not notices:
            return None
        return body, notices[0]

    body, notice = wait_for_fixture(_notice, timeout=5)
    assert body["failed_count"] == 0
    assert "canceled by user" in notice["message"].lower()


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


def test_management_call_pages_large_running_job_with_every_sort(
    main_http_client,
    node_push_client,
    local_dev_cluster,
    isolated_job_id,
    cleanup_job,
):
    job_id = cleanup_job(isolated_job_id())
    n_inputs = 5000
    _seed_running_job(main_http_client, job_id, n_inputs=n_inputs)
    now = time.time()
    _push_job_logs(
        node_push_client,
        job_id,
        [
            {
                "logs": [{"message": "running", "timestamp": now}],
                "input_index": 7,
                "is_error": False,
                "timestamp": now,
            },
            {
                "logs": [{"message": "failed", "timestamp": now}],
                "input_index": 11,
                "is_error": True,
                "timestamp": now,
            },
        ],
    )
    metrics = node_push_client.post(
        "/v1/nodes/pagination-test/metrics:batch",
        json={
            "samples": [
                {
                    "timestamp": now,
                    "duration_sec": 1,
                    "scope": "task",
                    "job_id": job_id,
                    "input_index": 13,
                    "worker_id": "worker-0",
                    "cpu_seconds": 0.5,
                    "cpu_percent": 50,
                    "memory_bytes": 1024,
                    "memory_percent": 1,
                    "network_rx_bytes": 0,
                    "network_tx_bytes": 0,
                    "disk_read_bytes": 0,
                    "disk_write_bytes": 0,
                    "gpu_percent": None,
                    "gpu_memory_bytes": None,
                    "gpu_memory_percent": None,
                }
            ]
        },
    )
    assert metrics.status_code == 200, metrics.text
    url = f"/v1/management/jobs/{job_id}/calls"
    pending_count = n_inputs - 3

    for sort in (
        "input_index",
        "started_at",
        "ended_at",
        "duration",
        "attempts",
        "status",
        "peak_cpu",
        "peak_memory",
    ):
        for order in ("asc", "desc"):
            params = {
                "status": "pending",
                "sort": sort,
                "order": order,
                "limit": 37,
            }
            first = main_http_client.get(url, params=params)
            assert first.status_code == 200, first.text
            first_page = first.json()
            assert first_page["total_count"] == pending_count
            assert first_page["has_more"] is True
            assert len(first_page["items"]) == 37

            second = main_http_client.get(
                url,
                params={**params, "cursor": first_page["next_cursor"]},
            )
            assert second.status_code == 200, second.text
            second_page = second.json()
            assert second_page["total_count"] == pending_count
            assert len(second_page["items"]) == 37
            assert {call["input_index"] for call in first_page["items"]}.isdisjoint(
                call["input_index"] for call in second_page["items"]
            )

    indexed = main_http_client.get(
        url,
        params={"input_index": n_inputs - 1, "status": "pending", "limit": 1},
    )
    assert indexed.status_code == 200, indexed.text
    assert indexed.json()["items"][0]["input_index"] == n_inputs - 1

    filtered_expectations = (
        ({"failed_only": "true", "status": "failed"}, [11]),
        ({"logs_only": "true"}, [7, 11]),
        ({"has_metrics": "true", "status": "running"}, [13]),
        ({"status": "running"}, [7, 13]),
    )
    for filters, expected_indices in filtered_expectations:
        response = main_http_client.get(url, params={**filters, "limit": 10})
        assert response.status_code == 200, response.text
        assert [
            item["input_index"] for item in response.json()["items"]
        ] == expected_indices
        assert response.json()["total_count"] == len(expected_indices)

    stopped = main_http_client.post(f"/v1/jobs/{job_id}/stop")
    assert stopped.status_code in (200, 204), stopped.text
    canceled = main_http_client.get(url, params={"status": "canceled", "limit": 10})
    assert canceled.status_code == 200, canceled.text
    assert [item["input_index"] for item in canceled.json()["items"]] == [7, 13]
    not_run = main_http_client.get(url, params={"status": "not_run", "limit": 10})
    assert not_run.status_code == 200, not_run.text
    assert not_run.json()["total_count"] == pending_count
