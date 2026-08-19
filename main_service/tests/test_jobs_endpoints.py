"""
Section 16 of the test plan: `GET /v1/jobs/{id}` and `PATCH /v1/jobs/{id}`.
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


def test_get_job_404_when_missing(main_http_client, local_dev_cluster):
    resp = main_http_client.get(f"/v1/jobs/nonexistent-job-xyz-{int(time.time())}")
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower() or "not found" in resp.text.lower()


def test_get_job_returns_dict_after_creation(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job
):
    job_id = cleanup_job(isolated_job_id())
    _seed_running_job(main_http_client, job_id, n_inputs=3)

    resp = main_http_client.get(f"/v1/jobs/{job_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["function_name"] == "test_function"
    assert body["status"] == "RUNNING"


def test_patch_job_nonexistent_returns_204(main_http_client, local_dev_cluster):
    """Patching a job the head doesn't know maps to 204 (caller swallows it)."""
    resp = main_http_client.patch(
        f"/v1/jobs/nonexistent-{int(time.time())}",
        json={"status": "FAILED"},
    )
    assert resp.status_code in (200, 204)


def test_patch_job_fail_reason_append_accumulates(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job, get_job,
    wait_for_fixture,
):
    job_id = cleanup_job(isolated_job_id())
    # start_job initializes fail_reason to [].
    _seed_running_job(main_http_client, job_id)

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

    doc = wait_for_fixture(lambda: get_job(job_id), timeout=5)
    reasons = doc.get("fail_reason") or []
    assert "reason A" in reasons
    assert "reason B" in reasons


def test_patch_job_empty_body_is_noop(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job
):
    job_id = cleanup_job(isolated_job_id())
    _seed_running_job(main_http_client, job_id)
    resp = main_http_client.patch(f"/v1/jobs/{job_id}", json={})
    # Empty body is acceptable - handler short-circuits.
    assert resp.status_code in (200, 204)


def test_patch_job_updates_fields_directly(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job, get_job,
    wait_for_fixture,
):
    job_id = cleanup_job(isolated_job_id())
    _seed_running_job(main_http_client, job_id)
    resp = main_http_client.patch(
        f"/v1/jobs/{job_id}",
        json={"client_has_all_results": True},
    )
    assert resp.status_code in (200, 204)

    def _flag():
        doc = get_job(job_id)
        return doc.get("client_has_all_results") if doc else None

    assert wait_for_fixture(_flag, timeout=5) is True


def test_patch_job_preserves_first_terminal_status(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job, get_job
):
    job_id = cleanup_job(isolated_job_id())
    _seed_running_job(main_http_client, job_id)

    canceled = main_http_client.patch(
        f"/v1/jobs/{job_id}", json={"status": "CANCELED"}
    )
    assert canceled.status_code in (200, 204)
    late_failure = main_http_client.patch(
        f"/v1/jobs/{job_id}",
        json={"status": "FAILED", "fail_reason_append": "late client cleanup"},
    )
    assert late_failure.status_code in (200, 204)

    job = get_job(job_id)
    assert job["status"] == "CANCELED"
    assert "late client cleanup" in job["fail_reason"]
