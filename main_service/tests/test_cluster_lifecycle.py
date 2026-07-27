"""
Section 18 of the test plan: POST /v1/cluster/restart and /v1/cluster/shutdown.

These mutate the cluster, so run them only when you're OK with the cluster
being reset. Gated on the `chaos` marker.
"""

from __future__ import annotations

import time

import pytest


pytestmark = [pytest.mark.chaos, pytest.mark.slow]


def _seed_running_job(main_http_client, job_id: str) -> None:
    """Job state can no longer be seeded directly in a database, so create a
    real RUNNING job through the same endpoint the client uses."""
    # Import is deferred so this file can be collected without burla installed.
    import burla

    config = {
        "n_inputs": 1,
        "func_cpu": 1,
        "func_ram": "dynamic",
        "max_parallelism": 1,
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


def test_restart_marks_running_jobs_cluster_restarted(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job, get_job,
    wait_for_fixture,
):
    """Seed a RUNNING job, hit /restart, verify the flag gets set synchronously
    before the restart returns."""
    job_id = cleanup_job(isolated_job_id())
    _seed_running_job(main_http_client, job_id)

    resp = main_http_client.post("/v1/cluster/restart")
    assert resp.status_code in (200, 204)

    # Immediately after the response, the flag must already be visible.
    doc = wait_for_fixture(lambda: get_job(job_id), timeout=3)
    assert doc.get("cluster_restarted") is True
    assert doc.get("status") == "CANCELED"


def test_shutdown_marks_running_jobs_cluster_shutdown(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job, get_job,
    wait_for_fixture,
):
    job_id = cleanup_job(isolated_job_id())
    _seed_running_job(main_http_client, job_id)

    resp = main_http_client.post("/v1/cluster/shutdown")
    # Shutdown runs synchronously and may 500 if the cloud API returns errors
    # during VM teardown, but the `_mark_running_jobs_with_lifecycle_event`
    # write must have landed before the VM calls.
    assert resp.status_code in (200, 204, 500)

    def _flag():
        doc = get_job(job_id)
        if doc and doc.get("cluster_shutdown"):
            return doc
        return None

    doc = wait_for_fixture(_flag, timeout=5)
    assert doc.get("cluster_shutdown") is True
    assert doc.get("status") == "CANCELED"
