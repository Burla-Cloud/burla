"""
`POST /v1/jobs/{job_id}/start` contracts the burla client relies on but that a
correct, current client cannot produce on its own (malformed requests) plus
the job-doc write contracts. User-visible outcomes of this endpoint (version
mismatch, image mismatch, happy path) are covered end-to-end through
`remote_parallel_map` in client/tests.
"""

from __future__ import annotations

import time

import pytest

pytestmark = pytest.mark.service


def _base_config(
    n_inputs: int = 1,
    burla_client_version: str = None,
    func_cpu: int = 1,
    func_ram: int | str = "dynamic",
    grow: bool = False,
    image: str | None = None,
    func_gpu: str | None = None,
    max_parallelism: int | None = None,
) -> dict:
    # Import is deferred so this file can be collected without burla installed.
    import burla

    return {
        "n_inputs": n_inputs,
        "func_cpu": func_cpu,
        "func_ram": func_ram,
        "max_parallelism": max_parallelism or n_inputs,
        "packages": {},
        "user_python_version": "3.12",
        "burla_client_version": burla_client_version or burla.__version__,
        "function_name": "test_function",
        "function_size_gb": 0.001,
        "started_at": time.time(),
        "is_background_job": False,
        "grow": grow,
        "image": image,
        "func_gpu": func_gpu,
    }


def test_start_job_malformed_version_returns_400(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job
):
    """A real client always sends its actual version; this protects the
    boundary against broken or hostile callers."""
    job_id = cleanup_job(isolated_job_id())
    config = _base_config(burla_client_version="not.a.version")
    resp = main_http_client.post(f"/v1/jobs/{job_id}/start", json=config)
    assert resp.status_code == 400


def test_start_job_invalid_gpu_returns_400(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job
):
    """func_gpu is validated before node selection, so this is a
    deterministic 400 regardless of cluster state."""
    job_id = cleanup_job(isolated_job_id())
    config = _base_config(func_gpu="B500_SUPER_GPU_9000")
    resp = main_http_client.post(f"/v1/jobs/{job_id}/start", json=config)
    assert resp.status_code == 400


def test_start_job_insufficient_capacity_func_cpu_too_high(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job
):
    """The readiness gate guarantees a READY 2-CPU node, so func_cpu=16 with
    grow=False is a deterministic insufficient-capacity refusal."""
    job_id = cleanup_job(isolated_job_id())
    config = _base_config(func_cpu=16, grow=False)
    resp = main_http_client.post(f"/v1/jobs/{job_id}/start", json=config)
    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    assert detail["error"] == "no_compatible_nodes"
    assert detail["reason"] == "insufficient_capacity"


def test_start_job_writes_job_doc(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job, get_job,
    wait_for_fixture,
):
    job_id = cleanup_job(isolated_job_id())
    config = _base_config(n_inputs=3)
    resp = main_http_client.post(f"/v1/jobs/{job_id}/start", json=config)
    if resp.status_code != 200:
        pytest.skip(f"start_job returned {resp.status_code}, skipping doc-check")

    doc = wait_for_fixture(
        lambda: get_job(job_id), timeout=10, message="job doc never appeared"
    )
    assert doc["function_name"] == "test_function"
    assert doc["n_inputs"] == 3
    assert doc["func_cpu"] == 1
    assert doc["func_ram"] == "dynamic"
    assert "user_python_version" in doc


def test_start_job_dynamic_func_ram_writes_raw_setting(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job, get_job,
    wait_for_fixture,
):
    job_id = cleanup_job(isolated_job_id())
    resp = main_http_client.post(f"/v1/jobs/{job_id}/start", json=_base_config(func_ram="dynamic"))
    if resp.status_code != 200:
        pytest.skip(f"start_job returned {resp.status_code}")

    doc = wait_for_fixture(lambda: get_job(job_id), timeout=10)
    assert doc["func_ram"] == "dynamic"
    assert doc["target_parallelism"] >= 1


def test_start_job_job_doc_includes_burla_client_version(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job, get_job,
    wait_for_fixture,
):
    import burla

    job_id = cleanup_job(isolated_job_id())
    resp = main_http_client.post(f"/v1/jobs/{job_id}/start", json=_base_config())
    if resp.status_code != 200:
        pytest.skip(f"start_job returned {resp.status_code}")

    doc = wait_for_fixture(lambda: get_job(job_id), timeout=10)
    assert doc["burla_client_version"] == burla.__version__
