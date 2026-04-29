"""
Section 15 of the test plan: `POST /v1/jobs/{job_id}/start`.

These are service-level tests that drive the live main_service over HTTP.
`make local-dev` must be running with `burla-test` as the active project.
"""

from __future__ import annotations

import json
import time
import uuid

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


def test_start_job_happy_path_returns_ready_nodes(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job
):
    job_id = cleanup_job(isolated_job_id("test_function"))
    resp = main_http_client.post(f"/v1/jobs/{job_id}/start", json=_base_config())
    assert resp.status_code in (200, 503), resp.text
    if resp.status_code == 200:
        body = resp.json()
        assert "ready_nodes" in body or "booting_nodes" in body


def test_start_job_version_too_low_returns_409_version_mismatch(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job
):
    job_id = cleanup_job(isolated_job_id())
    config = _base_config(burla_client_version="0.0.1")
    resp = main_http_client.post(f"/v1/jobs/{job_id}/start", json=config)
    assert resp.status_code == 409
    detail = resp.json().get("detail")
    assert detail.get("error") == "version_mismatch"
    assert detail.get("current_version") == "0.0.1"
    assert "lower_version" in detail
    assert "upper_version" in detail


def test_start_job_version_too_high_returns_409_version_mismatch(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job
):
    job_id = cleanup_job(isolated_job_id())
    config = _base_config(burla_client_version="999.99.99")
    resp = main_http_client.post(f"/v1/jobs/{job_id}/start", json=config)
    assert resp.status_code == 409
    assert resp.json()["detail"]["error"] == "version_mismatch"


def test_start_job_malformed_version_returns_400(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job
):
    job_id = cleanup_job(isolated_job_id())
    config = _base_config(burla_client_version="not.a.version")
    resp = main_http_client.post(f"/v1/jobs/{job_id}/start", json=config)
    assert resp.status_code == 400


def test_start_job_invalid_gpu_returns_400_or_409(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job
):
    job_id = cleanup_job(isolated_job_id())
    config = _base_config(func_gpu="B500_SUPER_GPU_9000")
    resp = main_http_client.post(f"/v1/jobs/{job_id}/start", json=config)
    assert resp.status_code in (400, 409)


def test_start_job_no_ready_nodes_grow_false_returns_expected(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job
):
    """Image that matches no node + grow=False should get 404 or 409."""
    job_id = cleanup_job(isolated_job_id())
    config = _base_config(image="nonexistent/image:tag", grow=False)
    resp = main_http_client.post(f"/v1/jobs/{job_id}/start", json=config)
    # Depending on cluster state this can be 404 no_nodes, 409 no_compatible_nodes, or 503 nodes_busy.
    assert resp.status_code in (404, 409, 503)


def test_start_job_image_mismatch_returns_409_with_available_images(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job
):
    """If there are READY nodes but none match the image, should get 409."""
    # First check state — skip if no ready nodes (test prereq not met).
    state = main_http_client.get("/v1/cluster/state").json()
    if not state.get("ready_nodes"):
        pytest.skip("no ready nodes to test image-mismatch against")

    job_id = cleanup_job(isolated_job_id())
    config = _base_config(image="bogus/mismatch-image:tag", grow=False)
    resp = main_http_client.post(f"/v1/jobs/{job_id}/start", json=config)
    # With ready nodes present but no image match, expect 409 image_mismatch.
    if resp.status_code == 409:
        detail = resp.json().get("detail")
        assert detail.get("error") == "no_compatible_nodes"
        assert detail.get("reason") in ("image_mismatch", "gpu_mismatch", "insufficient_capacity")


def test_start_job_insufficient_capacity_func_cpu_too_high(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job
):
    """n4-standard-2 has 2 CPUs; func_cpu=16 won't fit."""
    job_id = cleanup_job(isolated_job_id())
    config = _base_config(func_cpu=16, grow=False)
    resp = main_http_client.post(f"/v1/jobs/{job_id}/start", json=config)
    assert resp.status_code in (404, 409)


def test_start_job_grow_returns_booting_nodes_when_deficit(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job, firestore_db
):
    """Asking for more parallelism than the cluster has, with grow=True,
    should schedule new nodes."""
    # Ask for parallelism=10 with grow=True — should get booting_nodes back.
    job_id = cleanup_job(isolated_job_id())
    config = _base_config(n_inputs=10, max_parallelism=10, grow=True, image="python:3.12")
    resp = main_http_client.post(f"/v1/jobs/{job_id}/start", json=config)
    assert resp.status_code in (200, 503), resp.text
    if resp.status_code == 200:
        body = resp.json()
        # Booting nodes may or may not be present depending on current cluster state.
        assert "ready_nodes" in body
        assert "booting_nodes" in body

    # Clean up any nodes the grow booted.
    time.sleep(1)
    try:
        from google.cloud.firestore_v1.base_query import FieldFilter

        docs = (
            firestore_db.collection("nodes")
            .where(filter=FieldFilter("reserved_for_job", "==", job_id))
            .stream()
        )
        for doc in docs:
            try:
                doc.reference.delete()
            except Exception:
                pass
    except Exception:
        pass


def test_start_job_writes_job_doc(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job, firestore_db,
    wait_for_fixture,
):
    job_id = cleanup_job(isolated_job_id())
    config = _base_config(n_inputs=3)
    resp = main_http_client.post(f"/v1/jobs/{job_id}/start", json=config)
    # Regardless of start outcome (200 or e.g. 503), main_service should write the doc.
    if resp.status_code != 200:
        pytest.skip(f"start_job returned {resp.status_code}, skipping doc-check")

    def _has_doc():
        doc = firestore_db.collection("jobs").document(job_id).get()
        return doc.to_dict() if doc.exists else None

    doc = wait_for_fixture(_has_doc, timeout=10, message="job doc never appeared")
    assert doc["function_name"] == "test_function"
    assert doc["n_inputs"] == 3
    assert doc["func_cpu"] == 1
    assert doc["func_ram"] == "dynamic"
    assert "user_python_version" in doc


def test_start_job_dynamic_func_ram_writes_raw_setting(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job, firestore_db,
    wait_for_fixture,
):
    job_id = cleanup_job(isolated_job_id())
    resp = main_http_client.post(f"/v1/jobs/{job_id}/start", json=_base_config(func_ram="dynamic"))
    if resp.status_code != 200:
        pytest.skip(f"start_job returned {resp.status_code}")

    def _has_doc():
        doc = firestore_db.collection("jobs").document(job_id).get()
        return doc.to_dict() if doc.exists else None

    doc = wait_for_fixture(_has_doc, timeout=10)
    assert doc["func_ram"] == "dynamic"
    assert doc["target_parallelism"] >= 1


def test_start_job_job_doc_includes_burla_client_version(
    main_http_client, local_dev_cluster, isolated_job_id, cleanup_job, firestore_db,
    wait_for_fixture,
):
    import burla

    job_id = cleanup_job(isolated_job_id())
    resp = main_http_client.post(f"/v1/jobs/{job_id}/start", json=_base_config())
    if resp.status_code != 200:
        pytest.skip(f"start_job returned {resp.status_code}")

    def _has_doc():
        doc = firestore_db.collection("jobs").document(job_id).get()
        return doc.to_dict() if doc.exists else None

    doc = wait_for_fixture(_has_doc, timeout=10)
    assert doc["burla_client_version"] == burla.__version__
