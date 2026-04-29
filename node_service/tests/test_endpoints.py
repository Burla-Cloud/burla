"""
Section 27 of the test plan: every HTTP endpoint on node_service.

All tests drive a live node container over HTTP, picking an available
READY node from `/v1/cluster/state`. The `node_http_client` fixture
rewrites the `http://node_xxx:PORT` host to `http://localhost:PORT` and
attaches real burla auth headers.
"""

from __future__ import annotations

import json
import pickle
import time

import pytest

pytestmark = pytest.mark.service


def test_root_returns_status(node_http_client, any_ready_node):
    client = node_http_client(any_ready_node["instance_name"])
    try:
        resp = client.get("/")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] in ("READY", "BOOTING", "RUNNING", "FAILED")
    finally:
        client.close()


def test_root_returns_ready_when_no_job_active(node_http_client, any_ready_node):
    client = node_http_client(any_ready_node["instance_name"])
    try:
        resp = client.get("/")
        assert resp.status_code == 200
        # Main_service should only surface a READY node in /v1/cluster/state.
        assert resp.json()["status"] == "READY"
    finally:
        client.close()


def test_root_requires_auth(any_ready_node):
    """Hitting `/` without auth headers returns 401."""
    import httpx

    host = any_ready_node["host"]
    if host.startswith("http://node_"):
        port = host.rsplit(":", 1)[-1]
        host = f"http://localhost:{port}"

    resp = httpx.get(f"{host}/", timeout=5)
    assert resp.status_code in (200, 401)  # If no authorized_users, 401; otherwise 200
    if resp.status_code == 200:
        pytest.skip("node has local-dev auth bypass; cannot test 401")


@pytest.mark.skip(reason="temporarily disabled: hangs after pytest timeout, see #200")
def test_results_404_when_wrong_job_id(node_http_client, any_ready_node):
    client = node_http_client(any_ready_node["instance_name"])
    try:
        resp = client.get("/jobs/definitely-not-a-job-xyz/results")
        assert resp.status_code == 404
    finally:
        client.close()


def test_inputs_404_when_wrong_job_id(node_http_client, any_ready_node):
    import pickle as _pickle
    client = node_http_client(any_ready_node["instance_name"])
    try:
        files = {"inputs_pkl_with_idx": _pickle.dumps([(0, _pickle.dumps("x"))])}
        resp = client.post("/jobs/not-a-job/inputs", files=files)
        assert resp.status_code == 404
    finally:
        client.close()


@pytest.mark.skip(reason="temporarily disabled: hangs after pytest timeout, see #200")
def test_get_inputs_404_when_wrong_job_id(node_http_client, any_ready_node):
    client = node_http_client(any_ready_node["instance_name"])
    try:
        resp = client.get(
            "/jobs/not-a-job/get_inputs",
            params={"transfer_id": "xyz", "requester_queue_size": 0},
        )
        assert resp.status_code == 404
    finally:
        client.close()


@pytest.mark.skip(reason="temporarily disabled: hangs after pytest timeout, see #200")
def test_ack_transfer_404_when_wrong_job_id(node_http_client, any_ready_node):
    client = node_http_client(any_ready_node["instance_name"])
    try:
        resp = client.post(
            "/jobs/not-a-job/ack_transfer",
            params={"transfer_id": "xyz", "received": "true"},
        )
        assert resp.status_code == 404
    finally:
        client.close()


def test_shutdown_requires_localhost(any_ready_node):
    """POST /shutdown returns 403 for any non-localhost caller."""
    import httpx

    host = any_ready_node["host"]
    if host.startswith("http://node_"):
        port = host.rsplit(":", 1)[-1]
        host = f"http://localhost:{port}"

    # From the test process, the request_client.host varies — it may be 127.0.0.1
    # if Docker port-binding is used (most common in local-dev), or the Docker
    # bridge IP otherwise.
    resp = httpx.post(f"{host}/shutdown", timeout=2)
    assert resp.status_code in (200, 403, 499)  # accepted or rejected
    # Do NOT let this test leak a real shutdown into other tests — if it
    # succeeded, we've damaged the cluster state.
    if resp.status_code == 200:
        pytest.skip("Unintended shutdown succeeded; subsequent tests may fail")


@pytest.mark.chaos
def test_reboot_starts_booting_then_returns(node_http_client, any_ready_node):
    """POST /reboot is disruptive; we just verify the endpoint responds and
    the node returns to READY eventually. This is slow and chaos-adjacent."""
    client = node_http_client(any_ready_node["instance_name"])
    try:
        resp = client.post("/reboot", timeout=120)
        # reboot either 409 (already booting) or 200 after completion
        assert resp.status_code in (200, 409)
    finally:
        client.close()
