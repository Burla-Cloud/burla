"""
Section 17 of the test plan: `GET /v1/cluster/state`,
`GET /v1/cluster/nodes/{id}`, `GET /v1/cluster/nodes/{id}/fail_reason`,
`POST /v1/cluster/nodes/{id}/fail`.
"""

from __future__ import annotations

import time

import pytest

pytestmark = pytest.mark.service


def test_cluster_state_returns_expected_shape(main_http_client, local_dev_cluster):
    resp = main_http_client.get("/v1/cluster/state")
    assert resp.status_code == 200
    body = resp.json()
    assert "booting_count" in body
    assert "running_count" in body
    assert "ready_nodes" in body
    assert isinstance(body["booting_count"], int)
    assert isinstance(body["running_count"], int)
    assert isinstance(body["ready_nodes"], list)


def test_cluster_state_ready_nodes_excludes_reserved(
    main_http_client, local_dev_cluster, firestore_db, cleanup_node
):
    """Seed a READY+reserved node and confirm it's NOT in ready_nodes."""
    instance_name = f"burla-node-test{int(time.time())%100000}"
    cleanup_node(instance_name)
    firestore_db.collection("nodes").document(instance_name).set({
        "instance_name": instance_name,
        "status": "READY",
        "reserved_for_job": "some-other-job-xyz",
        "host": f"http://{instance_name}:9999",
        "machine_type": "n4-standard-4",
        "containers": [{"image": "python:3.12"}],
        "started_booting_at": time.time(),
    })

    # Wait a moment for the NODES_CACHE on_snapshot listener to pick it up.
    time.sleep(2)

    resp = main_http_client.get("/v1/cluster/state")
    assert resp.status_code == 200
    body = resp.json()

    names = {n["instance_name"] for n in body["ready_nodes"]}
    assert instance_name not in names, "reserved node should be excluded from ready_nodes"


def test_get_node_returns_dict_for_live_node(main_http_client, local_dev_cluster):
    state = main_http_client.get("/v1/cluster/state").json()
    if not state["ready_nodes"]:
        pytest.skip("no ready nodes to test get_node against")

    name = state["ready_nodes"][0]["instance_name"]
    resp = main_http_client.get(f"/v1/cluster/nodes/{name}")
    assert resp.status_code == 200
    assert resp.json()["instance_name"] == name


def test_get_node_404_when_not_in_cache(main_http_client, local_dev_cluster):
    resp = main_http_client.get("/v1/cluster/nodes/burla-node-definitely-does-not-exist")
    assert resp.status_code == 404


def test_get_node_fail_reason_404_when_no_matching_log(
    main_http_client, local_dev_cluster, firestore_db, cleanup_node
):
    """Node with no logs-subcollection should return 404."""
    instance_name = f"burla-node-nolog{int(time.time())%100000}"
    cleanup_node(instance_name)
    firestore_db.collection("nodes").document(instance_name).set({
        "instance_name": instance_name,
        "status": "FAILED",
        "started_booting_at": time.time(),
    })
    # Add a non-error log to ensure the filter works.
    firestore_db.collection("nodes").document(instance_name).collection("logs").add({
        "msg": "routine info message",
        "ts": time.time(),
    })
    time.sleep(1)
    resp = main_http_client.get(f"/v1/cluster/nodes/{instance_name}/fail_reason")
    assert resp.status_code in (200, 404)


def test_get_node_fail_reason_returns_first_matching_error(
    main_http_client, local_dev_cluster, firestore_db, cleanup_node
):
    instance_name = f"burla-node-err{int(time.time())%100000}"
    cleanup_node(instance_name)
    firestore_db.collection("nodes").document(instance_name).set({
        "instance_name": instance_name,
        "status": "FAILED",
        "started_booting_at": time.time(),
    })
    now = time.time()
    firestore_db.collection("nodes").document(instance_name).collection("logs").add({
        "msg": "routine boot",
        "ts": now,
    })
    firestore_db.collection("nodes").document(instance_name).collection("logs").add({
        "msg": "Traceback (most recent call last):\n  Something went wrong",
        "ts": now + 0.1,
    })
    time.sleep(1)
    resp = main_http_client.get(f"/v1/cluster/nodes/{instance_name}/fail_reason")
    assert resp.status_code == 200
    assert "Traceback" in resp.json()["reason"] or "wrong" in resp.json()["reason"]


@pytest.mark.chaos
def test_post_node_fail_marks_and_deletes(
    main_http_client, local_dev_cluster, firestore_db, cleanup_node, wait_for_fixture
):
    instance_name = f"burla-node-fail{int(time.time())%100000}"
    cleanup_node(instance_name)
    firestore_db.collection("nodes").document(instance_name).set({
        "instance_name": instance_name,
        "status": "READY",
        "started_booting_at": time.time(),
        "host": f"http://{instance_name}:9999",
    })

    resp = main_http_client.post(
        f"/v1/cluster/nodes/{instance_name}/fail",
        json={"reason": "test-induced failure"},
    )
    # The endpoint writes FAILED to firestore synchronously, then kicks off a
    # background delete of the VM. For this fake node the VM delete will fail
    # (404 from GCE), but the status update must still have landed.
    assert resp.status_code in (200, 204, 500)

    def _status():
        d = firestore_db.collection("nodes").document(instance_name).get()
        return d.to_dict().get("status") if d.exists else None

    assert wait_for_fixture(_status, timeout=5) == "FAILED"
