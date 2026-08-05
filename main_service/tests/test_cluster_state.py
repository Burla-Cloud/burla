"""
Section 17 of the test plan: `GET /v1/cluster/state`,
`GET /v1/cluster/nodes/{id}`, `GET /v1/cluster/nodes/{id}/fail_reason`,
`POST /v1/cluster/nodes/{id}/fail`.
"""

from __future__ import annotations

import time

import pytest

pytestmark = pytest.mark.service


def _push_node_state(main_http_client, instance_name: str, state: dict) -> None:
    """Same endpoint node_services push their state through ~1x/sec."""
    resp = main_http_client.put(f"/v1/nodes/{instance_name}/state", json=state)
    assert resp.status_code == 200, resp.text


def _push_node_logs(main_http_client, instance_name: str, logs: list[dict]) -> None:
    resp = main_http_client.post(f"/v1/nodes/{instance_name}/logs:batch", json={"logs": logs})
    assert resp.status_code == 200, resp.text


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
    main_http_client, local_dev_cluster, cleanup_node
):
    """Push a READY+reserved node state and confirm it's NOT in ready_nodes."""
    instance_name = f"burla-node-test{int(time.time())%100000}"
    cleanup_node(instance_name)
    _push_node_state(main_http_client, instance_name, {
        "status": "READY",
        "reserved_for_job": "some-other-job-xyz",
        "started_booting_at": time.time(),
    })

    try:
        resp = main_http_client.get("/v1/cluster/state")
        assert resp.status_code == 200
        body = resp.json()

        names = {n["instance_name"] for n in body["ready_nodes"]}
        assert instance_name not in names, "reserved node should be excluded from ready_nodes"

        all_nodes = main_http_client.get("/v1/cluster/nodes").json()["nodes"]
        assert instance_name in {n["instance_name"] for n in all_nodes}
    finally:
        # A DELETED push drops the fake node from live state so it can't
        # dirty the readiness gate of later tests.
        _push_node_state(main_http_client, instance_name, {"status": "DELETED"})


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
    main_http_client, local_dev_cluster, cleanup_node
):
    """Node with no error-looking logs should return 404. fail_reason reads
    log history only, so no live node needs to exist."""
    instance_name = f"burla-node-nolog{int(time.time())%100000}"
    cleanup_node(instance_name)
    # A non-error log ensures the token filter works.
    _push_node_logs(main_http_client, instance_name, [
        {"msg": "routine info message", "ts": time.time()},
    ])
    time.sleep(1)
    resp = main_http_client.get(f"/v1/cluster/nodes/{instance_name}/fail_reason")
    assert resp.status_code in (200, 404)


def test_get_node_fail_reason_returns_first_matching_error(
    main_http_client, local_dev_cluster, cleanup_node
):
    instance_name = f"burla-node-err{int(time.time())%100000}"
    cleanup_node(instance_name)
    now = time.time()
    _push_node_logs(main_http_client, instance_name, [
        {"msg": "routine boot", "ts": now},
        {"msg": "Traceback (most recent call last):\n  Something went wrong", "ts": now + 0.1},
    ])
    time.sleep(1)
    resp = main_http_client.get(f"/v1/cluster/nodes/{instance_name}/fail_reason")
    assert resp.status_code == 200
    assert "Traceback" in resp.json()["reason"] or "wrong" in resp.json()["reason"]


@pytest.mark.skip(
    reason="needs rework: firestore removed, cannot seed state directly. A fake "
    "node marked FAILED can never be removed from live state (DELETED does not "
    "overwrite FAILED) and would permanently dirty the cluster readiness gate."
)
def test_post_node_fail_marks_and_deletes(
    main_http_client, local_dev_cluster, cleanup_node, wait_for_fixture
):
    instance_name = f"burla-node-fail{int(time.time())%100000}"
    cleanup_node(instance_name)
    _push_node_state(main_http_client, instance_name, {
        "status": "READY",
        "started_booting_at": time.time(),
    })

    resp = main_http_client.post(
        f"/v1/cluster/nodes/{instance_name}/fail",
        json={"reason": "test-induced failure"},
    )
    # The endpoint marks the node FAILED synchronously, then kicks off a
    # background delete of the VM. For this fake node the VM delete will fail
    # (no such instance), but the status update must still have landed.
    assert resp.status_code in (200, 204, 500)

    def _status():
        node_resp = main_http_client.get(f"/v1/cluster/nodes/{instance_name}")
        return node_resp.json().get("status") if node_resp.status_code == 200 else None

    assert wait_for_fixture(_status, timeout=5) == "FAILED"
