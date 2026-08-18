"""
Node-state contracts the burla client and node_services rely on:
`GET /v1/cluster/nodes/{id}` (the client's node poll), the reserved-node
exclusion invariant, and fail-reason extraction from node logs. All state is
seeded through the same push endpoints real nodes use.
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
    resp = main_http_client.post(
        f"/v1/nodes/{instance_name}/logs:batch", json={"logs": logs}
    )
    assert resp.status_code == 200, resp.text


def test_cluster_state_ready_nodes_excludes_reserved(
    main_http_client, node_push_client, local_dev_cluster
):
    """Push a READY+reserved node state and confirm it's NOT in ready_nodes."""
    instance_name = f"burla-node-test{int(time.time())%100000}"
    _push_node_state(
        node_push_client,
        instance_name,
        {
            "status": "READY",
            "reserved_for_job": "some-other-job-xyz",
            "started_booting_at": time.time(),
        },
    )

    try:
        resp = main_http_client.get("/v1/cluster/state")
        assert resp.status_code == 200
        body = resp.json()

        names = {n["instance_name"] for n in body["ready_nodes"]}
        assert (
            instance_name not in names
        ), "reserved node should be excluded from ready_nodes"

        all_nodes = main_http_client.get("/v1/cluster/nodes").json()["nodes"]
        assert instance_name in {n["instance_name"] for n in all_nodes}
    finally:
        # A DELETED push drops the fake node from live state so it can't
        # dirty the readiness gate of later tests.
        _push_node_state(node_push_client, instance_name, {"status": "DELETED"})


def test_get_node_returns_dict_for_live_node(main_http_client, local_dev_cluster):
    state = main_http_client.get("/v1/cluster/state").json()
    assert state["ready_nodes"], "readiness gate returned no READY nodes"
    name = state["ready_nodes"][0]["instance_name"]
    resp = main_http_client.get(f"/v1/cluster/nodes/{name}")
    assert resp.status_code == 200
    assert resp.json()["instance_name"] == name


def test_get_node_404_when_not_in_cache(main_http_client, local_dev_cluster):
    """The client treats a 404 on a node it was polling as node failure, so
    absent nodes must read as 404, never as an empty 200."""
    resp = main_http_client.get(
        "/v1/cluster/nodes/burla-node-definitely-does-not-exist"
    )
    assert resp.status_code == 404


def test_get_node_fail_reason_returns_first_matching_error(
    main_http_client, node_push_client, local_dev_cluster
):
    instance_name = f"burla-node-err{int(time.time())%100000}"
    now = time.time()
    _push_node_logs(
        node_push_client,
        instance_name,
        [
            {"msg": "routine boot", "ts": now},
            {
                "msg": "Traceback (most recent call last):\n  Something went wrong",
                "ts": now + 0.1,
            },
        ],
    )
    time.sleep(1)
    resp = main_http_client.get(f"/v1/cluster/nodes/{instance_name}/fail_reason")
    assert resp.status_code == 200
    assert "Traceback" in resp.json()["reason"] or "wrong" in resp.json()["reason"]
