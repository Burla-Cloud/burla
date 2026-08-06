"""
The local-dev settings override: a safety contract (small fixed machines, one
node) that exists precisely so tests and dashboards can't reconfigure a dev
cluster into something expensive. The settings pages themselves are covered
by the browser tier in tests/dashboard/.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.service


def test_post_settings_local_dev_forces_n4_standard_2(
    main_http_client, local_dev_cluster
):
    """In local-dev, POST /v1/settings forces machine_type = n4-standard-2, quantity=1."""
    current = main_http_client.get("/v1/settings")
    assert current.status_code == 200

    # Only the fields this test is about. No `gcpRegion`: posting one from the
    # wrong cloud wedges the config in a state the dashboard's region validation
    # refuses to re-save (which broke the settings browser test whenever it ran
    # after this one). `users` echoes the current list because POST reconciles
    # it against the backend, and posting [] asks it to de-authorize everyone.
    payload = {
        "containerImage": "python:3.12",
        "machineType": "n4-standard-16",  # will be overridden
        "machineQuantity": 10,  # will be overridden to 1
        "diskSize": 20,
        "inactivityTimeout": 10,
        "users": current.json()["users"],
    }
    resp = main_http_client.post("/v1/settings", json=payload)
    if resp.status_code == 401:
        pytest.skip("auth required")
    assert resp.status_code in (200, 204), f"unexpected {resp.status_code}: {resp.text}"

    verify = main_http_client.get("/v1/settings")
    assert verify.status_code == 200
    body = verify.json()
    assert body["machineType"] == "n4-standard-2"
    assert body["machineQuantity"] == 1
