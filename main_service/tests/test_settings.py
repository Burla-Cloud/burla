"""
The local-dev settings override: a safety contract (small fixed machines, one
node) that exists precisely so tests and dashboards can't reconfigure a dev
cluster into something expensive. The settings pages themselves are covered
by the browser tier in tests/dashboard/.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.service


# Must match the mapping local-dev pins in main_service/__init__.py and
# endpoints/settings.py.
LOCAL_DEV_MACHINE_TYPES = {
    "gcp": "n4-standard-2",
    "aws": "m7i.large",
    "azure": "Standard_D2s_v5",
}


def test_post_settings_local_dev_forces_small_machine_and_one_node(
    main_http_client, local_dev_cluster
):
    """In local-dev, POST /v1/settings forces this cloud's smallest machine
    type and quantity=1, whatever was submitted."""
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
    assert body["machineType"] == LOCAL_DEV_MACHINE_TYPES[body["cloudProvider"]]
    assert body["machineQuantity"] == 1
