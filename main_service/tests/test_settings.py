"""
Section 21: GET /v1/settings, POST /v1/settings.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.service


def test_get_settings_returns_shape(main_http_client, local_dev_cluster):
    resp = main_http_client.get("/v1/settings")
    if resp.status_code == 401:
        pytest.skip("auth required")
    assert resp.status_code == 200
    body = resp.json()
    # Fields from settings.py endpoint contract.
    for key in (
        "containerImage",
        "machineType",
        "gcpRegion",
        "machineQuantity",
        "diskSize",
        "inactivityTimeout",
        "burlaVersion",
        "googleCloudProjectId",
    ):
        assert key in body


@pytest.mark.chaos
def test_post_settings_local_dev_forces_n4_standard_2(
    main_http_client, local_dev_cluster
):
    """In local-dev, POST /v1/settings forces machine_type = n4-standard-2, quantity=1."""
    payload = {
        "containerImage": "python:3.12",
        "machineType": "n4-standard-16",  # will be overridden
        "gcpRegion": "us-central1",
        "machineQuantity": 10,  # will be overridden to 1
        "diskSize": 20,
        "inactivityTimeout": 10,
        "users": [],
    }
    resp = main_http_client.post("/v1/settings", json=payload)
    if resp.status_code == 401:
        pytest.skip("auth required")
    # POST /v1/settings also reconciles authorized users with backend.burla.dev.
    # That backend call may 500 depending on cluster metadata; in that case the
    # firestore doc still got updated, so we verify via GET.
    if resp.status_code not in (200, 204, 500):
        pytest.fail(f"unexpected status {resp.status_code}: {resp.text}")

    verify = main_http_client.get("/v1/settings")
    assert verify.status_code == 200
    body = verify.json()
    assert body["machineType"] == "n4-standard-2"
    assert body["machineQuantity"] == 1
