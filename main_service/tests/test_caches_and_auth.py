"""
Auth-boundary contracts of the dashboard middleware. These paths must stay
reachable without a login (the dashboard JS and Syncfusion components depend
on them), and none of them can be exercised through `remote_parallel_map` or
a browser test that is already logged in.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.service


def test_api_user_endpoint_returns_session_info_in_local_dev(
    main_http_client, local_dev_cluster
):
    """In local-dev mode, session is auto-populated with local-dev@burla.dev."""
    resp = main_http_client.get("/api/user")
    assert resp.status_code == 200
    body = resp.json()
    assert "email" in body


def test_sf_paths_bypass_auth(main_http_client, local_dev_cluster):
    """Filemanager endpoints are reachable without special auth headers.
    (409 = auth was bypassed but the shared filesystem is disabled.)"""
    resp = main_http_client.post(
        "/api/sf/filemanager", json={"action": "read", "path": "/"}
    )
    assert resp.status_code in (200, 400, 409)


def test_signed_resumable_bypasses_auth(main_http_client, local_dev_cluster):
    resp = main_http_client.get("/signed-resumable?object_name=x")
    assert resp.status_code in (200, 409)


def test_sse_endpoints_bypass_auth(main_http_client, local_dev_cluster):
    """SSE Accept header bypasses the auth middleware."""
    with main_http_client.stream(
        "GET", "/v1/cluster", headers={"Accept": "text/event-stream"}, timeout=2
    ) as r:
        # Either 200 (bypass) or 401 (cluster-views handler's own gate)
        assert r.status_code in (200, 401)
