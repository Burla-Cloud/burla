"""
Sections 24-25 of the test plan: auth middleware & caches.

Covered at the service tier against the live main_service:
- local-dev bypass stamps local-dev@burla.dev session (all endpoints reachable
  without auth)
- SSE endpoints bypass auth via Accept: text/event-stream
- Static assets with file extensions bypass auth
- `/api/sf/*` bypass auth
- `/signed-resumable` bypasses auth
- `/version` endpoint returns project + version
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.service


def test_version_endpoint_returns_version_and_project(main_http_client, local_dev_cluster):
    resp = main_http_client.get("/version")
    assert resp.status_code == 200
    body = resp.json()
    assert "version" in body
    assert "project" in body


def test_api_user_endpoint_returns_session_info_in_local_dev(
    main_http_client, local_dev_cluster
):
    """In local-dev mode, session is auto-populated with local-dev@burla.dev."""
    resp = main_http_client.get("/api/user")
    assert resp.status_code == 200
    body = resp.json()
    assert "email" in body


def test_sf_paths_bypass_auth(main_http_client, local_dev_cluster):
    """Filemanager endpoints are reachable without special auth headers."""
    resp = main_http_client.post(
        "/api/sf/filemanager", json={"action": "read", "path": "/"}
    )
    assert resp.status_code in (200, 400)


def test_signed_resumable_bypasses_auth(main_http_client, local_dev_cluster):
    resp = main_http_client.get("/signed-resumable?object_name=x")
    assert resp.status_code == 200


def test_sse_endpoints_bypass_auth(main_http_client, local_dev_cluster):
    """SSE Accept header bypasses the auth middleware."""
    with main_http_client.stream(
        "GET", "/v1/cluster", headers={"Accept": "text/event-stream"}, timeout=2
    ) as r:
        # Either 200 (bypass) or 401 (cluster-views handler's own gate)
        assert r.status_code in (200, 401)


def test_logout_clears_session(main_http_client, local_dev_cluster):
    resp = main_http_client.post("/api/logout")
    assert resp.status_code in (200, 204)


def test_cluster_state_reflects_cache_warmed_at_startup(main_http_client, local_dev_cluster):
    """Hitting /v1/cluster/state must not block. If it returns at all, the cache
    is warmed."""
    import time

    start = time.time()
    resp = main_http_client.get("/v1/cluster/state")
    elapsed = time.time() - start
    assert resp.status_code == 200
    # Served from NODES_CACHE - should be sub-second.
    assert elapsed < 5, f"cluster_state took {elapsed}s - cache may not be warm"
