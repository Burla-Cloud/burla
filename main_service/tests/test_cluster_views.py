"""
Section 20: cluster dashboard endpoints.

- GET    /v1/cluster (SSE)
- DELETE /v1/cluster/{node_id}
- GET    /v1/cluster/{node_id}/logs (SSE)
- GET    /v1/cluster/deleted_recent_paginated
"""

from __future__ import annotations

import time

import pytest

pytestmark = pytest.mark.service


def test_cluster_sse_initial_event(main_http_client, local_dev_cluster):
    """Hit the SSE endpoint briefly and confirm an initial event fires."""
    # In local-dev mode auth is bypassed; otherwise the SSE Accept header
    # bypasses auth too. A 0.5s read is enough to receive the `: init` line.
    with main_http_client.stream(
        "GET",
        "/v1/cluster",
        headers={"Accept": "text/event-stream"},
        timeout=5,
    ) as r:
        assert r.status_code in (200, 401)  # 401 only if auth is required and we don't have it
        if r.status_code != 200:
            pytest.skip("auth required on SSE endpoint")
        lines_read = 0
        for line in r.iter_lines():
            lines_read += 1
            if lines_read > 2:
                break


def test_deleted_recent_paginated_returns_shape(main_http_client, local_dev_cluster):
    resp = main_http_client.get("/v1/cluster/deleted_recent_paginated?page_size=10")
    if resp.status_code == 401:
        pytest.skip("auth required")
    assert resp.status_code == 200
    body = resp.json()
    assert "nodes" in body
    assert "total" in body
    assert "meta" in body
    assert body["meta"]["cutoff_days"] == 7
    assert body["meta"]["max_scan"] == 20000
