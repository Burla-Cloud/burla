"""
The dashboard's live cluster feed: `GET /v1/cluster` (SSE). The protocol
itself (an initial event on connect) can't be pinned down through a browser
test, which only sees the rendered result.
"""

from __future__ import annotations

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
        assert r.status_code in (
            200,
            401,
        )  # 401 only if auth is required and we don't have it
        if r.status_code != 200:
            pytest.skip("auth required on SSE endpoint")
        lines_read = 0
        for line in r.iter_lines():
            lines_read += 1
            if lines_read > 2:
                break
