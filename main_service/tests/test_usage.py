"""
Usage-endpoint boundary validation. The usage UI itself (which consumes
/v1/nodes/daily_hours and /v1/nodes/month_nodes) is covered by the browser
tier in tests/dashboard/; the UI can never send a malformed month, so that
boundary is pinned here.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.service


def test_daily_hours_malformed_month_returns_400(main_http_client, local_dev_cluster):
    resp = main_http_client.get("/v1/nodes/daily_hours?month=bad-month")
    assert resp.status_code == 400
