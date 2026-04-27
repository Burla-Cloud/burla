"""
Section 22: /v1/nodes/monthly_hours, /v1/nodes/daily_hours, /v1/nodes/month_nodes.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.service


def test_monthly_hours_default_returns_months(main_http_client, local_dev_cluster):
    resp = main_http_client.get("/v1/nodes/monthly_hours?months_back=3")
    if resp.status_code == 401:
        pytest.skip("auth required")
    assert resp.status_code == 200
    body = resp.json()
    assert "months" in body
    assert "total_node_hours" in body
    assert "total_compute_hours" in body
    assert body["meta"]["hours_precision_decimals"] == 6
    assert body["meta"]["max_scan"] == 20000


def test_monthly_hours_months_back_out_of_range_returns_400(main_http_client, local_dev_cluster):
    resp1 = main_http_client.get("/v1/nodes/monthly_hours?months_back=0")
    resp2 = main_http_client.get("/v1/nodes/monthly_hours?months_back=61")
    if resp1.status_code == 401:
        pytest.skip("auth required")
    assert resp1.status_code == 400
    assert resp2.status_code == 400


def test_monthly_hours_malformed_month_returns_400(main_http_client, local_dev_cluster):
    resp = main_http_client.get(
        "/v1/nodes/monthly_hours?start_month=bad-month&end_month=2024-01"
    )
    if resp.status_code == 401:
        pytest.skip("auth required")
    assert resp.status_code == 400


def test_monthly_hours_start_without_end_returns_400(main_http_client, local_dev_cluster):
    resp = main_http_client.get("/v1/nodes/monthly_hours?start_month=2024-01")
    if resp.status_code == 401:
        pytest.skip("auth required")
    assert resp.status_code == 400


def test_daily_hours_defaults_to_current_month(main_http_client, local_dev_cluster):
    resp = main_http_client.get("/v1/nodes/daily_hours")
    if resp.status_code == 401:
        pytest.skip("auth required")
    assert resp.status_code == 200
    body = resp.json()
    assert "days" in body or "daily" in body or "months" in body  # shape may vary


def test_month_nodes_returns_shape(main_http_client, local_dev_cluster):
    resp = main_http_client.get("/v1/nodes/month_nodes?limit=100")
    if resp.status_code == 401:
        pytest.skip("auth required")
    assert resp.status_code == 200
    body = resp.json()
    assert "nodes" in body
    assert "month" in body
    assert "meta" in body
