"""
Section 14 of the test plan: nested `remote_parallel_map` inside a UDF.

Covers:
- the existing nested call works end-to-end
- `_local_host_from` rewrites `http://node_xxx:port` to `http://localhost:port`
  when _not_ on the Docker network, and preserves it when on the Docker network
"""

from __future__ import annotations

import pytest


# --------------------------------------- _local_host_from (unit)


@pytest.mark.unit
def test_local_host_from_preserves_non_node_hosts():
    from burla._cluster_client import _local_host_from

    assert _local_host_from("http://10.0.0.5:8080") == "http://10.0.0.5:8080"
    assert _local_host_from("https://some.host.com") == "https://some.host.com"


@pytest.mark.unit
def test_local_host_from_rewrites_node_host_when_off_cluster_network(monkeypatch):
    from burla import _cluster_client

    monkeypatch.setattr(_cluster_client, "_on_local_cluster_network", lambda: False)
    got = _cluster_client._local_host_from("http://node_abc123:8081")
    assert got == "http://localhost:8081"


@pytest.mark.unit
def test_local_host_from_preserves_node_host_when_on_cluster_network(monkeypatch):
    from burla import _cluster_client

    monkeypatch.setattr(_cluster_client, "_on_local_cluster_network", lambda: True)
    got = _cluster_client._local_host_from("http://node_abc123:8081")
    assert got == "http://node_abc123:8081"


@pytest.mark.unit
def test_on_local_cluster_network_detects_main_service_substring(monkeypatch):
    import burla
    from burla import _cluster_client

    # _on_local_cluster_network lazy-imports `get_cluster_dashboard_url` from
    # burla each call, so patch the attribute on the `burla` module itself.
    monkeypatch.setattr(burla, "get_cluster_dashboard_url", lambda: "http://main_service:5001")
    assert _cluster_client._on_local_cluster_network() is True


@pytest.mark.unit
def test_on_local_cluster_network_false_for_localhost(monkeypatch):
    import burla
    from burla import _cluster_client

    monkeypatch.setattr(burla, "get_cluster_dashboard_url", lambda: "http://localhost:5001")
    assert _cluster_client._on_local_cluster_network() is False


# --------------------------------------- nested RPM happy path (e2e)


@pytest.mark.e2e
@pytest.mark.slow
def test_nested_rpm_happy_path(rpm_subprocess, local_dev_cluster):
    source = (
        "def test_function(x):\n"
        "    from burla import remote_parallel_map\n"
        "    return remote_parallel_map(lambda n: n + 100, [x], spinner=False)[0]\n"
    )
    result = rpm_subprocess(source, [1], timeout_seconds=120)
    assert result["ok"], result.get("traceback")
    assert result["outputs"] == [101]
