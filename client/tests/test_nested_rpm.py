"""Nested `remote_parallel_map` inside a UDF."""

import pytest


pytestmark = [pytest.mark.e2e, pytest.mark.slow, pytest.mark.timeout(300)]


def test_nested_rpm_happy_path(rpm_subprocess, cluster_with_n_nodes):
    cluster_with_n_nodes(2)
    source = (
        "from burla import remote_parallel_map\n"
        "def test_function(x):\n"
        "    return remote_parallel_map(lambda n: n + 100, [x], spinner=False)[0]\n"
    )
    result = rpm_subprocess(source, [1], timeout_seconds=120)
    assert result["ok"], result.get("traceback")
    assert result["outputs"] == [101]
