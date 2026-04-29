"""
Scenario: replay a result batch when the first response is lost.

The fault injection drops one non-empty `/results` response after node_service
has served it, before the client records or ACKs it. This matches the failure
mode where the HTTP response disappears after node_service has removed results
from its queue.
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.slow]


def test_lost_results_response_is_replayed(rpm_subprocess, local_dev_cluster):
    source = (
        "def test_function(x):\n"
        "    return x * 10\n"
    )
    inputs = list(range(30))

    result = rpm_subprocess(
        source,
        inputs,
        timeout_seconds=90,
        env_overrides={"BURLA_TEST_DROP_FIRST_RESULT_BATCH": "1"},
        grow=True,
    )

    assert result["ok"], result.get("traceback")
    assert "BURLA_TEST_DROPPED_FIRST_RESULT_BATCH" in result["stdout"]
    assert sorted(result["outputs"]) == [x * 10 for x in inputs]
