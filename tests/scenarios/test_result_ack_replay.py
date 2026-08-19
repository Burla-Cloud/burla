"""
Scenario: replay a result batch when the first response is lost.

This is a regression test for jobs that stalled after a node finished all
inputs but the client never received one `/results` response. The client had
no way to ask for that batch again, because node_service had already removed
it from `SELF["results_queue"]`.

The fault injection drops one non-empty `/results` response after node_service
has served it, before the client records it or can acknowledge it on the next
poll. This matches the failure mode where the HTTP response disappears after
node_service has removed results from its queue.
"""

from __future__ import annotations

import time

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


def test_transient_result_502_is_retried(rpm_subprocess, local_dev_cluster):
    result = rpm_subprocess(
        "def test_function(x):\n    return x + 1\n",
        list(range(30)),
        timeout_seconds=90,
        env_overrides={"BURLA_TEST_RESULT_502S": "3"},
        grow=False,
    )

    assert result["ok"], result.get("traceback")
    assert result["stdout"].count("BURLA_TEST_INJECTED_RESULT_502") == 3
    assert sorted(result["outputs"]) == [x + 1 for x in range(30)]


def test_persistent_result_502_stops_after_ten_seconds(
    rpm_subprocess, local_dev_cluster
):
    started = time.monotonic()
    result = rpm_subprocess(
        "def test_function(x):\n    return x\n",
        list(range(30)),
        timeout_seconds=30,
        env_overrides={"BURLA_TEST_RESULT_502S": "100"},
        grow=False,
    )
    elapsed = time.monotonic() - started

    assert not result["ok"]
    assert "after 10s of HTTP 502 responses" in result["exception_message"]
    assert 9 <= elapsed < 15
