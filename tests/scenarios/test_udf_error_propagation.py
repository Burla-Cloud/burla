"""
Scenario 4: UDF error propagation end-to-end.

A UDF raises `ValueError` on a specific input. The client must receive it
with `exc.burla_input_index` set, the traceback preserved via `tblib`, a
Python 3.11+ `__notes__` entry, and a matching error log recorded by the
head (visible via the jobs HTTP API).
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.e2e


def test_udf_error_propagation(
    rpm_subprocess,
    local_dev_cluster,
    main_http_client,
    wait_for_fixture,
):
    source = (
        "def _inner(x):\n"
        "    raise ValueError(f'boom on {x}')\n"
        "def test_function(x):\n"
        "    if x == 7:\n"
        "        return _inner(x)\n"
        "    return x\n"
    )
    result = rpm_subprocess(source, list(range(20)), timeout_seconds=60, grow=True)

    assert not result["ok"], "UDF error was swallowed by rpm"
    assert result["exception_type"] == "ValueError"
    assert result["burla_input_index"] == 7
    assert "boom on 7" in result["exception_message"]

    # Remote traceback must include the user-function frame that actually raised.
    tb = result.get("traceback") or ""
    assert "_inner" in tb, f"traceback does not contain user inner frame:\n{tb}"
    # Python 3.11+ note attached for visibility.
    assert "[burla] failed on input index 7" in tb

    # Head-visible: find the matching job and check the management call/error
    # resources agree that input 7 failed.
    def _failed_call():
        jobs = main_http_client.get("/v1/management/jobs?limit=100").json()["items"]
        for job in jobs:
            if job.get("function_name") != "test_function":
                continue
            if job.get("status") == "completed":
                continue
            job_id = job["job_id"]
            calls = main_http_client.get(
                f"/v1/management/jobs/{job_id}/calls?failed_only=true"
            ).json()["items"]
            errors = main_http_client.get(
                f"/v1/management/jobs/{job_id}/errors"
            ).json()["items"]
            if any(call["input_index"] == 7 for call in calls):
                return calls, errors
        return None

    calls, errors = wait_for_fixture(
        _failed_call,
        timeout=30,
        message="no error log recorded for input_index=7",
    )
    assert any(call["input_index"] == 7 for call in calls)
    assert any(7 in group["sample_input_indexes"] for group in errors)
