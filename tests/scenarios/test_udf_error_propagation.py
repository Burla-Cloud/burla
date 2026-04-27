"""
Scenario 4: UDF error propagation end-to-end.

A UDF raises `ValueError` on a specific input. The client must receive it
with `exc.burla_input_index` set, the traceback preserved via `tblib`, a
Python 3.11+ `__notes__` entry, and a matching `is_error: True` log doc in
Firestore.
"""

from __future__ import annotations

import time

import pytest

pytestmark = pytest.mark.e2e


def test_udf_error_propagation(
    rpm_subprocess,
    local_dev_cluster,
    firestore_db,
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

    # Firestore: find the matching job and check its logs subcollection has an
    # is_error=True doc tagged with input_index=7.
    from google.cloud.firestore_v1.base_query import FieldFilter

    def _error_log():
        docs = (
            firestore_db.collection("jobs")
            .where(filter=FieldFilter("function_name", "==", "test_function"))
            .stream()
        )
        for job_doc in docs:
            job = job_doc.to_dict()
            if not job or job.get("status") == "COMPLETED":
                continue
            logs = (
                firestore_db.collection("jobs")
                .document(job_doc.id)
                .collection("logs")
                .where(filter=FieldFilter("is_error", "==", True))
                .stream()
            )
            for log in logs:
                data = log.to_dict() or {}
                if data.get("input_index") == 7:
                    return data
        return None

    err_log = wait_for_fixture(
        _error_log,
        timeout=30,
        message="no is_error=True log doc for input_index=7",
    )
    assert err_log["is_error"] is True
    assert err_log["input_index"] == 7
