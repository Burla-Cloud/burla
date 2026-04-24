"""
Scenario 5: detach / background job completes after client disconnect.

Submits a slow-ish job with `detach=True`, verifies the client returns
cleanly after uploading all inputs (stdout: "Done uploading inputs!"),
then polls Firestore until the job transitions to COMPLETED and the
`assigned_nodes` counters sum to n_inputs. This exercises the
`is_background_job` path in job_watcher where client-disconnect does NOT
mark the job FAILED as long as all_inputs_uploaded is True.
"""

from __future__ import annotations

import time

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.slow]


def test_detach_and_complete_async(
    rpm_subprocess,
    local_dev_cluster,
    firestore_db,
    wait_for_fixture,
):
    # UDF with a small sleep so the job takes a few seconds — long enough
    # for detach semantics to be meaningful (inputs upload well before
    # work is done, client exits while work continues).
    source = (
        "import time\n"
        "def test_function(x):\n"
        "    time.sleep(0.5)\n"
        "    return x + 1000\n"
    )
    inputs = list(range(8))

    before_start = time.time()
    result = rpm_subprocess(source, inputs, timeout_seconds=90, grow=True, detach=True)

    # detach mode: rpm returns None (not a list) once inputs are uploaded.
    assert result["ok"], result.get("traceback")
    # stdout must contain the documented detach marker.
    combined_out = (result.get("stdout") or "") + (result.get("stderr") or "")
    assert "Done uploading inputs" in combined_out, (
        f"detach mode should print 'Done uploading inputs!' once all inputs are up;\n"
        f"stdout was:\n{combined_out[:500]}"
    )

    # The job should be in firestore as is_background_job=True and still
    # running at the moment the client exited. Wait for it to finish.
    from google.cloud.firestore_v1.base_query import FieldFilter

    def _bg_job():
        docs = (
            firestore_db.collection("jobs")
            .where(filter=FieldFilter("function_name", "==", "test_function"))
            .stream()
        )
        candidates = []
        for doc in docs:
            data = doc.to_dict() or {}
            if not data.get("is_background_job"):
                continue
            if data.get("started_at", 0) < before_start - 5:
                continue
            candidates.append((doc.id, data))
        if not candidates:
            return None
        # Most-recent started_at wins.
        candidates.sort(key=lambda pair: pair[1].get("started_at", 0), reverse=True)
        return candidates[0]

    job_id, initial_job = wait_for_fixture(_bg_job, timeout=30)
    assert initial_job["is_background_job"] is True
    assert initial_job["all_inputs_uploaded"] is True
    assert initial_job["n_inputs"] == len(inputs)

    # Poll until the job reaches a terminal state.
    def _terminal():
        doc = firestore_db.collection("jobs").document(job_id).get()
        data = doc.to_dict() if doc.exists else None
        if data and data.get("status") in {"COMPLETED", "FAILED", "CANCELED"}:
            return data
        return None

    final = wait_for_fixture(
        _terminal,
        timeout=120,
        message=f"background job {job_id} never reached a terminal state",
    )
    assert final["status"] == "COMPLETED", (
        f"background job ended with status={final['status']} fail_reason={final.get('fail_reason')}"
    )

    # assigned_nodes counters must account for all inputs.
    assigned = list(
        firestore_db.collection("jobs").document(job_id).collection("assigned_nodes").stream()
    )
    assert assigned, "no assigned_nodes docs written"
    total = sum(doc.to_dict().get("current_num_results", 0) for doc in assigned)
    assert total == len(inputs), f"assigned_nodes sum {total} != n_inputs {len(inputs)}"
