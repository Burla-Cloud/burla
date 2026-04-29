"""
Scenario 1: full job lifecycle.

A single `remote_parallel_map` against a warm cluster exercises almost every
code path: client cloudpickling, start_job, node assignment, worker TCP
protocol, result draining, heartbeat, and cleanup. One test asserts the
whole chain via both client-visible output and Firestore state.
"""

from __future__ import annotations

import time

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.slow]

N_INPUTS = 100


def test_full_job_lifecycle(
    rpm_subprocess,
    local_dev_cluster,
    firestore_db,
    wait_for_fixture,
):
    source = (
        "def test_function(x):\n"
        "    print(f'running input {x}')\n"
        "    return x * x\n"
    )
    result = rpm_subprocess(source, list(range(N_INPUTS)), timeout_seconds=120, grow=True)
    assert result["ok"], result.get("traceback")

    # Client-visible: outputs match, stdout has one line per input.
    assert len(result["outputs"]) == N_INPUTS
    assert set(result["outputs"]) == {x * x for x in range(N_INPUTS)}
    # Log streaming is best-effort: MAX_PENDING_LOGS=20k caps the node-side
    # deque, and the final drain races job completion. Assert that streaming
    # works (many lines reached us) without pinning an exact count — that's
    # covered by the smaller `test_stdout_surfaced_to_local_terminal`.
    stdout_lines = [line.strip() for line in result["stdout"].splitlines()]
    running_lines = [line for line in stdout_lines if line.startswith("running input ")]
    assert running_lines, "no `running input` stdout lines came back — streaming broken"

    # Firestore-visible: find the job doc we just created via function_name.
    from google.cloud.firestore_v1.base_query import FieldFilter

    def _completed_job():
        docs = (
            firestore_db.collection("jobs")
            .where(filter=FieldFilter("function_name", "==", "test_function"))
            .stream()
        )
        # Find the most-recently completed test_function job.
        most_recent = None
        for doc in docs:
            data = doc.to_dict()
            if data.get("status") == "COMPLETED":
                if most_recent is None or data.get("started_at", 0) > most_recent[1].get("started_at", 0):
                    most_recent = (doc.id, data)
        return most_recent

    job_id, job = wait_for_fixture(_completed_job, timeout=30)
    assert job["status"] == "COMPLETED"
    assert job["n_inputs"] == N_INPUTS
    assert job["client_has_all_results"] is True
    assert job["all_inputs_uploaded"] is True

    # assigned_nodes subcollection: per-node counters sum to N_INPUTS.
    assigned = list(
        firestore_db.collection("jobs").document(job_id).collection("assigned_nodes").stream()
    )
    assert assigned, "no assigned_nodes docs written"
    total = sum(doc.to_dict().get("current_num_results", 0) for doc in assigned)
    assert total == N_INPUTS, f"assigned_nodes sum {total} != n_inputs {N_INPUTS}"
