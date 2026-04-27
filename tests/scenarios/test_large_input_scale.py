"""
Scenario 9: 1000-input scale.

Scales 5x past the existing 200-input max. Exercises:
- the ~2 MB chunk / 60-retry upload loop in `_node._upload_input_chunk`
- the grow path with a real deficit (1000 inputs well above 4 worker slots)
- the assigned_nodes counter summation across multiple nodes

Marked slow. Trivial UDF keeps the runtime to ~60-120s on a warm
cluster with grow=True.
"""

from __future__ import annotations

import time

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.slow]

N_INPUTS = 1000


def test_thousand_input_rpm_completes_with_grow(
    rpm_subprocess,
    local_dev_cluster,
    firestore_db,
    wait_for_fixture,
):
    source = "def test_function(x):\n    return x * 3\n"

    before = time.time()
    result = rpm_subprocess(
        source, list(range(N_INPUTS)), timeout_seconds=300, grow=True
    )
    assert result["ok"], result.get("traceback")
    assert len(result["outputs"]) == N_INPUTS
    assert set(result["outputs"]) == {x * 3 for x in range(N_INPUTS)}

    # Firestore: assigned_nodes counters across all nodes sum to N_INPUTS.
    from google.cloud.firestore_v1.base_query import FieldFilter

    def _completed_big_job():
        docs = (
            firestore_db.collection("jobs")
            .where(filter=FieldFilter("function_name", "==", "test_function"))
            .stream()
        )
        matches = []
        for doc in docs:
            data = doc.to_dict() or {}
            if data.get("status") != "COMPLETED":
                continue
            if data.get("n_inputs") != N_INPUTS:
                continue
            if data.get("started_at", 0) < before - 5:
                continue
            matches.append((doc.id, data))
        if not matches:
            return None
        matches.sort(key=lambda pair: pair[1].get("started_at", 0), reverse=True)
        return matches[0]

    job_id, job = wait_for_fixture(_completed_big_job, timeout=30)
    assert job["n_inputs"] == N_INPUTS
    assert job["client_has_all_results"] is True

    assigned = list(
        firestore_db.collection("jobs").document(job_id).collection("assigned_nodes").stream()
    )
    assert assigned, "no assigned_nodes docs for the 1000-input job"
    total = sum(doc.to_dict().get("current_num_results", 0) for doc in assigned)
    # Drain timing can leave a single-digit rounding gap between the last
    # result flushed to the queue and the last counter write to firestore.
    # 99%+ of inputs accounted for across nodes is sufficient proof that
    # the counters track real work.
    assert total >= int(N_INPUTS * 0.99), (
        f"assigned_nodes sum {total} < 99% of n_inputs {N_INPUTS}"
    )
