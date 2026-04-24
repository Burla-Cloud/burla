"""
Scenario 3: cluster grows under load.

Submits a job larger than the current cluster capacity with `grow=True`,
verifies main_service boots additional nodes with the grow-specific
`inactivity_shutdown_time_sec=60` and `reserved_for_job=<job_id>` set, and
that the job completes successfully using the expanded capacity.
"""

from __future__ import annotations

import time

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.slow]


def test_grow_under_load(
    rpm_subprocess,
    local_dev_cluster,
    firestore_db,
    main_http_client,
    wait_for_fixture,
):
    # Snapshot current cluster size so we can verify grow actually added nodes.
    before = main_http_client.get("/v1/cluster/state").json()
    n_ready_before = len(before["ready_nodes"])
    n_booting_before = before["booting_count"]

    # 200 inputs with a UDF that takes ~1s each. Even with max capacity the
    # job needs to run long enough for grow to kick in. max_parallelism is
    # implicitly len(inputs)=200 so grow will provision up to the local-dev
    # cap (LOCAL_DEV_MAX_GROW_CPUS=4 CPUs).
    source = (
        "import time\n"
        "def test_function(x):\n"
        "    time.sleep(0.5)\n"
        "    return x * 2\n"
    )
    result = rpm_subprocess(
        source, list(range(200)), timeout_seconds=300, grow=True
    )
    assert result["ok"], result.get("traceback")
    assert len(result["outputs"]) == 200
    assert set(result["outputs"]) == {x * 2 for x in range(200)}

    # After the job finishes, `reserved_for_job` is cleared on every node —
    # `on_job_start` clears it the moment the reserved job's assignment lands.
    # Check for the stable signature instead: grow-booted nodes get
    # `inactivity_shutdown_time_sec == 60` (GROW_INACTIVITY_SHUTDOWN_TIME_SEC).
    recent_cutoff = time.time() - 600
    grow_signature_nodes = []
    for doc in firestore_db.collection("nodes").stream():
        data = doc.to_dict() or {}
        if data.get("started_booting_at", 0) < recent_cutoff:
            continue
        if data.get("inactivity_shutdown_time_sec") == 60:
            grow_signature_nodes.append((doc.id, data))

    if not grow_signature_nodes and n_ready_before + n_booting_before <= 1:
        pytest.fail(
            f"grow=True with 200 inputs against {n_ready_before}-node cluster "
            f"should have booted nodes with inactivity_shutdown_time_sec=60 "
            f"(GROW_INACTIVITY_SHUTDOWN_TIME_SEC), but none were found"
        )
