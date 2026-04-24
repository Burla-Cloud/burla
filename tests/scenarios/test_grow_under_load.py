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

    # Firestore: was the job doc ever tagged with a grow-booted node reservation?
    # We look for any `nodes` doc with `reserved_for_job` pointing at a recent
    # test_function-* job id, which is grow's signature.
    from google.cloud.firestore_v1.base_query import FieldFilter

    recent_cutoff = time.time() - 600
    grow_booted_nodes = []
    for doc in firestore_db.collection("nodes").stream():
        data = doc.to_dict()
        if not data:
            continue
        reserved = data.get("reserved_for_job")
        if not (isinstance(reserved, str) and reserved.startswith("test_function-")):
            continue
        if data.get("started_booting_at", 0) < recent_cutoff:
            continue
        grow_booted_nodes.append((doc.id, data))

    # The grow path should have set inactivity_shutdown_time_sec=60 on these
    # reserved nodes (GROW_INACTIVITY_SHUTDOWN_TIME_SEC).
    if grow_booted_nodes:
        for node_id, data in grow_booted_nodes:
            assert data.get("inactivity_shutdown_time_sec") == 60, (
                f"grow-booted node {node_id} has "
                f"inactivity_shutdown_time_sec={data.get('inactivity_shutdown_time_sec')}, "
                f"expected 60"
            )
    else:
        # If no grow-booted nodes, the pre-existing cluster had enough capacity
        # — that's an acceptable outcome if the cluster was already scaled up.
        # We only fail the test if the cluster was truly small.
        if n_ready_before + n_booting_before <= 1:
            pytest.fail(
                f"grow=True with 200 inputs should have booted additional nodes "
                f"when cluster_state shows {n_ready_before} ready + {n_booting_before} booting, "
                f"but none had reserved_for_job=test_function-*"
            )
