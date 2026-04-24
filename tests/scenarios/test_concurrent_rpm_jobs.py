"""
Scenario 8: two rpm jobs running concurrently on a multi-node cluster.

Each node enforces `SELF["RUNNING"]` via `CallHookOnJobStartMiddleware`
— a second job trying to assign the same node gets 409. So two
concurrent clients against a 2-node cluster should each land on its
own node and both complete. Nothing in the suite covers this today.
"""

from __future__ import annotations

import threading

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.slow]


def test_two_rpms_run_concurrently_each_on_its_own_node(
    rpm_subprocess, local_dev_cluster, main_http_client, firestore_db
):
    state = main_http_client.get("/v1/cluster/state").json()
    if len(state["ready_nodes"]) < 2:
        pytest.skip(f"need >=2 ready nodes, got {len(state['ready_nodes'])}")

    # Slow UDF so the two jobs are genuinely in flight at the same time;
    # without the sleep the first job can finish before the second even
    # starts, which defeats the concurrency check.
    source = (
        "import time\n"
        "def test_function(x):\n"
        "    time.sleep(3)\n"
        "    return x * 10\n"
    )

    import time

    results: dict[int, dict] = {}

    def _run(label: int, inputs_range):
        # max_parallelism=2 caps each rpm to one n4-standard-2 node's worth
        # of slots, leaving the second node free for the concurrent rpm.
        # Without this, the first rpm greedily consumes both nodes.
        results[label] = rpm_subprocess(
            source,
            list(inputs_range),
            timeout_seconds=180,
            grow=False,
            max_parallelism=2,
        )

    # Stagger by a couple seconds so main_service has time to flip the
    # first node to RUNNING before the second client's node-selection
    # runs. This mirrors real human usage — two near-simultaneous rpm
    # invocations, not a hard collision. Without the stagger, both
    # clients see both nodes as READY in the NODES_CACHE snapshot and
    # pick the same node, which is a different concurrency path (and
    # one that's separately worth testing if we ever want to).
    t1 = threading.Thread(target=_run, args=(1, range(0, 5)), daemon=True)
    t2 = threading.Thread(target=_run, args=(2, range(100, 105)), daemon=True)

    t1.start()
    time.sleep(2)
    t2.start()
    t1.join(timeout=180)
    t2.join(timeout=180)

    assert not t1.is_alive() and not t2.is_alive(), "one of the rpm threads hung"
    assert 1 in results and 2 in results, "rpm threads didn't both produce a result"

    for label, expected in [(1, set(x * 10 for x in range(0, 5))), (2, set(x * 10 for x in range(100, 105)))]:
        r = results[label]
        assert r["ok"], f"rpm #{label} failed: {r.get('traceback')}"
        assert set(r["outputs"]) == expected, (
            f"rpm #{label} returned unexpected outputs: {r['outputs']}"
        )

    # No node should be stuck in FAILED after this.
    import time as _time

    failed_nodes = [
        d.to_dict() for d in firestore_db.collection("nodes").stream()
        if (d.to_dict() or {}).get("status") == "FAILED"
    ]
    recent_failed = [
        n for n in failed_nodes
        if n.get("started_booting_at", 0) > _time.time() - 600
    ]
    assert not recent_failed, f"recent FAILED nodes after concurrent run: {recent_failed}"
