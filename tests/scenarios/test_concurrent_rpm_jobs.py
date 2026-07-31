"""
Scenario 8: two rpm jobs running concurrently on a multi-node cluster.

Two simultaneous clients against a 2-node cluster must atomically claim
different nodes and both complete.
"""

from __future__ import annotations

import threading

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.slow]


def test_two_rpms_run_concurrently_each_on_its_own_node(
    rpm_subprocess, local_dev_cluster, main_http_client, wait_for_fixture
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

    results: dict[int, dict] = {}
    start_barrier = threading.Barrier(2)

    def _run(label: int, inputs_range):
        # max_parallelism=2 caps each rpm to one n4-standard-2 node's worth
        # of slots, leaving the second node free for the concurrent rpm.
        # Without this, the first rpm greedily consumes both nodes.
        start_barrier.wait()
        results[label] = rpm_subprocess(
            source,
            list(inputs_range),
            timeout_seconds=180,
            grow=False,
            max_parallelism=2,
        )

    t1 = threading.Thread(target=_run, args=(1, range(0, 5)), daemon=True)
    t2 = threading.Thread(target=_run, args=(2, range(100, 105)), daemon=True)

    t1.start()
    t2.start()

    def _two_distinct_jobs():
        nodes = main_http_client.get("/v1/cluster/nodes").json()["nodes"]
        job_ids = {
            node["current_job"]
            for node in nodes
            if node.get("status") == "RUNNING" and node.get("current_job")
        }
        return job_ids if len(job_ids) == 2 else None

    wait_for_fixture(
        _two_distinct_jobs,
        timeout=30,
        message="simultaneous jobs did not claim distinct nodes",
    )

    t1.join(timeout=180)
    t2.join(timeout=180)

    assert not t1.is_alive() and not t2.is_alive(), "one of the rpm threads hung"
    assert 1 in results and 2 in results, "rpm threads didn't both produce a result"

    for label, expected in [
        (1, set(x * 10 for x in range(0, 5))),
        (2, set(x * 10 for x in range(100, 105))),
    ]:
        r = results[label]
        assert r["ok"], f"rpm #{label} failed: {r.get('traceback')}"
        assert (
            set(r["outputs"]) == expected
        ), f"rpm #{label} returned unexpected outputs: {r['outputs']}"

    # No node should be stuck in FAILED after this.
    import time as _time

    all_nodes = main_http_client.get("/v1/cluster/nodes").json()["nodes"]
    recent_failed = [
        n
        for n in all_nodes
        if n.get("status") == "FAILED"
        and n.get("started_booting_at", 0) > _time.time() - 600
    ]
    assert (
        not recent_failed
    ), f"recent FAILED nodes after concurrent run: {recent_failed}"
