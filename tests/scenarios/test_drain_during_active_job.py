"""
Scenario: growth capacity drains and self-deletes during an active job.

A grow job whose queue empties while long-tail calls are still running must
not keep its growth machines idling until the job ends. The growth node is
expected to: steal work immediately after joining (before the job would have
fed it), run it, then once the whole ring is provably empty, drain and delete
itself while the job is still RUNNING on the baseline node.

Local-dev makes this deterministic: the two long inputs are already
in-flight on the baseline node's two workers before any growth container
finishes booting, so the tail can only live on the baseline node and every
growth node ends up with an empty ring while the job still runs.
"""

from __future__ import annotations

import threading
import time

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.slow, pytest.mark.local_dev]

LONG_CALL_SEC = 60
N_SHORT_INPUTS = 40


@pytest.mark.timeout(600)
def test_growth_node_drains_and_self_deletes_mid_job(
    rpm_subprocess,
    local_dev_cluster,
    cluster_with_n_nodes,
    main_http_client,
    wait_for_fixture,
):
    # Exactly one baseline node: local-dev's grow budget is
    # LOCAL_DEV_MAX_GROW_CPUS=4 minus existing capacity, so a second idle
    # baseline node would leave no budget to boot growth capacity from.
    initial_names = {
        n["instance_name"] for n in cluster_with_n_nodes(1)
    }

    # Inputs 0 and 1 sleep for a minute and are queued first, so the baseline
    # node's two workers grab them immediately; everything else is short work
    # the growth node steals and burns through, after which its ring stays
    # empty and it must leave.
    source = (
        f"import time\n"
        f"def test_function(x):\n"
        f"    time.sleep({LONG_CALL_SEC} if x < 2 else 0.5)\n"
        f"    return x\n"
    )
    n_inputs = 2 + N_SHORT_INPUTS
    result_box: dict = {}

    def _run():
        result_box["result"] = rpm_subprocess(
            source, list(range(n_inputs)), timeout_seconds=240, grow=True
        )

    rpm_thread = threading.Thread(target=_run, daemon=True)
    rpm_thread.start()

    def _live_nodes() -> list[dict]:
        return main_http_client.get("/v1/cluster/nodes").json()["nodes"]

    def _job_status(job_id: str) -> str | None:
        response = main_http_client.get(f"/v1/jobs/{job_id}")
        if response.status_code != 200:
            return None
        return response.json().get("status")

    try:
        # The job boots exactly one growth node (first-wave cap) which must
        # reach RUNNING on the job: work acquisition can't be verified on a
        # node that never joined.
        def _running_growth_node():
            growth = [
                n
                for n in _live_nodes()
                if n["instance_name"] not in initial_names
                and n.get("status") in ("BOOTING", "READY", "RUNNING")
            ]
            running = [
                n
                for n in growth
                if n.get("status") == "RUNNING" and n.get("current_job")
            ]
            return running[0] if running else None

        growth_node = wait_for_fixture(
            _running_growth_node,
            timeout=120,
            message="no growth node ever joined the job",
        )
        growth_name = growth_node["instance_name"]
        job_id = growth_node["current_job"]

        # Core assertion: the growth node disappears from the live node list
        # (drained, acked, self-deleted) while the job is still RUNNING on
        # the baseline node. If it instead idles until the job completes,
        # this times out at the job's end (~LONG_CALL_SEC).
        def _growth_gone_mid_job():
            names_alive = {
                n["instance_name"]
                for n in _live_nodes()
                if n.get("status") not in ("DELETED", "FAILED")
            }
            if growth_name in names_alive:
                return None
            return {"job_status": _job_status(job_id)}

        observation = wait_for_fixture(
            _growth_gone_mid_job,
            timeout=LONG_CALL_SEC - 5,
            message="growth node never drained + self-deleted during the job",
        )
        assert observation["job_status"] == "RUNNING", (
            "growth node exited only after the job ended "
            f"(job status was {observation['job_status']})"
        )
    finally:
        rpm_thread.join(timeout=240)

    assert "result" in result_box, "rpm thread never stored a result"
    result = result_box["result"]
    assert result["ok"], result.get("traceback")
    assert sorted(result["outputs"]) == list(range(n_inputs)), (
        "drain lost or duplicated an input"
    )
