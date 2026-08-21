"""
Scenario 3: cluster grows under load.

Submits a job larger than the current cluster capacity with `grow=True` and
watches the cluster while it runs. Verifies that grow-booted machines (the
ones carrying `inactivity_shutdown_time_sec=60`, GROW_INACTIVITY_SHUTDOWN_
TIME_SEC) actually joined this job's expanded capacity mid-run, that the
synchronous first wave stayed within its cap instead of provisioning to
max_parallelism, and that every input completed exactly once.
"""

from __future__ import annotations

import threading
import time

import pytest

# local-dev caps grow at LOCAL_DEV_MAX_GROW_CPUS=4, so only real VMs exercise
# a genuine capacity deficit and real boot latency.
pytestmark = [pytest.mark.e2e, pytest.mark.slow, pytest.mark.remote_dev]

GROW_SIGNATURE_SHUTDOWN_SEC = 60  # GROW_INACTIVITY_SHUTDOWN_TIME_SEC
FIRST_WAVE_CPU_CAP = 64  # GROW_CPUS_FIRST_WAVE (non-local-dev)


def test_grow_under_load(
    rpm_subprocess,
    local_dev_cluster,
    main_http_client,
    wait_for_fixture,
):
    before = main_http_client.get("/v1/cluster/state").json()
    initial_names = {n["instance_name"] for n in before["ready_nodes"]}

    # 200 inputs with a UDF slow enough that the queue outlives several boot
    # cycles, so grow has both the reason and the time to add machines.
    source = (
        "import time\n"
        "def test_function(x):\n"
        "    time.sleep(3)\n"
        "    return x * 2\n"
    )
    n_inputs = 200
    result_box: dict = {}

    def _run():
        result_box["result"] = rpm_subprocess(
            source, list(range(n_inputs)), timeout_seconds=600, grow=True
        )

    rpm_thread = threading.Thread(target=_run, daemon=True)
    rpm_thread.start()

    # Watch the live node list while the job runs. Nodes booted for this job
    # self-delete as soon as they finish their part, so evidence has to be
    # collected mid-run, not after.
    grow_nodes_seen: dict[str, dict] = {}
    grow_nodes_seen_running: set[str] = set()
    first_wave_names: set[str] = set()
    any_grow_running_yet = False
    try:
        deadline = time.time() + 600
        while rpm_thread.is_alive() and time.time() < deadline:
            nodes = main_http_client.get("/v1/cluster/nodes").json()["nodes"]
            for data in nodes:
                is_grow_node = (
                    data.get("instance_name") not in initial_names
                    and data.get("inactivity_shutdown_time_sec")
                    == GROW_SIGNATURE_SHUTDOWN_SEC
                )
                if not is_grow_node:
                    continue
                name = data["instance_name"]
                grow_nodes_seen[name] = data
                if data.get("status") == "RUNNING" and data.get("current_job"):
                    grow_nodes_seen_running.add(name)
                    any_grow_running_yet = True
                # Every grow node observed before any grow node is RUNNING
                # belongs to the synchronous first wave (reconciler waves
                # require running, saturated grow capacity to justify more).
                if not any_grow_running_yet:
                    first_wave_names.add(name)
            time.sleep(1)
    finally:
        rpm_thread.join(timeout=600)

    assert "result" in result_box, "rpm thread never stored a result"
    result = result_box["result"]
    assert result["ok"], result.get("traceback")
    assert len(result["outputs"]) == n_inputs
    assert sorted(result["outputs"]) == [
        x * 2 for x in range(n_inputs)
    ], "grow lost or duplicated an input"

    assert grow_nodes_seen, (
        f"grow=True with {n_inputs} slow inputs against a "
        f"{len(initial_names)}-node cluster booted no nodes with "
        f"inactivity_shutdown_time_sec={GROW_SIGNATURE_SHUTDOWN_SEC}"
    )
    assert grow_nodes_seen_running, (
        "grow nodes booted but none was ever observed RUNNING this job: "
        f"{sorted(grow_nodes_seen)}"
    )
    # max_parallelism (=200 here) is a ceiling, not a provisioning target:
    # the wave booted synchronously at job start must stay within its CPU
    # cap. For dynamic jobs one slot is one CPU, and target_parallelism is
    # the slot count each machine was booted to cover.
    first_wave_slots = sum(
        int(grow_nodes_seen[name].get("target_parallelism") or 0)
        for name in first_wave_names
    )
    assert first_wave_slots <= FIRST_WAVE_CPU_CAP, (
        f"first grow wave covered {first_wave_slots} slots, exceeding the "
        f"{FIRST_WAVE_CPU_CAP}-CPU first-wave cap; growth is supposed to "
        "arrive in staged, forecast-gated waves"
    )
