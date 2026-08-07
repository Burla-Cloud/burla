"""
Scenario: `max_parallelism` as a real throttle, and `generator=True` as a real
stream.

Every docs example that talks to a rate-limited API or a database relies on
`max_parallelism` holding a cap above 1 while more capacity exists, and on
`generator=True` handing results back as they finish rather than at the end.
The existing tests only pin `max_parallelism=1` and that a generator eventually
yields everything.
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.slow]

CAP = 2
UDF_SECONDS = 2
N_INPUTS = 12


def _peak_concurrency(intervals: list[tuple[float, float]]) -> int:
    events = []
    for started_at, ended_at in intervals:
        events.append((started_at, 1))
        events.append((ended_at, -1))
    peak = 0
    running = 0
    for _, delta in sorted(events):
        running += delta
        peak = max(peak, running)
    return peak


def _timed_udf_source() -> str:
    return (
        "import time\n"
        "def test_function(x):\n"
        "    started_at = time.time()\n"
        f"    time.sleep({UDF_SECONDS})\n"
        "    return (started_at, time.time())\n"
    )


def _run_capped_generator_job(rpm_subprocess) -> dict:
    # grow=True lets the cluster offer more slots than the cap, so a cap that
    # was ignored would show up as extra concurrency.
    result = rpm_subprocess(
        _timed_udf_source(),
        list(range(N_INPUTS)),
        timeout_seconds=300,
        generator=True,
        max_parallelism=CAP,
        grow=True,
    )
    assert result["ok"], result.get("traceback")
    assert len(result["outputs"]) == N_INPUTS
    return result


def test_max_parallelism_caps_concurrency(rpm_subprocess, local_dev_cluster):
    result = _run_capped_generator_job(rpm_subprocess)
    peak = _peak_concurrency(result["outputs"])
    assert peak <= CAP, f"{peak} calls ran at once with max_parallelism={CAP}"


@pytest.mark.xfail(
    strict=True,
    reason=(
        "generator=True does not stream: remote_parallel_map joins the job "
        "thread before building the generator, so every result is already in "
        "hand when the caller gets it"
    ),
)
def test_generator_yields_results_while_the_job_is_still_running(
    rpm_subprocess, local_dev_cluster
):
    result = _run_capped_generator_job(rpm_subprocess)
    times = result["output_times"]
    assert times[0] < times[-1] - UDF_SECONDS, (
        f"generator did not stream; first result at {times[0]:.1f}s, "
        f"last at {times[-1]:.1f}s"
    )
