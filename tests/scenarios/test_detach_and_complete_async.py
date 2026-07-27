"""
Scenario 5: detach / background job completes after client disconnect.

Submits a slow-ish job with `detach=True`, verifies the client returns
cleanly after uploading all inputs (stdout: "Done uploading inputs!"),
then polls the head until the job transitions to COMPLETED and its
result counters sum to n_inputs. This exercises the `is_background_job`
path in job_watcher where client-disconnect does NOT mark the job FAILED
as long as all_inputs_uploaded is True.
"""

from __future__ import annotations

import time

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.slow]


def test_detach_and_complete_async(
    rpm_subprocess,
    local_dev_cluster,
    main_http_client,
    wait_for_fixture,
):
    # UDF with a small sleep so the job takes a few seconds, long enough
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

    # The head should know the job as is_background_job=True with all inputs
    # uploaded at the moment the client exited. Wait for it to finish.
    def _bg_job():
        jobs = main_http_client.get("/v1/jobs?page=0").json()["jobs"]
        candidates = []
        for summary in jobs:
            if summary.get("function_name") != "test_function":
                continue
            if summary.get("started_at", 0) < before_start - 5:
                continue
            job = main_http_client.get(f"/v1/jobs/{summary['jobId']}").json()
            if not job.get("is_background_job"):
                continue
            candidates.append((summary["jobId"], job))
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
        resp = main_http_client.get(f"/v1/jobs/{job_id}")
        if resp.status_code != 200:
            return None
        job = resp.json()
        if job.get("status") in {"COMPLETED", "FAILED", "CANCELED"}:
            return job
        return None

    final = wait_for_fixture(
        _terminal,
        timeout=120,
        message=f"background job {job_id} never reached a terminal state",
    )
    assert final["status"] == "COMPLETED", (
        f"background job ended with status={final['status']} fail_reason={final.get('fail_reason')}"
    )

    # Per-node result counters must account for all inputs.
    stats = main_http_client.get(f"/v1/jobs/{job_id}/result-stats").json()
    assert stats["n_results"] == len(inputs), (
        f"n_results {stats['n_results']} != n_inputs {len(inputs)}"
    )
