"""
Scenario 7: worker process crashes mid-UDF (os._exit).

The worker dies while holding the TCP socket. node_service's
`_raise_if_worker_failed` distinguishes this from a normal UDF exception:
- If the container is still Running but the TCP reader broke, it raises
  a typed RuntimeError blaming `os._exit` / `sys.exit` / SystemExit / a
  C-extension crash.
- The resulting error travels back to the client as
  `is_infrastructure_error=True` (not a UDF error), which the client
  turns into `NodeDisconnected` rather than re-raising a UDF exception.

Never tested today. This scenario exercises the whole
infrastructure-error branch end-to-end.
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.slow]


def test_worker_crash_mid_udf_surfaces_NodeDisconnected(
    rpm_subprocess, local_dev_cluster
):
    source = (
        "import os\n"
        "def test_function(x):\n"
        "    if x == 3:\n"
        "        os._exit(1)\n"
        "    return x\n"
    )
    result = rpm_subprocess(source, list(range(8)), timeout_seconds=120, grow=False)

    assert not result["ok"], (
        "UDF killed the worker via os._exit but rpm completed without error: "
        f"outputs={result.get('outputs')!r}"
    )
    # Infrastructure errors surface as NodeDisconnected, NOT ValueError or
    # SystemExit. The client's `_gather_results` branches on
    # `is_infrastructure_error` and wraps in NodeDisconnected.
    assert result["exception_type"] == "NodeDisconnected", (
        f"expected NodeDisconnected, got {result['exception_type']}: "
        f"{result['exception_message']}"
    )
    # Infrastructure errors don't carry `burla_input_index` — that's only
    # set for typed UDF errors.
    assert result["burla_input_index"] is None

    # The message should name one of the plausible causes documented in
    # `_raise_if_worker_failed`.
    msg = result["exception_message"].lower() + result.get("traceback", "").lower()
    plausible_markers = [
        "stopped unexpectedly",
        "os._exit",
        "sys.exit",
        "systemexit",
        "c extension",
        "c-extension",
        "process ended unexpectedly",
        "worker container",
    ]
    assert any(m in msg for m in plausible_markers), (
        f"NodeDisconnected message doesn't mention any worker-crash cause;\n"
        f"message: {result['exception_message'][:400]}"
    )
