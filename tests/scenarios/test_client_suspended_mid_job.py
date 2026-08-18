"""
Scenario: the client process is suspended mid-job (laptop lid-close, SIGSTOP).

The client judges node silence by wall clock, so on wake it owes the whole nap
and would fail a healthy node the moment its first post-wake poll hits a
network error (WiFi takes 10-30s to reattach after wake). Instead the client
detects the gap and grants every node a fresh silence window, the same guard
the node's job watcher uses when it is starved.

Node heartbeats come from a separate ping process that SIGSTOP does not touch,
which is why a suspension this short does not kill the job node-side.
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.slow]

# Over the client's 10s suspension threshold, far under its 3-minute budget.
SUSPEND_SECONDS = 20


@pytest.mark.timeout(300)
def test_suspended_client_recovers_and_finishes(
    suspend_client_for, local_dev_cluster
):
    source = "import time\ndef test_function(x):\n    time.sleep(4)\n    return x * 2\n"
    inputs = list(range(8))

    result = suspend_client_for(
        source,
        inputs,
        suspend_s=SUSPEND_SECONDS,
        timeout_seconds=240,
        grow=False,
    )

    assert result["ok"], result.get("traceback")
    assert sorted(result["outputs"]) == [x * 2 for x in inputs]
    # The note proves the guard fired, not merely that nothing broke.
    assert "Client was suspended for" in result["stdout"], (
        "suspension guard never fired; stdout was:\n" + result["stdout"]
    )
