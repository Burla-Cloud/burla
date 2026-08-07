from __future__ import annotations

import uuid

import pytest

# Real OOM kills need a node whose memory is actually bounded. local-dev sets
# no memory limit on node or worker containers, so these would instead exhaust
# the whole docker VM and take unrelated containers down with them.
pytestmark = [pytest.mark.e2e, pytest.mark.slow, pytest.mark.remote_dev]


def _oom_like_source(marker_path: str, always_kill: bool = False):
    condition = "True" if always_kill else f"not os.path.exists({marker_path!r})"
    return (
        "import os\n"
        "import signal\n"
        "def test_function(x):\n"
        "    if x == 3 and " + condition + ":\n"
        f"        open({marker_path!r}, 'w').write('1')\n"
        "        os.kill(os.getpid(), signal.SIGKILL)\n"
        "    return x\n"
    )


def _real_oom_source():
    return (
        "def test_function(x):\n"
        "    if x == 3:\n"
        "        chunks = []\n"
        "        while True:\n"
        "            chunks.append(bytearray(256 * 1024 * 1024))\n"
        "    return x\n"
    )


def test_dynamic_func_ram_retries_after_worker_oom(rpm_subprocess, local_dev_cluster):
    marker_path = f"/workspace/shared/dynamic-ram-retry-{uuid.uuid4().hex}"
    result = rpm_subprocess(
        _oom_like_source(marker_path),
        list(range(8)),
        timeout_seconds=180,
        func_ram="dynamic",
        max_parallelism=2,
        grow=False,
    )

    assert result["ok"], result.get("traceback")
    assert sorted(result["outputs"]) == list(range(8))


def test_integer_func_ram_oom_fails_with_clear_message(
    rpm_subprocess, local_dev_cluster
):
    result = rpm_subprocess(
        _real_oom_source(),
        list(range(8)),
        timeout_seconds=120,
        func_ram=4,
        max_parallelism=2,
        grow=False,
    )

    assert not result["ok"]
    assert result["exception_type"] == "NodeDisconnected"
    message = result["exception_message"].lower() + result.get("traceback", "").lower()
    assert "out of memory" in message or "oom" in message
    assert "func_ram" in message


def test_dynamic_func_ram_oom_at_one_worker_fails(rpm_subprocess, local_dev_cluster):
    result = rpm_subprocess(
        _real_oom_source(),
        [3],
        timeout_seconds=120,
        func_ram="dynamic",
        max_parallelism=1,
        grow=False,
    )

    assert not result["ok"]
    assert result["exception_type"] == "NodeDisconnected"
    message = result["exception_message"].lower() + result.get("traceback", "").lower()
    assert "one active worker" in message
    assert "cannot give this input more memory" in message
