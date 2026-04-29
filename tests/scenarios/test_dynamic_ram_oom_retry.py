from __future__ import annotations

import uuid

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.slow]


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


def test_integer_func_ram_oom_fails_with_clear_message(rpm_subprocess, local_dev_cluster):
    marker_path = f"/workspace/shared/integer-ram-oom-{uuid.uuid4().hex}"
    result = rpm_subprocess(
        _oom_like_source(marker_path),
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
    marker_path = f"/workspace/shared/dynamic-ram-terminal-oom-{uuid.uuid4().hex}"
    result = rpm_subprocess(
        _oom_like_source(marker_path, always_kill=True),
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
