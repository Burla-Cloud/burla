"""End-to-end client validation and size-limit behavior."""

import pytest


pytestmark = pytest.mark.e2e


def test_function_too_big_raises_FunctionTooBig(rpm_subprocess, local_dev_cluster):
    source = (
        "big = bytes(110 * 1_000_000)\n"
        "def test_function(x):\n"
        "    return len(big) + x\n"
    )
    result = rpm_subprocess(source, [1], timeout_seconds=30)
    assert not result["ok"]
    assert result["exception_type"] == "FunctionTooBig"
    assert "test_function" in result["exception_message"]


@pytest.mark.slow
def test_input_too_big_raises_InputTooBig_with_index(
    rpm_subprocess, local_dev_cluster
):
    source = "def test_function(blob):\n    return len(blob)\n"
    huge = bytes(210 * 1_000_000)
    result = rpm_subprocess(source, [huge], timeout_seconds=60)
    assert not result["ok"]
    assert result["exception_type"] == "InputTooBig"
    assert (
        "index 0" in result["exception_message"]
        or result["burla_input_index"] == 0
    )


def test_non_pickleable_function_surfaces_error(rpm_subprocess, local_dev_cluster):
    source = (
        "import threading\n"
        "_lock = threading.Lock()\n"
        "def test_function(x):\n"
        "    with _lock:\n"
        "        return x\n"
    )
    result = rpm_subprocess(source, [1], timeout_seconds=30)
    assert not result["ok"], "the supposedly unpickleable function ran successfully"
    assert any(
        keyword in (result.get("traceback") or "").lower()
        for keyword in ("pickle", "cloudpickle", "lock", "cannot pickle", "unpicklable")
    )
