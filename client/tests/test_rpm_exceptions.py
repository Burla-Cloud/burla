"""End-to-end exception propagation and node-selection errors."""

import pytest


pytestmark = pytest.mark.e2e


def test_udf_error_re_raised_on_client(rpm_subprocess, local_dev_cluster):
    source = (
        "def test_function(x):\n"
        "    if x == 3:\n"
        "        raise ValueError('boom')\n"
        "    return x\n"
    )
    result = rpm_subprocess(source, list(range(10)), timeout_seconds=60)
    assert not result["ok"]
    assert result["exception_type"] == "ValueError"
    assert result["burla_input_index"] == 3


def test_udf_error_preserves_traceback(rpm_subprocess, local_dev_cluster):
    source = (
        "def inner(x):\n"
        "    raise RuntimeError('deep')\n"
        "def test_function(x):\n"
        "    return inner(x)\n"
    )
    result = rpm_subprocess(source, [1], timeout_seconds=30)
    assert not result["ok"]
    assert result["exception_type"] == "RuntimeError"
    assert "inner" in result["traceback"]


def test_udf_error_adds_burla_note_py311plus(rpm_subprocess, local_dev_cluster):
    source = (
        "def test_function(x):\n"
        "    if x == 2:\n"
        "        raise ValueError('bad')\n"
        "    return x\n"
    )
    result = rpm_subprocess(source, list(range(5)), timeout_seconds=30)
    assert not result["ok"]
    assert "[burla] failed on input index 2" in result["traceback"]


def test_udf_error_silences_subsequent_logs(rpm_subprocess, local_dev_cluster):
    source = (
        "import time\n"
        "def test_function(x):\n"
        "    print(f'hi-{x}')\n"
        "    time.sleep(0.2)\n"
        "    if x == 0:\n"
        "        raise ValueError('early')\n"
        "    return x\n"
    )
    result = rpm_subprocess(source, list(range(10)), timeout_seconds=60)
    assert not result["ok"]
    assert result["exception_type"] == "ValueError"


@pytest.mark.slow
def test_NoNodes_raised_when_grow_false_and_no_compatible_node(
    rpm_subprocess, local_dev_cluster
):
    source = "def test_function(x):\n    return x\n"
    result = rpm_subprocess(
        source,
        [1],
        timeout_seconds=30,
        image="some/image-that-really-does-not-exist:tag",
        grow=False,
    )
    assert not result["ok"]
    assert result["exception_type"] in ("NoNodes", "NoCompatibleNodes")
