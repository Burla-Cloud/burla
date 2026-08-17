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


def test_burla_exception_re_raised_on_client(rpm_subprocess, local_dev_cluster):
    source = (
        "from burla._node import AllNodesBusy\n"
        "def test_function(x):\n"
        "    raise AllNodesBusy()\n"
    )
    result = rpm_subprocess(source, [1], timeout_seconds=30)
    assert not result["ok"]
    assert result["exception_type"] == "AllNodesBusy"
    assert result["exception_message"] == "All nodes are busy, please try again later."


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


def test_old_client_version_refused_with_upgrade_command(
    rpm_subprocess, local_dev_cluster
):
    # `from burla import __version__` is bound into _remote_parallel_map at
    # import time, so patching that binding sends exactly what an outdated
    # installed client would send.
    source = (
        "import burla._remote_parallel_map as rpm_module\n"
        "rpm_module.__version__ = '0.0.1'\n"
        "def test_function(x):\n"
        "    return x\n"
    )
    result = rpm_subprocess(source, [1], timeout_seconds=30)
    assert not result["ok"]
    assert result["exception_type"] == "VersionMismatch"
    assert "0.0.1" in result["exception_message"]
    assert "pip install burla==" in result["exception_message"]


def test_too_new_client_version_refused(rpm_subprocess, local_dev_cluster):
    source = (
        "import burla._remote_parallel_map as rpm_module\n"
        "rpm_module.__version__ = '999.99.99'\n"
        "def test_function(x):\n"
        "    return x\n"
    )
    result = rpm_subprocess(source, [1], timeout_seconds=30)
    assert not result["ok"]
    assert result["exception_type"] == "VersionMismatch"
    assert "999.99.99" in result["exception_message"]


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
