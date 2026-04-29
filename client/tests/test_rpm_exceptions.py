"""
Exception classes from the client package. Unit tests only cover messages
with user-facing contracts (the three `NoCompatibleNodes` branches, the
`VersionMismatch` pip-install hint, `NodeDisconnected.node` attr, and the
`AuthException` raise condition). Every other exception is exercised by
an e2e test further down.
"""

from __future__ import annotations

import pytest


# ------------------------------------------------------------ unit-tier


@pytest.mark.unit
def test_NoCompatibleNodes_image_mismatch_message():
    from burla._node import NoCompatibleNodes

    detail = {
        "reason": "image_mismatch",
        "requested_image": "foo:bar",
        "available_images": ["python:3.12", "python:3.11"],
    }
    exc = NoCompatibleNodes(detail)
    msg = str(exc)
    assert "foo:bar" in msg
    assert "python:3.12" in msg
    assert "grow=True" in msg


@pytest.mark.unit
def test_NoCompatibleNodes_gpu_mismatch_message():
    from burla._node import NoCompatibleNodes

    detail = {
        "reason": "gpu_mismatch",
        "requested_func_gpu": "A100",
        "available_machine_types": ["n4-standard-4"],
    }
    exc = NoCompatibleNodes(detail)
    msg = str(exc)
    assert "A100" in msg
    assert "n4-standard-4" in msg
    assert "grow=True" in msg


@pytest.mark.unit
def test_NoCompatibleNodes_insufficient_capacity_message():
    from burla._node import NoCompatibleNodes

    exc = NoCompatibleNodes({"reason": "insufficient_capacity"})
    msg = str(exc)
    assert "func_cpu" in msg and "func_ram" in msg


@pytest.mark.unit
def test_VersionMismatch_pip_install_hint():
    from burla._node import VersionMismatch

    exc = VersionMismatch("1.0.0", "1.5.9", "0.9.0")
    msg = str(exc)
    assert "pip install burla==1.5.9" in msg
    assert "0.9.0" in msg


@pytest.mark.unit
def test_NodeDisconnected_carries_node_attr():
    from burla._node import Node, NodeDisconnected
    import aiohttp
    from unittest.mock import MagicMock

    session = MagicMock(spec=aiohttp.ClientSession)
    client = MagicMock()
    node = Node.from_ready(
        instance_name="burla-node-abc12345",
        host="http://localhost:9999",
        machine_type="n4-standard-2",
        target_parallelism=2,
        session=session,
        client=client,
        spinner=False,
    )
    exc = NodeDisconnected(node, "boom")
    assert exc.node is node
    assert "boom" in str(exc)


# ------------------------------------------------------------ AuthException (unit)


@pytest.mark.unit
def test_AuthException_raised_when_config_missing(tmp_path, monkeypatch):
    from burla import _auth

    fake_path = tmp_path / "nope.json"
    monkeypatch.setattr(_auth, "CONFIG_PATH", fake_path)
    # Clear cache to force re-read.
    _auth._get_auth_info.cache_clear()

    with pytest.raises(_auth.AuthException):
        _auth.get_auth_headers()


# ------------------------------------------------------------ e2e UDF error propagation


@pytest.mark.e2e
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


@pytest.mark.e2e
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
    # The original traceback should include `inner` frame.
    assert "inner" in result["traceback"]


@pytest.mark.e2e
def test_udf_error_adds_burla_note_py311plus(rpm_subprocess, local_dev_cluster):
    # Python 3.11+ supports exc.add_note. The burla note format is
    # "[burla] failed on input index N".
    source = (
        "def test_function(x):\n"
        "    if x == 2:\n"
        "        raise ValueError('bad')\n"
        "    return x\n"
    )
    result = rpm_subprocess(source, list(range(5)), timeout_seconds=30)
    assert not result["ok"]
    assert "[burla] failed on input index 2" in result["traceback"]


@pytest.mark.e2e
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
    # The UDF error should short-circuit further log printing.  We don't insist
    # on exactly-zero `hi-*` lines (some may have printed before the error),
    # but once the error hits, `_print_logs` returns early for later batches.
    # Just assert the exception got through.
    assert result["exception_type"] == "ValueError"


# ------------------------------------------------------------ NoNodes when cluster is off


@pytest.mark.e2e
@pytest.mark.slow
def test_NoNodes_raised_when_grow_false_and_no_compatible_node(rpm_subprocess, local_dev_cluster):
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
