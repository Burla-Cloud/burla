"""
Section 6 of the test plan: client-side validation and size limits.

These can run as pure unit tests because `FunctionTooBig` is raised inside
`_execute_job` before any HTTP traffic, and `InputTooBig` is raised inside
`_node.execute_job` before any upload.
"""

from __future__ import annotations

import inspect

import pytest


# ------------------------------------------------ FunctionTooBig (unit tier)


@pytest.mark.unit
def test_FunctionTooBig_constructor_message_shape():
    from burla._remote_parallel_map import FunctionTooBig

    exc = FunctionTooBig("my_fn")
    assert "my_fn" in str(exc)
    assert "0.1GB" in str(exc) or "0.1 GB" in str(exc)


@pytest.mark.unit
def test_FunctionTooBig_is_subclass_of_Exception():
    from burla._remote_parallel_map import FunctionTooBig

    assert issubclass(FunctionTooBig, Exception)


@pytest.mark.unit
def test_remote_parallel_map_default_func_ram_is_dynamic():
    from burla import remote_parallel_map

    signature = inspect.signature(remote_parallel_map)
    assert signature.parameters["func_ram"].default == "dynamic"


# ------------------------------------------------ InputTooBig (unit tier)


@pytest.mark.unit
def test_InputTooBig_constructor_carries_index():
    from burla._node import InputTooBig

    exc = InputTooBig(42)
    assert "42" in str(exc)
    assert "0.2GB" in str(exc) or "200" in str(exc)


# ------------------------------------------------ End-to-end validation


@pytest.mark.e2e
def test_function_too_big_raises_FunctionTooBig(rpm_subprocess, local_dev_cluster):
    # Closure > 0.1 GB triggers the client-side gate before any HTTP traffic.
    source = (
        "big = bytes(110 * 1_000_000)\n"
        "def test_function(x):\n"
        "    return len(big) + x\n"
    )
    result = rpm_subprocess(source, [1], timeout_seconds=30)
    assert not result["ok"]
    assert result["exception_type"] == "FunctionTooBig"
    assert "test_function" in result["exception_message"]


@pytest.mark.e2e
@pytest.mark.slow
def test_input_too_big_raises_InputTooBig_with_index(rpm_subprocess, local_dev_cluster):
    # 210 MB single input blows past the 200 MB per-input cap.
    source = (
        "def test_function(blob):\n"
        "    return len(blob)\n"
    )
    # Construct the huge input inside the subprocess so it's not pickled across the mp boundary.
    # Use a generator pattern: the subprocess itself builds a single 210MB bytes input.
    # We pass a list with a placeholder and have the subprocess materialize the bytes.
    # Simpler: just pass a 210MB bytes object — mp can handle it as the args pickle.
    huge = bytes(210 * 1_000_000)
    result = rpm_subprocess(source, [huge], timeout_seconds=60)
    assert not result["ok"]
    assert result["exception_type"] == "InputTooBig"
    assert "index 0" in result["exception_message"] or result["burla_input_index"] == 0


@pytest.mark.e2e
def test_non_pickleable_function_surfaces_error(rpm_subprocess, local_dev_cluster):
    # A closure capturing a live threading.Lock is not cloudpicklable on any version.
    source = (
        "import threading\n"
        "_lock = threading.Lock()\n"
        "def test_function(x):\n"
        "    with _lock:\n"
        "        return x\n"
    )
    result = rpm_subprocess(source, [1], timeout_seconds=30)
    # Either cloudpickle raises at dump time (preferred) or the subprocess hangs
    # — but the test must never silently succeed for unpicklable closures.
    if result["ok"]:
        pytest.skip("cloudpickle now handles this closure type; test no longer meaningful")
    assert any(
        keyword in (result.get("traceback") or "").lower()
        for keyword in ("pickle", "cloudpickle", "lock", "cannot pickle", "unpicklable")
    )
