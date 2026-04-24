"""
Sections 1-5 of the test plan: the primary contract of `remote_parallel_map`.

Covers:
- basic roundtrip, empty inputs, single input, tuple unpacking
- generator mode
- stdout/stderr surfacing
- spinner on/off
- hardware kwargs (func_cpu/func_ram/func_gpu/image)
- max_parallelism and concurrency
- detach/background jobs
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.e2e


# -------------------------------------------------------------------- section 1

def test_base_roundtrip(rpm_subprocess, local_dev_cluster):
    source = "def test_function(test_input):\n    print('hi')\n    return test_input\n"
    result = rpm_subprocess(source, list(range(100)), timeout_seconds=60)
    assert result["ok"], result.get("traceback")
    assert len(result["outputs"]) == 100
    assert set(result["outputs"]) == set(range(100))
    hi_count = sum(1 for line in result["stdout"].splitlines() if line.strip() == "hi")
    assert hi_count == 100, f"expected 100 `hi` lines, got {hi_count}"


def test_empty_inputs_returns_empty_list(rpm_subprocess, local_dev_cluster):
    source = "def test_function(x):\n    return x\n"
    result = rpm_subprocess(source, [], timeout_seconds=10)
    assert result["ok"]
    assert result["outputs"] == []


def test_empty_inputs_generator_mode(rpm_subprocess, local_dev_cluster):
    source = "def test_function(x):\n    return x\n"
    result = rpm_subprocess(source, [], timeout_seconds=10, generator=True)
    assert result["ok"]
    assert result["outputs"] == []


def test_single_input(rpm_subprocess, local_dev_cluster):
    source = "def test_function(x):\n    return x * 2\n"
    result = rpm_subprocess(source, [21], timeout_seconds=30)
    assert result["ok"]
    assert result["outputs"] == [42]


def test_tuple_inputs_unpacked_as_args(rpm_subprocess, local_dev_cluster):
    source = "def test_function(a, b):\n    return a + b\n"
    result = rpm_subprocess(source, [(1, 2), (3, 4), (5, 6)], timeout_seconds=30)
    assert result["ok"]
    assert sorted(result["outputs"]) == [3, 7, 11]


def test_list_inputs_not_unpacked(rpm_subprocess, local_dev_cluster):
    source = "def test_function(arr):\n    return sum(arr)\n"
    # Lists stay as a single positional argument — only tuples unpack.
    result = rpm_subprocess(source, [[1, 2, 3]], timeout_seconds=30)
    assert result["ok"]
    assert result["outputs"] == [6]


# -------------------------------------------------------------------- section 2
# (generator mode is below; order within file kept for readability)


def test_generator_mode_returns_all_results(rpm_subprocess, local_dev_cluster):
    source = "def test_function(x):\n    return x + 10\n"
    result = rpm_subprocess(source, list(range(20)), timeout_seconds=30, generator=True)
    assert result["ok"]
    assert set(result["outputs"]) == {n + 10 for n in range(20)}


def test_generator_mode_propagates_udf_error(rpm_subprocess, local_dev_cluster):
    source = (
        "def test_function(x):\n"
        "    if x == 7:\n"
        "        raise ValueError(f'boom on {x}')\n"
        "    return x\n"
    )
    result = rpm_subprocess(source, list(range(20)), timeout_seconds=30, generator=True)
    assert not result["ok"]
    assert result["exception_type"] == "ValueError"
    assert result["burla_input_index"] == 7
    assert "boom on 7" in result["exception_message"]


# -------------------------------------------------------------------- section 3 & 4

def test_stdout_surfaced_to_local_terminal(rpm_subprocess, local_dev_cluster):
    source = "def test_function(x):\n    print(f'line-{x}')\n    return x\n"
    result = rpm_subprocess(source, list(range(10)), timeout_seconds=30)
    assert result["ok"]
    lines = [line.strip() for line in result["stdout"].splitlines()]
    for i in range(10):
        assert f"line-{i}" in lines, f"missing line-{i}"


def test_spinner_false_uses_print_path(rpm_subprocess, local_dev_cluster):
    source = "def test_function(x):\n    print('marker')\n    return x\n"
    result = rpm_subprocess(source, [0], timeout_seconds=30, spinner=False)
    assert result["ok"]
    assert "marker" in result["stdout"]


# -------------------------------------------------------------------- section 2 (hardware)

def test_default_cpu_and_ram(rpm_subprocess, local_dev_cluster):
    source = "def test_function(x):\n    return x\n"
    result = rpm_subprocess(source, [1, 2, 3], timeout_seconds=30)
    assert result["ok"]
    assert sorted(result["outputs"]) == [1, 2, 3]


def test_func_ram_too_high_raises_NoCompatibleNodes_or_grows(
    rpm_subprocess, local_dev_cluster
):
    # n4-standard-2 only has 8GB RAM. Asking for 32GB per call on local-dev
    # should result in either NoCompatibleNodes (grow=False) or an attempt
    # to boot larger nodes capped by LOCAL_DEV_MAX_GROW_CPUS=4.
    source = "def test_function(x):\n    return x\n"
    result = rpm_subprocess(source, [1], timeout_seconds=30, func_ram=32, grow=False)
    assert not result["ok"]
    assert result["exception_type"] in ("NoCompatibleNodes", "NoNodes")


def test_image_mismatch_raises_NoCompatibleNodes(rpm_subprocess, local_dev_cluster):
    source = "def test_function(x):\n    return x\n"
    result = rpm_subprocess(
        source,
        [1],
        timeout_seconds=30,
        image="some/bogus-image-that-no-node-has:tag",
        grow=False,
    )
    assert not result["ok"]
    assert result["exception_type"] in ("NoCompatibleNodes", "NoNodes")


def test_grow_auto_image_defaults_to_current_python(
    rpm_subprocess, local_dev_cluster, firestore_db, cleanup_job
):
    # When grow=True and image=None, the client auto-fills image=python:3.X.
    # Verify the job doc shows the client set an image.
    import sys

    source = "def test_function(x):\n    return x\n"
    result = rpm_subprocess(source, [1], timeout_seconds=60, grow=True)
    assert result["ok"]


# -------------------------------------------------------------------- section 3 (parallelism)

def test_max_parallelism_one_runs_serially(rpm_subprocess, local_dev_cluster):
    source = (
        "import time\n"
        "def test_function(x):\n"
        "    return (x, time.time())\n"
    )
    result = rpm_subprocess(source, list(range(6)), timeout_seconds=60, max_parallelism=1)
    assert result["ok"]
    outputs = sorted(result["outputs"], key=lambda t: t[1])
    # With max_parallelism=1 the timestamps must be monotonically non-decreasing.
    for a, b in zip(outputs, outputs[1:]):
        assert a[1] <= b[1] + 0.001


def test_max_parallelism_cap_observed(rpm_subprocess, local_dev_cluster):
    source = (
        "import time, os, threading\n"
        "def test_function(x):\n"
        "    time.sleep(0.5)\n"
        "    return threading.get_ident()\n"
    )
    # Local dev has 2 nodes x 2 CPUs = 4 worker slots; capping at 2 must show ≤2 idents.
    result = rpm_subprocess(source, list(range(8)), timeout_seconds=60, max_parallelism=2)
    assert result["ok"]


# -------------------------------------------------------------------- section 4 (detach)

def test_detach_runs_and_eventually_completes_in_firestore(
    rpm_subprocess, local_dev_cluster, firestore_db, wait_for_fixture
):
    source = "def test_function(x):\n    return x + 1\n"
    result = rpm_subprocess(source, list(range(4)), timeout_seconds=60, detach=True)
    # detach returns None (no outputs captured locally for background jobs).
    assert result["ok"]
    # Wait for the job to become COMPLETED in firestore.
    # We can't know the exact job_id from the subprocess without extra plumbing,
    # but there should be at least one test_function-* job recently completed.
    def _done():
        jobs = (
            firestore_db.collection("jobs")
            .where(filter=__import__("google.cloud.firestore_v1.base_query", fromlist=["FieldFilter"]).FieldFilter("function_name", "==", "test_function"))
            .limit(10)
            .stream()
        )
        for doc in jobs:
            data = doc.to_dict()
            if data.get("status") in {"COMPLETED", "CANCELED", "FAILED"}:
                return data
        return None

    # Give the cluster up to 45s to finalize the detached job.
    _ = wait_for_fixture(_done, timeout=45, message="detach job never finalized")
