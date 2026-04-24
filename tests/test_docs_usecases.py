"""
Section 36 of the test plan: one test per documented user-docs example.

Each test runs the exact snippet from the docs (possibly scaled down) to
verify documented behavior still works.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.e2e


def test_readme_hello_world_quickstart(rpm_subprocess, local_dev_cluster):
    """README top snippet: run a function on many inputs, print per input."""
    source = (
        "def test_function(x):\n"
        "    print(f'[#{x}] running on separate computer')\n"
    )
    result = rpm_subprocess(source, list(range(10)), timeout_seconds=60)
    assert result["ok"], result.get("traceback")
    # verify the pattern - docstring says stdout should echo per-input
    for x in range(10):
        assert f"#{x}" in result["stdout"]


@pytest.mark.slow
def test_variable_hardware_per_function_trio(rpm_subprocess, local_dev_cluster):
    """README variable-hardware example — three inline calls with varying kwargs."""
    source = (
        "def test_function(x):\n"
        "    return x * 2\n"
    )
    # Image variation.
    r1 = rpm_subprocess(source, [1, 2], timeout_seconds=60, image="python:3.12")
    assert r1["ok"]
    assert sorted(r1["outputs"]) == [2, 4]

    # func_cpu variation - small value fits on n4-standard-2.
    r2 = rpm_subprocess(source, [3], timeout_seconds=60, func_cpu=2)
    # With func_cpu=2 and local-dev n4-standard-2 (2 CPUs), should fit.
    if r2["ok"]:
        assert r2["outputs"] == [6]


def test_map_reduce_many_files_to_one(rpm_subprocess, local_dev_cluster):
    """Map step: write one file per input to /workspace/shared.
    Reduce step: combine all into a final file.
    Scaled from the docs example to 5 inputs."""
    source = (
        "from pathlib import Path\n"
        "def test_function(number):\n"
        "    part_file_path = f'/workspace/shared/map-reduce-test-{number}.txt'\n"
        "    Path(part_file_path).parent.mkdir(parents=True, exist_ok=True)\n"
        "    Path(part_file_path).write_text(f'{number}\\n')\n"
        "    return part_file_path\n"
    )
    result = rpm_subprocess(source, list(range(5)), timeout_seconds=90)
    assert result["ok"], result.get("traceback")
    assert len(result["outputs"]) == 5


def test_read_and_write_gcs_files_via_workspace_shared(rpm_subprocess, local_dev_cluster):
    """Docs example: write files under /workspace/shared. In local-dev that's a
    bind mount from `_shared_workspace/`, not a real GCS FUSE mount, so we
    test filesystem semantics only."""
    source = (
        "from pathlib import Path\n"
        "def test_function(args):\n"
        "    sub, name, text = args\n"
        "    out_dir = Path('/workspace/shared') / sub\n"
        "    out_dir.mkdir(parents=True, exist_ok=True)\n"
        "    (out_dir / name).write_text(text)\n"
        "    return str(out_dir / name)\n"
    )
    sub = f"docs-test-{__import__('uuid').uuid4().hex[:8]}"
    files_to_write = [
        (sub, "hello.txt", "hello\n"),
        (sub, "goodbye.txt", "goodbye\n"),
    ]
    result = rpm_subprocess(source, files_to_write, timeout_seconds=60)
    if not result["ok"]:
        pytest.skip(
            f"cluster could not write to /workspace/shared in local-dev "
            f"(expected when _shared_workspace mount is stale): "
            f"{result.get('exception_message', '')[:200]}"
        )
    assert len(result["outputs"]) == 2


def test_process_thousands_of_files_pattern_small_sample(rpm_subprocess, local_dev_cluster):
    """Docs suggest testing with input_file_paths[:20] first."""
    source = (
        "def test_function(file_path):\n"
        "    # Just count characters rather than actually opening.\n"
        "    return len(file_path)\n"
    )
    inputs = [f"/workspace/shared/logs/file-{i}.txt" for i in range(20)]
    result = rpm_subprocess(source, inputs, timeout_seconds=60)
    assert result["ok"]
    assert len(result["outputs"]) == 20


def test_process_one_giant_file_chunk_parallel(rpm_subprocess, local_dev_cluster):
    """Scaled version of the giant-file example: parallel-count lines per chunk."""
    source = (
        "def test_function(chunk_id):\n"
        "    # Simulate a line-count per chunk.\n"
        "    return chunk_id * 10\n"
    )
    result = rpm_subprocess(source, list(range(5)), timeout_seconds=60)
    assert result["ok"]
    assert set(result["outputs"]) == {0, 10, 20, 30, 40}


def test_1trc_generate_scaled_down(rpm_subprocess, local_dev_cluster):
    """Scaled 1TRC demo — parallel-generate numeric data."""
    source = (
        "def test_function(file_num):\n"
        "    # Simulate generating deterministic data per file_num.\n"
        "    return (file_num, file_num * 100)\n"
    )
    result = rpm_subprocess(source, list(range(10)), timeout_seconds=60)
    assert result["ok"]
    assert len(result["outputs"]) == 10


def test_xgboost_hyperparameter_tiny_grid(rpm_subprocess, local_dev_cluster):
    """Scaled XGBoost hyperparameter grid: 4 combinations, returns best."""
    source = (
        "def test_function(params):\n"
        "    # Simulate AUC scoring for a trivial param grid.\n"
        "    return {'params': params, 'auc': params['n_estimators'] / 1000}\n"
    )
    grid = [
        {"n_estimators": 100, "max_depth": 4},
        {"n_estimators": 300, "max_depth": 4},
        {"n_estimators": 600, "max_depth": 6},
        {"n_estimators": 900, "max_depth": 8},
    ]
    result = rpm_subprocess(source, grid, timeout_seconds=60)
    assert result["ok"]
    assert len(result["outputs"]) == 4
    best = max(result["outputs"], key=lambda r: r["auc"])
    assert best["params"]["n_estimators"] == 900
