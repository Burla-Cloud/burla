"""
Scenario: `func_gpu`.

GPUs are a headline feature with no coverage beyond a service test that rejects
an invalid GPU name. Local-dev nodes are containers with no GPU, and a real GPU
node costs real money, so this one is opt-in: run it against a remote-dev
cluster with `BURLA_TEST_GPU=1`.
"""

from __future__ import annotations

import os

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.slow]


def test_func_gpu_job_runs_on_a_gpu_node(
    rpm_subprocess, local_dev_cluster, main_http_client
):
    requested_gpu = os.environ.get("BURLA_TEST_GPU")
    if not requested_gpu:
        pytest.skip(
            "GPU nodes cost money and local-dev nodes are containers with no GPU; "
            "set BURLA_TEST_GPU=1 (or =H100 etc, the cheapest machine differs "
            "per cloud) against a remote-dev cluster to run this"
        )
    func_gpu = "A100" if requested_gpu == "1" else requested_gpu

    source = (
        "import subprocess\n"
        "def test_function(x):\n"
        "    result = subprocess.run(\n"
        "        ['nvidia-smi', '--query-gpu=name', '--format=csv,noheader'],\n"
        "        capture_output=True,\n"
        "        text=True,\n"
        "    )\n"
        "    return result.stdout.strip()\n"
    )
    result = rpm_subprocess(
        source, [0], timeout_seconds=1200, func_gpu=func_gpu, func_cpu=4, grow=True
    )
    assert result["ok"], result.get("traceback")
    assert result["outputs"][0], "nvidia-smi reported no GPU inside the worker"

    nodes = main_http_client.get("/v1/cluster/nodes").json()["nodes"]
    assert any(node.get("num_gpus") for node in nodes), "no GPU node was booted"
