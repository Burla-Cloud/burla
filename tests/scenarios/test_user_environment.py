"""
Scenario: the environment a UDF actually runs in.

Every other e2e test runs a dependency-free UDF with one CPU on the stock
image, so nothing covered the three things the docs promise about the remote
environment: packages the local process imported are installed on the worker,
local modules the UDF calls are shipped with it, and `/workspace/shared` is one
filesystem shared by every worker. These also carry `image=` and `func_cpu=2`,
which no e2e test passed before.
"""

from __future__ import annotations

import uuid

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.slow]

# Not the cluster's configured image, so the job only runs if `image=` really
# boots (or selects) a node running it.
CUSTOM_IMAGE = "python:3.12-slim"


def test_package_install_custom_image_and_shared_filesystem(
    rpm_subprocess, local_dev_cluster, main_http_client
):
    import pandas

    shared_path = f"/workspace/shared/env-{uuid.uuid4().hex[:8]}.csv"
    write_source = (
        "import pandas\n"
        f"SHARED_PATH = {shared_path!r}\n"
        "def test_function(row_count):\n"
        "    frame = pandas.DataFrame({'n': list(range(row_count))})\n"
        "    frame.to_csv(SHARED_PATH, index=False)\n"
        "    return pandas.__version__\n"
    )
    write = rpm_subprocess(
        write_source,
        [5],
        timeout_seconds=300,
        func_cpu=2,
        image=CUSTOM_IMAGE,
        grow=True,
    )
    assert write["ok"], write.get("traceback")
    # `python:3.12-slim` has no pandas: matching the local version proves the
    # client detected it and the worker installed that exact version.
    assert write["outputs"] == [pandas.__version__]

    images = {
        container["image"]
        for node in main_http_client.get("/v1/cluster/nodes").json()["nodes"]
        for container in node["containers"]
    }
    assert CUSTOM_IMAGE in images, f"no node ran the requested image: {images}"

    # A second job, on the cluster's default image, must see the same file.
    read_source = (
        "import pandas\n"
        f"SHARED_PATH = {shared_path!r}\n"
        "def test_function(_):\n"
        "    return int(pandas.read_csv(SHARED_PATH)['n'].sum())\n"
    )
    read = rpm_subprocess(read_source, [0], timeout_seconds=300, grow=True)
    assert read["ok"], read.get("traceback")
    assert read["outputs"] == [sum(range(5))]


def test_udf_can_call_a_local_module(rpm_subprocess, local_dev_cluster, tmp_path):
    """Local modules are pickled by value, so a UDF can call code the worker
    cannot import. The directory name is deliberate: modules were once
    classified as Burla's own, and skipped, on any path containing "burla"."""
    module_dir = tmp_path / "burla_helpers"
    module_dir.mkdir()
    (module_dir / "udf_helpers.py").write_text(
        "import threading\n"
        "\n"
        "GREETING = 'hello-from-a-local-module'\n"
        "UNRELATED_STATE = threading.Lock()\n"
        "\n"
        "def label(value):\n"
        "    return f'{GREETING}-{value}'\n"
    )

    source = (
        "import sys\n"
        f"sys.path.insert(0, {str(module_dir)!r})\n"
        "import udf_helpers\n"
        "def test_function(x):\n"
        "    return udf_helpers.label(x)\n"
    )
    result = rpm_subprocess(source, [1, 2], timeout_seconds=300, grow=True)
    assert result["ok"], result.get("traceback")
    assert sorted(result["outputs"]) == [
        "hello-from-a-local-module-1",
        "hello-from-a-local-module-2",
    ]
