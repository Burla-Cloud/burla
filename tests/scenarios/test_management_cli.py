from __future__ import annotations

import json
import os
import selectors
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import pytest


pytestmark = pytest.mark.e2e

EXPECTED_MANAGEMENT_COMMANDS = {
    "auth.status",
    "cluster.restart",
    "cluster.start",
    "cluster.status",
    "cluster.stop",
    "cluster.watch",
    "jobs.calls.list",
    "jobs.calls.logs",
    "jobs.calls.metrics",
    "jobs.calls.show",
    "jobs.cancel",
    "jobs.errors",
    "jobs.list",
    "jobs.metrics",
    "jobs.show",
    "jobs.watch",
    "nodes.list",
    "nodes.logs",
    "nodes.show",
    "settings.show",
    "settings.update",
    "usage.show",
}


def _run_cli(
    executable: Path, env: dict, *arguments: str, timeout: float = 180
) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(executable), *arguments],
        cwd=env["HOME"],
        env=env,
        stdin=subprocess.DEVNULL,
        text=True,
        capture_output=True,
        timeout=timeout,
    )


def _json_result(result: subprocess.CompletedProcess) -> dict:
    assert result.returncode == 0, result.stderr or result.stdout
    assert result.stdout.count("\n") == 1, result.stdout
    document = json.loads(result.stdout)
    assert document["ok"] is True, document
    return document


def _run_json(
    executable: Path, env: dict, coverage: set[str], *arguments: str
) -> dict:
    document = _json_result(_run_cli(executable, env, *arguments))
    coverage.add(document["command"])
    return document["data"]


def _run_ndjson(
    executable: Path, env: dict, coverage: set[str], *arguments: str
) -> list[dict]:
    result = _run_cli(executable, env, *arguments)
    assert result.returncode == 0, result.stderr or result.stdout
    documents = [json.loads(line) for line in result.stdout.splitlines()]
    assert documents[0]["event"] == "stream_start"
    assert documents[-1]["event"] == "stream_end"
    assert all(document["ok"] is True for document in documents)
    coverage.add(documents[0]["command"])
    return documents


def _start_cli(executable: Path, env: dict, *arguments: str) -> subprocess.Popen:
    return subprocess.Popen(
        [str(executable), *arguments],
        cwd=env["HOME"],
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _next_stream_document(process: subprocess.Popen, timeout: float = 15) -> dict:
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    assert selector.select(timeout), "CLI stream did not produce output"
    line = process.stdout.readline()
    assert line, f"CLI stream ended with exit code {process.poll()}"
    return json.loads(line)


def _wait_for_stream_event(
    process: subprocess.Popen,
    coverage: set[str],
    event: str,
    timeout: float = 30,
) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        document = _next_stream_document(process, deadline - time.time())
        coverage.add(document["command"])
        assert document["ok"] is True, document
        if document["event"] == event:
            return document
    raise AssertionError(f"stream did not emit {event!r}")


def _terminate(process: subprocess.Popen) -> None:
    process.terminate()
    try:
        process.wait(5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def _wait_for_job(
    executable: Path,
    env: dict,
    coverage: set[str],
    function_name: str,
    status: str | None = None,
) -> dict:
    for _ in range(120):
        arguments = ["jobs", "list", "--function", function_name]
        if status:
            arguments.extend(["--status", status])
        jobs = _run_json(executable, env, coverage, *arguments)
        if jobs["items"]:
            return jobs["items"][0]
        time.sleep(0.25)
    raise AssertionError(f"job for {function_name!r} did not appear")


@pytest.mark.timeout(300)
def test_installed_management_cli_drives_dashboard_resources(
    tmp_path,
    dashboard_url,
    local_dev_cluster,
    rpm_subprocess,
):
    repository = Path(__file__).resolve().parents[2]
    dist = tmp_path / "dist"
    dist.mkdir()
    subprocess.run(
        ["uv", "build", "--directory", str(repository / "client"), "--out-dir", str(dist)],
        check=True,
        text=True,
        capture_output=True,
    )
    wheel = next(dist.glob("burla-*.whl"))
    venv = tmp_path / "venv"
    subprocess.run(["uv", "venv", str(venv)], check=True, capture_output=True, text=True)
    python = venv / "bin" / "python"
    subprocess.run(
        ["uv", "pip", "install", "--python", str(python), str(wheel)],
        check=True,
        text=True,
        capture_output=True,
    )
    executable = venv / "bin" / "burla"

    home = tmp_path / "home"
    home.mkdir()
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"BURLA_ENVIRONMENT", "BURLA_BACKEND_URL", "PYTHONPATH"}
    }
    env.update(
        {
            "HOME": str(home),
            "BURLA_CLUSTER_DASHBOARD_URL": dashboard_url,
        }
    )
    coverage: set[str] = set()

    version = _run_cli(executable, env, "--version")
    assert version.returncode == 0
    assert version.stdout.strip()

    legacy = _run_cli(executable, env, "config", "set", "cloud", "aws")
    assert legacy.returncode == 0
    assert legacy.stdout.strip() == "cloud = aws"
    legacy = _run_cli(executable, env, "config", "get", "cloud")
    assert legacy.returncode == 0
    assert legacy.stdout.strip() == "aws"

    auth = _run_json(executable, env, coverage, "auth", "status")
    assert auth["reachable"] is True
    assert auth["authenticated"] is True

    cluster = _run_json(executable, env, coverage, "cluster", "status")
    assert cluster["status"] in {"ready", "running"}
    cluster_watch = _start_cli(executable, env, "cluster", "watch")
    try:
        snapshot = _wait_for_stream_event(cluster_watch, coverage, "snapshot")
        assert "cluster" in snapshot["data"]
    finally:
        _terminate(cluster_watch)
    started = _run_json(executable, env, coverage, "cluster", "start")
    assert started["changed"] is False

    settings = _run_json(executable, env, coverage, "settings", "show")
    original_disk = settings["disk_gb"]
    original_timeout = settings["inactivity_timeout_seconds"]
    changed = _run_json(
        executable,
        env,
        coverage,
        "settings",
        "update",
        "--disk-gb",
        str(original_disk + 1),
        "--inactivity-timeout-seconds",
        str(original_timeout + 1),
    )
    assert changed["disk_gb"] == original_disk + 1
    restored = _run_json(
        executable,
        env,
        coverage,
        "settings",
        "update",
        "--disk-gb",
        str(original_disk),
        "--inactivity-timeout-seconds",
        str(original_timeout),
    )
    assert restored["disk_gb"] == original_disk
    assert restored["inactivity_timeout_seconds"] == original_timeout
    restarted = _run_json(executable, env, coverage, "cluster", "restart")
    assert restarted["changed"] is True

    nodes = _run_json(
        executable,
        env,
        coverage,
        "nodes",
        "list",
        "--status",
        "active",
        "--limit",
        "1",
    )
    node = nodes["items"][0]
    node_id = node["node_id"]
    shown_node = _run_json(executable, env, coverage, "nodes", "show", node_id)
    assert shown_node["node_id"] == node_id
    for sort in ("started_at", "ended_at", "status", "machine_type"):
        for order in ("asc", "desc"):
            _run_json(
                executable,
                env,
                coverage,
                "nodes",
                "list",
                "--status",
                "all",
                "--sort",
                sort,
                "--order",
                order,
                "--limit",
                "2",
            )
    for arguments in (
        ("--status", "ready"),
        ("--region", node["region"]),
        ("--started-after", "1970-01-01T00:00:00Z"),
        ("--ended-after", "1970-01-01T00:00:00Z", "--status", "all"),
    ):
        _run_json(executable, env, coverage, "nodes", "list", *arguments)
    node_page = _run_json(
        executable,
        env,
        coverage,
        "nodes",
        "list",
        "--status",
        "all",
        "--limit",
        "1",
    )
    assert node_page["next_cursor"]
    _run_json(
        executable,
        env,
        coverage,
        "nodes",
        "list",
        "--status",
        "all",
        "--limit",
        "1",
        "--cursor",
        node_page["next_cursor"],
    )

    node_logs = _run_json(
        executable, env, coverage, "nodes", "logs", node_id, "--limit", "1"
    )
    assert node_logs["items"]
    assert node_logs["next_cursor"]
    next_node_logs = _run_json(
        executable,
        env,
        coverage,
        "nodes",
        "logs",
        node_id,
        "--after",
        node_logs["next_cursor"],
        "--limit",
        "1",
    )
    assert next_node_logs["items"]
    assert next_node_logs["next_cursor"]
    previous_node_logs = _run_json(
        executable,
        env,
        coverage,
        "nodes",
        "logs",
        node_id,
        "--before",
        next_node_logs["next_cursor"],
        "--limit",
        "1",
    )
    assert previous_node_logs["items"]
    node_log_follow = _start_cli(
        executable, env, "nodes", "logs", node_id, "--follow"
    )
    try:
        followed = _wait_for_stream_event(node_log_follow, coverage, "log")
        assert followed["data"]["message"]
    finally:
        _terminate(node_log_follow)

    marker = f"management_cli_{uuid.uuid4().hex[:8]}"
    source = (
        "import time\n"
        "def test_function(x):\n"
        "    print(f'input={x} line=one')\n"
        "    print(f'input={x} line=two')\n"
        "    print(f'input={x} line=three')\n"
        "    time.sleep(6)\n"
        "    return x * 2\n"
        f"test_function.__name__ = '{marker}'\n"
    )
    job = rpm_subprocess(source, [0, 1, 2, 3], timeout_seconds=60, grow=False)
    assert job["ok"], job.get("traceback")

    completed_job = _wait_for_job(executable, env, coverage, marker, "completed")
    job_id = completed_job["job_id"]

    shown = _run_json(executable, env, coverage, "jobs", "show", job_id)
    assert shown["result_count"] == 4
    assert shown["status"] == "completed"
    watched = _run_ndjson(executable, env, coverage, "jobs", "watch", job_id)
    assert any(document["event"] == "snapshot" for document in watched)

    for _ in range(20):
        calls = _run_json(
            executable,
            env,
            coverage,
            "jobs",
            "calls",
            "list",
            job_id,
            "--sort",
            "input_index",
            "--order",
            "asc",
        )
        if all(call["duration_seconds"] is not None for call in calls["items"]):
            break
        time.sleep(0.5)
    assert [call["input_index"] for call in calls["items"]] == [0, 1, 2, 3]
    assert all(call["attempt_count"] == 1 for call in calls["items"])
    assert all(call["duration_seconds"] >= 5.9 for call in calls["items"])

    call = _run_json(
        executable, env, coverage, "jobs", "calls", "show", job_id, "0"
    )
    assert call["input_index"] == 0
    for sort in (
        "input_index",
        "started_at",
        "ended_at",
        "duration",
        "attempts",
        "status",
        "peak_cpu",
        "peak_memory",
    ):
        for order in ("asc", "desc"):
            _run_json(
                executable,
                env,
                coverage,
                "jobs",
                "calls",
                "list",
                job_id,
                "--sort",
                sort,
                "--order",
                order,
                "--limit",
                "2",
            )
    for arguments in (
        ("--input-index", "0"),
        ("--status", "succeeded"),
        ("--logs-only",),
        ("--has-metrics",),
    ):
        filtered = _run_json(
            executable, env, coverage, "jobs", "calls", "list", job_id, *arguments
        )
        assert filtered["items"]
    call_page = _run_json(
        executable,
        env,
        coverage,
        "jobs",
        "calls",
        "list",
        job_id,
        "--limit",
        "2",
    )
    assert call_page["next_cursor"]
    next_call_page = _run_json(
        executable,
        env,
        coverage,
        "jobs",
        "calls",
        "list",
        job_id,
        "--limit",
        "2",
        "--cursor",
        call_page["next_cursor"],
    )
    assert {item["input_index"] for item in call_page["items"]}.isdisjoint(
        item["input_index"] for item in next_call_page["items"]
    )

    call_logs = _run_json(
        executable,
        env,
        coverage,
        "jobs",
        "calls",
        "logs",
        job_id,
        "0",
        "--limit",
        "1",
    )
    assert "input=0" in call_logs["items"][0]["message"]
    assert call_logs["next_cursor"]
    next_call_logs = _run_json(
        executable,
        env,
        coverage,
        "jobs",
        "calls",
        "logs",
        job_id,
        "0",
        "--after",
        call_logs["next_cursor"],
        "--limit",
        "1",
    )
    assert next_call_logs["items"]
    assert next_call_logs["next_cursor"]
    previous_call_logs = _run_json(
        executable,
        env,
        coverage,
        "jobs",
        "calls",
        "logs",
        job_id,
        "0",
        "--before",
        next_call_logs["next_cursor"],
        "--limit",
        "1",
    )
    assert previous_call_logs["items"]

    for _ in range(40):
        metrics = _run_json(executable, env, coverage, "jobs", "metrics", job_id)
        call_metrics = _run_json(
            executable, env, coverage, "jobs", "calls", "metrics", job_id, "0"
        )
        if metrics["has_metrics"] and call_metrics["has_metrics"]:
            break
        time.sleep(0.25)
    assert metrics["points"]
    assert call_metrics["points"]

    raw_job_metrics = _run_ndjson(
        executable,
        env,
        coverage,
        "jobs",
        "metrics",
        job_id,
        "--raw",
        "--limit",
        "1",
    )
    raw_job_points = [
        document for document in raw_job_metrics if document["event"] == "metric"
    ]
    assert len(raw_job_points) == 1
    resumed_job_metrics = _run_ndjson(
        executable,
        env,
        coverage,
        "jobs",
        "metrics",
        job_id,
        "--raw",
        "--limit",
        "1",
        "--cursor",
        raw_job_points[0]["cursor"],
    )
    assert any(document["event"] == "metric" for document in resumed_job_metrics)
    raw_call_metrics = _run_ndjson(
        executable,
        env,
        coverage,
        "jobs",
        "calls",
        "metrics",
        job_id,
        "0",
        "--raw",
        "--limit",
        "1",
    )
    raw_call_points = [
        document for document in raw_call_metrics if document["event"] == "metric"
    ]
    assert len(raw_call_points) == 1
    resumed_call_metrics = _run_ndjson(
        executable,
        env,
        coverage,
        "jobs",
        "calls",
        "metrics",
        job_id,
        "0",
        "--raw",
        "--limit",
        "1",
        "--cursor",
        raw_call_points[0]["cursor"],
    )
    assert any(document["event"] == "metric" for document in resumed_call_metrics)

    failure_marker = f"management_cli_failure_{uuid.uuid4().hex[:8]}"
    failure_source = (
        "import time\n"
        "def test_function(x):\n"
        "    time.sleep(1)\n"
        "    if x == 0:\n"
        "        raise ValueError('alpha failure')\n"
        "    raise RuntimeError('beta failure')\n"
        f"test_function.__name__ = '{failure_marker}'\n"
    )
    failed = rpm_subprocess(
        failure_source,
        [0, 1],
        timeout_seconds=60,
        grow=False,
        max_parallelism=2,
    )
    assert failed["ok"] is False
    failed_job = _wait_for_job(executable, env, coverage, failure_marker, "failed")
    failed_job_id = failed_job["job_id"]
    for _ in range(40):
        error_page = _run_json(
            executable,
            env,
            coverage,
            "jobs",
            "errors",
            failed_job_id,
            "--limit",
            "1",
        )
        if error_page["has_more"]:
            break
        time.sleep(0.25)
    assert error_page["next_cursor"]
    second_error_page = _run_json(
        executable,
        env,
        coverage,
        "jobs",
        "errors",
        failed_job_id,
        "--limit",
        "1",
        "--cursor",
        error_page["next_cursor"],
    )
    assert second_error_page["items"]
    failed_index = error_page["items"][0]["sample_input_indexes"][0]
    failed_calls = _run_json(
        executable,
        env,
        coverage,
        "jobs",
        "calls",
        "list",
        failed_job_id,
        "--failed-only",
        "--status",
        "failed",
    )
    assert failed_calls["items"]
    error_logs = _run_json(
        executable,
        env,
        coverage,
        "jobs",
        "calls",
        "logs",
        failed_job_id,
        str(failed_index),
        "--errors-only",
    )
    assert error_logs["items"]
    assert all(item["is_error"] for item in error_logs["items"])

    cancel_marker = f"management_cli_cancel_{uuid.uuid4().hex[:8]}"
    cancel_source = (
        "import time\n"
        "def test_function(x):\n"
        "    time.sleep(30)\n"
        "    return x\n"
        f"test_function.__name__ = '{cancel_marker}'\n"
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            rpm_subprocess,
            cancel_source,
            list(range(8)),
            timeout_seconds=90,
            grow=False,
            max_parallelism=1,
        )
        running_job = _wait_for_job(
            executable, env, coverage, cancel_marker, "running"
        )
        canceled_job_id = running_job["job_id"]
        job_watch = _start_cli(
            executable, env, "jobs", "watch", canceled_job_id
        )
        try:
            watch_snapshot = _wait_for_stream_event(job_watch, coverage, "snapshot")
            assert watch_snapshot["data"]["status"] == "running"
            canceled = _run_json(
                executable, env, coverage, "jobs", "cancel", canceled_job_id
            )
            assert canceled["changed"] is True
            stream_end = _wait_for_stream_event(job_watch, coverage, "stream_end")
            assert stream_end["data"]["status"] == "canceled"
            assert job_watch.wait(10) == 8
        finally:
            if job_watch.poll() is None:
                _terminate(job_watch)
        canceled_result = future.result()
    assert canceled_result["ok"] is False
    canceled_show = _run_json(
        executable, env, coverage, "jobs", "show", canceled_job_id
    )
    assert canceled_show["status"] == "canceled"
    not_run = _run_json(
        executable,
        env,
        coverage,
        "jobs",
        "calls",
        "list",
        canceled_job_id,
        "--status",
        "not_run",
    )
    assert not_run["items"]

    for sort in (
        "started_at",
        "ended_at",
        "duration",
        "status",
        "input_count",
        "result_count",
        "failed_count",
    ):
        for order in ("asc", "desc"):
            _run_json(
                executable,
                env,
                coverage,
                "jobs",
                "list",
                "--sort",
                sort,
                "--order",
                order,
                "--limit",
                "2",
            )
    for arguments in (
        ("--status", "completed"),
        ("--user", completed_job["user"]),
        ("--function", marker),
        ("--started-after", "1970-01-01T00:00:00Z"),
        ("--started-before", "2999-01-01T00:00:00Z"),
    ):
        filtered_jobs = _run_json(
            executable, env, coverage, "jobs", "list", *arguments
        )
        assert filtered_jobs["items"]
    job_page = _run_json(
        executable, env, coverage, "jobs", "list", "--limit", "1"
    )
    assert job_page["next_cursor"]
    _run_json(
        executable,
        env,
        coverage,
        "jobs",
        "list",
        "--limit",
        "1",
        "--cursor",
        job_page["next_cursor"],
    )
    _run_json(
        executable,
        env,
        coverage,
        "nodes",
        "list",
        "--job",
        job_id,
        "--status",
        "all",
    )

    month = datetime.now(timezone.utc).strftime("%Y-%m")
    usage = _run_json(
        executable, env, coverage, "usage", "show", "--month", month
    )
    assert usage["month"] == month

    stopped = _run_json(executable, env, coverage, "cluster", "stop")
    assert stopped["changed"] is True
    stopped_cluster = _run_json(executable, env, coverage, "cluster", "status")
    assert stopped_cluster["status"] == "off"
    started = _run_json(executable, env, coverage, "cluster", "start")
    assert started["changed"] is True
    live_cluster = _run_json(executable, env, coverage, "cluster", "status")
    assert live_cluster["status"] in {"ready", "running"}

    assert coverage == EXPECTED_MANAGEMENT_COMMANDS
