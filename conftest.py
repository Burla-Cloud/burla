"""
Repo-root conftest for the Burla test suite. All tiers (unit, service, e2e, chaos)
share these helpers so the subprocess-isolation pattern, cluster-readiness gate, and
GCP/Firestore integration live in one place.

All service / e2e / chaos tests assume `make local-dev` is running against the
`burla-test` GCP project. A readiness gate fixture verifies this before any test
that uses it runs.
"""

from __future__ import annotations

import base64
import contextlib
import io
import json
import multiprocessing as mp
import os
import queue
import signal
import socket
import sys
import time
import traceback
import uuid
from pathlib import Path
from typing import Any, Callable, Iterable

import pytest

DASHBOARD_URL = os.environ.get("BURLA_CLUSTER_DASHBOARD_URL", "http://localhost:5001")
EXPECTED_GCP_PROJECT = os.environ.get("BURLA_TEST_PROJECT", "burla-test")
READINESS_TIMEOUT_SEC = 30
CLEAN_CLUSTER_TIMEOUT_SEC = 120


def _port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def _main_service_reachable() -> bool:
    # Honor the DASHBOARD_URL env override so dev-VM tunnels on non-5001
    # ports (e.g. 15001 for agent 01) satisfy the readiness gate.
    from urllib.parse import urlparse

    parsed = urlparse(DASHBOARD_URL)
    host = parsed.hostname or "localhost"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    return _port_open(host, port)


def _resolve_active_gcp_project() -> str | None:
    project_from_env = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if project_from_env:
        return project_from_env
    try:
        import subprocess

        result = subprocess.run(
            ["gcloud", "config", "get-value", "project"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        value = result.stdout.strip()
        return value or None
    except Exception:
        return None


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "unit: pure unit test, no cluster needed")
    config.addinivalue_line("markers", "service: service-level test, requires make local-dev")
    config.addinivalue_line("markers", "e2e: full end-to-end test, requires make local-dev")
    config.addinivalue_line("markers", "chaos: failure-injection test, requires make local-dev")
    config.addinivalue_line("markers", "slow: slow test (>30s)")
    config.addinivalue_line("markers", "dashboard: requires Playwright, browser, and dashboard UI")


def _active_node_docs(firestore_db) -> list[dict[str, Any]]:
    from google.cloud.firestore_v1.base_query import FieldFilter

    docs = (
        firestore_db.collection("nodes")
        .where(filter=FieldFilter("status", "in", ["READY", "BOOTING", "RUNNING", "FAILED"]))
        .stream()
    )
    return [doc.to_dict() for doc in docs]


def _test_runner_node_url(host: str) -> str:
    if host.startswith("http://node_"):
        port = host.rsplit(":", 1)[-1]
        return f"http://localhost:{port}"
    return host


def _ready_nodes_unreachable(state: dict[str, Any], auth_headers: dict[str, str]) -> str | None:
    import requests

    for node in state["ready_nodes"]:
        url = _test_runner_node_url(node["host"])
        try:
            resp = requests.get(url, headers=auth_headers, timeout=2)
        except requests.RequestException as e:
            return f"{node['instance_name']} unreachable at {url}: {e}"
        if resp.status_code != 200:
            return f"{node['instance_name']} returned {resp.status_code} at {url}"
    return None


def _expected_ready_node_count(auth_headers: dict[str, str]) -> int:
    import requests

    resp = requests.get(f"{DASHBOARD_URL}/v1/settings", headers=auth_headers, timeout=5)
    resp.raise_for_status()
    return resp.json()["machineQuantity"]


def _cluster_dirty_reason(
    state: dict[str, Any],
    active_nodes: list[dict[str, Any]],
    auth_headers: dict[str, str],
    expected_ready_nodes: int,
) -> str | None:
    if state["booting_count"]:
        return f"{state['booting_count']} node(s) still booting"
    if state["running_count"]:
        return f"{state['running_count']} node(s) still running a job"
    if len(state["ready_nodes"]) < expected_ready_nodes:
        return f"{len(state['ready_nodes'])}/{expected_ready_nodes} ready nodes"

    dirty_nodes = [
        node
        for node in active_nodes
        if node.get("status") != "READY" or node.get("current_job") or node.get("reserved_for_job")
    ]
    if dirty_nodes:
        summaries = [
            f"{node.get('instance_name')}:{node.get('status')}"
            for node in dirty_nodes
        ]
        return "dirty node docs: " + ", ".join(summaries)

    active_ready_names = {node["instance_name"] for node in active_nodes}
    visible_ready_names = {node["instance_name"] for node in state["ready_nodes"]}
    if active_ready_names != visible_ready_names:
        return "ready node cache does not match active READY docs"

    reachability_issue = _ready_nodes_unreachable(state, auth_headers)
    if reachability_issue is not None:
        return reachability_issue

    return None


def _wait_for_clean_cluster(
    firestore_db,
    auth_headers: dict[str, str],
    expected_ready_nodes: int,
    timeout: float,
) -> dict[str, Any]:
    deadline = time.time() + timeout
    last_reason = "cluster not checked"
    while time.time() < deadline:
        state = _cluster_state_via_http()
        active_nodes = _active_node_docs(firestore_db)
        last_reason = _cluster_dirty_reason(
            state,
            active_nodes,
            auth_headers,
            expected_ready_nodes,
        )
        if last_reason is None:
            return state
        time.sleep(0.5)
    raise AssertionError(f"cluster did not become clean within {timeout}s: {last_reason}")


def _restart_cluster(auth_headers: dict[str, str]) -> None:
    import requests

    resp = requests.post(
        f"{DASHBOARD_URL}/v1/cluster/restart",
        headers=auth_headers,
        timeout=10,
    )
    resp.raise_for_status()


@pytest.fixture
def local_dev_cluster(firestore_db, burla_auth_headers) -> dict[str, Any]:
    """
    Readiness gate for service / e2e / chaos tiers.

    Fails fast with an actionable message when `make local-dev` isn't running,
    and resets the cluster only when the previous test left dirty state behind.
    Returns basic cluster metadata the rest of the tests need.
    """
    if not _main_service_reachable():
        pytest.skip(
            f"main_service is not reachable at {DASHBOARD_URL}. "
            "These tests must run on a dev VM, not your laptop — see "
            "client/tests/README.md. Start the cluster on the VM with "
            "`scripts/dev_vm_start.sh --agent <id> --mode local-dev`, run "
            "`scripts/dev_vm_tunnel.sh --agent <id>`, and set "
            "BURLA_CLUSTER_DASHBOARD_URL to the tunnel URL (or run tests "
            "directly on the VM, which is recommended)."
        )

    project = _resolve_active_gcp_project()
    if project != EXPECTED_GCP_PROJECT:
        pytest.skip(
            f"Expected gcloud project `{EXPECTED_GCP_PROJECT}`, got `{project}`. "
            f"Run `gcloud config set project {EXPECTED_GCP_PROJECT}` first."
        )

    import requests

    deadline = time.time() + READINESS_TIMEOUT_SEC
    last_err: str | None = None
    while time.time() < deadline:
        try:
            resp = requests.get(f"{DASHBOARD_URL}/version", timeout=2)
            if resp.status_code == 200:
                version_info = resp.json()
                break
        except Exception as e:
            last_err = str(e)
        time.sleep(0.5)
    else:
        pytest.skip(f"main_service /version not reachable within {READINESS_TIMEOUT_SEC}s: {last_err}")

    expected_ready_nodes = _expected_ready_node_count(burla_auth_headers)
    state = _cluster_state_via_http()
    active_nodes = _active_node_docs(firestore_db)
    if _cluster_dirty_reason(
        state,
        active_nodes,
        burla_auth_headers,
        expected_ready_nodes,
    ) is not None:
        _restart_cluster(burla_auth_headers)
        state = _wait_for_clean_cluster(
            firestore_db,
            burla_auth_headers,
            expected_ready_nodes,
            CLEAN_CLUSTER_TIMEOUT_SEC,
        )

    return {
        "url": DASHBOARD_URL,
        "project_id": project,
        "version": version_info.get("version"),
        "state": state,
    }


@pytest.fixture(autouse=True)
def clean_local_dev_cluster_before_cluster_tests(request):
    cluster_markers = ("service", "e2e", "chaos")
    if any(request.node.get_closest_marker(marker) for marker in cluster_markers):
        request.getfixturevalue("local_dev_cluster")


def _cluster_state_via_http() -> dict[str, Any]:
    import requests

    try:
        resp = requests.get(f"{DASHBOARD_URL}/v1/cluster/state", timeout=5)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return {"booting_count": 0, "running_count": 0, "ready_nodes": []}


@pytest.fixture
def isolated_job_id() -> Callable[[str], str]:
    """
    Returns a factory that mints a unique job_id matching the client's format:
    `{function_name}-{urlsafe_base64_9_bytes}`.
    """

    def _factory(function_name: str = "test") -> str:
        uid = base64.urlsafe_b64encode(uuid.uuid4().bytes[:9]).decode()
        return f"{function_name}-{uid}"

    return _factory


@pytest.fixture(scope="session")
def firestore_db():
    """
    A Firestore client pointed at database `burla` in the active project.
    Tests that use this fixture own the responsibility of cleaning up any docs
    they write — see `cleanup_job` / `cleanup_node`.
    """
    try:
        from google.cloud import firestore
    except Exception as e:
        pytest.skip(f"google-cloud-firestore not installed: {e}")
    return firestore.Client(database="burla")


@pytest.fixture
def cleanup_job(firestore_db):
    created: list[str] = []

    def _register(job_id: str) -> str:
        created.append(job_id)
        return job_id

    yield _register

    for job_id in created:
        try:
            for sub in ("logs", "assigned_nodes"):
                for doc in firestore_db.collection("jobs").document(job_id).collection(sub).stream():
                    doc.reference.delete()
            firestore_db.collection("jobs").document(job_id).delete()
        except Exception:
            pass


@pytest.fixture
def cleanup_node(firestore_db):
    created: list[str] = []

    def _register(instance_name: str) -> str:
        created.append(instance_name)
        return instance_name

    yield _register

    for instance_name in created:
        try:
            for doc in firestore_db.collection("nodes").document(instance_name).collection("logs").stream():
                doc.reference.delete()
            firestore_db.collection("nodes").document(instance_name).delete()
        except Exception:
            pass


@pytest.fixture(scope="session")
def main_http_client(burla_auth_headers):
    """
    Session-scoped httpx client for the main_service. Carries real burla
    auth headers so main_service's outbound calls to nodes (which still
    validate auth) use a token the node's authorized_users recognizes.
    """
    try:
        import httpx
    except Exception as e:
        pytest.skip(f"httpx not installed: {e}")

    with httpx.Client(
        base_url=DASHBOARD_URL,
        timeout=30,
        headers=burla_auth_headers,
    ) as client:
        yield client


@pytest.fixture(scope="session")
def main_async_client():
    import asyncio

    try:
        import httpx
    except Exception as e:
        pytest.skip(f"httpx not installed: {e}")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    client = httpx.AsyncClient(base_url=DASHBOARD_URL, timeout=30)

    yield client

    loop.run_until_complete(client.aclose())
    loop.close()


@pytest.fixture(scope="session")
def burla_auth_headers() -> dict[str, str]:
    """
    Auth headers the pypi client would send. Nodes validate against
    `authorized_users` populated from backend.burla.dev — we forward the
    user's `burla login` credentials.
    """
    try:
        from burla._auth import get_auth_headers, AuthException
    except Exception as e:
        pytest.skip(f"burla not importable: {e}")

    try:
        return get_auth_headers()
    except AuthException:
        pytest.skip(
            "Burla credentials missing. Run `burla login --no_browser=True`, open the URL, "
            "and authorize before running node-level tests."
        )


@pytest.fixture
def node_http_client(main_http_client, burla_auth_headers):
    """
    Factory for per-node httpx clients. A node's `host` field is
    `http://node_xxx:8081` on the local-burla-cluster Docker network. From
    the host we reach nodes via `http://localhost:<port>` since each node
    publishes its port. This fixture handles that rewriting and attaches
    the standard burla auth headers.
    """
    import httpx

    state = main_http_client.get("/v1/cluster/state").json()

    def _factory(instance_name: str | None = None):
        nodes = state.get("ready_nodes") or []
        if not nodes:
            pytest.skip("No READY nodes to talk to.")
        target = nodes[0]
        if instance_name:
            target = next((n for n in nodes if n["instance_name"] == instance_name), None)
            if target is None:
                pytest.skip(f"Node {instance_name} not in ready_nodes.")
        host = target["host"]
        if host.startswith("http://node_"):
            port = host.rsplit(":", 1)[-1]
            host = f"http://localhost:{port}"
        return httpx.Client(base_url=host, timeout=30, headers=burla_auth_headers)

    return _factory


@pytest.fixture
def any_ready_node(main_http_client):
    state = main_http_client.get("/v1/cluster/state").json()
    nodes = state.get("ready_nodes") or []
    if not nodes:
        pytest.skip("No READY nodes.")
    return nodes[0]


# ---------------------------------------------------------------------------
# Subprocess isolation — the existing pattern from client/tests/test.py,
# generalized so every client-side test can use it without duplicating code.
#
# The actual subprocess body lives in tests/_rpm_subprocess_helper.py because
# mp.get_context('spawn') re-imports the target function's module in the
# child; `conftest` isn't importable as a regular package.
# ---------------------------------------------------------------------------


_HELPER_DIR = Path(__file__).parent / "tests"
if str(_HELPER_DIR) not in sys.path:
    sys.path.insert(0, str(_HELPER_DIR))


def run_rpm_in_subprocess(
    function_source: str,
    inputs: list,
    timeout_seconds: float = 60,
    env_overrides: dict | None = None,
    signal_after_seconds: float | None = None,
    signal_name: str = "SIGINT",
    **rpm_kwargs: Any,
) -> dict:
    """
    Spawn a subprocess that runs `remote_parallel_map(...)` against the
    running local-dev cluster. Returns a result dict with `ok`, `outputs`,
    `stdout`, `stderr`, and (on failure) exception info + traceback.
    """
    env_overrides = env_overrides or {}
    rpm_kwargs.setdefault("spinner", False)
    rpm_kwargs.setdefault("grow", True)

    from _rpm_subprocess_helper import run_rpm_in_subprocess as _target

    context = mp.get_context("spawn")
    result_queue = context.Queue()
    process = context.Process(
        target=_target,
        args=(result_queue, function_source, inputs, rpm_kwargs, env_overrides, DASHBOARD_URL),
    )
    process.start()

    if signal_after_seconds is not None:
        sig = getattr(signal, signal_name)
        start = time.time()
        while time.time() - start < signal_after_seconds:
            if not process.is_alive():
                break
            time.sleep(0.1)
        if process.is_alive():
            try:
                os.kill(process.pid, sig)
            except ProcessLookupError:
                pass

    process.join(timeout_seconds)

    if process.is_alive():
        process.terminate()
        process.join(5)
        if process.is_alive():
            process.kill()
            process.join()
        pytest.fail(f"test did not finish within {timeout_seconds}s")

    try:
        result = result_queue.get(timeout=1)
    except queue.Empty:
        pytest.fail(
            f"test subprocess ended without returning a result (exitcode={process.exitcode})"
        )

    return result


@pytest.fixture
def rpm_subprocess():
    return run_rpm_in_subprocess


@pytest.fixture
def ctrl_c_after():
    """
    Helper for chaos tests that need to send SIGINT after N seconds. Usage:
        result = ctrl_c_after(source, inputs, delay_s=2, **kwargs)
    """

    def _send(function_source: str, inputs: list, delay_s: float, **kwargs: Any) -> dict:
        return run_rpm_in_subprocess(
            function_source,
            inputs,
            signal_after_seconds=delay_s,
            signal_name="SIGINT",
            timeout_seconds=60,
            **kwargs,
        )

    return _send


# ---------------------------------------------------------------------------
# Polling helper — every test that needs to wait for a Firestore / cluster
# state change should use this instead of ad-hoc sleeps.
# ---------------------------------------------------------------------------


def wait_for(
    predicate: Callable[[], Any],
    timeout: float = 30,
    interval: float = 0.25,
    message: str = "predicate never became truthy",
) -> Any:
    deadline = time.time() + timeout
    last: Any = None
    while time.time() < deadline:
        try:
            last = predicate()
        except Exception as e:
            last = e
        if last and not isinstance(last, Exception):
            return last
        time.sleep(interval)
    raise AssertionError(f"wait_for timed out after {timeout}s: {message} (last={last!r})")


@pytest.fixture
def wait_for_fixture():
    return wait_for


# ---------------------------------------------------------------------------
# Test-data fixtures — small tokens / flags / strings reused across files.
# ---------------------------------------------------------------------------


@pytest.fixture
def burla_version_current() -> str:
    try:
        from burla import __version__

        return __version__
    except Exception:
        pytest.skip("burla not importable")


# ---------------------------------------------------------------------------
# Auto-skip service/e2e/chaos tests if the local-dev cluster isn't running.
# Unit tests never require it.
# ---------------------------------------------------------------------------


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    requires_cluster = {"service", "e2e", "chaos"}
    cluster_up = _main_service_reachable()
    for item in items:
        markers = {m.name for m in item.iter_markers()}
        if markers & requires_cluster and not cluster_up:
            item.add_marker(
                pytest.mark.skip(reason=f"local-dev cluster not running at {DASHBOARD_URL}")
            )
