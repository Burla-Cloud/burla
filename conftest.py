"""
Repo-root conftest for the Burla test suite. Both tiers (service and e2e)
share these helpers so the subprocess-isolation pattern and cluster-readiness gate
live in one place.

All service and e2e tests assume a dev cluster is running for this
checkout (`make local-dev`, or `make remote-dev` for real cloud nodes). A
readiness gate fixture verifies this before any test that uses it runs. All
cluster state is read/written over the main_service HTTP API - there is no
database to inspect.
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
import ssl
import sys
import time
import traceback
import uuid
from pathlib import Path
from typing import Any, Callable, Iterable

import pytest

# The test suite must never touch the production backend.
if os.environ.get("BURLA_BACKEND_URL") == "https://backend.burla.dev":
    raise RuntimeError("The test suite cannot use the production Burla backend.")
os.environ["BURLA_ENVIRONMENT"] = "test"
os.environ.setdefault("BURLA_BACKEND_URL", "https://test.backend.burla.dev")

# `make test*` passes this checkout's head port so tests never reach another
# checkout's cluster on the same machine.
DASHBOARD_URL = os.environ.get("BURLA_CLUSTER_DASHBOARD_URL", "http://localhost:5001")
REQUIRE_CLUSTER = os.environ.get("BURLA_REQUIRE_CLUSTER") == "1"
READINESS_TIMEOUT_SEC = 30
# local-dev containers reset in ~20s; real VMs need minutes. Remote e2e runs
# (BURLA_CLUSTER_DASHBOARD_URL pointed at a live cluster) should raise this.
CLEAN_CLUSTER_TIMEOUT_SEC = int(os.environ.get("BURLA_CLEAN_CLUSTER_TIMEOUT_SEC", 120))
# How long the readiness gate lets post-job dirt (cancel propagation, worker
# reboots) settle on its own before paying for a full cluster restart.
SETTLE_TIMEOUT_SEC = 30
# Whether the head under test is a local-dev head; resolved on first use.
_LOCAL_DEV_MODE: bool | None = None


def _port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def _request_headers() -> dict[str, str]:
    """Burla auth headers when available. local-dev ignores them; a prod-mode
    head (remote e2e runs) requires them on every endpoint."""
    try:
        from burla._auth import get_auth_headers

        return get_auth_headers()
    except Exception:
        return {}


def _main_service_reachable() -> bool:
    # Honor the DASHBOARD_URL env override: each checkout's cluster publishes
    # the head on its own port, so 5001 is only a fallback.
    from urllib.parse import urlparse

    parsed = urlparse(DASHBOARD_URL)
    host = parsed.hostname or "localhost"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    return _port_open(host, port)


def _local_dashboard(url: str) -> bool:
    from urllib.parse import urlparse

    return (urlparse(url).hostname or "") in {"localhost", "127.0.0.1"}


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers", "service: service-level test, requires make local-dev"
    )
    config.addinivalue_line(
        "markers", "e2e: full end-to-end test, requires make local-dev"
    )
    config.addinivalue_line("markers", "slow: slow test (>30s)")
    config.addinivalue_line(
        "markers", "dashboard: requires Playwright, browser, and dashboard UI"
    )
    config.addinivalue_line(
        "markers",
        "remote_dev: only meaningful against real VMs, requires make remote-dev",
    )


def _active_node_docs() -> list[dict[str, Any]]:
    import requests

    resp = requests.get(
        f"{DASHBOARD_URL}/v1/cluster/nodes", headers=_request_headers(), timeout=5
    )
    resp.raise_for_status()
    return resp.json()["nodes"]


def _test_runner_node_url(host: str) -> str:
    if host.startswith("http://node_"):
        port = host.rsplit(":", 1)[-1]
        return f"http://localhost:{port}"
    return host


def _ready_nodes_unreachable(
    state: dict[str, Any], auth_headers: dict[str, str]
) -> str | None:
    import httpx

    for node in state["ready_nodes"]:
        url = _test_runner_node_url(node["host"])
        verify = (
            ssl.create_default_context(cadata=state["cluster_ca"])
            if url.startswith("https://")
            else True
        )
        try:
            resp = httpx.get(url, headers=auth_headers, timeout=2, verify=verify)
        except httpx.HTTPError as e:
            return f"{node['instance_name']} unreachable at {url}: {e}"
        if resp.status_code != 200:
            return f"{node['instance_name']} returned {resp.status_code} at {url}"
    return None


def _head_in_local_dev_mode() -> bool:
    """local-dev heads mint no cluster CA: their nodes are containers reached
    over plain http. Every other head has one."""
    global _LOCAL_DEV_MODE
    if _LOCAL_DEV_MODE is None:
        _LOCAL_DEV_MODE = _cluster_state_via_http().get("cluster_ca") is None
    return _LOCAL_DEV_MODE


def _set_local_dev_node_quantity(quantity: int) -> None:
    import requests

    resp = requests.post(
        f"{DASHBOARD_URL}/v1/local-dev/node-quantity/{quantity}",
        headers=_request_headers(),
        timeout=10,
    )
    resp.raise_for_status()


def _wait_for_ready_nodes(n: int, timeout: float) -> list[dict[str, Any]]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ready_nodes = _cluster_state_via_http()["ready_nodes"]
        if len(ready_nodes) >= n:
            return ready_nodes
        time.sleep(0.5)
    raise AssertionError(f"cluster never reached {n} READY nodes within {timeout}s")


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
        if node.get("status") != "READY"
        or node.get("current_job")
        or node.get("reserved_for_job")
    ]
    if dirty_nodes:
        summaries = [
            f"{node.get('instance_name')}:{node.get('status')}" for node in dirty_nodes
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
    auth_headers: dict[str, str],
    expected_ready_nodes: int,
    timeout: float,
) -> dict[str, Any]:
    deadline = time.time() + timeout
    last_reason = "cluster not checked"
    while time.time() < deadline:
        state = _cluster_state_via_http()
        active_nodes = _active_node_docs()
        last_reason = _cluster_dirty_reason(
            state,
            active_nodes,
            auth_headers,
            expected_ready_nodes,
        )
        if last_reason is None:
            return state
        time.sleep(0.5)
    raise AssertionError(
        f"cluster did not become clean within {timeout}s: {last_reason}"
    )


def _restart_cluster(auth_headers: dict[str, str]) -> set[str]:
    import requests

    old_active_names = {
        node["instance_name"]
        for node in _active_node_docs()
        if node.get("status") in {"BOOTING", "READY", "RUNNING"}
    }
    resp = requests.post(
        f"{DASHBOARD_URL}/v1/cluster/restart",
        headers=auth_headers,
        timeout=10,
    )
    resp.raise_for_status()
    return old_active_names


def _wait_for_replacement_nodes(old_active_names: set[str], timeout: float) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        current_active_names = {
            node["instance_name"]
            for node in _active_node_docs()
            if node.get("status") in {"BOOTING", "READY", "RUNNING"}
        }
        if old_active_names.isdisjoint(current_active_names):
            return
        time.sleep(0.5)
    raise AssertionError("cluster restart did not replace the previous nodes")


@pytest.fixture
def local_dev_cluster(burla_auth_headers) -> dict[str, Any]:
    """
    Readiness gate for service and e2e tiers.

    Fails fast with an actionable message when `make local-dev` isn't running,
    and resets the cluster only when the previous test left dirty state behind.
    Returns basic cluster metadata the rest of the tests need.
    """
    if not _main_service_reachable():
        message = (
            f"main_service is not reachable at {DASHBOARD_URL}. Start this "
            "checkout's dev cluster in another terminal with `make local-dev` "
            "(or `make remote-dev` for real cloud nodes); see "
            "client/tests/README.md."
        )
        pytest.fail(message) if REQUIRE_CLUSTER else pytest.skip(message)

    # These tiers restart and mutate the cluster, so refuse to run against
    # anything but a dev cluster on this machine.
    if not _local_dashboard(DASHBOARD_URL):
        message = (
            f"{DASHBOARD_URL} is not a local dev cluster. service/e2e "
            "tests restart and mutate the cluster and must never run against a "
            "deployed one."
        )
        pytest.fail(message) if REQUIRE_CLUSTER else pytest.skip(message)

    import requests

    deadline = time.time() + READINESS_TIMEOUT_SEC
    last_err: str | None = None
    while time.time() < deadline:
        try:
            resp = requests.get(
                f"{DASHBOARD_URL}/version", headers=_request_headers(), timeout=2
            )
            if resp.status_code == 200:
                version_info = resp.json()
                break
        except Exception as e:
            last_err = str(e)
        time.sleep(0.5)
    else:
        message = f"main_service /version not reachable within {READINESS_TIMEOUT_SEC}s: {last_err}"
        pytest.fail(message) if REQUIRE_CLUSTER else pytest.skip(message)

    import burla

    if version_info.get("version") != burla.__version__:
        pytest.fail(
            f"Expected head version `{burla.__version__}`, "
            f"got `{version_info.get('version')}`"
        )

    expected_ready_nodes = _expected_ready_node_count(burla_auth_headers)
    # A restart costs a whole node boot, so it is the last resort: dirt left
    # by the previous test usually settles on its own within seconds.
    try:
        state = _wait_for_clean_cluster(
            burla_auth_headers,
            expected_ready_nodes,
            SETTLE_TIMEOUT_SEC,
        )
    except AssertionError:
        old_active_names = _restart_cluster(burla_auth_headers)
        _wait_for_replacement_nodes(old_active_names, CLEAN_CLUSTER_TIMEOUT_SEC)
        state = _wait_for_clean_cluster(
            burla_auth_headers,
            expected_ready_nodes,
            CLEAN_CLUSTER_TIMEOUT_SEC,
        )

    return {
        "url": DASHBOARD_URL,
        "project_id": version_info.get("project"),
        "version": version_info.get("version"),
        "state": state,
    }


@pytest.fixture(autouse=True)
def clean_local_dev_cluster_before_cluster_tests(request):
    cluster_markers = ("service", "e2e", "dashboard")
    if not any(request.node.get_closest_marker(marker) for marker in cluster_markers):
        return
    # Checked before the readiness gate so a wrong-mode test doesn't pay for a
    # cluster reset it will never use.
    if request.node.get_closest_marker("remote_dev") and _head_in_local_dev_mode():
        # Always a skip, never a failure: running the whole suite against a
        # local-dev cluster is normal, these tests just aren't part of it.
        pytest.skip(
            "this test only means anything against real VMs; start this "
            "checkout's cluster with `make remote-dev`."
        )
    request.getfixturevalue("local_dev_cluster")


@pytest.fixture
def cluster_with_n_nodes(main_http_client, local_dev_cluster):
    """Grow this checkout's cluster to `n` READY nodes for tests that need
    more than one, then put it back. local-dev fixes the node count at head
    startup and resets it on every settings write, so the head exposes a
    test-only knob for this."""
    original_quantity = None

    def _ensure(n: int) -> list[dict[str, Any]]:
        nonlocal original_quantity
        state = _cluster_state_via_http()
        if len(state["ready_nodes"]) >= n:
            return state["ready_nodes"]

        if not _head_in_local_dev_mode():
            pytest.fail(
                f"this test needs {n} READY nodes but the cluster has "
                f"{len(state['ready_nodes'])}; raise machineQuantity in settings "
                "and restart the cluster."
            )

        original_quantity = _expected_ready_node_count(_request_headers())
        _set_local_dev_node_quantity(n)
        main_http_client.post("/v1/cluster/restart").raise_for_status()
        return _wait_for_ready_nodes(n, CLEAN_CLUSTER_TIMEOUT_SEC)

    yield _ensure

    if original_quantity is not None:
        _set_local_dev_node_quantity(original_quantity)
        main_http_client.post("/v1/cluster/restart")
        _wait_for_ready_nodes(original_quantity, CLEAN_CLUSTER_TIMEOUT_SEC)


@pytest.fixture(scope="session")
def dashboard_url() -> str:
    return DASHBOARD_URL


def _cluster_state_via_http() -> dict[str, Any]:
    import requests

    try:
        resp = requests.get(
            f"{DASHBOARD_URL}/v1/cluster/state", headers=_request_headers(), timeout=5
        )
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


def get_job_via_http(job_id: str) -> dict[str, Any] | None:
    """The job dict as the head sees it (in-memory, falling back to history)."""
    import requests

    resp = requests.get(
        f"{DASHBOARD_URL}/v1/jobs/{job_id}", headers=_request_headers(), timeout=5
    )
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json() or None


@pytest.fixture
def get_job():
    return get_job_via_http


@pytest.fixture
def cleanup_job():
    """Cancel every job the test admitted, then wait for its node
    reservations to clear. An admitted job stays RUNNING and reserves its
    nodes until canceled (or reaped after 300s), and a leaked reservation
    forces the next test's readiness gate into a full cluster restart."""
    import requests

    job_ids: list[str] = []

    def _register(job_id: str) -> str:
        job_ids.append(job_id)
        return job_id

    yield _register

    if not job_ids:
        return
    try:
        for job_id in job_ids:
            job = get_job_via_http(job_id)
            if job and job.get("status") == "RUNNING":
                requests.post(
                    f"{DASHBOARD_URL}/v1/jobs/{job_id}/stop",
                    headers=_request_headers(),
                    timeout=10,
                )
        deadline = time.time() + 30
        while time.time() < deadline:
            reserved = [
                node
                for node in _active_node_docs()
                if node.get("reserved_for_job") in job_ids
                or node.get("current_job") in job_ids
            ]
            if not reserved:
                return
            time.sleep(0.5)
    except requests.RequestException:
        pass  # head unreachable mid-teardown; the gate recovers


@pytest.fixture
def cleanup_node():
    def _register(instance_name: str) -> str:
        return instance_name

    yield _register


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
    `http://node_xxx:<port>` on that cluster's own Docker network. From the
    host we reach nodes via `http://localhost:<port>` since each node
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
            target = next(
                (n for n in nodes if n["instance_name"] == instance_name), None
            )
            if target is None:
                pytest.skip(f"Node {instance_name} not in ready_nodes.")
        host = target["host"]
        if host.startswith("http://node_"):
            port = host.rsplit(":", 1)[-1]
            host = f"http://localhost:{port}"
        verify = (
            ssl.create_default_context(cadata=state["cluster_ca"])
            if host.startswith("https://")
            else True
        )
        return httpx.Client(
            base_url=host,
            timeout=30,
            headers=burla_auth_headers,
            verify=verify,
        )

    return _factory


@pytest.fixture
def any_ready_node(main_http_client):
    state = main_http_client.get("/v1/cluster/state").json()
    nodes = state.get("ready_nodes") or []
    if not nodes:
        pytest.skip("No READY nodes.")
    return nodes[0]


# ---------------------------------------------------------------------------
# Shared subprocess isolation for client-side tests.
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
    resume_after_seconds: float | None = None,
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
        args=(
            result_queue,
            function_source,
            inputs,
            rpm_kwargs,
            env_overrides,
            DASHBOARD_URL,
        ),
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
        if resume_after_seconds is not None and process.is_alive():
            time.sleep(resume_after_seconds)
            try:
                os.kill(process.pid, signal.SIGCONT)
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
    Helper for e2e tests that need to send SIGINT after N seconds. Usage:
        result = ctrl_c_after(source, inputs, delay_s=2, **kwargs)
    """

    def _send(
        function_source: str, inputs: list, delay_s: float, **kwargs: Any
    ) -> dict:
        return run_rpm_in_subprocess(
            function_source,
            inputs,
            signal_after_seconds=delay_s,
            signal_name="SIGINT",
            timeout_seconds=60,
            **kwargs,
        )

    return _send


@pytest.fixture
def suspend_client_for():
    """
    Helper for e2e tests that suspend the client process mid-job: SIGSTOP
    after delay_s, SIGCONT suspend_s later. Usage:
        result = suspend_client_for(source, inputs, delay_s=6, suspend_s=20, **kwargs)
    """

    def _suspend(
        function_source: str,
        inputs: list,
        delay_s: float,
        suspend_s: float,
        **kwargs: Any,
    ) -> dict:
        return run_rpm_in_subprocess(
            function_source,
            inputs,
            signal_after_seconds=delay_s,
            signal_name="SIGSTOP",
            resume_after_seconds=suspend_s,
            **kwargs,
        )

    return _suspend


# ---------------------------------------------------------------------------
# Polling helper — every test that needs to wait for a cluster state change
# should use this instead of ad-hoc sleeps.
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
    raise AssertionError(
        f"wait_for timed out after {timeout}s: {message} (last={last!r})"
    )


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
# Auto-skip tests if the local-dev cluster isn't running.
# ---------------------------------------------------------------------------


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    requires_cluster = {"service", "e2e", "dashboard"}
    cluster_up = _main_service_reachable()
    if REQUIRE_CLUSTER and not cluster_up:
        raise pytest.UsageError(f"local-dev cluster not running at {DASHBOARD_URL}")
    for item in items:
        markers = {m.name for m in item.iter_markers()}
        if markers & requires_cluster and not cluster_up:
            item.add_marker(
                pytest.mark.skip(
                    reason=f"local-dev cluster not running at {DASHBOARD_URL}"
                )
            )
