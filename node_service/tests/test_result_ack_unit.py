from __future__ import annotations

import asyncio
import importlib.util
import pickle
import sys
import types
from collections import deque
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_job_endpoints(monkeypatch):
    fake_self = {
        "current_job": "job-123",
        "results_queue": asyncio.Queue(),
        "pending_result_batches": {},
        "pending_logs": deque(),
        "current_parallelism": 0,
        "pending_cluster_shutdown": False,
        "pending_cluster_restarted": False,
        "pending_dashboard_canceled": False,
    }

    fake_node_service = types.ModuleType("node_service")
    fake_node_service.SELF = fake_self
    fake_node_service.PROJECT_ID = "test-project"
    fake_node_service.INSTANCE_NAME = "test-node"
    fake_node_service.IN_LOCAL_DEV_MODE = True
    fake_node_service.NODE_AUTH_CREDENTIALS_PATH = Path("/tmp/burla-test-creds.json")
    fake_node_service.get_request_json = lambda: None
    fake_node_service.get_logger = lambda: None
    fake_node_service.get_request_files = lambda: None

    fake_helpers = types.ModuleType("node_service.helpers")
    fake_helpers.Logger = object

    fake_job_watcher = types.ModuleType("node_service.job_watcher")
    fake_job_watcher.job_watcher_logged = lambda *args, **kwargs: None

    monkeypatch.setitem(sys.modules, "node_service", fake_node_service)
    monkeypatch.setitem(sys.modules, "node_service.helpers", fake_helpers)
    monkeypatch.setitem(sys.modules, "node_service.job_watcher", fake_job_watcher)

    path = REPO_ROOT / "node_service" / "src" / "node_service" / "job_endpoints.py"
    spec = importlib.util.spec_from_file_location("job_endpoints_result_ack_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, fake_self


def test_results_are_resent_until_ack(monkeypatch):
    module, fake_self = _load_job_endpoints(monkeypatch)
    result = (7, False, pickle.dumps("value"))
    fake_self["results_queue"].put_nowait(result)

    async def _run():
        first_response = await module.get_results("job-123")
        first_payload = pickle.loads(first_response.body)
        second_response = await module.get_results("job-123")
        second_payload = pickle.loads(second_response.body)
        await module.ack_results("job-123", first_payload["result_batch_id"])
        return first_payload, second_payload

    first_payload, second_payload = asyncio.run(_run())

    assert first_payload["result_batch_id"]
    assert second_payload["result_batch_id"] == first_payload["result_batch_id"]
    assert second_payload["results"] == [result]
    assert fake_self["results_queue"].empty()
    assert fake_self["pending_result_batches"] == {}


def test_result_ack_is_idempotent(monkeypatch):
    module, fake_self = _load_job_endpoints(monkeypatch)
    fake_self["pending_result_batches"]["missing-ok"] = [(1, False, b"x")]

    async def _run():
        first_response = await module.ack_results("job-123", "missing-ok")
        second_response = await module.ack_results("job-123", "missing-ok")
        return first_response, second_response

    first_response, second_response = asyncio.run(_run())

    assert first_response.status_code == 200
    assert second_response.status_code == 200
    assert fake_self["pending_result_batches"] == {}


def test_job_watcher_waits_for_pending_result_batches():
    source = (REPO_ROOT / "node_service" / "src" / "node_service" / "job_watcher.py").read_text()

    assert "pending_results_empty = not SELF[\"pending_result_batches\"]" in source
    assert "SELF[\"results_queue\"].empty() and pending_results_empty and all_workers_idle" in source
