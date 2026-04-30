from __future__ import annotations

import asyncio
import importlib.util
import pickle
import sys
import types
from pathlib import Path

import pytest


class _SizedQueue:
    def __init__(self):
        self.items = []

    async def put(self, item, size_bytes):
        self.items.append((item, size_bytes))


class _LogWriter:
    def __init__(self):
        self.errors = []
        self.warnings = []

    async def write_error(self, input_index, message):
        self.errors.append((input_index, message))

    async def write_warning(self, input_index, message):
        self.warnings.append((input_index, message))


class _NodeLogger:
    entries = []

    async def log(self, message, severity="INFO", **kw):
        self.entries.append((message, severity, kw))


class _Reader:
    def __init__(self, chunks):
        self.chunks = list(chunks)

    async def readexactly(self, byte_count):
        chunk = self.chunks.pop(0)
        assert len(chunk) == byte_count
        return chunk


class _Writer:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class _Memory:
    total = 100
    available = 10


async def _fake_restart_container(self):
    self.restart_count += 1
    self.writer = _Writer()
    self.reader = object()


async def _fake_retire_for_dynamic_memory_pressure(self):
    self.kill_count += 1


def _load_worker_client_module(monkeypatch):
    _NodeLogger.entries = []
    fake_node_service = types.ModuleType("node_service")
    fake_node_service.SELF = {}
    fake_node_service.ASYNC_DB = object()
    fake_node_service.INSTANCE_NAME = "burla-node-test"
    fake_node_service.IN_LOCAL_DEV_MODE = True
    fake_node_service.NUM_GPUS = 0
    fake_node_service.Logger = _NodeLogger
    fake_node_service.__version__ = "test"
    monkeypatch.setitem(sys.modules, "node_service", fake_node_service)

    fake_aiodocker = types.ModuleType("aiodocker")
    fake_aiodocker.Docker = object
    fake_aiodocker.DockerError = Exception
    monkeypatch.setitem(sys.modules, "aiodocker", fake_aiodocker)

    fake_psutil = types.ModuleType("psutil")
    fake_psutil.virtual_memory = lambda: _Memory()
    monkeypatch.setitem(sys.modules, "psutil", fake_psutil)

    fake_tblib = types.ModuleType("tblib")
    fake_tblib.Traceback = object
    monkeypatch.setitem(sys.modules, "tblib", fake_tblib)

    module_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "node_service"
        / "worker_client.py"
    )
    spec = importlib.util.spec_from_file_location("worker_client_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _setup_dynamic_worker_pair(module):
    worker = module.WorkerClient.__new__(module.WorkerClient)
    other_worker = module.WorkerClient.__new__(module.WorkerClient)
    worker.retired = False
    other_worker.retired = False
    worker.is_idle = False
    worker.log_writer = _LogWriter()
    worker.logstream_task = None
    worker.container = types.SimpleNamespace(delete=lambda force: None)
    worker.writer = _Writer()
    worker.reader = object()
    worker.restart_count = 0
    worker.container_id = "container-id"
    worker.worker_host_pid = None
    worker._delete_container = types.MethodType(_fake_restart_container, worker)
    worker._restart_container = types.MethodType(_fake_restart_container, worker)
    module.SELF.update(
        {
            "workers": [worker, other_worker],
            "inputs_queue": _SizedQueue(),
            "dynamic_ram_lock": asyncio.Lock(),
            "reboot_containers_after_job": False,
            "current_job": "job-test",
        }
    )
    return worker


def _worker(module, input_index, rss_bytes):
    worker = module.WorkerClient.__new__(module.WorkerClient)
    worker.retired = False
    worker.is_idle = False
    worker.current_input = (input_index, b"input")
    worker.log_writer = _LogWriter()
    worker.logstream_task = None
    worker.kill_count = 0
    worker.memory_rss_bytes = lambda: rss_bytes
    worker.retire_for_dynamic_memory_pressure = types.MethodType(
        _fake_retire_for_dynamic_memory_pressure, worker
    )
    return worker


@pytest.mark.unit
def test_oom_kill_marker_count_ignores_plain_killed(monkeypatch):
    module = _load_worker_client_module(monkeypatch)

    assert module.oom_kill_marker_count("3.12\n__burla_oom_kill__:0->1\n3.12\n") == 1
    assert module.oom_kill_marker_count("3.12\nKilled\n3.12\n") == 0


@pytest.mark.unit
def test_dynamic_oom_requeues_input_and_retires_worker(monkeypatch):
    module = _load_worker_client_module(monkeypatch)
    worker = _setup_dynamic_worker_pair(module)

    result = asyncio.run(
        worker._retire_after_dynamic_worker_failure(
            7, b"input", module.WorkerOutOfMemoryError("oom")
        )
    )

    assert result is None
    assert worker.retired
    assert worker.is_idle
    assert worker.restart_count == 1
    assert worker.writer is not None
    assert module.SELF["inputs_queue"].items == [((7, b"input"), len(b"input"))]
    assert module.SELF["reboot_containers_after_job"]
    assert module.Logger.entries == [
        (
            "Node parallelism decreased from 2 to 1 due to memory pressure.",
            "WARNING",
            {
                "job_id": "job-test",
                "input_index": 7,
                "reason": "worker OOM",
                "old_parallelism": 2,
                "new_parallelism": 1,
            },
        )
    ]
    assert "lower node parallelism" in worker.log_writer.errors[0][1]


@pytest.mark.unit
def test_dynamic_worker_exit_requeues_input_and_retires_worker(monkeypatch):
    module = _load_worker_client_module(monkeypatch)
    worker = _setup_dynamic_worker_pair(module)

    result = asyncio.run(
        worker._retire_after_dynamic_worker_failure(
            9, b"input", module.WorkerProcessTerminatedError("worker died")
        )
    )

    assert result is None
    assert worker.retired
    assert worker.restart_count == 1
    assert module.SELF["inputs_queue"].items == [((9, b"input"), len(b"input"))]
    assert (
        module.Logger.entries[0][0]
        == "Node parallelism decreased from 2 to 1 due to memory pressure."
    )
    assert "worker died" in worker.log_writer.errors[0][1]
    assert "lower node parallelism" in worker.log_writer.errors[0][1]


@pytest.mark.unit
def test_dynamic_oom_at_one_worker_returns_terminal_error(monkeypatch):
    module = _load_worker_client_module(monkeypatch)
    worker = module.WorkerClient.__new__(module.WorkerClient)
    worker.retired = False
    module.SELF.update(
        {
            "workers": [worker],
            "inputs_queue": _SizedQueue(),
            "dynamic_ram_lock": asyncio.Lock(),
            "reboot_containers_after_job": False,
        }
    )

    input_index, is_error, payload = asyncio.run(
        worker._retire_after_dynamic_worker_failure(
            3, b"input", module.WorkerOutOfMemoryError("oom")
        )
    )
    error_info = pickle.loads(payload)

    assert input_index == 3
    assert is_error
    assert error_info["is_infrastructure_error"]
    assert "one active worker" in error_info["traceback_str"]


@pytest.mark.unit
def test_worker_error_response_keeps_user_exception_opaque(monkeypatch):
    module = _load_worker_client_module(monkeypatch)
    worker = module.WorkerClient.__new__(module.WorkerClient)
    opaque_error_info = b"not unpickled by node_service"
    payload = pickle.dumps(
        {
            "error_info_pkl": opaque_error_info,
            "traceback_str": "worker traceback",
        }
    )
    worker.reader = _Reader([b"e", len(payload).to_bytes(8, "big"), payload])

    with pytest.raises(module.WorkerFunctionError) as exc_info:
        asyncio.run(worker._read_response())

    assert exc_info.value.error_info_pkl == opaque_error_info
    assert str(exc_info.value) == "worker traceback"
