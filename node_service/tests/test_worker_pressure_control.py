import asyncio
import importlib
import os
import sys
from pathlib import Path

import pytest

NODE_SERVICE_SRC = str(Path(__file__).parents[1] / "src")
NODE_ENV_DEFAULTS = {
    "PROJECT_ID": "test-project",
    "MAIN_SERVICE_URL": "http://localhost",
    "CLUSTER_ID_TOKEN": "test-token",
    "NUM_GPUS": "0",
    "INSTANCE_NAME": "test-node",
}
previous_node_env = {name: os.environ.get(name) for name in NODE_ENV_DEFAULTS}
sys.path.insert(0, NODE_SERVICE_SRC)
try:
    for name, value in NODE_ENV_DEFAULTS.items():
        os.environ.setdefault(name, value)
    sys.modules.pop("node_service", None)
    worker_client = importlib.import_module("node_service.worker_client")
finally:
    sys.path.remove(NODE_SERVICE_SRC)
    for name, previous_value in previous_node_env.items():
        if previous_value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = previous_value


class ExistingPressureFile:
    def exists(self):
        return True


class FixedStallTracker:
    def __init__(self, stall_fraction=0.0):
        self.stall_fraction = stall_fraction

    def max_stall_fraction(self, _workers):
        return self.stall_fraction


class FixedCpuSampler:
    def __init__(self, utilization):
        self.utilization = utilization

    def sample(self, _workers):
        return self.utilization


class NoAddGatePressure:
    def sample(self):
        return 0.0, 0.0, 0.0  # io stall, network utilization, memory stall


class FakeWorker:
    def __init__(self, index, *, throttled=False, swap_parked=False):
        self.index = index
        self.retired = False
        self.throttled = throttled
        self.swap_parked = swap_parked
        self.is_idle = False
        self.current_input = (index, b"input")
        # An impossible pid (above any real pid_max) so the stall tracker's
        # /proc read raises OSError and skips this fake, exactly as it skips
        # a real worker mid-relaunch.
        self.worker_host_pid = 10_000_000 + index


@pytest.fixture
def dynamic_state():
    keys = (
        "workers",
        "dynamic_func_cpu",
        "dynamic_func_ram",
        "dynamic_retire_lock",
        "last_pressure_retirement_at",
        "current_job",
    )
    previous = {key: worker_client.SELF[key] for key in keys}
    worker_client.SELF.update(
        workers=[],
        dynamic_func_cpu=False,
        dynamic_func_ram=False,
        dynamic_retire_lock=asyncio.Lock(),
        last_pressure_retirement_at=0.0,
        current_job="test-job",
    )
    yield
    worker_client.SELF.update(previous)


def _two_tick_sleep(flag_name):
    """Fake asyncio.sleep that ends the monitored loop after two ticks."""
    ticks = []

    async def fake_sleep(_interval):
        ticks.append(_interval)
        if len(ticks) >= 2:
            worker_client.SELF[flag_name] = False

    return fake_sleep


@pytest.mark.asyncio
async def test_recovery_restores_parked_worker_when_cores_idle(
    monkeypatch, dynamic_state
):
    """Recovery is driven by idle cores; per-worker PSI no longer gates it."""
    workers = [FakeWorker(0, throttled=True), FakeWorker(1, throttled=True)]
    sleep_intervals = []
    recovery_calls = []
    worker_client.SELF["workers"] = workers
    worker_client.SELF["dynamic_func_cpu"] = True

    async def fake_sleep(interval):
        sleep_intervals.append(interval)

    async def capture_recovery(reason, via):
        recovery_calls.append((reason, via))
        worker_client.SELF["dynamic_func_cpu"] = False

    monkeypatch.setattr(worker_client, "CPU_PRESSURE_FILE", ExistingPressureFile())
    monkeypatch.setattr(
        worker_client, "WorkerStallTracker", lambda: FixedStallTracker(0.5)
    )
    monkeypatch.setattr(worker_client, "SliceCpuSampler", lambda: FixedCpuSampler(0.30))
    monkeypatch.setattr(worker_client, "AddGateSampler", NoAddGatePressure)
    monkeypatch.setattr(worker_client.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(
        worker_client, "_unthrottle_one_parked_worker", capture_recovery
    )

    await worker_client.dynamic_worker_readd_loop()

    assert sleep_intervals == [1, 1, 1]
    assert recovery_calls == [("cores are idle", "recovery_loop")]


@pytest.mark.asyncio
async def test_recovery_holds_while_cores_busy(monkeypatch, dynamic_state):
    """No recovery above the utilization add-ceiling: cores are saturated
    enough that another runner would only add contention."""
    worker_client.SELF["workers"] = [FakeWorker(0, throttled=True)]
    worker_client.SELF["dynamic_func_cpu"] = True
    recovery_calls = []

    async def capture_recovery(reason, via):
        recovery_calls.append((reason, via))

    monkeypatch.setattr(worker_client, "SliceCpuSampler", lambda: FixedCpuSampler(0.98))
    monkeypatch.setattr(worker_client, "AddGateSampler", NoAddGatePressure)
    monkeypatch.setattr(
        worker_client.asyncio, "sleep", _two_tick_sleep("dynamic_func_cpu")
    )
    monkeypatch.setattr(
        worker_client, "_unthrottle_one_parked_worker", capture_recovery
    )

    await worker_client.dynamic_worker_readd_loop()

    assert recovery_calls == []


@pytest.mark.asyncio
async def test_memory_parked_worker_recovers_when_cores_idle(
    monkeypatch, dynamic_state
):
    worker_client.SELF["workers"] = [FakeWorker(0, throttled=True, swap_parked=True)]
    worker_client.SELF["dynamic_func_ram"] = True
    sleep_intervals = []
    recovery_calls = []

    async def fake_sleep(interval):
        sleep_intervals.append(interval)

    async def capture_recovery(reason, via):
        recovery_calls.append((reason, via))
        worker_client.SELF["dynamic_func_ram"] = False

    monkeypatch.setattr(worker_client, "SliceCpuSampler", lambda: FixedCpuSampler(0.30))
    monkeypatch.setattr(worker_client, "AddGateSampler", NoAddGatePressure)
    monkeypatch.setattr(worker_client.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(
        worker_client, "_unthrottle_one_parked_worker", capture_recovery
    )

    await worker_client.dynamic_worker_readd_loop()

    assert sleep_intervals == [1]
    assert recovery_calls == [("cores are idle", "recovery_loop")]

