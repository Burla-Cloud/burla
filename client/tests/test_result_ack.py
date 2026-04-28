from __future__ import annotations

import asyncio
import pickle
from queue import Queue
from unittest.mock import MagicMock

import cloudpickle
import pytest
from tblib import Traceback

pytestmark = pytest.mark.unit


def _node(monkeypatch):
    import aiohttp

    import burla._node as node_module
    from burla._node import Node

    monkeypatch.setattr(node_module, "get_auth_headers", lambda: {})
    session = MagicMock(spec=aiohttp.ClientSession)
    client = MagicMock()
    node = Node.from_ready(
        instance_name="burla-node-abc12345",
        host="http://localhost:9999",
        machine_type="n4-standard-2",
        target_parallelism=2,
        session=session,
        client=client,
        spinner=False,
    )
    node.job_id = "job-123"
    return node


def test_record_result_batch_acks_after_counting_results(monkeypatch):
    node = _node(monkeypatch)
    acked = []

    async def _ack(batch_id):
        acked.append(batch_id)
        return True

    node._ack_result_batch = _ack
    return_queue = Queue()
    node_results = {
        "result_batch_id": "batch-1",
        "results": [
            (1, False, cloudpickle.dumps("one")),
            (2, False, cloudpickle.dumps("two")),
        ],
        "current_parallelism": 3,
    }

    asyncio.run(node._record_result_batch(node_results, return_queue))

    assert acked == ["batch-1"]
    assert node.result_count == 2
    assert node.received_result_indices == {1, 2}
    assert return_queue.get_nowait() == "one"
    assert return_queue.get_nowait() == "two"


def test_record_result_batch_dedupes_resent_results_before_ack(monkeypatch):
    node = _node(monkeypatch)
    acked = []

    async def _ack(batch_id):
        acked.append(batch_id)
        return True

    node._ack_result_batch = _ack
    return_queue = Queue()
    node_results = {
        "result_batch_id": "batch-1",
        "results": [(1, False, cloudpickle.dumps("one"))],
        "current_parallelism": 0,
    }

    asyncio.run(node._record_result_batch(node_results, return_queue))
    asyncio.run(node._record_result_batch(node_results, return_queue))

    assert acked == ["batch-1", "batch-1"]
    assert node.result_count == 1
    assert node.received_result_indices == {1}
    assert return_queue.get_nowait() == "one"
    assert return_queue.empty()


def test_record_result_batch_acks_reconstructed_udf_error(monkeypatch):
    node = _node(monkeypatch)
    node.udf_error_event = MagicMock()
    acked = []

    async def _ack(batch_id):
        acked.append(batch_id)
        return True

    async def _log_error(job_id, session):
        return None

    import burla._node as node_module

    monkeypatch.setattr(
        node_module.RemoteParallelMapReporter,
        "log_user_function_error_async",
        _log_error,
    )
    node._ack_result_batch = _ack
    try:
        raise ValueError("boom")
    except ValueError as error:
        error_info = {
            "exception": error,
            "traceback_dict": Traceback(error.__traceback__).to_dict(),
        }
    node_results = {
        "result_batch_id": "batch-error",
        "results": [(9, True, pickle.dumps(error_info))],
        "current_parallelism": 0,
    }

    with pytest.raises(ValueError):
        asyncio.run(node._record_result_batch(node_results, Queue()))

    assert acked == ["batch-error"]
