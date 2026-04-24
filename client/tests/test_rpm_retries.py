"""
Section 8: retry / timeout constants.

Most retry behavior is tested as pure-unit tests that call the internal
helpers (`_post_with_retries`, `_run_network_request_with_retries`) with
fake sessions, since reproducing network errors e2e against a live
cluster is flaky.
"""

from __future__ import annotations

import asyncio
import pytest


pytestmark = pytest.mark.unit


# --------------------------------------------------- _run_network_request_with_retries


async def _always_fails(exc_type):
    raise exc_type("boom")


def test_run_network_request_with_retries_default_is_5():
    from burla._node import NETWORK_RETRY_ATTEMPTS

    assert NETWORK_RETRY_ATTEMPTS == 5


def test_run_network_request_with_retries_delay_is_1s():
    from burla._node import NETWORK_RETRY_DELAY_SECONDS

    assert NETWORK_RETRY_DELAY_SECONDS == 1


def test_network_error_types_includes_expected():
    import aiohttp
    from burla._node import NETWORK_ERROR_TYPES

    assert aiohttp.ClientConnectorError in NETWORK_ERROR_TYPES
    assert aiohttp.ClientOSError in NETWORK_ERROR_TYPES
    assert aiohttp.ClientError in NETWORK_ERROR_TYPES
    assert asyncio.TimeoutError in NETWORK_ERROR_TYPES
    assert OSError in NETWORK_ERROR_TYPES


async def _count_tries(attempts_box):
    attempts_box[0] += 1
    raise asyncio.TimeoutError("boom")


def test_run_network_request_with_retries_retries_max_times():
    from burla._node import _run_network_request_with_retries
    import asyncio as _asyncio

    attempts = [0]

    async def _run():
        try:
            await _run_network_request_with_retries(lambda: _count_tries(attempts), max_retries=3)
        except Exception:
            pass
        return attempts[0]

    # With 3 max retries the function should be called exactly 3 times.
    n = asyncio.new_event_loop().run_until_complete(_run())
    assert n == 3


def test_run_network_request_with_retries_raises_last_error():
    from burla._node import _run_network_request_with_retries
    import asyncio as _asyncio

    async def _raise():
        raise asyncio.TimeoutError("timeout")

    loop = asyncio.new_event_loop()
    with pytest.raises(asyncio.TimeoutError):
        loop.run_until_complete(_run_network_request_with_retries(_raise, max_retries=2))


def test_run_network_request_with_retries_returns_first_success():
    from burla._node import _run_network_request_with_retries
    import asyncio as _asyncio

    call_count = [0]

    async def _fn():
        call_count[0] += 1
        return "ok"

    loop = asyncio.new_event_loop()
    result = loop.run_until_complete(_run_network_request_with_retries(_fn, max_retries=5))
    assert result == "ok"
    assert call_count[0] == 1


# --------------------------------------------------- _post_with_retries


def test_post_with_retries_retries_5x_on_server_disconnected():
    """_post_with_retries should retry aiohttp.ServerDisconnectedError up to max_retries."""
    import aiohttp

    from burla._node import _post_with_retries

    class FakeCtx:
        def __init__(self, exc):
            self._exc = exc

        async def __aenter__(self):
            raise self._exc

        async def __aexit__(self, *a):
            return False

    call_count = [0]

    class FakeSession:
        def post(self, url, data=None, headers=None):
            call_count[0] += 1
            return FakeCtx(aiohttp.client_exceptions.ServerDisconnectedError())

    async def _run():
        try:
            await _post_with_retries(FakeSession(), "http://x", {}, b"")
        except aiohttp.client_exceptions.ServerDisconnectedError:
            pass
        return call_count[0]

    loop = asyncio.new_event_loop()
    n = loop.run_until_complete(_run())
    assert n == 5


# --------------------------------------------------- NODE_SILENCE_TIMEOUT constants


def test_NODE_SILENCE_TIMEOUT_SECONDS_is_120():
    from burla._node import NODE_SILENCE_TIMEOUT_SECONDS

    assert NODE_SILENCE_TIMEOUT_SECONDS == 120


def test_NODE_BOOT_DEADLINE_SEC_is_600():
    from burla._node import NODE_BOOT_DEADLINE_SEC

    assert NODE_BOOT_DEADLINE_SEC == 600


def test_LOGIN_TIMEOUT_SEC_is_10():
    from burla._node import LOGIN_TIMEOUT_SEC

    assert LOGIN_TIMEOUT_SEC == 10


def test_MAX_INPUT_SIZE_BYTES_is_200MB():
    from burla._node import MAX_INPUT_SIZE_BYTES

    assert MAX_INPUT_SIZE_BYTES == 200_000_000


def test_MAX_CHUNK_SIZE_BYTES_is_2MB():
    from burla._node import MAX_CHUNK_SIZE_BYTES

    assert MAX_CHUNK_SIZE_BYTES == 2_000_000


# --------------------------------------------------- ClusterClient._TIMEOUT


def test_cluster_client_timeout_is_30s():
    from burla._cluster_client import _TIMEOUT

    assert _TIMEOUT.total == 30
