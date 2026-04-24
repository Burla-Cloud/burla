"""
Retry behavior of `_run_network_request_with_retries` and
`_post_with_retries`. Tests invoke the helpers directly with fake
sessions and count attempts — actual logic coverage, not constants.
"""

from __future__ import annotations

import asyncio

import pytest

pytestmark = pytest.mark.unit


# --------------------------------------------------- _run_network_request_with_retries


async def _count_tries(attempts_box):
    attempts_box[0] += 1
    raise asyncio.TimeoutError("boom")


def test_run_network_request_with_retries_retries_max_times():
    from burla._node import _run_network_request_with_retries

    attempts = [0]

    async def _run():
        try:
            await _run_network_request_with_retries(
                lambda: _count_tries(attempts), max_retries=3
            )
        except Exception:
            pass
        return attempts[0]

    # With 3 max retries the function should be called exactly 3 times.
    n = asyncio.new_event_loop().run_until_complete(_run())
    assert n == 3


def test_run_network_request_with_retries_raises_last_error():
    from burla._node import _run_network_request_with_retries

    async def _raise():
        raise asyncio.TimeoutError("timeout")

    loop = asyncio.new_event_loop()
    with pytest.raises(asyncio.TimeoutError):
        loop.run_until_complete(
            _run_network_request_with_retries(_raise, max_retries=2)
        )


def test_run_network_request_with_retries_returns_first_success():
    from burla._node import _run_network_request_with_retries

    call_count = [0]

    async def _fn():
        call_count[0] += 1
        return "ok"

    loop = asyncio.new_event_loop()
    result = loop.run_until_complete(
        _run_network_request_with_retries(_fn, max_retries=5)
    )
    assert result == "ok"
    assert call_count[0] == 1


# --------------------------------------------------- _post_with_retries


def test_post_with_retries_retries_5x_on_server_disconnected():
    """_post_with_retries retries aiohttp.ServerDisconnectedError up to max_retries."""
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
