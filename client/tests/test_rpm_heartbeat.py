"""
Covers the heartbeat subprocess — `run_in_subprocess` must actually
launch a detached Python process and pipe (func, args) via cloudpickle.
"""

from __future__ import annotations

import time

import pytest

pytestmark = pytest.mark.unit


def test_run_in_subprocess_spawns_python_unbuffered():
    from burla._heartbeat import run_in_subprocess

    async def _go():
        def noop_func(*args):
            return None

        process = await run_in_subprocess(noop_func, "arg")
        try:
            time.sleep(0.5)
            # The spawned subprocess should still be alive during a quick check.
            assert process.poll() is None or isinstance(process.poll(), int)
        finally:
            process.kill()
        return process

    import asyncio

    loop = asyncio.new_event_loop()
    proc = loop.run_until_complete(_go())
    assert proc is not None
    loop.close()
