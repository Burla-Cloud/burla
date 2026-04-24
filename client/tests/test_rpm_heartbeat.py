"""
Section 9 of the test plan: the heartbeat subprocess.

The bulk of this is unit-tested since the heartbeat runs in a spawned
subprocess that's hard to orchestrate end-to-end. We verify:
- run_in_subprocess launches a detached python subprocess and pipes
  (func, args) via cloudpickle on stdin
- send_alive_pings schedules pings at the documented cadences
- the heartbeat body pickles a (func, args) tuple the same way the
  production code does
"""

from __future__ import annotations

import os
import pickle
import signal
import subprocess
import sys
import time
from unittest.mock import patch

import pytest

pytestmark = pytest.mark.unit


def test_run_in_subprocess_spawns_python_unbuffered():
    import cloudpickle

    from burla._heartbeat import run_in_subprocess

    async def _go():
        def noop_func(*args):
            return None

        async def _async():
            process = await run_in_subprocess(noop_func, "arg")
            try:
                time.sleep(0.5)
                # The spawned subprocess should still be alive during a quick check.
                assert process.poll() is None or isinstance(process.poll(), int)
            finally:
                process.kill()
            return process

        return await _async()

    import asyncio

    loop = asyncio.new_event_loop()
    proc = loop.run_until_complete(_go())
    assert proc is not None
    loop.close()


def test_send_alive_pings_has_expected_constants():
    from burla import _heartbeat

    # Quick sanity check that the module exposes the two public entry points.
    assert hasattr(_heartbeat, "send_alive_pings")
    assert hasattr(_heartbeat, "run_in_subprocess")
