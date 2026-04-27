"""
The `burla` CLI — real subprocess invocations only. Signature-grep
tests were removed; the real subprocess runs catch behavioral regressions.
"""

from __future__ import annotations

import subprocess

import pytest

pytestmark = pytest.mark.unit


def _burla(*args, timeout=10):
    # Use the `burla` console script installed by the pyproject entry point.
    # `python -m burla` doesn't work because burla.__init__ calls Fire via
    # init_cli(), and the module has no `__main__.py`.
    return subprocess.run(
        ["burla", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def test_burla_version_prints_version():
    from burla import __version__

    result = _burla("--version")
    combined = result.stdout + result.stderr
    assert __version__ in combined


def test_burla_v_alias_prints_version():
    from burla import __version__

    result = _burla("-v")
    combined = result.stdout + result.stderr
    assert __version__ in combined


def test_burla_unknown_subcommand_nonzero_exit():
    result = _burla("totally-not-a-command-xyz", timeout=10)
    assert result.returncode != 0
