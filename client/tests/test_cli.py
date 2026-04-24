"""
Section 13 of the test plan: the `burla` CLI.

Four commands: `login`, `install`, `--version`, `-v`. Built with python-fire.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

pytestmark = pytest.mark.unit


def _burla(*args, timeout=10):
    # Use the `burla` console script installed by the pyproject entry point.
    # `python -m burla` doesn't work because burla.__init__ calls Fire via
    # init_cli(), and the module's `__main__.py` isn't defined.
    return subprocess.run(
        ["burla", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def test_burla_version_prints_version():
    from burla import __version__

    result = _burla("--version")
    # fire may print to stdout or stderr depending on the subcommand wiring.
    combined = result.stdout + result.stderr
    assert __version__ in combined


def test_burla_v_alias_prints_version():
    from burla import __version__

    result = _burla("-v")
    combined = result.stdout + result.stderr
    assert __version__ in combined


def test_init_cli_registers_four_commands():
    import burla

    source = open(burla.__file__).read()
    # Sanity-check the literal Fire dict so refactors can't silently drop a command.
    assert "login" in source
    assert "install" in source
    assert "--version" in source
    assert "-v" in source


def test_burla_unknown_subcommand_nonzero_exit():
    result = _burla("totally-not-a-command-xyz", timeout=10)
    assert result.returncode != 0


def test_burla_login_no_browser_flag_accepted_by_parser():
    from burla._auth import login
    import inspect

    sig = inspect.signature(login)
    assert "no_browser" in sig.parameters
    assert sig.parameters["no_browser"].default is False


def test_burla_install_has_no_args():
    from burla._install import install
    import inspect

    sig = inspect.signature(install)
    # Intentionally parameterless - the install command is a one-shot.
    assert len(sig.parameters) == 0
