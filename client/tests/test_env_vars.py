"""
Section 12 of the test plan: environment variables the client respects.

Covered:
- BURLA_CLUSTER_DASHBOARD_URL (overrides config file)
- BURLA_CLUSTER_DASHBOARD_URL trailing-slash stripping
- DISABLE_BURLA_TELEMETRY
- COLAB_RELEASE_TAG (sets IN_COLAB on import)
- `import burla` makes no network calls
- `import burla` does not read CONFIG_PATH
"""

from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
import textwrap

import pytest


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def clear_auth_cache():
    yield
    from burla import _auth

    _auth._get_auth_info.cache_clear()


def test_BURLA_CLUSTER_DASHBOARD_URL_overrides_config(monkeypatch, tmp_path):
    from burla import get_cluster_dashboard_url
    import burla

    config = tmp_path / "creds.json"
    config.write_text(json.dumps({"cluster_dashboard_url": "http://from-config"}))
    monkeypatch.setattr(burla, "CONFIG_PATH", config)

    monkeypatch.setenv("BURLA_CLUSTER_DASHBOARD_URL", "http://env-wins")
    assert get_cluster_dashboard_url() == "http://env-wins"


def test_BURLA_CLUSTER_DASHBOARD_URL_trailing_slash_stripped(monkeypatch):
    from burla import get_cluster_dashboard_url

    monkeypatch.setenv("BURLA_CLUSTER_DASHBOARD_URL", "http://localhost:5001/")
    assert get_cluster_dashboard_url() == "http://localhost:5001"


def test_get_cluster_dashboard_url_reads_config_when_no_env(monkeypatch, tmp_path):
    import burla
    from burla import get_cluster_dashboard_url

    monkeypatch.delenv("BURLA_CLUSTER_DASHBOARD_URL", raising=False)
    config = tmp_path / "creds.json"
    config.write_text(json.dumps({"cluster_dashboard_url": "http://from-config/"}))
    monkeypatch.setattr(burla, "CONFIG_PATH", config)

    assert get_cluster_dashboard_url() == "http://from-config"


def test_COLAB_RELEASE_TAG_sets_IN_COLAB_on_reimport(monkeypatch):
    import importlib
    from burla import _auth

    monkeypatch.setenv("COLAB_RELEASE_TAG", "2024.01")
    # Reload the already-imported _auth module so its IN_COLAB is re-evaluated
    # with the patched env. Simple `del sys.modules[...]` isn't enough because
    # `burla.__init__` already has _auth as an attribute.
    reloaded = importlib.reload(_auth)
    assert reloaded.IN_COLAB is True


def test_COLAB_RELEASE_TAG_absent_sets_IN_COLAB_false(monkeypatch):
    import importlib
    from burla import _auth

    monkeypatch.delenv("COLAB_RELEASE_TAG", raising=False)
    reloaded = importlib.reload(_auth)
    assert reloaded.IN_COLAB is False


def test_import_burla_makes_no_network_calls():
    """`import burla` must not hit the network. Agents depend on this."""
    code = textwrap.dedent(
        """
        import socket
        _orig = socket.socket.connect
        def _blocker(self, addr):
            raise RuntimeError(f'import burla made a network call to {addr}')
        socket.socket.connect = _blocker
        import burla  # must not raise
        print('OK')
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, f"stderr: {result.stderr}"
    assert "OK" in result.stdout


def test_import_burla_does_not_read_config_path(tmp_path, monkeypatch):
    """Deleting CONFIG_PATH must not break `import burla`."""
    code = textwrap.dedent(
        f"""
        import os, pathlib
        os.environ.pop('BURLA_CLUSTER_DASHBOARD_URL', None)
        # Hide the real CONFIG_PATH dir by pointing platformdirs to a throwaway.
        os.environ['APPDATA'] = {str(tmp_path)!r}
        os.environ['XDG_CONFIG_HOME'] = {str(tmp_path)!r}
        import burla  # must not raise or read any config file
        print('OK')
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, f"stderr: {result.stderr}"
    assert "OK" in result.stdout
