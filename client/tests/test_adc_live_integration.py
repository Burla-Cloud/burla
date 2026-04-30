from __future__ import annotations

import json

import pytest


pytestmark = pytest.mark.slow


def test_remote_parallel_map_bootstraps_from_adc_without_burla_credentials(monkeypatch, tmp_path):
    import google.auth

    import burla
    from burla import _auth, remote_parallel_map

    _, adc_project_id = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )

    config_path = tmp_path / "burla_credentials.json"
    monkeypatch.setattr(burla, "CONFIG_PATH", config_path)
    monkeypatch.setattr(_auth, "CONFIG_PATH", config_path)
    _auth._get_auth_info.cache_clear()

    def add_one(x):
        return x + 1

    results = remote_parallel_map(add_one, [1], grow=True, max_parallelism=1, spinner=False)
    auth_info = json.loads(config_path.read_text())

    assert results == [2]
    assert auth_info["project_id"] == adc_project_id
    assert auth_info["email"]
    assert auth_info["auth_token"]
    assert auth_info["cluster_dashboard_url"]
