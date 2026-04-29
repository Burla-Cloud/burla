from __future__ import annotations

import json

import pytest

pytestmark = pytest.mark.unit


class _FakeAiohttpResponse:
    def __init__(self, status: int, body: dict):
        self.status = status
        self._body = body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def json(self):
        return self._body


class _FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.urls = []

    def request(self, method, url, **kwargs):
        self.urls.append(url)
        return self.responses.pop(0)


class _FakeRequestsResponse:
    status_code = 200

    def json(self):
        return {
            "auth_token": "fresh-token",
            "email": "agent@project.iam.gserviceaccount.com",
            "project_id": "project-1",
            "cluster_dashboard_url": "http://fresh-main",
        }

    def raise_for_status(self):
        pass


class _FakeCredentials:
    service_account_email = "agent@project.iam.gserviceaccount.com"


@pytest.mark.asyncio
async def test_start_job_retries_once_after_adc_bootstrap(monkeypatch, tmp_path):
    import burla
    from burla import _auth
    from burla._cluster_client import ClusterClient

    config = tmp_path / "burla_credentials.json"
    config.write_text(
        json.dumps(
            {
                "auth_token": "stale-token",
                "email": "old@example.com",
                "project_id": "old-project",
                "cluster_dashboard_url": "http://stale-main",
            }
        )
    )
    monkeypatch.setattr(burla, "CONFIG_PATH", config)
    monkeypatch.setattr(_auth, "CONFIG_PATH", config)
    _auth._get_auth_info.cache_clear()
    monkeypatch.setattr(
        _auth,
        "_get_adc_credentials",
        lambda: (_FakeCredentials(), "google-token", "project-1"),
    )
    monkeypatch.setattr(_auth, "_get_cluster_token", lambda access_token, project_id: "cluster-token")
    monkeypatch.setattr(_auth.requests, "post", lambda *args, **kwargs: _FakeRequestsResponse())

    session = _FakeSession(
        [
            _FakeAiohttpResponse(401, {}),
            _FakeAiohttpResponse(200, {"ready_nodes": [], "booting_nodes": []}),
        ]
    )

    result = await ClusterClient(session).start_job("job-1", {})

    assert result == {"ready_nodes": [], "booting_nodes": []}
    assert session.urls == [
        "http://stale-main/v1/jobs/job-1/start",
        "http://fresh-main/v1/jobs/job-1/start",
    ]
    assert json.loads(config.read_text())["auth_token"] == "fresh-token"
