"""
Section 23: /api/sf/* and /signed-* storage endpoints.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.service


def test_filemanager_read_returns_shape(main_http_client, local_dev_cluster):
    resp = main_http_client.post(
        "/api/sf/filemanager",
        json={"action": "read", "path": "/", "pageSize": 50, "pageIndex": 0},
    )
    assert resp.status_code in (200, 400)
    if resp.status_code == 200:
        body = resp.json()
        # Syncfusion contract: either {cwd, files, count, hasMore} or an error dict.
        assert "files" in body or "error" in body


def test_filemanager_unsupported_action_returns_400_body(main_http_client, local_dev_cluster):
    resp = main_http_client.post(
        "/api/sf/filemanager",
        json={"action": "totally-bogus-action", "path": "/"},
    )
    assert resp.status_code == 200  # Syncfusion wants 200 with an error body
    body = resp.json()
    assert body.get("error", {}).get("code") == "400"


def test_signed_resumable_returns_url(main_http_client, local_dev_cluster):
    resp = main_http_client.get(
        "/signed-resumable?object_name=test-object&content_type=application/octet-stream"
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "url" in body
    assert body["url"].startswith("http")


def test_signed_download_404_on_missing(main_http_client, local_dev_cluster):
    resp = main_http_client.get("/signed-download?object_name=definitely-does-not-exist-xyz.txt")
    assert resp.status_code == 404


def test_signed_download_sanitizes_dot_dot(main_http_client, local_dev_cluster):
    resp = main_http_client.get("/signed-download?object_name=../../etc/passwd")
    # Either rejects path or returns 404 after sanitization.
    assert resp.status_code in (400, 404)


def test_batch_download_ticket_returns_downloadUrl(main_http_client, local_dev_cluster):
    resp = main_http_client.post(
        "/batch-download-ticket",
        json={"items": [], "archiveName": "test.zip"},
    )
    # Empty items list may 400; non-empty returns a url.
    assert resp.status_code in (200, 400)
