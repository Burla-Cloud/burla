"""frps server-plugin enforcing per-cluster tunnel ownership.

frps calls this service before accepting a client login or proxy
registration (see `httpPlugins` in frps.toml):

- Login: frpc sends `user = <project_id>` and `metadatas.token = <cluster
  token>`. The token is validated against the Burla backend, which returns
  200 on `GET /v1/clusters/{project_id}/dashboard_url` only for the real
  cluster token.
- NewProxy: the subdomain must be the project id itself (head/dashboard
  tunnel) or end with `--<project_id>` (node tunnels), so no cluster can
  register hostnames that route another cluster's traffic.
"""

import os
import threading
from time import time

import requests
from fastapi import FastAPI, Request

BURLA_BACKEND_URL = os.environ.get(
    "BURLA_BACKEND_URL", "https://backend.burla.dev"
).rstrip("/")
TOKEN_CACHE_TTL_SEC = 10 * 60

app = FastAPI(docs_url=None, redoc_url=None)

_token_cache: dict[tuple[str, str], float] = {}  # (project_id, token) -> expires_at
_cache_lock = threading.Lock()

ALLOW = {"reject": False, "unchange": True}


def _reject(reason: str) -> dict:
    return {"reject": True, "reject_reason": reason}


def _token_is_valid(project_id: str, token: str) -> bool:
    with _cache_lock:
        expires_at = _token_cache.get((project_id, token))
        if expires_at is not None and time() < expires_at:
            return True

    url = f"{BURLA_BACKEND_URL}/v1/clusters/{project_id}/dashboard_url"
    response = requests.get(
        url, headers={"Authorization": f"Bearer {token}"}, timeout=10
    )
    # 409 = token accepted but no dashboard registered yet (fresh cluster);
    # 401/403/404/5xx all mean we can't prove ownership, so reject.
    if response.status_code not in (200, 409):
        return False

    with _cache_lock:
        _token_cache[(project_id, token)] = time() + TOKEN_CACHE_TTL_SEC
        if len(_token_cache) > 10_000:
            now = time()
            for key, expiry in list(_token_cache.items()):
                if expiry <= now:
                    _token_cache.pop(key, None)
    return True


@app.post("/handler")
def handler(request: Request, payload: dict):
    op = request.query_params.get("op")
    content = payload.get("content") or {}

    if op == "Login":
        project_id = content.get("user") or ""
        token = (content.get("metas") or {}).get("token") or ""
        if not project_id or not token:
            return _reject("frpc must set user=<project_id> and metadatas.token")
        if not _token_is_valid(project_id, token):
            return _reject(f"invalid cluster token for project {project_id}")
        return ALLOW

    if op == "NewProxy":
        project_id = (content.get("user") or {}).get("user") or ""
        subdomain = content.get("subdomain") or ""
        if content.get("proxy_type") != "https":
            return _reject("only https (SNI passthrough) proxies are allowed")
        if not subdomain:
            return _reject("proxies must set a subdomain")
        if subdomain != project_id and not subdomain.endswith(f"--{project_id}"):
            return _reject(
                f"subdomain {subdomain} does not belong to project {project_id}"
            )
        return ALLOW

    return ALLOW
