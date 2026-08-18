"""frps server-plugin enforcing per-cluster tunnel ownership.

frps calls this service before accepting a client login or proxy
registration (see `httpPlugins` in frps.toml):

- Login: frpc sends `user = <project_id>` and `metadatas.token = <cluster
  token>`. The token is validated against the Burla backend, which returns
  200 on `GET /v1/clusters/{project_id}/dashboard_url` only for the real
  cluster token.
- NewProxy: only the exact deployed-head, client-head, and node hostname
  shapes for the authenticated project are accepted.
"""

import os
import re
import threading
from time import time
from urllib.parse import urlparse

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
    try:
        response = requests.get(
            url, headers={"Authorization": f"Bearer {token}"}, timeout=10
        )
    except requests.RequestException:
        return False
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


def _routing_state(project_id: str, token: str) -> dict | None:
    url = f"{BURLA_BACKEND_URL}/v1/clusters/{project_id}/dashboard_url"
    try:
        response = requests.get(
            url, headers={"Authorization": f"Bearer {token}"}, timeout=10
        )
    except requests.RequestException:
        return None
    if response.status_code != 200:
        return None
    return response.json()


def _subdomain_belongs_to_project(subdomain: str, project_id: str) -> bool:
    if subdomain == f"head--{project_id}":
        return True
    ephemeral = rf"(?:head|burla-node)-[0-9a-f]{{8}}--{re.escape(project_id)}"
    return re.fullmatch(ephemeral, subdomain) is not None


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
        user = content.get("user") or {}
        project_id = user.get("user") or ""
        token = (user.get("metas") or {}).get("token") or ""
        subdomain = content.get("subdomain") or ""
        custom_domains = content.get("custom_domains") or []
        if content.get("proxy_type") != "https":
            return _reject("only https (SNI passthrough) proxies are allowed")
        if subdomain and custom_domains:
            return _reject("canonical and custom hostnames require separate proxies")
        if custom_domains:
            if len(custom_domains) != 1 or "*" in custom_domains[0]:
                return _reject(
                    "custom proxies must set exactly one non-wildcard hostname"
                )
            routing_state = _routing_state(project_id, token)
            if routing_state is None:
                return _reject(f"custom hostname authorization failed for {project_id}")
            relay_hostname = urlparse(
                routing_state.get("relay_dashboard_url") or ""
            ).hostname
            active_hostname = urlparse(
                routing_state.get("dashboard_url") or ""
            ).hostname
            allowed_hostnames = {routing_state.get("pending_custom_hostname")}
            if active_hostname and active_hostname != relay_hostname:
                allowed_hostnames.add(active_hostname)
            custom_hostname = custom_domains[0]
            if custom_hostname not in allowed_hostnames:
                return _reject(
                    f"custom hostname {custom_hostname} is not assigned to {project_id}"
                )
            return ALLOW
        if not subdomain:
            return _reject("proxies must set a subdomain")
        if not _subdomain_belongs_to_project(subdomain, project_id):
            return _reject(
                f"subdomain {subdomain} does not belong to project {project_id}"
            )
        return ALLOW

    return ALLOW
