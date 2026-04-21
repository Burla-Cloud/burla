"""
Async HTTP client for the endpoints `main_service` exposes for the burla
client. This is all the state-mutation / state-read the client does against
the cluster; firestore is never touched from the client directly.

Each method maps one-to-one to a main_service endpoint.
"""

from typing import Optional

import aiohttp
import requests

from burla import get_cluster_dashboard_url
from burla._auth import get_auth_headers


_TIMEOUT = aiohttp.ClientTimeout(total=30)


class NodesBusy(Exception):
    """
    Raised by `ClusterClient.start_job` on a 503 nodes_busy response - no
    ready nodes for this UDF yet, but some are booting / running. Caller
    decides whether to wait and retry or give up.
    """


def _local_host_from(host: str) -> str:
    """
    Rewrite docker-compose internal hostnames (`http://node_...`) back to
    `http://localhost:PORT` so the client can reach nodes in local-dev mode.
    No-op for real cluster hosts. Also no-op when the caller itself is on the
    local-burla-cluster docker network (i.e. a nested remote_parallel_map
    running inside a worker container), since that caller can reach
    `node_xxx:PORT` directly and localhost would point at its own container.
    """
    if host.startswith("http://node_") and not _on_local_cluster_network():
        return f"http://localhost:{host.split(':')[-1]}"
    return host


def _on_local_cluster_network() -> bool:
    from burla import get_cluster_dashboard_url

    return "main_service" in get_cluster_dashboard_url()


def _build_patch_job_body(
    updates: Optional[dict],
    append_fail_reason: Optional[str],
) -> dict:
    body = dict(updates or {})
    if append_fail_reason is not None:
        body["fail_reason_append"] = append_fail_reason
    return body


class ClusterClient:
    """
    Thin wrapper around main_service HTTP endpoints. Construction is free -
    only resolves the cluster dashboard URL; every call goes over the
    provided `aiohttp.ClientSession`.
    """

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self._url = get_cluster_dashboard_url()

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Optional[dict] = None,
    ):
        url = f"{self._url}{path}"
        async with self.session.request(
            method,
            url,
            json=json_body,
            headers=get_auth_headers(),
            timeout=_TIMEOUT,
        ) as response:
            if response.status == 404:
                return None
            response.raise_for_status()
            if response.content_length == 0:
                return None
            try:
                return await response.json()
            except aiohttp.ContentTypeError:
                return None

    # ---------- jobs/{id} ----------

    async def start_job(self, job_id: str, config: dict) -> dict:
        """
        One-shot: picks ready nodes from main_service's in-memory cache,
        grows the cluster if `grow=True`, writes the initial job doc, and
        returns `{ready_nodes, booting_nodes}`. Each list element is a dict
        with at least `instance_name` and `target_parallelism`.

        Replaces the old client-side sequence (fetch cluster state -> locally
        pick ready nodes -> `POST /v1/cluster/grow` -> `POST /v1/jobs/{id}`)
        with a single round-trip.

        Raises `VersionMismatch`, `NoCompatibleNodes`, `NoNodes`,
        `UnauthorizedError` for their respective 4xx responses and the
        module-level `NodesBusy` for 503 `nodes_busy` - the caller decides
        whether to wait-and-retry.
        """
        # Lazy import: `_node` imports `ClusterClient` at module level, so
        # importing the exception classes up top would be a circular import.
        from burla._node import (
            NoCompatibleNodes,
            NoNodes,
            UnauthorizedError,
            VersionMismatch,
        )

        url = f"{self._url}/v1/jobs/{job_id}/start"
        async with self.session.request(
            "POST",
            url,
            json=config,
            headers=get_auth_headers(),
            timeout=_TIMEOUT,
        ) as response:
            status = response.status
            try:
                body = (await response.json()) or {}
            except aiohttp.ContentTypeError:
                body = {}

        if 200 <= status < 300:
            return body

        detail = body.get("detail") if isinstance(body, dict) else None
        if status == 409 and isinstance(detail, dict) and detail.get("error") == "version_mismatch":
            raise VersionMismatch(
                detail["lower_version"],
                detail["upper_version"],
                detail["current_version"],
            )
        if status == 409 and detail == "no_compatible_nodes":
            raise NoCompatibleNodes()
        if status == 503 and isinstance(detail, dict) and detail.get("error") == "nodes_busy":
            raise NodesBusy()
        if status == 404:
            raise NoNodes("\n\nZero nodes are ready. Is your cluster turned on?\n")
        if status == 401:
            raise UnauthorizedError()
        raise Exception(f"POST /v1/jobs/{job_id}/start failed: {status} {body!r}")

    async def get_job(self, job_id: str) -> Optional[dict]:
        return await self._request("GET", f"/v1/jobs/{job_id}")

    async def patch_job(
        self,
        job_id: str,
        updates: Optional[dict] = None,
        append_fail_reason: Optional[str] = None,
    ) -> None:
        body = _build_patch_job_body(updates, append_fail_reason)
        await self._request("PATCH", f"/v1/jobs/{job_id}", json_body=body)

    @staticmethod
    def patch_job_sync(
        job_id: str,
        updates: Optional[dict] = None,
        append_fail_reason: Optional[str] = None,
    ) -> None:
        """
        Best-effort synchronous PATCH /v1/jobs/{id}. Used by the signal
        handler (and the final-except block in `remote_parallel_map`) where
        an event loop is not available. Swallows any failure - if
        main_service is unreachable the cancel flow still proceeds locally.
        """
        url = f"{get_cluster_dashboard_url()}/v1/jobs/{job_id}"
        body = _build_patch_job_body(updates, append_fail_reason)
        try:
            requests.patch(url, json=body, headers=get_auth_headers(), timeout=10)
        except Exception:
            pass

    # ---------- cluster / nodes ----------

    async def get_cluster_state(self) -> dict:
        """
        Returns {booting_count, running_count, ready_nodes: [...]}. Replaces
        the three separate `where(status == ...)` firestore queries the
        client used to make.
        """
        state = await self._request("GET", "/v1/cluster/state")
        return state or {"booting_count": 0, "running_count": 0, "ready_nodes": []}

    async def get_node(self, node_id: str) -> Optional[dict]:
        return await self._request("GET", f"/v1/cluster/nodes/{node_id}")

    async def fail_node(self, node_id: str, reason: str) -> None:
        await self._request(
            "POST",
            f"/v1/cluster/nodes/{node_id}/fail",
            json_body={"reason": reason},
        )
