"""
HTTP client for the head (main_service). Replaces every Firestore read/write/
watch this service used to make. Authenticates with the cluster token.

The core pattern is `push_state`: the node reports its status (and, mid-job,
its progress) and the response carries the head's current view back down:
`host` (needed while booting) and the job signal set (cancellation,
all_inputs_uploaded, client_has_all_results, quorum info). A background loop
in __init__.py calls it every second; transition points call it directly.
"""

import asyncio
import json
import os
import ssl
from typing import Optional

import aiohttp

from node_service import (
    AZURE_DELETE_LEASE_PATH,
    CLUSTER_ID_TOKEN,
    INSTANCE_NAME,
    MAIN_SERVICE_URL,
    SELF,
)

_HEADERS = {"Authorization": f"Bearer {CLUSTER_ID_TOKEN}"}
_NODE_AUTH_TOKEN = os.environ.get("BURLA_NODE_AUTH_TOKEN")
_STATE_HEADERS = dict(_HEADERS)
if _NODE_AUTH_TOKEN:
    _STATE_HEADERS["X-Burla-Node-Token"] = _NODE_AUTH_TOKEN
_TIMEOUT = aiohttp.ClientTimeout(total=10)
_CA_PATH = os.environ.get("CLUSTER_CA_PATH")
_SSL_CONTEXT = ssl.create_default_context(cafile=_CA_PATH) if _CA_PATH else None

_session: Optional[aiohttp.ClientSession] = None
_push_lock = asyncio.Lock()


def _install_delete_lease(lease: dict):
    temporary_path = AZURE_DELETE_LEASE_PATH.with_suffix(".tmp")
    descriptor = os.open(temporary_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "w") as lease_file:
        json.dump(lease, lease_file)
    os.replace(temporary_path, AZURE_DELETE_LEASE_PATH)
    SELF["delete_lease_expires_at"] = lease["expires_at"]


def _get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        connector = aiohttp.TCPConnector(ssl=_SSL_CONTEXT)
        _session = aiohttp.ClientSession(timeout=_TIMEOUT, connector=connector)
    return _session


async def push_state(
    status: Optional[str] = None,
    include_job_progress: bool = False,
    **fields,
) -> dict:
    """PUT this node's state to the head; returns the head's view:
    {"status", "host", "reserved_for_job", "job": {...} | None}."""
    async with _push_lock:
        body = dict(fields)
        body["delete_lease_expires_at"] = SELF["delete_lease_expires_at"]
        if status is not None:
            body["status"] = SELF["reported_status"]
        if include_job_progress and SELF["current_job"]:
            body["job_progress"] = {
                "job_id": SELF["current_job"],
                "current_num_results": SELF["num_results_received"],
                "client_contact_last_1s": SELF.get("client_contact_last_1s", True),
            }
        session = _get_session()
        url = f"{MAIN_SERVICE_URL}/v1/nodes/{INSTANCE_NAME}/state"
        async with session.put(url, json=body, headers=_STATE_HEADERS) as response:
            response.raise_for_status()
            view = await response.json()
            lease = view.get("delete_lease")
            if lease:
                _install_delete_lease(lease)
            return view


def apply_job_signals(job_view: Optional[dict]):
    """Copy the job signal set from a push response into SELF, mirroring what
    the old per-job firestore on_snapshot callback did."""
    if not job_view or not job_view.get("exists"):
        return
    if job_view.get("all_inputs_uploaded"):
        SELF["all_inputs_uploaded"] = True
    if job_view.get("cluster_shutdown"):
        SELF["pending_cluster_shutdown"] = True
    if job_view.get("cluster_restarted"):
        SELF["pending_cluster_restarted"] = True
    if job_view.get("dashboard_canceled"):
        SELF["pending_dashboard_canceled"] = True
    SELF["job_view"] = job_view


async def get_job(job_id: str) -> Optional[dict]:
    session = _get_session()
    url = f"{MAIN_SERVICE_URL}/v1/jobs/{job_id}"
    async with session.get(url, headers=_HEADERS) as response:
        if response.status == 404:
            return None
        response.raise_for_status()
        return await response.json()


async def update_job(
    job_id: str, updates: dict, append_fail_reason: Optional[str] = None
):
    body = dict(updates)
    if append_fail_reason is not None:
        body["fail_reason_append"] = append_fail_reason
    session = _get_session()
    url = f"{MAIN_SERVICE_URL}/v1/jobs/{job_id}"
    async with session.patch(url, json=body, headers=_HEADERS) as response:
        response.raise_for_status()


async def issue_certificate(csr: str) -> str:
    session = _get_session()
    url = f"{MAIN_SERVICE_URL}/v1/nodes/{INSTANCE_NAME}/certificate"
    async with session.post(url, json={"csr": csr}, headers=_HEADERS) as response:
        response.raise_for_status()
        return (await response.json())["certificate"]


async def get_peers(job_id: str) -> dict:
    """{"peers": [{"instance_name", "host"}, ...], "booting_node_ids": [...]}"""
    session = _get_session()
    url = f"{MAIN_SERVICE_URL}/v1/jobs/{job_id}/peers"
    async with session.get(url, headers=_HEADERS) as response:
        response.raise_for_status()
        return await response.json()


async def request_replacement_nodes(
    job_id: str, missing_slots: int, request_id: str
) -> dict:
    """Ask the head to boot machines covering slots this node permanently
    lost to pressure retirement. Returns {"booted": [...], "slots_booted"}.
    `request_id` makes retries after a lost response safe (the head replays
    the original plan instead of booting again)."""
    session = _get_session()
    url = f"{MAIN_SERVICE_URL}/v1/jobs/{job_id}/replacement_nodes"
    body = {
        "requesting_node": INSTANCE_NAME,
        "missing_slots": missing_slots,
        "request_id": request_id,
    }
    async with session.post(url, json=body, headers=_HEADERS) as response:
        response.raise_for_status()
        return await response.json()


async def post_node_logs(logs: list[dict]):
    session = _get_session()
    url = f"{MAIN_SERVICE_URL}/v1/nodes/{INSTANCE_NAME}/logs:batch"
    async with session.post(url, json={"logs": logs}, headers=_HEADERS) as response:
        response.raise_for_status()


async def post_resource_metrics(samples: list[dict], call_events: list[dict]):
    session = _get_session()
    url = f"{MAIN_SERVICE_URL}/v1/nodes/{INSTANCE_NAME}/metrics:batch"
    async with session.post(
        url, json={"samples": samples, "call_events": call_events}, headers=_HEADERS
    ) as response:
        response.raise_for_status()


async def post_job_logs(job_id: str, documents: list[dict]):
    session = _get_session()
    url = f"{MAIN_SERVICE_URL}/v1/jobs/{job_id}/logs:batch"
    async with session.post(
        url, json={"documents": documents}, headers=_HEADERS
    ) as response:
        response.raise_for_status()


async def request_self_delete():
    session = _get_session()
    url = f"{MAIN_SERVICE_URL}/v1/nodes/{INSTANCE_NAME}/self_delete"
    async with session.post(url, headers=_HEADERS) as response:
        response.raise_for_status()
