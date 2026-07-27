"""
Endpoints node_services (and VM startup scripts) call, authenticated with the
cluster token. These replace every node -> Firestore write and watch:

- PUT  /v1/nodes/{id}/state      node pushes status/progress ~1x/sec; the
                                 response carries the head's view back down
                                 (host during boot, job signals during a job).
- POST /v1/nodes/{id}/logs:batch node + startup-script log lines.
- POST /v1/nodes/{id}/self_delete node asks the head to delete its VM
                                 (inactivity shutdown, boot failure).
- GET  /v1/jobs/{id}/peers       input-stealing ring (replaces the firestore
                                 neighbor query).
- POST /v1/jobs/{id}/logs:batch  UDF log documents from JobLogWriter.
"""

import asyncio

from fastapi import APIRouter, Request, Depends, HTTPException

from main_service import get_logger, get_add_background_task_function
from main_service import cluster_state, history
from main_service.helpers import Logger
from main_service.node import Node
from main_service.providers import get_provider
from main_service.transport_tls import cluster_ca_pem, sign_node_csr

router = APIRouter()

_NODE_STATE_FIELDS = (
    "status",
    "current_job",
    "reserved_for_job",
    "started_booting_at",
    "ended_at",
)


@router.put("/v1/nodes/{instance_name}/state")
async def push_node_state(instance_name: str, request: Request):
    body = await request.json()

    updates = {key: body[key] for key in _NODE_STATE_FIELDS if key in body}
    merged = cluster_state.record_node_push(instance_name, updates)

    progress = body.get("job_progress")
    if progress:
        cluster_state.update_job_progress(
            progress["job_id"],
            instance_name,
            current_num_results=progress.get("current_num_results"),
            client_contact_last_1s=progress.get("client_contact_last_1s"),
        )

    job_id = (progress or {}).get("job_id") or merged.get("current_job")
    response = {
        "status": merged.get("status"),
        "host": merged.get("host"),
        "reserved_for_job": merged.get("reserved_for_job"),
        "job": cluster_state.job_view(job_id) if job_id else None,
    }
    return response


@router.post("/v1/nodes/{instance_name}/logs:batch")
async def push_node_logs(instance_name: str, request: Request):
    body = await request.json()
    await asyncio.to_thread(cluster_state.add_node_logs, instance_name, body["logs"])


@router.post("/v1/nodes/{instance_name}/certificate")
async def issue_node_certificate(instance_name: str, request: Request):
    node = cluster_state.get_node(instance_name)
    if node is None or not node.get("public_ip") or not node.get("private_ip"):
        raise HTTPException(status_code=409, detail="Node addresses are not registered")
    body = await request.json()
    certificate = sign_node_csr(
        body["csr"],
        node["public_ip"],
        node["private_ip"],
    )
    return {"certificate": certificate, "cluster_ca": cluster_ca_pem()}


@router.post("/v1/nodes/{instance_name}/self_delete")
async def self_delete_node(
    instance_name: str,
    add_background_task=Depends(get_add_background_task_function),
    logger: Logger = Depends(get_logger),
):
    node_dict = cluster_state.get_node(instance_name) or {
        "instance_name": instance_name
    }
    node = Node.from_state(logger, node_dict, auth_headers={}, provider=get_provider())

    def _delete_vm_only():
        # The node already pushed its terminal status (DELETED or FAILED);
        # only the VM itself is left to clean up.
        node.provider.delete_instance(instance_name, node.zone)
        cluster_state.update_node(instance_name, {"status": "DELETED"})

    add_background_task(_delete_vm_only)


@router.get("/v1/jobs/{job_id}/peers")
async def get_job_peers(job_id: str):
    return cluster_state.peers_for_job(job_id)


@router.post("/v1/jobs/{job_id}/logs:batch")
async def push_job_logs(job_id: str, request: Request):
    body = await request.json()
    await asyncio.to_thread(history.add_job_logs, job_id, body["documents"])
