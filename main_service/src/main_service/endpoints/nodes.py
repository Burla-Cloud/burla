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
from hmac import compare_digest
from time import time

from fastapi import APIRouter, Depends, HTTPException, Request
from main_service.endpoints.cluster_lifecycle import (
    GROW_INACTIVITY_SHUTDOWN_TIME_SEC,
    _get_cluster_config,
    _start_nodes,
)
from main_service.helpers import Logger
from main_service.node import Node
from main_service.providers import get_provider
from main_service.scaling import plan_grow_nodes, planned_cpu_count
from main_service.transport_tls import cluster_ca_pem, node_auth_token, sign_node_csr

from main_service import (
    CLOUD_PROVIDER,
    CLUSTER_ID_TOKEN,
    IN_CLIENT_HOSTED_MODE,
    cluster_state,
    get_add_background_task_function,
    get_logger,
    history,
    relay_fqdn,
)


def _require_node_auth(request: Request):
    if request.headers.get("Authorization") != f"Bearer {CLUSTER_ID_TOKEN}":
        raise HTTPException(status_code=401, detail="Node authentication required")


router = APIRouter(dependencies=[Depends(_require_node_auth)])

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
    if CLOUD_PROVIDER == "azure" and IN_CLIENT_HOSTED_MODE:
        from main_service.providers.azure import (
            DELETE_LEASE_REFRESH_SEC,
            azure_delete_lease,
        )

        lease_expires_at = body.get("delete_lease_expires_at")
        if (
            lease_expires_at is not None
            and lease_expires_at - time() <= DELETE_LEASE_REFRESH_SEC
        ):
            supplied_token = request.headers.get("X-Burla-Node-Token", "")
            if not compare_digest(supplied_token, node_auth_token(instance_name)):
                raise HTTPException(
                    status_code=403, detail="Node token required for Azure lease"
                )
            lease = await asyncio.to_thread(
                azure_delete_lease, instance_name, merged.get("zone")
            )
            if lease["expires_at"] > lease_expires_at:
                response["delete_lease"] = lease
    return response


@router.post("/v1/nodes/{instance_name}/logs:batch")
async def push_node_logs(instance_name: str, request: Request):
    body = await request.json()
    # Same pipe, two sinks: entries carrying a "debug" payload are structured
    # engineering events (never user-visible), everything else is the
    # user-facing node-log story.
    debug_entries = [
        {**log["debug"], "ts": log.get("ts")} for log in body["logs"] if log.get("debug")
    ]
    user_logs = [log for log in body["logs"] if not log.get("debug")]
    if debug_entries:
        await asyncio.to_thread(history.add_debug_logs, instance_name, debug_entries)
    if user_logs:
        await asyncio.to_thread(cluster_state.add_node_logs, instance_name, user_logs)


@router.post("/v1/nodes/{instance_name}/certificate")
async def issue_node_certificate(instance_name: str, request: Request):
    node = cluster_state.get_node(instance_name)
    if node is None or not node.get("private_ip"):
        raise HTTPException(status_code=409, detail="Node addresses are not registered")
    body = await request.json()
    # The client connects to the node's relay hostname, so the cert needs it
    # as a DNS SAN for client-side hostname verification.
    certificate = sign_node_csr(
        body["csr"],
        node["public_ip"],
        node["private_ip"],
        dns_names=[relay_fqdn(instance_name)],
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
    node = Node.from_state(logger, node_dict, provider=get_provider())

    def _delete_vm_only():
        # The node already pushed its terminal status (DELETED or FAILED);
        # only the VM itself is left to clean up.
        node.provider.delete_instance(instance_name, node.zone)
        cluster_state.update_node(instance_name, {"status": "DELETED"})

    add_background_task(_delete_vm_only)


@router.get("/v1/jobs/{job_id}/peers")
async def get_job_peers(job_id: str):
    return cluster_state.peers_for_job(job_id)


@router.post("/v1/jobs/{job_id}/replacement_nodes")
async def boot_replacement_nodes(
    job_id: str,
    request: Request,
    add_background_task=Depends(get_add_background_task_function),
    logger: Logger = Depends(get_logger),
):
    """A job node permanently lost workers to pressure retirement and asks for
    its missing slots to be booted as fresh machines. Pure execution: all
    policy (when to ask, how many slots) lives on the node, this endpoint only
    plans machine types, enforces the job's grow-CPU budget, and boots. Slots
    are conserved - the caller gives up the slots this boots.

    Body: {"requesting_node", "missing_slots", "request_id"}. Replaying the
    same request_id returns the original plan instead of booting again, so a
    node that never saw the response can retry safely.
    """
    body = await request.json()
    requesting_node = body["requesting_node"]
    missing_slots = int(body["missing_slots"])
    request_id = body["request_id"]

    if missing_slots <= 0:
        raise HTTPException(status_code=400, detail="missing_slots must be > 0")
    if cluster_state.job_admission_paused():
        raise HTTPException(status_code=409, detail="job admission is paused")

    job = cluster_state.get_job(job_id)
    if job is None or job.get("status") != "RUNNING" or not job.get("grow"):
        raise HTTPException(
            status_code=409, detail="job is not RUNNING with grow=True"
        )

    previous = (job.get("replacement_requests") or {}).get(requesting_node)
    if previous and previous.get("request_id") == request_id:
        return {
            "booted": previous["booted"],
            "slots_booted": previous["slots_booted"],
        }

    config = _get_cluster_config()
    planned = plan_grow_nodes(
        missing_slots=missing_slots,
        func_cpu=job["func_cpu"],
        func_ram=job["func_ram"],
        func_gpu=job.get("func_gpu"),
        config=config,
        max_additional_cpus=job.get("grow_cpus_remaining"),
    )
    if not planned:
        raise HTTPException(
            status_code=409, detail="replacement refused: grow CPU budget exhausted"
        )

    image = job.get("image")
    containers_override = [{"image": image}] if image else None
    add_background_task(
        _start_nodes,
        logger,
        config,
        len(planned),
        [p["instance_name"] for p in planned],
        job_id,
        [p["machine_type"] for p in planned],
        containers_override,
        GROW_INACTIVITY_SHUTDOWN_TIME_SEC,
        [p["target_parallelism"] for p in planned],
    )
    slots_booted = sum(p["target_parallelism"] for p in planned)
    cluster_state.record_replacement_request(
        job_id,
        requesting_node,
        {"request_id": request_id, "booted": planned, "slots_booted": slots_booted},
        cpus_booted=planned_cpu_count(planned),
    )
    names = [p["instance_name"] for p in planned]
    logger.log(
        f"Booting {len(planned)} replacement node(s) {names} covering "
        f"{slots_booted} slots for job {job_id} (requested by {requesting_node})."
    )
    # The head's side of the transaction, in the same flight recorder as the
    # nodes' events: records the plan and the budget spent on it.
    updated_job = cluster_state.get_job(job_id) or {}
    await asyncio.to_thread(
        history.add_debug_logs,
        "head",
        [
            {
                "job_id": job_id,
                "event": "replacement_planned",
                "fields": {
                    "requested_by": requesting_node,
                    "request_id": request_id,
                    "missing_slots": missing_slots,
                    "booted": planned,
                    "grow_cpus_remaining": updated_job.get("grow_cpus_remaining"),
                },
            }
        ],
    )
    return {"booted": planned, "slots_booted": slots_booted}


@router.post("/v1/jobs/{job_id}/logs:batch")
async def push_job_logs(job_id: str, request: Request):
    body = await request.json()
    await asyncio.to_thread(history.add_job_logs, job_id, body["documents"])
