"""
Endpoints that the burla pypi client talks to.

Everything in this file is meant to serve the `burla` Python package running
on a user's laptop during `remote_parallel_map`. No dashboard / React-frontend
code is wired in here. Dashboard endpoints live in:
- cluster_lifecycle.py
- cluster_views.py
- jobs.py
- settings.py
- storage.py
- usage.py

If you add an endpoint here, its caller should be the burla pypi client; if
you add a dashboard endpoint, put it in one of the files above.
"""

import asyncio
from time import time
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from starlette.requests import ClientDisconnect

from main_service import (
    CLOUD_PROVIDER,
    CURRENT_BURLA_VERSION,
    IN_LOCAL_DEV_MODE,
    MIN_COMPATIBLE_CLIENT_VERSION,
    get_add_background_task_function,
    get_auth_headers,
    get_logger,
)
from main_service import cluster_state, history
from main_service.helpers import Logger, parse_version
from main_service.node import Node
from main_service.transport_tls import cluster_ca_pem
from main_service.providers.catalog import (
    gpu_machine_prefix,
    gpu_machine_type,
    parallelism_capacity,
)
from main_service.scaling import (
    plan_grow_nodes,
    planned_cpu_count,
    required_cpus_per_call,
)
from main_service.endpoints.cluster_lifecycle import (
    GROW_INACTIVITY_SHUTDOWN_TIME_SEC,
    LOCAL_DEV_MAX_GROW_CPUS,
    MAX_GROW_CPUS,
    _get_cluster_config,
    _start_nodes,
    config_with_job_overrides,
    verify_nodes_can_reach_head,
)

router = APIRouter()


# ------------------------------------------------------------------
# Jobs: single-doc CRUD used by the client (and nodes) instead of
# talking to a database directly.
# ------------------------------------------------------------------


@router.get("/v1/jobs/{job_id}")
async def get_job_doc(job_id: str):
    """Return the job doc. 404 if missing."""
    job = cluster_state.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@router.patch("/v1/jobs/{job_id}")
async def patch_job_doc(job_id: str, request: Request):
    """
    Partial update. Body is merged into the in-memory job dict, except
    `fail_reason_append` - if present, that value is appended onto the
    `fail_reason` list and removed from the plain-update payload.
    """
    # A client giving up mid-request (e.g. its timeout fired) is routine.
    try:
        body = await request.json()
    except ClientDisconnect:
        return
    append = body.pop("fail_reason_append", None)
    if not body and append is None:
        return
    found = cluster_state.update_job(job_id, body, append_fail_reason=append)
    if not found:
        # Caller swallows the failure; 500 here is just log noise.
        return Response(status_code=204)


# ------------------------------------------------------------------
# Combined job-start entry point.
# ------------------------------------------------------------------


def _select_ready_nodes_from_state(
    func_cpu: int | str,
    func_ram: int | str,
    max_parallelism: int,
    image: Optional[str],
    func_gpu: Optional[str],
    region: Optional[str],
):
    """Walk the live node state, picking unreserved READY nodes that fit the
    requested per-function resources, up to `max_parallelism` total slots.
    When `image` is set, only nodes running that container are eligible.
    When `func_gpu` is set, only nodes on a matching GPU family are eligible.
    When `region` is set, only nodes in that region are eligible.

    Returns `(selected, total_parallelism, ready_after_filters,
    ready_after_gpu, ready_after_image, unfiltered_ready)`. The list-tail
    values let `start_job` tell "cluster is empty", "no ready node has the
    image", "none match the GPU", "none are in the requested region", and
    "matching nodes are too small" apart.
    """
    machine_prefix = gpu_machine_prefix(func_gpu, CLOUD_PROVIDER)
    all_nodes = cluster_state.list_nodes()
    unfiltered_ready = [
        n
        for n in all_nodes
        if n.get("status") == "READY"
        and not n.get("current_job")
        and not n.get("reserved_for_job")
        and cluster_state.node_is_fresh(n)
    ]
    ready_after_image = unfiltered_ready
    if image:
        ready_after_image = [
            n
            for n in unfiltered_ready
            if image in [c["image"] for c in n.get("containers") or []]
        ]
    ready_after_gpu = ready_after_image
    if machine_prefix:
        ready_after_gpu = [
            n
            for n in ready_after_image
            if (n.get("machine_type") or "").startswith(machine_prefix)
        ]
    ready_after_filters = ready_after_gpu
    if region:
        ready_after_filters = [
            n for n in ready_after_gpu if n.get("gcp_region") == region
        ]

    selected = []
    total_parallelism = 0
    for node_data in ready_after_filters:
        deficit = max_parallelism - total_parallelism
        if deficit <= 0:
            break
        node_parallelism = min(
            deficit,
            parallelism_capacity(node_data["machine_type"], func_cpu, func_ram),
        )
        if node_parallelism <= 0:
            continue
        selected.append(
            {
                "instance_name": node_data["instance_name"],
                "host": node_data["host"],
                "machine_type": node_data["machine_type"],
                "target_parallelism": node_parallelism,
            }
        )
        total_parallelism += node_parallelism
    return (
        selected,
        total_parallelism,
        ready_after_filters,
        ready_after_gpu,
        ready_after_image,
        unfiltered_ready,
    )


def _grow_if_needed(
    target_parallelism: int,
    n_inputs: int,
    max_parallelism: int,
    func_cpu: int | str,
    func_ram: int | str,
    image: Optional[str],
    func_gpu: Optional[str],
    region: Optional[str],
    disk_gb: Optional[int],
    job_id: str,
    logger: Logger,
    add_background_task,
) -> tuple[list[dict], Optional[int]]:
    """Schedules `_start_nodes` in the background and returns
    `(booting_nodes, grow_cpus_remaining)`: one `{instance_name,
    target_parallelism}` dict per reserved booting node (empty if no growth
    was needed), and the job's remaining grow-CPU budget for mid-job
    replacement boots (None = uncapped, i.e. GPU jobs).

    When `func_gpu` is set, each new node is one of the mapped GPU machine
    types. When `image` is set, the new nodes run that image instead of the
    cluster default.
    """
    if gpu_machine_type(func_gpu, CLOUD_PROVIDER):
        max_additional_cpus = None
    else:
        max_cpu = LOCAL_DEV_MAX_GROW_CPUS if IN_LOCAL_DEV_MODE else MAX_GROW_CPUS
        current_cpus = target_parallelism * required_cpus_per_call(func_cpu, func_ram)
        max_additional_cpus = max(0, max_cpu - current_cpus)

    requested_parallelism = min(n_inputs, max_parallelism)
    missing_slots = max(0, requested_parallelism - target_parallelism)
    if missing_slots <= 0:
        return [], max_additional_cpus

    config = config_with_job_overrides(_get_cluster_config(), region, disk_gb)
    planned = plan_grow_nodes(
        missing_slots=missing_slots,
        func_cpu=func_cpu,
        func_ram=func_ram,
        func_gpu=func_gpu,
        config=config,
        max_additional_cpus=max_additional_cpus,
    )
    if not planned:
        return [], max_additional_cpus

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
    booting_nodes = [
        {
            "instance_name": p["instance_name"],
            "target_parallelism": p["target_parallelism"],
        }
        for p in planned
    ]
    if max_additional_cpus is not None:
        max_additional_cpus = max(0, max_additional_cpus - planned_cpu_count(planned))
    return booting_nodes, max_additional_cpus


@router.post("/v1/jobs/{job_id}/start")
async def start_job(
    job_id: str,
    request: Request,
    auth_headers: dict = Depends(get_auth_headers),
    add_background_task=Depends(get_add_background_task_function),
    logger: Logger = Depends(get_logger),
):
    """
    Pick ready nodes + (optionally) grow the cluster + create the job, all
    in one round-trip.

    Request body:
        func_cpu, func_ram, n_inputs, max_parallelism, packages,
        user_python_version, burla_client_version, function_name,
        function_size_gb, started_at, is_background_job, grow,
        region (only nodes in this region serve the job; new nodes boot
        there), disk_gb (boot disk size for new nodes).

    Response on success:
        {
          "ready_nodes":   [{"instance_name", "host", "machine_type",
                             "target_parallelism"}, ...],
          "booting_nodes": [{"instance_name", "target_parallelism"}, ...],
        }

    Error responses:
        409 {"detail": {"error": "version_mismatch", ...}}    - client is outside compatible range
        409 {"detail": {"error": "no_compatible_nodes",
             "reason": "image_mismatch"
                     | "gpu_mismatch"
                     | "insufficient_capacity",
             "requested_image", "requested_func_gpu",
             "available_images"?, "available_machine_types"?}}
                                                              - ready nodes exist but none fit
        503 {"detail": {"error": "nodes_busy",
             "booting_count", "running_count"}}               - no ready nodes, some booting /
                                                                running; client should retry
        404 {"detail": "no_nodes"}                            - empty cluster, grow=False
    """
    if cluster_state.job_admission_paused():
        # `admit_job` also refuses under the state lock; this check exists to
        # give the clearer error.
        raise HTTPException(
            status_code=409,
            detail="`burla deploy` is migrating this cluster's history to a "
            "deployed cluster; retry once it finishes.",
        )

    body = await request.json()
    func_cpu = body["func_cpu"]
    if func_cpu != "dynamic":
        func_cpu = int(func_cpu)
    func_ram = body["func_ram"]
    if func_ram != "dynamic":
        func_ram = int(func_ram)
    n_inputs = int(body["n_inputs"])
    max_parallelism = int(body.get("max_parallelism") or n_inputs)
    grow = bool(body.get("grow"))
    image = body.get("image")
    func_gpu = body.get("func_gpu")
    region = body.get("region")
    disk_gb = int(body["disk_gb"]) if body.get("disk_gb") else None
    client_version = body["burla_client_version"]

    # --- version check ---
    try:
        lower = parse_version(MIN_COMPATIBLE_CLIENT_VERSION)
        upper = parse_version(CURRENT_BURLA_VERSION)
        current = parse_version(client_version)
    except Exception:
        raise HTTPException(status_code=400, detail="malformed version")
    if not lower <= current <= upper:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "version_mismatch",
                "lower_version": MIN_COMPATIBLE_CLIENT_VERSION,
                "upper_version": CURRENT_BURLA_VERSION,
                "current_version": client_version,
            },
        )

    # --- validate func_gpu early so both selection and grow can assume it maps cleanly ---
    try:
        gpu_machine_type(func_gpu, CLOUD_PROVIDER)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))

    # --- select from live ready nodes ---
    (
        ready,
        target_parallelism,
        all_ready,
        ready_after_gpu,
        ready_after_image,
        unfiltered_ready,
    ) = _select_ready_nodes_from_state(
        func_cpu=func_cpu,
        func_ram=func_ram,
        max_parallelism=max_parallelism,
        image=image,
        func_gpu=func_gpu,
        region=region,
    )

    if not ready and not grow:
        # Distinguish "cluster is booting, come back" from "cluster is empty".
        state_snapshot = cluster_state.list_nodes()
        booting_count = sum(1 for n in state_snapshot if n.get("status") == "BOOTING")
        running_count = sum(1 for n in state_snapshot if n.get("status") == "RUNNING")
        if booting_count or running_count:
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "nodes_busy",
                    "booting_count": booting_count,
                    "running_count": running_count,
                },
            )
        if not unfiltered_ready:
            raise HTTPException(status_code=404, detail="no_nodes")
        # Ready nodes exist but none are selectable for this job. Pick the
        # most specific reason so the client can tell the user what to do.
        if image and not ready_after_image:
            reason = "image_mismatch"
        elif func_gpu and ready_after_image and not ready_after_gpu:
            reason = "gpu_mismatch"
        elif region and ready_after_gpu and not all_ready:
            reason = "region_mismatch"
        else:
            reason = "insufficient_capacity"
        detail: dict = {
            "error": "no_compatible_nodes",
            "reason": reason,
            "requested_image": image,
            "requested_func_gpu": func_gpu,
            "requested_region": region,
        }
        if reason == "image_mismatch":
            detail["available_images"] = sorted(
                {
                    c["image"]
                    for n in unfiltered_ready
                    for c in (n.get("containers") or [])
                }
            )
        elif reason == "gpu_mismatch":
            detail["available_machine_types"] = sorted(
                {
                    n.get("machine_type")
                    for n in ready_after_image
                    if n.get("machine_type")
                }
            )
        elif reason == "region_mismatch":
            detail["available_regions"] = sorted(
                {n.get("gcp_region") for n in ready_after_gpu if n.get("gcp_region")}
            )
        raise HTTPException(status_code=409, detail=detail)

    # --- grow, if requested and short on capacity ---
    booting_nodes: list[dict] = []
    grow_cpus_remaining: Optional[int] = None
    if grow:
        if min(n_inputs, max_parallelism) > target_parallelism:
            # In a thread because the verification request arrives back
            # through this same event loop.
            await asyncio.to_thread(verify_nodes_can_reach_head)
        booting_nodes, grow_cpus_remaining = _grow_if_needed(
            target_parallelism=target_parallelism,
            n_inputs=n_inputs,
            max_parallelism=max_parallelism,
            func_cpu=func_cpu,
            func_ram=func_ram,
            image=image,
            func_gpu=func_gpu,
            region=region,
            disk_gb=disk_gb,
            job_id=job_id,
            logger=logger,
            add_background_task=add_background_task,
        )

    # --- create the job and claim warm nodes atomically ---
    job = {
        "n_inputs": n_inputs,
        "func_cpu": func_cpu,
        "func_ram": func_ram,
        "image": image,
        "func_gpu": func_gpu,
        # Kept on the job so mid-job replacement boots honor them too.
        "region": region,
        "disk_gb": disk_gb,
        "grow": grow,
        # Remaining CPU budget for mid-job replacement boots (None = uncapped).
        "grow_cpus_remaining": grow_cpus_remaining,
        "packages": body.get("packages") or {},
        "status": "RUNNING",
        "burla_client_version": client_version,
        "user_python_version": body["user_python_version"],
        "target_parallelism": target_parallelism,
        "max_parallelism": max_parallelism,
        "user": auth_headers["X-User-Email"],
        "function_name": body["function_name"],
        "function_size_gb": float(body.get("function_size_gb") or 0.0),
        "started_at": float(body.get("started_at") or time()),
        "is_background_job": bool(body.get("is_background_job")),
        "all_inputs_uploaded": False,
        "client_has_all_results": False,
        "fail_reason": [],
    }
    selected_instance_names = [node["instance_name"] for node in ready]
    if not cluster_state.admit_job(job_id, job, selected_instance_names):
        raise HTTPException(status_code=503, detail={"error": "nodes_busy"})

    return {
        "ready_nodes": ready,
        "booting_nodes": booting_nodes,
        "cluster_ca": None if IN_LOCAL_DEV_MODE else cluster_ca_pem(),
    }


# ------------------------------------------------------------------
# Cluster state reads used during node selection and BOOTING polling.
# ------------------------------------------------------------------


@router.get("/v1/cluster/state")
async def get_cluster_state():
    """
    Returns the data `wait_for_nodes_to_be_ready` needs in one round-trip:
    counts of BOOTING / RUNNING nodes plus the list of unreserved READY
    node docs. `reserved_for_job` nodes are filtered here so the client
    doesn't re-filter (matches `_select_ready_nodes_from_state`).
    """
    nodes_snapshot = cluster_state.list_nodes()
    booting_count = 0
    running_count = 0
    ready_nodes = []
    for data in nodes_snapshot:
        status = data.get("status")
        if status == "BOOTING" and not data.get("loaded_from_history"):
            booting_count += 1
        elif status == "RUNNING" and cluster_state.node_is_fresh(data):
            running_count += 1
        elif (
            status == "READY"
            and not data.get("current_job")
            and not data.get("reserved_for_job")
            and cluster_state.node_is_fresh(data)
        ):
            ready_nodes.append(data)
    return {
        "booting_count": booting_count,
        "running_count": running_count,
        "ready_nodes": ready_nodes,
        "cluster_ca": None if IN_LOCAL_DEV_MODE else cluster_ca_pem(),
    }


@router.get("/v1/cluster/nodes")
async def list_cluster_nodes():
    """Every live node dict (BOOTING/READY/RUNNING/FAILED). Used by tests and
    tooling that previously queried the database directly."""
    return {"nodes": cluster_state.list_nodes()}


@router.get("/v1/cluster/nodes/{node_id}")
async def get_cluster_node(node_id: str):
    """
    Read a single node's live state. Used by the client to poll a BOOTING node.
    """
    data = cluster_state.get_node(node_id)
    if data is None:
        raise HTTPException(status_code=404, detail="node not found")
    return data


@router.get("/v1/jobs/{job_id}/nodes")
async def get_job_nodes(job_id: str):
    """Nodes running or reserved for this job. Polled by the client during a
    job to discover mid-job replacement nodes, which it must assign the job
    to (only the client holds the pickled function)."""
    return {"nodes": cluster_state.nodes_for_job(job_id)}


# Earliest log matching one of these is usually the root cause; later logs
# ("Startup script failed!", timeout tracebacks) are cascades.
_FAIL_LOG_TOKENS = ("Error", "error", "failed", "Traceback", "Exception")


# 404 on "no match" lets the client distinguish real failure explanations
# from innocuous info logs and fall back cleanly.
@router.get("/v1/cluster/nodes/{node_id}/fail_reason")
async def get_node_fail_reason(node_id: str):
    reason = await asyncio.to_thread(
        history.first_failure_log, node_id, _FAIL_LOG_TOKENS
    )
    if reason:
        return {"reason": reason}
    raise HTTPException(status_code=404, detail="no failure log for node")


@router.post("/v1/cluster/nodes/{node_id}/fail")
async def fail_cluster_node(
    node_id: str,
    request: Request,
    add_background_task=Depends(get_add_background_task_function),
    logger: Logger = Depends(get_logger),
):
    """
    Marks the node FAILED, records the reason, then triggers VM deletion.
    Single call replaces the client's old three-op `_fail_and_delete` sequence.
    """
    try:
        body = await request.json()
    except Exception:
        body = {}
    reason = str((body or {}).get("reason") or "")

    node_dict = cluster_state.update_node(node_id, {"status": "FAILED"})
    if reason:
        cluster_state.add_node_log(node_id, reason)

    node = Node.from_state(logger, node_dict)
    add_background_task(node.delete)
