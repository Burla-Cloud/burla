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
import math
from time import time
from typing import Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from google.api_core.exceptions import NotFound
from google.cloud import firestore
from google.cloud.firestore import ArrayUnion

from main_service import (
    CURRENT_BURLA_VERSION,
    DB,
    IN_LOCAL_DEV_MODE,
    MIN_COMPATIBLE_CLIENT_VERSION,
    NODES_CACHE,
    PROJECT_ID,
    _nodes_cache_lock,
    get_add_background_task_function,
    get_auth_headers,
    get_logger,
)
from main_service.helpers import (
    Logger,
    gpu_machine_prefix,
    gpu_machine_type,
    parallelism_capacity,
    parse_version,
)
from main_service.node import Node
from main_service.endpoints.cluster_lifecycle import (
    GROW_INACTIVITY_SHUTDOWN_TIME_SEC,
    LOCAL_DEV_MAX_GROW_CPUS,
    MAX_GROW_CPUS,
    _get_cluster_config,
    _machine_type_cpu_count,
    _pack_n4_standard_machines,
    _pack_n4_standard_machines_up_to,
    _prepare_node_boot_plan,
    _start_nodes,
)
from main_service.quota import (
    INSTANCE_BUCKET,
    N4_CPU_BUCKET,
    active_machine_types_for_region,
    n4_cpu_count,
    quota_status,
)

router = APIRouter()

ASYNC_DB = firestore.AsyncClient(project=PROJECT_ID, database="burla")

# The initial job-doc write in `start_job` is the single biggest piece of
# latency in that endpoint (~150-400ms round-trip to firestore). Since the
# client's very next step is a network hop to each node, there is plenty of
# time for the write to land before any node actually reads the doc, so we
# fire it asynchronously and return the response immediately.
#
# We keep a strong reference to the scheduled task so the event loop does
# not GC it mid-flight.
_in_flight_job_doc_writes: set[asyncio.Task] = set()


async def _write_initial_job_doc(job_id: str, job_doc: dict, logger: Logger):
    try:
        await ASYNC_DB.collection("jobs").document(job_id).set(job_doc)
    except Exception as e:
        logger.log(
            f"Failed to write initial job doc for job {job_id}: {e}",
            severity="ERROR",
        )


# ------------------------------------------------------------------
# Jobs: single-doc CRUD used by the client instead of talking to
# firestore directly.
# ------------------------------------------------------------------


@router.get("/v1/jobs/{job_id}")
async def get_job_doc(job_id: str):
    """Return the job doc. 404 if missing."""
    snapshot = await ASYNC_DB.collection("jobs").document(job_id).get()
    if not snapshot.exists:
        raise HTTPException(status_code=404, detail="job not found")
    return snapshot.to_dict()


@router.patch("/v1/jobs/{job_id}")
async def patch_job_doc(job_id: str, request: Request):
    """
    Partial update. Body is forwarded to firestore.update() verbatim, except
    `fail_reason_append` - if present, that value is ArrayUnion-ed onto the
    `fail_reason` field and removed from the plain-update payload.
    """
    body = await request.json()
    append = body.pop("fail_reason_append", None)
    update = dict(body)
    if append is not None:
        update["fail_reason"] = ArrayUnion([append])
    if not update:
        return
    try:
        await ASYNC_DB.collection("jobs").document(job_id).update(update)
    except NotFound:
        # Caller swallows the failure; 500 here is just log noise.
        return Response(status_code=204)


# ------------------------------------------------------------------
# Combined job-start entry point.
#
# Replaces three round-trips the client used to make (`GET /v1/cluster/state`
# -> local node selection -> `POST /v1/cluster/grow` -> `POST /v1/jobs/{id}`)
# with a single call served entirely from the in-memory `NODES_CACHE`.
# ------------------------------------------------------------------


def _select_ready_nodes_from_cache(
    func_cpu: int,
    func_ram: int,
    max_parallelism: int,
    image: Optional[str],
    func_gpu: Optional[str],
):
    """Walk the ready-node cache, picking unreserved ones that fit the
    requested per-function resources, up to `max_parallelism` total slots.
    When `image` is set, only nodes running that container are eligible.
    When `func_gpu` is set, only nodes on a matching GPU family are eligible.

    Returns `(selected, total_parallelism, ready_after_filters,
    ready_after_image, unfiltered_ready)`. The three list-tail values let
    `start_job` tell "cluster is empty", "ready nodes exist but none have
    the image", "ready nodes have the image but none match the GPU", and
    "ready nodes match image+GPU but are too small" apart.
    """
    machine_prefix = gpu_machine_prefix(func_gpu)
    with _nodes_cache_lock:
        all_nodes = list(NODES_CACHE.values())
    unfiltered_ready = [
        n for n in all_nodes
        if n.get("status") == "READY" and not n.get("reserved_for_job")
    ]
    ready_after_image = unfiltered_ready
    if image:
        ready_after_image = [
            n for n in unfiltered_ready
            if image in [c["image"] for c in n.get("containers") or []]
        ]
    ready_after_filters = ready_after_image
    if machine_prefix:
        ready_after_filters = [
            n for n in ready_after_image
            if (n.get("machine_type") or "").startswith(machine_prefix)
        ]

    selected = []
    total_parallelism = 0
    for node_data in ready_after_filters:
        deficit = max_parallelism - total_parallelism
        if deficit <= 0:
            break
        node_parallelism = parallelism_capacity(
            node_data["machine_type"], func_cpu, func_ram
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
        ready_after_image,
        unfiltered_ready,
    )


def _active_machine_types(gcp_region: str) -> list[str]:
    with _nodes_cache_lock:
        nodes = list(NODES_CACHE.values())
    return active_machine_types_for_region(nodes, gcp_region)


def _n4_quota_warning(
    requested_cpus: int,
    allowed_machine_types: list[str],
    gcp_region: str,
    active_machine_types: list[str],
) -> dict:
    status = quota_status(N4_CPU_BUCKET, gcp_region, active_machine_types)
    allowed_cpus = sum(n4_cpu_count(machine_type) for machine_type in allowed_machine_types)
    return {
        "type": "quota_capped",
        "machine_type": "n4-standard",
        "region": gcp_region,
        "requested": requested_cpus,
        "allowed": allowed_cpus,
        "count_unit": "vCPUs",
        "limit": status["limit"],
        "used": status["used"],
        "available": status["available"],
        "quota": status["quota"],
        "units": status["units"],
    }


def _grow_if_needed(
    target_parallelism: int,
    n_inputs: int,
    max_parallelism: int,
    func_cpu: int,
    func_ram: int,
    image: Optional[str],
    func_gpu: Optional[str],
    job_id: str,
    logger: Logger,
    auth_headers: dict,
    add_background_task,
) -> tuple[list[dict], list[dict]]:
    requested_parallelism = min(n_inputs, max_parallelism)
    gpu_mt = gpu_machine_type(func_gpu)
    quota_warnings: list[dict] = []

    if gpu_mt:
        missing_nodes = max(0, requested_parallelism - target_parallelism)
        if missing_nodes <= 0:
            return [], []
        node_machine_types = [gpu_mt] * missing_nodes
        config = _get_cluster_config()
    else:
        required_cpus_for_ram = (func_ram + 3) // 4
        required_cpus_per_call = max(func_cpu, required_cpus_for_ram)
        target_cpus = requested_parallelism * required_cpus_per_call
        current_cpus = target_parallelism * required_cpus_per_call
        missing_cpus = max(0, target_cpus - current_cpus)
        if missing_cpus <= 0:
            return [], []

        max_cpu = LOCAL_DEV_MAX_GROW_CPUS if IN_LOCAL_DEV_MODE else MAX_GROW_CPUS
        max_additional_cpus = max(0, max_cpu - current_cpus)
        num_cpus_to_add = min(missing_cpus, max_additional_cpus)
        if num_cpus_to_add <= 0:
            return [], []

        config = _get_cluster_config()
        node_spec = config["Nodes"][0]
        configured_machine_type = node_spec["machine_type"]

        # For CPU (n4-standard) clusters, ignore the configured size and pack the
        # required CPUs into as many n4-standard-80s as fit, with the remainder
        # covered by the smallest n4-standard that fits. GPU clusters keep using
        # the configured machine type so GPU jobs still land on GPU hardware.
        # Local dev stays homogeneous because node containers hard-code 2 workers
        # regardless of the advertised machine_type (see INSTANCE_N_CPUS).
        pack_by_size = (
            not IN_LOCAL_DEV_MODE
            and configured_machine_type.startswith("n4-standard-")
        )

        if pack_by_size:
            gcp_region = config["Nodes"][0]["gcp_region"]
            active_machine_types = _active_machine_types(gcp_region)
            cpu_status = quota_status(N4_CPU_BUCKET, gcp_region, active_machine_types)
            instance_status = quota_status(INSTANCE_BUCKET, gcp_region, active_machine_types)
            requested_machine_types = _pack_n4_standard_machines(num_cpus_to_add)
            requested_cpus = sum(n4_cpu_count(mt) for mt in requested_machine_types)
            if (
                requested_cpus <= cpu_status["available"]
                and len(requested_machine_types) <= instance_status["available"]
            ):
                node_machine_types = requested_machine_types
            else:
                quota_limited_cpus = min(num_cpus_to_add, cpu_status["available"])
                node_machine_types = _pack_n4_standard_machines_up_to(quota_limited_cpus)
                node_machine_types = node_machine_types[: instance_status["available"]]
            allowed_cpus = sum(n4_cpu_count(mt) for mt in node_machine_types)
            if allowed_cpus < num_cpus_to_add or len(node_machine_types) < len(requested_machine_types):
                quota_warnings.append(
                    _n4_quota_warning(
                        requested_cpus=num_cpus_to_add,
                        allowed_machine_types=node_machine_types,
                        gcp_region=gcp_region,
                        active_machine_types=active_machine_types,
                    )
                )
        else:
            cpu_per_node = _machine_type_cpu_count(configured_machine_type)
            n_nodes_to_add = math.ceil(num_cpus_to_add / cpu_per_node)
            node_machine_types = [configured_machine_type] * n_nodes_to_add

    # A machine_type whose capacity is 0 for this func_cpu/func_ram would boot a
    # node that can't run a single call, and the client would then send
    # parallelism=0 to it, producing a misleading 409 from the node.
    node_machine_types = [
        mt for mt in node_machine_types
        if parallelism_capacity(mt, func_cpu, func_ram) > 0
    ]
    if not node_machine_types:
        if quota_warnings:
            gcp_region = config["Nodes"][0]["gcp_region"]
            if target_parallelism > 0:
                return [], quota_warnings
            raise HTTPException(
                status_code=400,
                detail={
                    "error_code": "quota_exceeded",
                    "region": gcp_region,
                    "caps": quota_warnings,
                    "message": (
                        "Cluster is at its N4 CPU quota in "
                        f"{gcp_region}; nothing can grow for this job shape."
                    ),
                },
            )
        return [], []

    gcp_region = config["Nodes"][0]["gcp_region"]
    node_machine_types, shared_warnings = _prepare_node_boot_plan(
        config,
        node_machine_types,
        active_machine_types=_active_machine_types(gcp_region),
        raise_on_zero=False,
    )
    caps = quota_warnings + shared_warnings

    if not node_machine_types and caps and target_parallelism > 0:
        return [], caps
    if not node_machine_types and caps:
        raise HTTPException(
            status_code=400,
            detail={
                "error_code": "quota_exceeded",
                "region": gcp_region,
                "caps": caps,
                "message": (
                    "Cluster is at its per-machine-type quota in "
                    f"{gcp_region}; nothing can grow for this job shape."
                ),
            },
        )
    if not node_machine_types:
        return [], []

    node_instance_names = [f"burla-node-{uuid4().hex[:8]}" for _ in node_machine_types]
    containers_override = [{"image": image}] if image else None

    add_background_task(
        _start_nodes,
        logger,
        auth_headers,
        config,
        len(node_instance_names),
        node_instance_names,
        job_id,
        node_machine_types,
        containers_override,
        GROW_INACTIVITY_SHUTDOWN_TIME_SEC,
        quota_checked=True,
    )
    booting_nodes = [
        {
            "instance_name": name,
            "target_parallelism": parallelism_capacity(machine_type, func_cpu, func_ram),
        }
        for name, machine_type in zip(node_instance_names, node_machine_types)
    ]
    return booting_nodes, caps


@router.post("/v1/jobs/{job_id}/start")
async def start_job(
    job_id: str,
    request: Request,
    auth_headers: dict = Depends(get_auth_headers),
    add_background_task=Depends(get_add_background_task_function),
    logger: Logger = Depends(get_logger),
):
    """
    Pick ready nodes + (optionally) grow the cluster + write the job doc, all
    in one round-trip. Replaces the old three-call sequence the client used
    to make (`GET /v1/cluster/state` -> local selection -> `POST /v1/cluster/grow`
    -> `POST /v1/jobs/{id}`).

    Request body:
        func_cpu, func_ram, n_inputs, max_parallelism, packages,
        user_python_version, burla_client_version, function_name,
        function_size_gb, started_at, is_background_job, grow.

    Response on success:
        {
          "ready_nodes":   [{"instance_name", "host", "machine_type",
                             "target_parallelism"}, ...],
          "booting_nodes": [{"instance_name", "target_parallelism"}, ...],
          "warnings"?:     [{"type": "quota_capped", "machine_type",
                             "region", "requested", "allowed", "limit"}, ...]
        }

    Error responses:
        400 {"detail": {"error_code": "quota_exceeded", "region", "caps", "message"}}
                                                              - grow was requested but every
                                                                chosen machine_type is already
                                                                at its per-region quota; nothing
                                                                would boot for this job shape
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
    body = await request.json()
    func_cpu = int(body["func_cpu"])
    func_ram = int(body["func_ram"])
    n_inputs = int(body["n_inputs"])
    max_parallelism = int(body.get("max_parallelism") or n_inputs)
    grow = bool(body.get("grow"))
    image = body.get("image")
    func_gpu = body.get("func_gpu")
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
        gpu_machine_type(func_gpu)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))

    # --- select from cached ready nodes ---
    (
        ready,
        target_parallelism,
        all_ready,
        ready_after_image,
        unfiltered_ready,
    ) = _select_ready_nodes_from_cache(
        func_cpu=func_cpu,
        func_ram=func_ram,
        max_parallelism=max_parallelism,
        image=image,
        func_gpu=func_gpu,
    )

    if not ready and not grow:
        # Distinguish "cluster is booting, come back" from "cluster is empty".
        with _nodes_cache_lock:
            cache_snapshot = list(NODES_CACHE.values())
        booting_count = sum(1 for n in cache_snapshot if n.get("status") == "BOOTING")
        running_count = sum(1 for n in cache_snapshot if n.get("status") == "RUNNING")
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
        elif func_gpu and ready_after_image and not all_ready:
            reason = "gpu_mismatch"
        else:
            reason = "insufficient_capacity"
        detail: dict = {
            "error": "no_compatible_nodes",
            "reason": reason,
            "requested_image": image,
            "requested_func_gpu": func_gpu,
        }
        if reason == "image_mismatch":
            detail["available_images"] = sorted({
                c["image"]
                for n in unfiltered_ready
                for c in (n.get("containers") or [])
            })
        elif reason == "gpu_mismatch":
            detail["available_machine_types"] = sorted({
                n.get("machine_type")
                for n in ready_after_image
                if n.get("machine_type")
            })
        raise HTTPException(status_code=409, detail=detail)

    # --- grow, if requested and short on capacity ---
    booting_nodes: list[dict] = []
    warnings: list[dict] = []
    if grow:
        booting_nodes, warnings = _grow_if_needed(
            target_parallelism=target_parallelism,
            n_inputs=n_inputs,
            max_parallelism=max_parallelism,
            func_cpu=func_cpu,
            func_ram=func_ram,
            image=image,
            func_gpu=func_gpu,
            job_id=job_id,
            logger=logger,
            auth_headers=auth_headers,
            add_background_task=add_background_task,
        )

    # --- write the job doc (fire-and-forget) ---
    # Kicked off on the event loop and tracked in `_in_flight_job_doc_writes`
    # so the response can return as soon as in-memory work is done. See the
    # comment on that set above for why this is safe.
    job_doc = {
        "n_inputs": n_inputs,
        "func_cpu": func_cpu,
        "func_ram": func_ram,
        "image": image,
        "func_gpu": func_gpu,
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
    write_task = asyncio.create_task(_write_initial_job_doc(job_id, job_doc, logger))
    _in_flight_job_doc_writes.add(write_task)
    write_task.add_done_callback(_in_flight_job_doc_writes.discard)

    response: dict = {
        "ready_nodes": ready,
        "booting_nodes": booting_nodes,
    }
    if warnings:
        response["warnings"] = warnings
    return response


# ------------------------------------------------------------------
# Cluster state reads used during node selection and BOOTING polling.
# ------------------------------------------------------------------


@router.get("/v1/cluster/state")
async def cluster_state():
    """
    Returns the data `wait_for_nodes_to_be_ready` needs in one round-trip:
    counts of BOOTING / RUNNING nodes plus the list of unreserved READY
    node docs. `reserved_for_job` nodes are filtered here so the client
    doesn't re-filter (matches `_select_ready_nodes_from_cache`).

    Served from the in-memory `NODES_CACHE`, which a firestore on_snapshot
    listener keeps continuously in sync. Zero firestore calls per request.
    """
    with _nodes_cache_lock:
        nodes_snapshot = list(NODES_CACHE.values())
    booting_count = 0
    running_count = 0
    ready_nodes = []
    for data in nodes_snapshot:
        status = data.get("status")
        if status == "BOOTING":
            booting_count += 1
        elif status == "RUNNING":
            running_count += 1
        elif status == "READY" and not data.get("reserved_for_job"):
            ready_nodes.append(data)
    return {
        "booting_count": booting_count,
        "running_count": running_count,
        "ready_nodes": ready_nodes,
    }


@router.get("/v1/cluster/nodes/{node_id}")
async def get_cluster_node(node_id: str):
    """
    Read a single node doc. Used by the client to poll a BOOTING node.
    Served from `NODES_CACHE`.
    """
    with _nodes_cache_lock:
        data = NODES_CACHE.get(node_id)
    if data is None:
        raise HTTPException(status_code=404, detail="node not found")
    return data


# Earliest log matching one of these is usually the root cause; later logs
# ("Startup script failed!", timeout tracebacks) are cascades.
_FAIL_LOG_TOKENS = ("Error", "error", "failed", "Traceback", "Exception")


# 404 on "no match" lets the client distinguish real failure explanations
# from innocuous info logs and fall back cleanly.
@router.get("/v1/cluster/nodes/{node_id}/fail_reason")
async def get_node_fail_reason(node_id: str):
    logs_ref = ASYNC_DB.collection("nodes").document(node_id).collection("logs")
    async for doc in logs_ref.order_by("ts").stream():
        msg = ((doc.to_dict() or {}).get("msg") or "").strip()
        if msg and any(tok in msg for tok in _FAIL_LOG_TOKENS):
            return {"reason": msg}
    raise HTTPException(status_code=404, detail="no failure log for node")


@router.post("/v1/cluster/nodes/{node_id}/fail")
async def fail_cluster_node(
    node_id: str,
    request: Request,
    auth_headers: dict = Depends(get_auth_headers),
    add_background_task=Depends(get_add_background_task_function),
    logger: Logger = Depends(get_logger),
):
    """
    Marks the node FAILED, writes a log subdoc, then triggers VM deletion.
    Single call replaces the client's old three-op `_fail_and_delete` sequence.
    """
    try:
        body = await request.json()
    except Exception:
        body = {}
    reason = str((body or {}).get("reason") or "")

    node_ref = ASYNC_DB.collection("nodes").document(node_id)
    await node_ref.update({"status": "FAILED"})
    if reason:
        await node_ref.collection("logs").add({"msg": reason, "ts": time()})

    node_snapshot = DB.collection("nodes").document(node_id).get()
    if node_snapshot.exists:
        node = Node.from_snapshot(DB, logger, node_snapshot, auth_headers)
        add_background_task(node.delete)


