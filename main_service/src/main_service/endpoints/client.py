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

import math
from time import time
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request
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
    parallelism_capacity,
    parse_version,
)
from main_service.node import Node
from main_service.endpoints.cluster_lifecycle import (
    LOCAL_DEV_MAX_GROW_CPUS,
    MAX_GROW_CPUS,
    _get_cluster_config,
    _machine_type_cpu_count,
    _start_nodes,
)

router = APIRouter()

ASYNC_DB = firestore.AsyncClient(project=PROJECT_ID, database="burla")


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
    await ASYNC_DB.collection("jobs").document(job_id).update(update)


# ------------------------------------------------------------------
# Combined job-start entry point.
#
# Replaces three round-trips the client used to make (`GET /v1/cluster/state`
# -> local node selection -> `POST /v1/cluster/grow` -> `POST /v1/jobs/{id}`)
# with a single call served entirely from the in-memory `NODES_CACHE`.
# ------------------------------------------------------------------


def _select_ready_nodes_from_cache(func_cpu: int, func_ram: int, max_parallelism: int):
    """Walk the ready-node cache, picking unreserved ones that fit the
    requested per-function resources, up to `max_parallelism` total slots.
    Returns (selected_nodes, total_parallelism).
    """
    with _nodes_cache_lock:
        all_nodes = list(NODES_CACHE.values())
    ready_nodes = [
        n for n in all_nodes
        if n.get("status") == "READY" and not n.get("reserved_for_job")
    ]

    selected = []
    total_parallelism = 0
    for node_data in ready_nodes:
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
    return selected, total_parallelism, ready_nodes


def _grow_if_needed(
    target_parallelism: int,
    n_inputs: int,
    max_parallelism: int,
    func_cpu: int,
    func_ram: int,
    job_id: str,
    logger: Logger,
    auth_headers: dict,
    add_background_task,
) -> list[str]:
    """Mirror of the client's old grow math in _execute_job. Schedules
    `_start_nodes` in the background and returns the instance names it
    reserved for this job (empty list if no growth needed)."""
    requested_parallelism = min(n_inputs, max_parallelism)
    required_cpus_for_ram = (func_ram + 3) // 4
    required_cpus_per_call = max(func_cpu, required_cpus_for_ram)
    target_cpus = requested_parallelism * required_cpus_per_call
    current_cpus = target_parallelism * required_cpus_per_call
    missing_cpus = max(0, target_cpus - current_cpus)
    if missing_cpus <= 0:
        return []

    max_cpu = LOCAL_DEV_MAX_GROW_CPUS if IN_LOCAL_DEV_MODE else MAX_GROW_CPUS
    max_additional_cpus = max(0, max_cpu - current_cpus)
    num_cpus_to_add = min(missing_cpus, max_additional_cpus)
    if num_cpus_to_add <= 0:
        return []

    config = _get_cluster_config()
    node_spec = config["Nodes"][0]
    cpu_per_node = _machine_type_cpu_count(node_spec["machine_type"])
    n_nodes_to_add = math.ceil(num_cpus_to_add / cpu_per_node)
    node_instance_names = [f"burla-node-{uuid4().hex[:8]}" for _ in range(n_nodes_to_add)]

    add_background_task(
        _start_nodes,
        logger,
        auth_headers,
        config,
        n_nodes_to_add,
        node_instance_names,
        job_id,
    )
    return node_instance_names


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
          "ready_nodes": [{"instance_name", "host", "machine_type",
                           "target_parallelism"}, ...],
          "booting_node_names": [...],   # populated only when grow triggered growth
          "target_parallelism": <sum across ready_nodes>,
        }

    Error responses:
        409 {"detail": "version_mismatch", "lower_version", "upper_version",
             "current_version"}  - client is outside compatible range
        409 {"detail": "no_compatible_nodes"}                 - ready nodes exist but none fit
        503 {"detail": "nodes_busy",                           - no ready nodes, some booting /
             "booting_count", "running_count"}                   running; client should retry
        404 {"detail": "no_nodes"}                            - empty cluster, grow=False
    """
    body = await request.json()
    func_cpu = int(body["func_cpu"])
    func_ram = int(body["func_ram"])
    n_inputs = int(body["n_inputs"])
    max_parallelism = int(body.get("max_parallelism") or n_inputs)
    grow = bool(body.get("grow"))
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

    # --- select from cached ready nodes ---
    ready, target_parallelism, all_ready = _select_ready_nodes_from_cache(
        func_cpu=func_cpu, func_ram=func_ram, max_parallelism=max_parallelism
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
        if all_ready:
            # Ready nodes exist but none have enough capacity for this UDF.
            raise HTTPException(status_code=409, detail="no_compatible_nodes")
        raise HTTPException(status_code=404, detail="no_nodes")

    # --- grow, if requested and short on capacity ---
    booting_node_names: list[str] = []
    if grow:
        booting_node_names = _grow_if_needed(
            target_parallelism=target_parallelism,
            n_inputs=n_inputs,
            max_parallelism=max_parallelism,
            func_cpu=func_cpu,
            func_ram=func_ram,
            job_id=job_id,
            logger=logger,
            auth_headers=auth_headers,
            add_background_task=add_background_task,
        )

    # --- write the job doc ---
    job_doc = {
        "n_inputs": n_inputs,
        "func_cpu": func_cpu,
        "func_ram": func_ram,
        "packages": body.get("packages") or {},
        "status": "RUNNING",
        "burla_client_version": client_version,
        "user_python_version": body["user_python_version"],
        "target_parallelism": target_parallelism,
        "user": auth_headers["X-User-Email"],
        "function_name": body["function_name"],
        "function_size_gb": float(body.get("function_size_gb") or 0.0),
        "started_at": float(body.get("started_at") or time()),
        "is_background_job": bool(body.get("is_background_job")),
        "all_inputs_uploaded": False,
        "client_has_all_results": False,
        "fail_reason": [],
    }
    await ASYNC_DB.collection("jobs").document(job_id).set(job_doc)

    return {
        "ready_nodes": ready,
        "booting_node_names": booting_node_names,
        "target_parallelism": target_parallelism,
    }


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


