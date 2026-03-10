# import json
# import asyncio
# import docker
# import requests
# from time import time
# from datetime import datetime, timedelta, timezone
# from typing import Optional

# import pytz
# import textwrap

# from fastapi import APIRouter, Depends, Request, HTTPException
# from google.cloud import firestore
# from google.cloud.firestore_v1.base_query import FieldFilter
# from google.cloud.firestore_v1.field_path import FieldPath
# from google.cloud.compute_v1 import InstancesClient
# from starlette.responses import StreamingResponse
# from concurrent.futures import ThreadPoolExecutor

# from main_service import (
#     DB,
#     IN_LOCAL_DEV_MODE,
#     LOCAL_DEV_CONFIG,
#     DEFAULT_CONFIG,
#     PROJECT_ID,
#     BURLA_BACKEND_URL,
#     get_logger,
#     get_add_background_task_function,
# )
# from main_service.node import Container, Node
# from main_service.helpers import Logger

# router = APIRouter()


# # ============================
# # Helpers
# # ============================

# def _require_auth(request: Request) -> dict:
#     email = request.session.get("X-User-Email")
#     authorization = request.session.get("Authorization")
#     if not email or not authorization:
#         raise HTTPException(status_code=401, detail="Not authenticated")
#     return {"Authorization": authorization, "X-User-Email": email}


# def _to_epoch_ms(ts):
#     if ts is None:
#         return None
#     if isinstance(ts, (int, float)):
#         return int(ts * 1000) if ts < 2_000_000_000 else int(ts)
#     return int(ts.timestamp() * 1000)


# def _as_utc_datetime(ts) -> Optional[datetime]:
#     if ts is None:
#         return None

#     if isinstance(ts, datetime):
#         dt = ts
#     elif isinstance(ts, (int, float)):
#         v = float(ts)
#         if v > 2_000_000_000_000:
#             dt = datetime.fromtimestamp(v / 1000.0, tz=timezone.utc)
#         else:
#             dt = datetime.fromtimestamp(v, tz=timezone.utc)
#     else:
#         try:
#             dt = datetime.fromtimestamp(ts.timestamp(), tz=timezone.utc)
#         except Exception:
#             return None

#     if dt.tzinfo is None:
#         return dt.replace(tzinfo=timezone.utc)
#     return dt.astimezone(timezone.utc)


# def _month_start(dt: datetime) -> datetime:
#     dt = dt.astimezone(timezone.utc)
#     return datetime(dt.year, dt.month, 1, tzinfo=timezone.utc)


# def _day_start(dt: datetime) -> datetime:
#     dt = dt.astimezone(timezone.utc)
#     return datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)


# def _add_days(dt: datetime, days: int) -> datetime:
#     return (dt + timedelta(days=days)).astimezone(timezone.utc)


# def _add_months(dt: datetime, months: int) -> datetime:
#     dt = dt.astimezone(timezone.utc)
#     y = dt.year
#     m = dt.month + months
#     while m > 12:
#         y += 1
#         m -= 12
#     while m < 1:
#         y -= 1
#         m += 12
#     return datetime(y, m, 1, tzinfo=timezone.utc)


# def _month_key(dt: datetime) -> str:
#     dt = dt.astimezone(timezone.utc)
#     return f"{dt.year:04d}-{dt.month:02d}"


# def _date_key(dt: datetime) -> str:
#     dt = dt.astimezone(timezone.utc)
#     return f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d}"


# def _parse_yyyy_mm(s: str) -> datetime:
#     try:
#         y, m = s.split("-")
#         return datetime(int(y), int(m), 1, tzinfo=timezone.utc)
#     except Exception:
#         raise HTTPException(status_code=400, detail="Month must be YYYY-MM")


# def _quantize_hours(hours: float, step: float = 0.25) -> float:
#     return round(hours / step) * step


# # ============================
# # Cluster lifecycle
# # ============================

# def _restart_cluster(request: Request, logger: Logger):
#     start = time()
#     instance_client = InstancesClient()

#     auth_headers = _require_auth(request)

#     futures = []
#     executor = ThreadPoolExecutor(max_workers=32)

#     node_filter = FieldFilter("status", "in", ["READY", "BOOTING", "RUNNING"])
#     for node_snapshot in DB.collection("nodes").where(filter=node_filter).stream():
#         node = Node.from_snapshot(DB, logger, node_snapshot, auth_headers, instance_client)
#         futures.append(executor.submit(node.delete))

#     docker_client = None
#     if IN_LOCAL_DEV_MODE:
#         docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
#         for container in docker_client.containers():
#             if container["Names"][0].startswith("/node"):
#                 docker_client.remove_container(container["Id"], force=True)

#     config_doc = DB.collection("cluster_config").document("cluster_config").get()
#     if not config_doc.exists:
#         config_doc.reference.set(DEFAULT_CONFIG)
#         config = DEFAULT_CONFIG
#     else:
#         config = LOCAL_DEV_CONFIG if IN_LOCAL_DEV_MODE else config_doc.to_dict()

#     node_service_port = 8080

#     try:
#         msg = f"Booting {config['Nodes'][0]['quantity']} {config['Nodes'][0]['machine_type']} nodes"
#         payload = {"project_id": PROJECT_ID, "message": msg}
#         requests.post(f"{BURLA_BACKEND_URL}/v1/telemetry/log/INFO", json=payload, timeout=1)
#     except Exception:
#         pass

#     def _add_node_logged(**node_start_kwargs):
#         return Node.start(**node_start_kwargs).instance_name

#     for node_spec in config["Nodes"]:
#         for _ in range(node_spec["quantity"]):
#             if IN_LOCAL_DEV_MODE:
#                 node_service_port += 1
#             node_start_kwargs = dict(
#                 db=DB,
#                 logger=logger,
#                 machine_type=node_spec["machine_type"],
#                 gcp_region=node_spec["gcp_region"],
#                 containers=[Container.from_dict(c) for c in node_spec["containers"]],
#                 auth_headers=auth_headers,
#                 service_port=node_service_port,
#                 sync_gcs_bucket_name=config["gcs_bucket_name"],
#                 as_local_container=IN_LOCAL_DEV_MODE,
#                 inactivity_shutdown_time_sec=node_spec.get("inactivity_shutdown_time_sec"),
#                 disk_size=node_spec.get("disk_size_gb"),
#             )
#             futures.append(executor.submit(_add_node_logged, **node_start_kwargs))

#     exec_results = [future.result() for future in futures]
#     node_instance_names = [result for result in exec_results if result is not None]
#     executor.shutdown(wait=True)

#     if IN_LOCAL_DEV_MODE and docker_client is not None:
#         node_ids = [name[11:] for name in node_instance_names]
#         for container in docker_client.containers(all=True):
#             name = container["Names"][0]
#             is_main_service = name.startswith("/main_service")
#             belongs_to_current_node = any([id_ in name for id_ in node_ids])
#             if not (is_main_service or belongs_to_current_node):
#                 docker_client.remove_container(container["Id"], force=True)

#     duration = time() - start
#     logger.log(f"Restarted after {duration//60}m {duration%60}s")


# @router.post("/v1/cluster/restart")
# def restart_cluster(
#     request: Request,
#     logger: Logger = Depends(get_logger),
#     add_background_task=Depends(get_add_background_task_function),
# ):
#     add_background_task(_restart_cluster, request, logger)


# @router.post("/v1/cluster/shutdown")
# async def shutdown_cluster(request: Request, logger: Logger = Depends(get_logger)):
#     start = time()
#     instance_client = InstancesClient()

#     auth_headers = _require_auth(request)

#     try:
#         payload = {"project_id": PROJECT_ID, "message": "Cluster turned off."}
#         requests.post(f"{BURLA_BACKEND_URL}/v1/telemetry/log/INFO", json=payload, timeout=1)
#     except Exception:
#         pass

#     futures = []
#     executor = ThreadPoolExecutor(max_workers=32)

#     node_filter = FieldFilter("status", "in", ["READY", "BOOTING", "RUNNING"])
#     for node_snapshot in DB.collection("nodes").where(filter=node_filter).stream():
#         node = Node.from_snapshot(DB, logger, node_snapshot, auth_headers, instance_client)
#         futures.append(executor.submit(node.delete))

#     if IN_LOCAL_DEV_MODE:
#         docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
#         for container in docker_client.containers():
#             is_node_service_container = container["Names"][0].startswith("/node")
#             is_worker_service_container = "worker" in container["Names"][0]
#             if is_node_service_container or is_worker_service_container:
#                 docker_client.remove_container(container["Id"], force=True)

#     [future.result() for future in futures]
#     executor.shutdown(wait=True)

#     duration = time() - start
#     logger.log(f"Shut down after {duration//60}m {duration%60}s")


# # ============================
# # Usage endpoints
# # ============================

# @router.get("/v1/nodes/monthly_hours")
# def nodes_monthly_hours(
#     request: Request,
#     months_back: int = 3,
#     start_month: Optional[str] = None,
#     end_month: Optional[str] = None,  # inclusive
# ):
#     _require_auth(request)

#     if start_month or end_month:
#         if not (start_month and end_month):
#             raise HTTPException(status_code=400, detail="Provide both start_month and end_month (YYYY-MM)")
#         start_month_dt = _parse_yyyy_mm(start_month)
#         end_month_dt = _parse_yyyy_mm(end_month)
#         if end_month_dt < start_month_dt:
#             raise HTTPException(status_code=400, detail="end_month must be >= start_month")
#         end_boundary = _add_months(end_month_dt, 1)
#     else:
#         if months_back < 1:
#             raise HTTPException(status_code=400, detail="months_back must be >= 1")
#         if months_back > 60:
#             raise HTTPException(status_code=400, detail="months_back too large")

#         now = datetime.now(timezone.utc)
#         current_month_start = _month_start(now)
#         start_month_dt = _add_months(current_month_start, -(months_back - 1))
#         end_boundary = _add_months(current_month_start, 1)

#     month_keys: list[str] = []
#     cur = start_month_dt
#     while cur < end_boundary:
#         month_keys.append(_month_key(cur))
#         cur = _add_months(cur, 1)

#     cutoff_sec = start_month_dt.timestamp()
#     buckets: dict[str, dict[str, float]] = {mk: {} for mk in month_keys}

#     last_doc = None
#     scanned = 0
#     max_scan = 20000

#     while True:
#         q = (
#             DB.collection("nodes")
#             .where(filter=FieldFilter("ended_at", ">=", cutoff_sec))
#             .order_by("ended_at", direction=firestore.Query.DESCENDING)
#         )
#         if last_doc is not None:
#             q = q.start_after(last_doc)

#         docs = list(q.limit(500).stream())
#         if not docs:
#             break

#         last_doc = docs[-1]
#         scanned += len(docs)

#         for doc in docs:
#             data = doc.to_dict() or {}

#             ended_raw = data.get("ended_at")
#             started_raw = data.get("started_at") or data.get("started_booting_at")
#             if ended_raw is None or started_raw is None:
#                 continue

#             start_dt = _as_utc_datetime(started_raw)
#             end_dt = _as_utc_datetime(ended_raw)
#             if not start_dt or not end_dt or end_dt <= start_dt:
#                 continue

#             machine_type = data.get("machine_type")
#             gcp_region = data.get("gcp_region")
#             spot_bool = bool(data.get("spot")) if data.get("spot") is not None else False
#             if not machine_type or not gcp_region:
#                 continue

#             group_key = f"{machine_type}|{gcp_region}|{1 if spot_bool else 0}"

#             window_start = max(start_dt, start_month_dt)
#             window_end = min(end_dt, end_boundary)
#             if window_end <= window_start:
#                 continue

#             m = _month_start(window_start)
#             while m < window_end:
#                 m_next = _add_months(m, 1)
#                 seg_start = max(window_start, m)
#                 seg_end = min(window_end, m_next)
#                 if seg_end > seg_start:
#                     raw_hours = (seg_end - seg_start).total_seconds() / 3600.0
#                     mk = _month_key(m)
#                     if mk in buckets:
#                         buckets[mk][group_key] = buckets[mk].get(group_key, 0.0) + raw_hours
#                 m = m_next

#         if scanned >= max_scan:
#             break

#     months_out = []
#     grand_total_raw = 0.0

#     for mk in month_keys:
#         groups_out = []
#         month_total_raw = 0.0

#         for group_key, raw in buckets[mk].items():
#             machine_type, gcp_region, spot_int = group_key.split("|", 2)
#             spot_bool = spot_int == "1"

#             hrs = _quantize_hours(raw, step=0.25)
#             if hrs == 0:
#                 continue

#             month_total_raw += raw
#             groups_out.append(
#                 {
#                     "machine_type": machine_type,
#                     "gcp_region": gcp_region,
#                     "spot": spot_bool,
#                     "total_node_hours": hrs,
#                 }
#             )

#         groups_out.sort(key=lambda g: g["total_node_hours"], reverse=True)
#         month_total = _quantize_hours(month_total_raw, step=0.25)

#         grand_total_raw += month_total_raw

#         months_out.append(
#             {
#                 "month": mk,
#                 "total_node_hours": month_total,
#                 "groups": groups_out,
#             }
#         )

#     return {
#         "months": months_out,
#         "total_node_hours": _quantize_hours(grand_total_raw, step=0.25),
#         "range": {
#             "start_month": month_keys[0] if month_keys else None,
#             "end_month": month_keys[-1] if month_keys else None,
#         },
#         "meta": {
#             "rounded_to_hours_step": 0.25,
#             "scanned": scanned,
#             "max_scan": max_scan,
#         },
#     }


# @router.get("/v1/nodes/daily_hours")
# def nodes_daily_hours(
#     request: Request,
#     month: Optional[str] = None,  # "YYYY-MM", default current month UTC
# ):
#     _require_auth(request)

#     now = datetime.now(timezone.utc)
#     month_dt = _parse_yyyy_mm(month) if month else _month_start(now)
#     month_start = month_dt
#     month_end = _add_months(month_dt, 1)  # exclusive

#     day_keys: list[str] = []
#     cur = _day_start(month_start)
#     while cur < month_end:
#         day_keys.append(_date_key(cur))
#         cur = _add_days(cur, 1)

#     cutoff_sec = month_start.timestamp()
#     buckets: dict[str, dict[str, float]] = {dk: {} for dk in day_keys}

#     last_doc = None
#     scanned = 0
#     max_scan = 20000

#     while True:
#         q = (
#             DB.collection("nodes")
#             .where(filter=FieldFilter("ended_at", ">=", cutoff_sec))
#             .order_by("ended_at", direction=firestore.Query.DESCENDING)
#         )
#         if last_doc is not None:
#             q = q.start_after(last_doc)

#         docs = list(q.limit(500).stream())
#         if not docs:
#             break

#         last_doc = docs[-1]
#         scanned += len(docs)

#         for doc in docs:
#             data = doc.to_dict() or {}

#             ended_raw = data.get("ended_at")
#             started_raw = data.get("started_at") or data.get("started_booting_at")
#             if ended_raw is None or started_raw is None:
#                 continue

#             start_dt = _as_utc_datetime(started_raw)
#             end_dt = _as_utc_datetime(ended_raw)
#             if not start_dt or not end_dt or end_dt <= start_dt:
#                 continue

#             machine_type = data.get("machine_type")
#             gcp_region = data.get("gcp_region")
#             spot_bool = bool(data.get("spot")) if data.get("spot") is not None else False
#             if not machine_type or not gcp_region:
#                 continue

#             window_start = max(start_dt, month_start)
#             window_end = min(end_dt, month_end)
#             if window_end <= window_start:
#                 continue

#             group_key = f"{machine_type}|{gcp_region}|{1 if spot_bool else 0}"

#             d = _day_start(window_start)
#             while d < window_end:
#                 d_next = _add_days(d, 1)
#                 seg_start = max(window_start, d)
#                 seg_end = min(window_end, d_next)

#                 if seg_end > seg_start:
#                     raw_hours = (seg_end - seg_start).total_seconds() / 3600.0
#                     dk = _date_key(d)
#                     if dk in buckets:
#                         buckets[dk][group_key] = buckets[dk].get(group_key, 0.0) + raw_hours

#                 d = d_next

#         if scanned >= max_scan:
#             break

#     days_out = []
#     total_raw = 0.0

#     for dk in day_keys:
#         groups_out = []
#         day_total_raw = 0.0

#         for group_key, raw in buckets[dk].items():
#             machine_type, gcp_region, spot_int = group_key.split("|", 2)
#             spot_bool = spot_int == "1"

#             hrs = _quantize_hours(raw, step=0.25)
#             if hrs == 0:
#                 continue

#             day_total_raw += raw
#             groups_out.append(
#                 {
#                     "machine_type": machine_type,
#                     "gcp_region": gcp_region,
#                     "spot": spot_bool,
#                     "total_node_hours": hrs,
#                 }
#             )

#         groups_out.sort(key=lambda g: g["total_node_hours"], reverse=True)
#         total_raw += day_total_raw

#         days_out.append(
#             {
#                 "date": dk,
#                 "total_node_hours": _quantize_hours(day_total_raw, step=0.25),
#                 "groups": groups_out,
#             }
#         )

#     return {
#         "month": _month_key(month_start),
#         "days": days_out,
#         "total_node_hours": _quantize_hours(total_raw, step=0.25),
#         "meta": {
#             "rounded_to_hours_step": 0.25,
#             "scanned": scanned,
#             "max_scan": max_scan,
#         },
#     }


# # ============================
# # month_nodes paginates
# # ============================

# @router.get("/v1/nodes/month_nodes")
# def nodes_month_nodes(
#     request: Request,
#     month: Optional[str] = None,  # "YYYY-MM", default current month UTC
#     limit: int = 2000,
#     cursor_ended_at: Optional[float] = None,  # seconds
#     cursor_id: Optional[str] = None,
# ):
#     _require_auth(request)

#     limit = max(1, min(limit, 5000))

#     now = datetime.now(timezone.utc)
#     month_dt = _parse_yyyy_mm(month) if month else _month_start(now)
#     month_start = month_dt
#     month_end = _add_months(month_dt, 1)

#     cutoff_sec = month_start.timestamp()
#     doc_id_field = FieldPath.document_id()

#     q = (
#         DB.collection("nodes")
#         .where(filter=FieldFilter("ended_at", ">=", cutoff_sec))
#         .order_by("ended_at", direction=firestore.Query.DESCENDING)
#         .order_by(doc_id_field, direction=firestore.Query.DESCENDING)
#     )

#     if cursor_ended_at is not None and cursor_id is not None:
#         q = q.start_after({"ended_at": float(cursor_ended_at), doc_id_field: cursor_id})

#     docs = list(q.limit(limit).stream())

#     nodes_out = []
#     for doc in docs:
#         data = doc.to_dict() or {}

#         ended_raw = data.get("ended_at")
#         started_raw = data.get("started_at") or data.get("started_booting_at")
#         if ended_raw is None or started_raw is None:
#             continue

#         start_dt = _as_utc_datetime(started_raw)
#         end_dt = _as_utc_datetime(ended_raw)
#         if not start_dt or not end_dt or end_dt <= start_dt:
#             continue

#         window_start = max(start_dt, month_start)
#         window_end = min(end_dt, month_end)
#         if window_end <= window_start:
#             continue

#         duration_hours_raw = (window_end - window_start).total_seconds() / 3600.0
#         duration_hours = _quantize_hours(duration_hours_raw, step=0.25)
#         if duration_hours == 0:
#             continue

#         spot_bool = bool(data.get("spot")) if data.get("spot") is not None else False

#         nodes_out.append(
#             {
#                 "id": doc.id,
#                 "instance_name": data.get("instance_name", doc.id),
#                 "machine_type": data.get("machine_type"),
#                 "gcp_region": data.get("gcp_region"),
#                 "spot": spot_bool,
#                 "started_at_ms": _to_epoch_ms(window_start),
#                 "ended_at_ms": _to_epoch_ms(window_end),
#                 "duration_hours": duration_hours,
#             }
#         )

#     next_cursor = None
#     if len(docs) == limit:
#         last = docs[-1]
#         last_data = last.to_dict() or {}
#         last_ended = last_data.get("ended_at")
#         if last_ended is not None:
#             next_cursor = {"ended_at": float(last_ended), "id": last.id}

#     return {
#         "month": _month_key(month_start),
#         "nodes": nodes_out,
#         "nextCursor": next_cursor,
#         "meta": {"limit": limit, "returned": len(nodes_out)},
#     }


# # ============================
# # Cluster SSE and actions
# # ============================

# @router.get("/v1/cluster")
# async def cluster_info(request: Request, logger: Logger = Depends(get_logger)):
#     _require_auth(request)

#     queue = asyncio.Queue()
#     current_loop = asyncio.get_running_loop()

#     async def node_stream():
#         display_filter = FieldFilter("display_in_dashboard", "==", True)
#         query = DB.collection("nodes").where(filter=display_filter)
#         if len([doc for doc in query.stream()]) == 0:
#             yield f"data: {json.dumps({'type': 'empty'})}\n\n"

#         def on_snapshot(query_snapshot, changes, read_time):
#             for change in changes:
#                 doc_data = change.document.to_dict() or {}
#                 instance_name = doc_data.get("instance_name")

#                 if change.type.name == "REMOVED":
#                     event_data = {"nodeId": instance_name, "deleted": True}
#                 else:
#                     event_data = {
#                         "nodeId": instance_name,
#                         "status": doc_data.get("status"),
#                         "type": doc_data.get("machine_type"),
#                         "started_booting_at": _to_epoch_ms(doc_data.get("started_booting_at")),
#                     }
#                 current_loop.call_soon_threadsafe(queue.put_nowait, event_data)

#         node_watch = DB.collection("nodes").where(filter=display_filter).on_snapshot(on_snapshot)
#         try:
#             yield "retry: 5000\n\n"
#             yield ": init\n\n"
#             while True:
#                 try:
#                     event = await asyncio.wait_for(queue.get(), timeout=2)
#                     yield f"data: {json.dumps(event)}\n\n"
#                 except asyncio.TimeoutError:
#                     yield ": keep-alive\n\n"
#         finally:
#             node_watch.unsubscribe()

#     return StreamingResponse(
#         node_stream(),
#         media_type="text/event-stream",
#         headers={"Cache-Control": "no-cache, no-transform"},
#     )


# @router.delete("/v1/cluster/{node_id}")
# def delete_node(
#     node_id: str,
#     request: Request,
#     hide_if_failed: bool = True,
#     add_background_task=Depends(get_add_background_task_function),
#     logger: Logger = Depends(get_logger),
# ):
#     auth_headers = _require_auth(request)

#     node_doc = DB.collection("nodes").document(node_id).get()
#     node = Node.from_snapshot(DB, logger, node_doc, auth_headers)
#     add_background_task(node.delete, hide_if_failed=hide_if_failed)


# @router.get("/v1/cluster/{node_id}/logs")
# async def node_log_stream(node_id: str, request: Request):
#     _require_auth(request)

#     queue = asyncio.Queue()
#     current_loop = asyncio.get_running_loop()

#     tz_name = request.cookies.get("timezone", "UTC")
#     try:
#         tz = pytz.timezone(tz_name)
#     except Exception:
#         tz = pytz.timezone("UTC")

#     def ts_to_str(ts: float) -> str:
#         return f"[{datetime.fromtimestamp(ts, tz).strftime('%I:%M %p').lstrip('0')}]"

#     last_date_str = None
#     first_log_processed = False

#     def on_snapshot(query_snapshot, changes, read_time):
#         nonlocal last_date_str, first_log_processed
#         sorted_changes = sorted(changes, key=lambda c: (c.document.to_dict() or {}).get("ts") or 0)

#         for change in sorted_changes:
#             log_doc_dict = change.document.to_dict() or {}
#             timestamp = log_doc_dict.get("ts")
#             if not timestamp:
#                 continue

#             current_date_str = datetime.fromtimestamp(timestamp, tz).strftime("%B %d, %Y (%Z)")
#             if not first_log_processed or current_date_str != last_date_str:
#                 padding_size = max(0, (120 - 2 - len(current_date_str)) // 2)
#                 msg = f"{'-' * padding_size} {current_date_str} {'-' * padding_size}"
#                 current_loop.call_soon_threadsafe(queue.put_nowait, {"message": msg})
#                 last_date_str = current_date_str
#                 first_log_processed = True

#             timestamp_str = ts_to_str(timestamp)
#             msg_raw = str(log_doc_dict.get("msg") or "").rstrip()

#             line_len = max(20, 120 - len(timestamp_str))
#             wrapper = textwrap.TextWrapper(line_len, break_long_words=True, break_on_hyphens=True)

#             formatted_lines = []
#             for original_line in msg_raw.splitlines() or [""]:
#                 wrapped_segments = wrapper.wrap(original_line) or [""]
#                 for segment in wrapped_segments:
#                     if not formatted_lines:
#                         formatted_lines.append(f"{timestamp_str} {segment}")
#                     else:
#                         formatted_lines.append(f" {' ' * len(timestamp_str)}{segment}")

#             msg_clean = "\n".join(formatted_lines)
#             current_loop.call_soon_threadsafe(queue.put_nowait, {"message": msg_clean})

#     logs_ref = DB.collection("nodes").document(node_id).collection("logs")
#     watch = logs_ref.on_snapshot(on_snapshot)

#     async def log_generator():
#         try:
#             yield "retry: 5000\n\n"
#             yield ": init\n\n"
#             while True:
#                 try:
#                     event = await asyncio.wait_for(queue.get(), timeout=2)
#                     yield f"data: {json.dumps(event)}\n\n"
#                 except asyncio.TimeoutError:
#                     yield ": keep-alive\n\n"
#         finally:
#             watch.unsubscribe()

#     return StreamingResponse(
#         log_generator(),
#         media_type="text/event-stream",
#         headers={"Cache-Control": "no-cache, no-transform"},
#     )


# # ============================
# # Deleted/failed nodes (last 7 days), paginated
# # ============================

# @router.get("/v1/cluster/deleted_recent_paginated")
# def get_deleted_recent_paginated(
#     request: Request,
#     page: Optional[int] = None,
#     page_size: Optional[int] = None,
#     offset: Optional[int] = None,
#     limit: Optional[int] = None,
# ):
#     _require_auth(request)

#     # Accept either (page,page_size) or (offset,limit)
#     if offset is None or limit is None:
#         p = max(int(page or 0), 0)
#         ps = max(int(page_size or 15), 1)
#         offset = p * ps
#         limit = ps
#     else:
#         offset = max(int(offset), 0)
#         limit = max(int(limit), 1)

#     deleted_statuses = {"DELETED", "FAILED"}

#     cutoff_ms = int((datetime.utcnow() - timedelta(days=7)).timestamp() * 1000)

#     def _sort_ms_from_doc(data: dict) -> int:
#         deleted_ms = _to_epoch_ms(data.get("deleted_at"))
#         started_ms = _to_epoch_ms(data.get("started_booting_at"))
#         return (deleted_ms or started_ms or 0)

#     def _within_last_7_days(data: dict) -> bool:
#         return _sort_ms_from_doc(data) >= cutoff_ms

#     # NOTE: Firestore cannot "order by max(deleted_at, started_booting_at)" so we scan
#     # recent docs ordered by started_booting_at and filter to last 7 days by computed sort_ms.
#     # This is OK because the window is 7 days, which should be small.
#     others: list[dict] = []
#     last_doc = None
#     scanned = 0
#     max_scan = 20000

#     while True:
#         query = DB.collection("nodes").order_by(
#             "started_booting_at", direction=firestore.Query.DESCENDING
#         )
#         if last_doc:
#             query = query.start_after(last_doc)

#         docs = list(query.limit(500).stream())
#         if not docs:
#             break

#         last_doc = docs[-1]
#         scanned += len(docs)

#         for doc in docs:
#             data = doc.to_dict() or {}
#             status = str(data.get("status") or "").upper()
#             if status not in deleted_statuses:
#                 continue

#             if not _within_last_7_days(data):
#                 continue

#             deleted_ms = _to_epoch_ms(data.get("deleted_at"))
#             started_ms = _to_epoch_ms(data.get("started_booting_at"))
#             sort_ms = (deleted_ms or started_ms or 0)

#             others.append(
#                 {
#                     "id": doc.id,
#                     "name": data.get("instance_name", doc.id),
#                     "status": data.get("status"),
#                     "type": data.get("machine_type"),
#                     "cpus": data.get("num_cpus"),
#                     "gpus": data.get("num_gpus"),
#                     "memory": data.get("memory"),
#                     "deletedAt": deleted_ms if deleted_ms is not None else sort_ms,
#                     "started_booting_at": started_ms,
#                     "_sort_ms": sort_ms,
#                 }
#             )

#         if scanned >= max_scan:
#             break

#         # If we've scanned a decent amount and we are far past cutoff by started_booting_at,
#         # we could break, but started_booting_at might be missing. Keep it simple.

#     # Sort by recency (max(deletedAt, started_booting_at)) desc
#     others.sort(key=lambda n: (n.get("_sort_ms") or 0), reverse=True)

#     total = len(others)

#     # Page slice
#     start = offset
#     end = offset + limit
#     paged = others[start:end]

#     for n in paged:
#         n.pop("_sort_ms", None)

#     return {
#         "nodes": paged,
#         "total": total,
#         "meta": {
#             "offset": offset,
#             "limit": limit,
#             "returned": len(paged),
#             "scanned": scanned,
#             "max_scan": max_scan,
#             "cutoff_days": 7,
#         },
#     }

import json
import asyncio
import docker
import re
import requests
from time import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import pytz
import textwrap

from fastapi import APIRouter, Depends, Request, HTTPException
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from google.cloud.firestore_v1.field_path import FieldPath
from google.cloud.compute_v1 import InstancesClient
from starlette.responses import StreamingResponse
from concurrent.futures import ThreadPoolExecutor

from main_service import (
    DB,
    IN_LOCAL_DEV_MODE,
    LOCAL_DEV_CONFIG,
    DEFAULT_CONFIG,
    PROJECT_ID,
    BURLA_BACKEND_URL,
    CLUSTER_ID_TOKEN,
    get_logger,
    get_add_background_task_function,
)
from main_service.node import Container, Node
from main_service.helpers import Logger

router = APIRouter()


ON_DEMAND_HOURLY_USD_BY_MACHINE_TYPE = {
    "a2-highgpu-1g": 3.673385,
    "a2-highgpu-2g": 7.34677,
    "a2-highgpu-4g": 14.69354,
    "a2-highgpu-8g": 29.38708,
    "a2-ultragpu-1g": 5.06879789,
    "a2-ultragpu-2g": 10.137595781,
    "a2-ultragpu-4g": 20.275191562,
    "a2-ultragpu-8g": 40.550383123,
    "a3-highgpu-1g": 11.0612,
    "a3-highgpu-2g": 22.1225,
    "a3-highgpu-4g": 44.245,
    "a3-highgpu-8g": 88.490000119,
    "a3-ultragpu-8g": 84.806908493,
    "n4-standard-2": 0.0907,
    "n4-standard-4": 0.1814,
    "n4-standard-8": 0.3628,
    "n4-standard-16": 0.7256,
    "n4-standard-32": 1.4512,
    "n4-standard-64": 2.9024,
    "n4-standard-80": 3.628,
}


# ============================
# Helpers
# ============================

def _require_auth(request: Request) -> dict:
    email = request.session.get("X-User-Email")
    authorization = request.session.get("Authorization")
    if not email or not authorization:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return {"Authorization": authorization, "X-User-Email": email}


def _to_epoch_ms(ts):
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return int(ts * 1000) if ts < 2_000_000_000 else int(ts)
    return int(ts.timestamp() * 1000)


def _as_utc_datetime(ts) -> Optional[datetime]:
    if ts is None:
        return None

    if isinstance(ts, datetime):
        dt = ts
    elif isinstance(ts, (int, float)):
        v = float(ts)
        abs_v = abs(v)

        # Handle epoch values provided in seconds, milliseconds, microseconds, or nanoseconds.
        if abs_v >= 1_000_000_000_000_000_000:
            v = v / 1_000_000_000.0  # ns -> s
        elif abs_v >= 1_000_000_000_000_000:
            v = v / 1_000_000.0  # us -> s
        elif abs_v >= 100_000_000_000:
            v = v / 1_000.0  # ms -> s

        try:
            dt = datetime.fromtimestamp(v, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    elif isinstance(ts, str):
        try:
            return _as_utc_datetime(float(ts))
        except (TypeError, ValueError):
            return None
    else:
        try:
            dt = datetime.fromtimestamp(ts.timestamp(), tz=timezone.utc)
        except Exception:
            return None

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _month_start(dt: datetime) -> datetime:
    dt = dt.astimezone(timezone.utc)
    return datetime(dt.year, dt.month, 1, tzinfo=timezone.utc)


def _day_start(dt: datetime) -> datetime:
    dt = dt.astimezone(timezone.utc)
    return datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)


def _add_days(dt: datetime, days: int) -> datetime:
    return (dt + timedelta(days=days)).astimezone(timezone.utc)


def _add_months(dt: datetime, months: int) -> datetime:
    dt = dt.astimezone(timezone.utc)
    y = dt.year
    m = dt.month + months
    while m > 12:
        y += 1
        m -= 12
    while m < 1:
        y -= 1
        m += 12
    return datetime(y, m, 1, tzinfo=timezone.utc)


def _month_key(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return f"{dt.year:04d}-{dt.month:02d}"


def _date_key(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d}"


def _parse_yyyy_mm(s: str) -> datetime:
    try:
        y, m = s.split("-")
        return datetime(int(y), int(m), 1, tzinfo=timezone.utc)
    except Exception:
        raise HTTPException(status_code=400, detail="Month must be YYYY-MM")


def _quantize_hours(hours: float, decimals: int = 6) -> float:
    """
    IMPORTANT:
    Do NOT round to 0.25h increments here. That deletes short-lived nodes.
    We keep real hours (with sane precision) so even 30 seconds is counted.
    """
    if hours is None:
        return 0.0
    h = float(hours)
    if h <= 0:
        return 0.0
    return round(h, decimals)


def _safe_float(value, default: float = 0.0) -> float:
    try:
        out = float(value)
        if out != out or out in (float("inf"), float("-inf")):
            return default
        return out
    except (TypeError, ValueError):
        return default


def _safe_bool(value, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False
    return default


def _get_cluster_credit_config() -> dict:
    try:
        config_dict = DB.collection("cluster_config").document("cluster_config").get().to_dict() or {}
        return {
            "credits": _safe_bool(config_dict.get("credits"), False),
            "discount_credit_usd": max(_safe_float(config_dict.get("discount_credit_usd"), 0.0), 0.0),
            "credits_used_usd": max(_safe_float(config_dict.get("credits_used_usd"), 0.0), 0.0),
            "credits_remaining_usd": max(_safe_float(config_dict.get("credits_remaining_usd"), 0.0), 0.0),
        }
    except Exception:
        if IN_LOCAL_DEV_MODE and isinstance(LOCAL_DEV_CONFIG, dict):
            return {
                "credits": _safe_bool(LOCAL_DEV_CONFIG.get("credits"), False),
                "discount_credit_usd": max(_safe_float(LOCAL_DEV_CONFIG.get("discount_credit_usd"), 0.0), 0.0),
                "credits_used_usd": max(_safe_float(LOCAL_DEV_CONFIG.get("credits_used_usd"), 0.0), 0.0),
                "credits_remaining_usd": max(_safe_float(LOCAL_DEV_CONFIG.get("credits_remaining_usd"), 0.0), 0.0),
            }
        return {
            "credits": False,
            "discount_credit_usd": 0.0,
            "credits_used_usd": 0.0,
            "credits_remaining_usd": 0.0,
        }


def _get_cluster_billing_state() -> dict:
    try:
        billing_doc = DB.collection("billing").document("billing").get().to_dict() or {}
        return {
            "has_payment_method": _safe_bool(billing_doc.get("has_payment_method"), False),
        }
    except Exception:
        return {"has_payment_method": False}


def _is_valid_month_key(value: str) -> bool:
    return bool(re.match(r"^\d{4}-\d{2}$", str(value or "").strip()))


def _sorted_monthly_usage_docs() -> list[dict]:
    out = []
    for snapshot in DB.collection("monthly_usage").stream():
        doc = snapshot.to_dict() or {}
        month_key = str(doc.get("month") or snapshot.id or "").strip()
        if not _is_valid_month_key(month_key):
            continue
        out.append(
            {
                "month": month_key,
                "spend_dollars": round(max(_safe_float(doc.get("spend_dollars"), 0.0), 0.0), 2),
                "doc_ref": snapshot.reference,
                "doc": doc,
            }
        )
    out.sort(key=lambda item: item["month"])
    return out


def _reconcile_monthly_usage_credits_from_scratch() -> dict:
    config_ref = DB.collection("cluster_config").document("cluster_config")
    config_dict = config_ref.get().to_dict() or {}
    discount_credit_usd = round(max(_safe_float(config_dict.get("discount_credit_usd"), 0.0), 0.0), 2)
    remaining_credits = discount_credit_usd
    now_iso = datetime.now(timezone.utc).isoformat()
    existing_credits_flag = _safe_bool(config_dict.get("credits"), False)

    billing_snapshot = DB.collection("billing").document("billing").get()
    billing_doc = billing_snapshot.to_dict() if billing_snapshot.exists else {}
    has_payment_method = _safe_bool((billing_doc or {}).get("has_payment_method"), False)

    monthly_docs = _sorted_monthly_usage_docs()

    for item in monthly_docs:
        spend_dollars = item["spend_dollars"]
        credits_applied_dollars = round(min(spend_dollars, remaining_credits), 2)
        billable_spend_dollars = round(max(spend_dollars - credits_applied_dollars, 0.0), 2)
        remaining_credits = round(max(remaining_credits - credits_applied_dollars, 0.0), 2)

        existing_doc = item["doc"]
        existing_applied = round(max(_safe_float(existing_doc.get("credits_applied_dollars"), 0.0), 0.0), 2)
        existing_billable = round(max(_safe_float(existing_doc.get("billable_spend_dollars"), 0.0), 0.0), 2)

        if (
            existing_applied != credits_applied_dollars
            or existing_billable != billable_spend_dollars
            or str(existing_doc.get("month") or "") != item["month"]
        ):
            item["doc_ref"].set(
                {
                    "month": item["month"],
                    "credits_applied_dollars": credits_applied_dollars,
                    "billable_spend_dollars": billable_spend_dollars,
                    "updated_at": now_iso,
                },
                merge=True,
            )

    credits_used_usd = round(max(discount_credit_usd - remaining_credits, 0.0), 2)
    credits_remaining_usd = round(max(remaining_credits, 0.0), 2)

    # Keep credits=true after exhaustion until a payment method exists.
    if credits_remaining_usd > 0:
        credits_enabled = True
    else:
        if has_payment_method:
            credits_enabled = False
        elif discount_credit_usd > 0:
            credits_enabled = True
        else:
            credits_enabled = existing_credits_flag

    config_ref.set(
        {
            "credits_used_usd": credits_used_usd,
            "credits_remaining_usd": credits_remaining_usd,
            "credits": credits_enabled,
        },
        merge=True,
    )

    if IN_LOCAL_DEV_MODE and isinstance(LOCAL_DEV_CONFIG, dict):
        LOCAL_DEV_CONFIG["credits_used_usd"] = credits_used_usd
        LOCAL_DEV_CONFIG["credits_remaining_usd"] = credits_remaining_usd
        LOCAL_DEV_CONFIG["credits"] = credits_enabled

    return {
        "credits": credits_enabled,
        "credits_usd": discount_credit_usd,
        "credits_used_usd": credits_used_usd,
        "credits_remaining_usd": credits_remaining_usd,
    }


def _fetch_lifetime_spend_usd_from_backend(auth_headers: dict) -> Optional[float]:
    url = f"{BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}/billing"
    header_candidates = [
        auth_headers,
        {"Authorization": f"Bearer {CLUSTER_ID_TOKEN}"},
    ]

    for headers in header_candidates:
        try:
            response = requests.get(url, headers=headers, timeout=5)
            if response.status_code != 200:
                continue

            payload = response.json() or {}
            raw_lifetime_spend = payload.get("lifetime_spend_usd")
            if raw_lifetime_spend is None:
                continue

            return max(_safe_float(raw_lifetime_spend, 0.0), 0.0)
        except Exception:
            continue

    return None


def _compute_lifetime_spend_usd_from_nodes(max_scan: int = 200000) -> float:
    last_doc = None
    scanned = 0
    lifetime_spend_usd = 0.0

    while True:
        q = DB.collection("nodes").order_by("ended_at", direction=firestore.Query.DESCENDING)
        if last_doc is not None:
            q = q.start_after(last_doc)

        docs = list(q.limit(500).stream())
        if not docs:
            break

        last_doc = docs[-1]
        scanned += len(docs)

        for doc in docs:
            data = doc.to_dict() or {}

            ended_raw = data.get("ended_at")
            started_raw = data.get("started_at") or data.get("started_booting_at")
            if ended_raw is None or started_raw is None:
                continue

            start_dt = _as_utc_datetime(started_raw)
            end_dt = _as_utc_datetime(ended_raw)
            if not start_dt or not end_dt or end_dt <= start_dt:
                continue

            machine_type = str(data.get("machine_type") or "")
            hourly_rate_usd = ON_DEMAND_HOURLY_USD_BY_MACHINE_TYPE.get(machine_type)
            if hourly_rate_usd is None:
                continue

            duration_hours_raw = (end_dt - start_dt).total_seconds() / 3600.0
            if duration_hours_raw <= 0:
                continue

            lifetime_spend_usd += duration_hours_raw * hourly_rate_usd

        if scanned >= max_scan:
            break

    return max(round(lifetime_spend_usd, 2), 0.0)


def _get_cluster_billing_summary(auth_headers: dict) -> dict:
    _ = auth_headers
    cluster_billing_state = _get_cluster_billing_state()
    has_payment_method = cluster_billing_state["has_payment_method"]

    summary = _reconcile_monthly_usage_credits_from_scratch()
    return {
        "credits": _safe_bool(summary.get("credits"), False),
        "has_payment_method": has_payment_method,
        "credits_usd": round(max(_safe_float(summary.get("credits_usd"), 0.0), 0.0), 2),
        "credits_used_usd": round(max(_safe_float(summary.get("credits_used_usd"), 0.0), 0.0), 2),
        "remaining_free_credit_usd": round(
            max(_safe_float(summary.get("credits_remaining_usd"), 0.0), 0.0),
            2,
        ),
    }


_N4_RE = re.compile(r"^n4-standard-(\d+)$")


def _cpu_hour_multiplier(machine_type: str) -> int:
    # Only apply to CPU VM types: n4-standard-{2,4,8,16,32,64,80}
    m = _N4_RE.match(str(machine_type or ""))
    if not m:
        return 1
    vcpu = int(m.group(1))
    return vcpu if vcpu > 0 else 1


# ============================
# Cluster lifecycle
# ============================

def _restart_cluster(request: Request, logger: Logger):
    start = time()
    instance_client = InstancesClient()

    auth_headers = _require_auth(request)

    futures = []
    executor = ThreadPoolExecutor(max_workers=32)

    node_filter = FieldFilter("status", "in", ["READY", "BOOTING", "RUNNING"])
    for node_snapshot in DB.collection("nodes").where(filter=node_filter).stream():
        node = Node.from_snapshot(DB, logger, node_snapshot, auth_headers, instance_client)
        futures.append(executor.submit(node.delete))

    docker_client = None
    if IN_LOCAL_DEV_MODE:
        docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
        for container in docker_client.containers():
            if container["Names"][0].startswith("/node"):
                docker_client.remove_container(container["Id"], force=True)

    config_doc = DB.collection("cluster_config").document("cluster_config").get()
    if not config_doc.exists:
        config_doc.reference.set(DEFAULT_CONFIG)
        config = DEFAULT_CONFIG
    else:
        config = LOCAL_DEV_CONFIG if IN_LOCAL_DEV_MODE else config_doc.to_dict()

    node_service_port = 8080

    try:
        msg = f"Booting {config['Nodes'][0]['quantity']} {config['Nodes'][0]['machine_type']} nodes"
        payload = {"project_id": PROJECT_ID, "message": msg}
        requests.post(f"{BURLA_BACKEND_URL}/v1/telemetry/log/INFO", json=payload, timeout=1)
    except Exception:
        pass

    def _add_node_logged(**node_start_kwargs):
        return Node.start(**node_start_kwargs).instance_name

    for node_spec in config["Nodes"]:
        for _ in range(node_spec["quantity"]):
            if IN_LOCAL_DEV_MODE:
                node_service_port += 1
            node_start_kwargs = dict(
                db=DB,
                logger=logger,
                machine_type=node_spec["machine_type"],
                gcp_region=node_spec["gcp_region"],
                containers=[Container.from_dict(c) for c in node_spec["containers"]],
                auth_headers=auth_headers,
                service_port=node_service_port,
                sync_gcs_bucket_name=config["gcs_bucket_name"],
                as_local_container=IN_LOCAL_DEV_MODE,
                inactivity_shutdown_time_sec=node_spec.get("inactivity_shutdown_time_sec"),
                disk_size=node_spec.get("disk_size_gb"),
            )
            futures.append(executor.submit(_add_node_logged, **node_start_kwargs))

    exec_results = [future.result() for future in futures]
    node_instance_names = [result for result in exec_results if result is not None]
    executor.shutdown(wait=True)

    if IN_LOCAL_DEV_MODE and docker_client is not None:
        node_ids = [name[11:] for name in node_instance_names]
        for container in docker_client.containers(all=True):
            name = container["Names"][0]
            is_main_service = name.startswith("/main_service")
            belongs_to_current_node = any([id_ in name for id_ in node_ids])
            if not (is_main_service or belongs_to_current_node):
                docker_client.remove_container(container["Id"], force=True)

    duration = time() - start
    logger.log(f"Restarted after {duration//60}m {duration%60}s")


@router.post("/v1/cluster/restart")
def restart_cluster(
    request: Request,
    logger: Logger = Depends(get_logger),
    add_background_task=Depends(get_add_background_task_function),
):
    add_background_task(_restart_cluster, request, logger)


@router.post("/v1/cluster/shutdown")
async def shutdown_cluster(request: Request, logger: Logger = Depends(get_logger)):
    start = time()
    instance_client = InstancesClient()

    auth_headers = _require_auth(request)

    try:
        payload = {"project_id": PROJECT_ID, "message": "Cluster turned off."}
        requests.post(f"{BURLA_BACKEND_URL}/v1/telemetry/log/INFO", json=payload, timeout=1)
    except Exception:
        pass

    futures = []
    executor = ThreadPoolExecutor(max_workers=32)

    node_filter = FieldFilter("status", "in", ["READY", "BOOTING", "RUNNING"])
    for node_snapshot in DB.collection("nodes").where(filter=node_filter).stream():
        node = Node.from_snapshot(DB, logger, node_snapshot, auth_headers, instance_client)
        futures.append(executor.submit(node.delete))

    if IN_LOCAL_DEV_MODE:
        docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
        for container in docker_client.containers():
            is_node_service_container = container["Names"][0].startswith("/node")
            is_worker_service_container = "worker" in container["Names"][0]
            if is_node_service_container or is_worker_service_container:
                docker_client.remove_container(container["Id"], force=True)

    [future.result() for future in futures]
    executor.shutdown(wait=True)

    duration = time() - start
    logger.log(f"Shut down after {duration//60}m {duration%60}s")


# ============================
# Usage endpoints
# ============================


def _calculate_spend_dollars_from_groups(groups: list[dict]) -> float:
    spend = 0.0
    for group in groups or []:
        machine_type = str(group.get("machine_type") or "")
        hourly_rate_usd = ON_DEMAND_HOURLY_USD_BY_MACHINE_TYPE.get(machine_type)
        if hourly_rate_usd is None:
            continue
        node_hours = max(_safe_float(group.get("total_node_hours"), 0.0), 0.0)
        spend += node_hours * hourly_rate_usd
    return round(spend, 2)


def _aggregate_monthly_usage(
    start_month_dt: datetime,
    end_boundary: datetime,
    max_scan: int = 20000,
) -> dict:
    month_keys: list[str] = []
    cur = start_month_dt
    while cur < end_boundary:
        month_keys.append(_month_key(cur))
        cur = _add_months(cur, 1)

    cutoff_sec = start_month_dt.timestamp()
    # VM-hours per group per month
    buckets: dict[str, dict[str, float]] = {mk: {} for mk in month_keys}
    # CPU compute-hours per group per month
    compute_buckets: dict[str, dict[str, float]] = {mk: {} for mk in month_keys}

    last_doc = None
    scanned = 0

    while True:
        q = (
            DB.collection("nodes")
            .where(filter=FieldFilter("ended_at", ">=", cutoff_sec))
            .order_by("ended_at", direction=firestore.Query.DESCENDING)
        )
        if last_doc is not None:
            q = q.start_after(last_doc)

        docs = list(q.limit(500).stream())
        if not docs:
            break

        last_doc = docs[-1]
        scanned += len(docs)

        for doc in docs:
            data = doc.to_dict() or {}

            ended_raw = data.get("ended_at")
            started_raw = data.get("started_at") or data.get("started_booting_at")
            if ended_raw is None or started_raw is None:
                continue

            start_dt = _as_utc_datetime(started_raw)
            end_dt = _as_utc_datetime(ended_raw)
            if not start_dt or not end_dt or end_dt <= start_dt:
                continue

            machine_type = data.get("machine_type")
            gcp_region = data.get("gcp_region")
            spot_bool = bool(data.get("spot")) if data.get("spot") is not None else False
            if not machine_type or not gcp_region:
                continue

            group_key = f"{machine_type}|{gcp_region}|{1 if spot_bool else 0}"
            mult = _cpu_hour_multiplier(machine_type)

            window_start = max(start_dt, start_month_dt)
            window_end = min(end_dt, end_boundary)
            if window_end <= window_start:
                continue

            m = _month_start(window_start)
            while m < window_end:
                m_next = _add_months(m, 1)
                seg_start = max(window_start, m)
                seg_end = min(window_end, m_next)
                if seg_end > seg_start:
                    raw_hours = (seg_end - seg_start).total_seconds() / 3600.0
                    mk = _month_key(m)
                    if mk in buckets:
                        buckets[mk][group_key] = buckets[mk].get(group_key, 0.0) + raw_hours
                    if mk in compute_buckets:
                        compute_buckets[mk][group_key] = (
                            compute_buckets[mk].get(group_key, 0.0) + (raw_hours * mult)
                        )
                m = m_next

        if scanned >= max_scan:
            break

    months_out = []
    grand_total_raw = 0.0
    grand_total_compute_raw = 0.0

    for mk in month_keys:
        groups_out = []
        month_total_raw = 0.0
        month_total_compute_raw = 0.0

        for group_key, raw in buckets[mk].items():
            machine_type, gcp_region, spot_int = group_key.split("|", 2)
            spot_bool = spot_int == "1"

            if raw <= 0:
                continue

            raw_compute = compute_buckets[mk].get(group_key, 0.0)

            month_total_raw += raw
            month_total_compute_raw += raw_compute
            groups_out.append(
                {
                    "machine_type": machine_type,
                    "gcp_region": gcp_region,
                    "spot": spot_bool,
                    "total_node_hours": _quantize_hours(raw),
                    "total_compute_hours": _quantize_hours(raw_compute),
                }
            )

        groups_out.sort(key=lambda g: g["total_node_hours"], reverse=True)
        grand_total_raw += month_total_raw
        grand_total_compute_raw += month_total_compute_raw

        months_out.append(
            {
                "month": mk,
                "total_node_hours": _quantize_hours(month_total_raw),
                "total_compute_hours": _quantize_hours(month_total_compute_raw),
                "groups": groups_out,
            }
        )

    return {
        "months": months_out,
        "total_node_hours": _quantize_hours(grand_total_raw),
        "total_compute_hours": _quantize_hours(grand_total_compute_raw),
        "range": {
            "start_month": month_keys[0] if month_keys else None,
            "end_month": month_keys[-1] if month_keys else None,
        },
        "meta": {
            "hours_precision_decimals": 6,
            "scanned": scanned,
            "max_scan": max_scan,
        },
    }


def _write_monthly_usage_cache(months: list[dict]):
    now_iso = datetime.now(timezone.utc).isoformat()
    current_month_key = _month_key(_month_start(datetime.now(timezone.utc)))
    monthly_usage_collection = DB.collection("monthly_usage")

    for month_data in months or []:
        month_key = str(month_data.get("month") or "")
        if not month_key:
            continue

        spend_dollars = _calculate_spend_dollars_from_groups(month_data.get("groups") or [])
        usage_hours = round(max(_safe_float(month_data.get("total_compute_hours"), 0.0), 0.0), 2)
        has_non_zero_usage = spend_dollars > 0 or usage_hours > 0

        doc_ref = monthly_usage_collection.document(month_key)
        snapshot = doc_ref.get()
        exists = snapshot.exists

        if month_key == current_month_key:
            if not exists and not has_non_zero_usage:
                continue
            payload = {
                "month": month_key,
                "spend_dollars": spend_dollars,
                "usage_hours": usage_hours,
                "status": "open",
                "updated_at": now_iso,
            }
            if not exists:
                payload["created_at"] = now_iso
            doc_ref.set(payload, merge=True)
            continue

        if not exists and not has_non_zero_usage:
            continue

        payload = {
            "month": month_key,
            "spend_dollars": spend_dollars,
            "usage_hours": usage_hours,
            "status": "closed",
            "updated_at": now_iso,
        }
        if not exists:
            payload["created_at"] = now_iso
        doc_ref.set(payload, merge=True)

    _reconcile_monthly_usage_credits_from_scratch()


def _get_monthly_usage_cache_doc(month_key: str) -> Optional[dict]:
    snapshot = DB.collection("monthly_usage").document(month_key).get()
    if not snapshot.exists:
        return None

    doc = snapshot.to_dict() or {}
    spend_dollars = round(max(_safe_float(doc.get("spend_dollars"), 0.0), 0.0), 2)
    billable_spend_dollars = round(
        max(_safe_float(doc.get("billable_spend_dollars"), spend_dollars), 0.0),
        2,
    )
    credits_applied_dollars = round(
        max(_safe_float(doc.get("credits_applied_dollars"), spend_dollars - billable_spend_dollars), 0.0),
        2,
    )
    return {
        "month": month_key,
        "spend_dollars": spend_dollars,
        "billable_spend_dollars": billable_spend_dollars,
        "credits_applied_dollars": credits_applied_dollars,
        "usage_hours": round(max(_safe_float(doc.get("usage_hours"), 0.0), 0.0), 2),
        "status": str(doc.get("status") or ""),
    }


def _ensure_monthly_usage_cache_for_month(month_dt: datetime) -> dict:
    month_key = _month_key(month_dt)
    existing = _get_monthly_usage_cache_doc(month_key)
    if existing:
        return existing

    month_aggregation = _aggregate_monthly_usage(month_dt, _add_months(month_dt, 1))
    _write_monthly_usage_cache(month_aggregation.get("months") or [])
    cached = _get_monthly_usage_cache_doc(month_key)
    if cached:
        return cached

    month_row = (month_aggregation.get("months") or [{}])[0]
    fallback_groups = month_row.get("groups") or []
    return {
        "month": month_key,
        "spend_dollars": _calculate_spend_dollars_from_groups(fallback_groups),
        "billable_spend_dollars": _calculate_spend_dollars_from_groups(fallback_groups),
        "credits_applied_dollars": 0.0,
        "usage_hours": round(max(_safe_float(month_row.get("total_compute_hours"), 0.0), 0.0), 2),
        "status": "open" if month_key == _month_key(_month_start(datetime.now(timezone.utc))) else "closed",
    }


def _list_invoiceable_months() -> list[dict]:
    monthly_docs = _sorted_monthly_usage_docs()
    invoiceable = []
    seen_months = set()

    for item in monthly_docs:
        month_key = item["month"]
        if month_key in seen_months:
            continue

        doc = item["doc"]
        status = str(doc.get("status") or "").strip().lower()
        if status != "closed":
            continue

        spend_dollars = round(max(_safe_float(doc.get("spend_dollars"), 0.0), 0.0), 2)
        billable_spend_dollars = round(
            max(_safe_float(doc.get("billable_spend_dollars"), spend_dollars), 0.0),
            2,
        )
        if billable_spend_dollars <= 0:
            continue

        credits_applied_dollars = round(
            max(_safe_float(doc.get("credits_applied_dollars"), spend_dollars - billable_spend_dollars), 0.0),
            2,
        )

        invoiceable.append(
            {
                "month": month_key,
                "spend_dollars": spend_dollars,
                "credits_applied_dollars": credits_applied_dollars,
                "billable_spend_dollars": billable_spend_dollars,
            }
        )
        seen_months.add(month_key)

    return invoiceable


@router.get("/v1/nodes/monthly_hours")
def nodes_monthly_hours(
    request: Request,
    months_back: int = 3,
    start_month: Optional[str] = None,
    end_month: Optional[str] = None,  # inclusive
):
    _require_auth(request)

    if start_month or end_month:
        if not (start_month and end_month):
            raise HTTPException(status_code=400, detail="Provide both start_month and end_month (YYYY-MM)")
        start_month_dt = _parse_yyyy_mm(start_month)
        end_month_dt = _parse_yyyy_mm(end_month)
        if end_month_dt < start_month_dt:
            raise HTTPException(status_code=400, detail="end_month must be >= start_month")
        end_boundary = _add_months(end_month_dt, 1)
    else:
        if months_back < 1:
            raise HTTPException(status_code=400, detail="months_back must be >= 1")
        if months_back > 60:
            raise HTTPException(status_code=400, detail="months_back too large")

        now = datetime.now(timezone.utc)
        current_month_start = _month_start(now)
        start_month_dt = _add_months(current_month_start, -(months_back - 1))
        end_boundary = _add_months(current_month_start, 1)

    aggregation = _aggregate_monthly_usage(start_month_dt, end_boundary)
    _write_monthly_usage_cache(aggregation.get("months") or [])
    return aggregation


@router.post("/v1/billing/reconcile-credits")
def reconcile_monthly_usage_credits(request: Request):
    _require_auth(request)
    return _reconcile_monthly_usage_credits_from_scratch()


@router.get("/v1/billing/invoiceable-months")
def get_invoiceable_months(request: Request):
    _require_auth(request)
    _reconcile_monthly_usage_credits_from_scratch()
    return {"months": _list_invoiceable_months()}


@router.get("/v1/nodes/daily_hours")
def nodes_daily_hours(
    request: Request,
    month: Optional[str] = None,  # "YYYY-MM", default current month UTC
):
    auth_headers = _require_auth(request)
    billing_summary = _get_cluster_billing_summary(auth_headers)

    now = datetime.now(timezone.utc)
    month_dt = _parse_yyyy_mm(month) if month else _month_start(now)
    month_start = month_dt
    month_end = _add_months(month_dt, 1)  # exclusive

    day_keys: list[str] = []
    cur = _day_start(month_start)
    while cur < month_end:
        day_keys.append(_date_key(cur))
        cur = _add_days(cur, 1)

    cutoff_sec = month_start.timestamp()
    # VM-hours per group per day
    buckets: dict[str, dict[str, float]] = {dk: {} for dk in day_keys}
    # CPU compute-hours per group per day
    compute_buckets: dict[str, dict[str, float]] = {dk: {} for dk in day_keys}

    last_doc = None
    scanned = 0
    max_scan = 20000

    while True:
        q = (
            DB.collection("nodes")
            .where(filter=FieldFilter("ended_at", ">=", cutoff_sec))
            .order_by("ended_at", direction=firestore.Query.DESCENDING)
        )
        if last_doc is not None:
            q = q.start_after(last_doc)

        docs = list(q.limit(500).stream())
        if not docs:
            break

        last_doc = docs[-1]
        scanned += len(docs)

        for doc in docs:
            data = doc.to_dict() or {}

            ended_raw = data.get("ended_at")
            started_raw = data.get("started_at") or data.get("started_booting_at")
            if ended_raw is None or started_raw is None:
                continue

            start_dt = _as_utc_datetime(started_raw)
            end_dt = _as_utc_datetime(ended_raw)
            if not start_dt or not end_dt or end_dt <= start_dt:
                continue

            machine_type = data.get("machine_type")
            gcp_region = data.get("gcp_region")
            spot_bool = bool(data.get("spot")) if data.get("spot") is not None else False
            if not machine_type or not gcp_region:
                continue

            window_start = max(start_dt, month_start)
            window_end = min(end_dt, month_end)
            if window_end <= window_start:
                continue

            group_key = f"{machine_type}|{gcp_region}|{1 if spot_bool else 0}"
            mult = _cpu_hour_multiplier(machine_type)

            d = _day_start(window_start)
            while d < window_end:
                d_next = _add_days(d, 1)
                seg_start = max(window_start, d)
                seg_end = min(window_end, d_next)

                if seg_end > seg_start:
                    raw_hours = (seg_end - seg_start).total_seconds() / 3600.0
                    dk = _date_key(d)
                    if dk in buckets:
                        buckets[dk][group_key] = buckets[dk].get(group_key, 0.0) + raw_hours
                    if dk in compute_buckets:
                        compute_buckets[dk][group_key] = (
                            compute_buckets[dk].get(group_key, 0.0) + (raw_hours * mult)
                        )

                d = d_next

        if scanned >= max_scan:
            break

    days_out = []
    total_raw = 0.0
    total_compute_raw = 0.0

    for dk in day_keys:
        groups_out = []
        day_total_raw = 0.0
        day_total_compute_raw = 0.0

        for group_key, raw in buckets[dk].items():
            machine_type, gcp_region, spot_int = group_key.split("|", 2)
            spot_bool = spot_int == "1"

            if raw <= 0:
                continue

            raw_compute = compute_buckets[dk].get(group_key, 0.0)

            day_total_raw += raw
            day_total_compute_raw += raw_compute
            groups_out.append(
                {
                    "machine_type": machine_type,
                    "gcp_region": gcp_region,
                    "spot": spot_bool,
                    "total_node_hours": _quantize_hours(raw),
                    "total_compute_hours": _quantize_hours(raw_compute),
                }
            )

        groups_out.sort(key=lambda g: g["total_node_hours"], reverse=True)
        total_raw += day_total_raw
        total_compute_raw += day_total_compute_raw

        days_out.append(
            {
                "date": dk,
                "total_node_hours": _quantize_hours(day_total_raw),
                "total_compute_hours": _quantize_hours(day_total_compute_raw),
                "groups": groups_out,
            }
        )

    monthly_usage_cache = _ensure_monthly_usage_cache_for_month(month_start)
    monthly_usage_hours = round(
        max(_safe_float(monthly_usage_cache.get("usage_hours"), _quantize_hours(total_compute_raw)), 0.0),
        2,
    )
    monthly_spend_dollars = round(
        max(_safe_float(monthly_usage_cache.get("spend_dollars"), 0.0), 0.0),
        2,
    )

    return {
        "month": _month_key(month_start),
        "days": days_out,
        "total_node_hours": _quantize_hours(total_raw),
        "total_compute_hours": _quantize_hours(total_compute_raw),
        "monthly_usage_hours": monthly_usage_hours,
        "monthly_spend_dollars": monthly_spend_dollars,
        **billing_summary,
        "meta": {
            "hours_precision_decimals": 6,
            "scanned": scanned,
            "max_scan": max_scan,
        },
    }


# ============================
# month_nodes paginates
# ============================

@router.get("/v1/nodes/month_nodes")
def nodes_month_nodes(
    request: Request,
    month: Optional[str] = None,  # "YYYY-MM", default current month UTC
    limit: int = 2000,
    cursor_ended_at: Optional[float] = None,  # seconds
    cursor_id: Optional[str] = None,
):
    _require_auth(request)

    limit = max(1, min(limit, 5000))

    now = datetime.now(timezone.utc)
    month_dt = _parse_yyyy_mm(month) if month else _month_start(now)
    month_start = month_dt
    month_end = _add_months(month_dt, 1)

    cutoff_sec = month_start.timestamp()
    doc_id_field = FieldPath.document_id()

    q = (
        DB.collection("nodes")
        .where(filter=FieldFilter("ended_at", ">=", cutoff_sec))
        .order_by("ended_at", direction=firestore.Query.DESCENDING)
        .order_by(doc_id_field, direction=firestore.Query.DESCENDING)
    )

    if cursor_ended_at is not None and cursor_id is not None:
        q = q.start_after({"ended_at": float(cursor_ended_at), doc_id_field: cursor_id})

    docs = list(q.limit(limit).stream())

    nodes_out = []
    for doc in docs:
        data = doc.to_dict() or {}

        ended_raw = data.get("ended_at")
        started_raw = data.get("started_at") or data.get("started_booting_at")
        if ended_raw is None or started_raw is None:
            continue

        start_dt = _as_utc_datetime(started_raw)
        end_dt = _as_utc_datetime(ended_raw)
        if not start_dt or not end_dt or end_dt <= start_dt:
            continue

        window_start = max(start_dt, month_start)
        window_end = min(end_dt, month_end)
        if window_end <= window_start:
            continue

        duration_hours_raw = (window_end - window_start).total_seconds() / 3600.0
        if duration_hours_raw <= 0:
            continue

        duration_hours = _quantize_hours(duration_hours_raw)
        machine_type = data.get("machine_type")
        mult = _cpu_hour_multiplier(machine_type)
        duration_compute_hours = _quantize_hours(duration_hours_raw * mult)

        spot_bool = bool(data.get("spot")) if data.get("spot") is not None else False

        nodes_out.append(
            {
                "id": doc.id,
                "instance_name": data.get("instance_name", doc.id),
                "machine_type": machine_type,
                "gcp_region": data.get("gcp_region"),
                "spot": spot_bool,
                "started_at_ms": _to_epoch_ms(window_start),
                "ended_at_ms": _to_epoch_ms(window_end),
                "duration_hours": duration_hours,
                "duration_compute_hours": duration_compute_hours,
            }
        )

    next_cursor = None
    if len(docs) == limit:
        last = docs[-1]
        last_data = last.to_dict() or {}
        last_ended = last_data.get("ended_at")
        if last_ended is not None:
            next_cursor = {"ended_at": float(last_ended), "id": last.id}

    return {
        "month": _month_key(month_start),
        "nodes": nodes_out,
        "nextCursor": next_cursor,
        "meta": {"limit": limit, "returned": len(nodes_out), "hours_precision_decimals": 6},
    }


# ============================
# Cluster SSE and actions
# ============================

@router.get("/v1/cluster")
async def cluster_info(request: Request, logger: Logger = Depends(get_logger)):
    _require_auth(request)

    queue = asyncio.Queue()
    current_loop = asyncio.get_running_loop()

    async def node_stream():
        display_filter = FieldFilter("display_in_dashboard", "==", True)
        query = DB.collection("nodes").where(filter=display_filter)
        if len([doc for doc in query.stream()]) == 0:
            yield f"data: {json.dumps({'type': 'empty'})}\n\n"

        def on_snapshot(query_snapshot, changes, read_time):
            for change in changes:
                doc_data = change.document.to_dict() or {}
                instance_name = doc_data.get("instance_name")

                if change.type.name == "REMOVED":
                    event_data = {"nodeId": instance_name, "deleted": True}
                else:
                    event_data = {
                        "nodeId": instance_name,
                        "status": doc_data.get("status"),
                        "type": doc_data.get("machine_type"),
                        "started_booting_at": _to_epoch_ms(doc_data.get("started_booting_at")),
                    }
                current_loop.call_soon_threadsafe(queue.put_nowait, event_data)

        node_watch = DB.collection("nodes").where(filter=display_filter).on_snapshot(on_snapshot)
        try:
            yield "retry: 5000\n\n"
            yield ": init\n\n"
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=2)
                    yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            node_watch.unsubscribe()

    return StreamingResponse(
        node_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache, no-transform"},
    )


@router.delete("/v1/cluster/{node_id}")
def delete_node(
    node_id: str,
    request: Request,
    hide_if_failed: bool = True,
    add_background_task=Depends(get_add_background_task_function),
    logger: Logger = Depends(get_logger),
):
    auth_headers = _require_auth(request)

    node_doc = DB.collection("nodes").document(node_id).get()
    node = Node.from_snapshot(DB, logger, node_doc, auth_headers)
    add_background_task(node.delete, hide_if_failed=hide_if_failed)


@router.get("/v1/cluster/{node_id}/logs")
async def node_log_stream(node_id: str, request: Request):
    _require_auth(request)

    queue = asyncio.Queue()
    current_loop = asyncio.get_running_loop()

    tz_name = request.cookies.get("timezone", "UTC")
    try:
        tz = pytz.timezone(tz_name)
    except Exception:
        tz = pytz.timezone("UTC")

    def ts_to_str(ts: float) -> str:
        return f"[{datetime.fromtimestamp(ts, tz).strftime('%I:%M %p').lstrip('0')}]"

    last_date_str = None
    first_log_processed = False

    def on_snapshot(query_snapshot, changes, read_time):
        nonlocal last_date_str, first_log_processed
        sorted_changes = sorted(changes, key=lambda c: (c.document.to_dict() or {}).get("ts") or 0)

        for change in sorted_changes:
            log_doc_dict = change.document.to_dict() or {}
            timestamp = log_doc_dict.get("ts")
            if not timestamp:
                continue

            current_date_str = datetime.fromtimestamp(timestamp, tz).strftime("%B %d, %Y (%Z)")
            if not first_log_processed or current_date_str != last_date_str:
                padding_size = max(0, (120 - 2 - len(current_date_str)) // 2)
                msg = f"{'-' * padding_size} {current_date_str} {'-' * padding_size}"
                current_loop.call_soon_threadsafe(queue.put_nowait, {"message": msg})
                last_date_str = current_date_str
                first_log_processed = True

            timestamp_str = ts_to_str(timestamp)
            msg_raw = str(log_doc_dict.get("msg") or "").rstrip()

            line_len = max(20, 120 - len(timestamp_str))
            wrapper = textwrap.TextWrapper(line_len, break_long_words=True, break_on_hyphens=True)

            formatted_lines = []
            for original_line in msg_raw.splitlines() or [""]:
                wrapped_segments = wrapper.wrap(original_line) or [""]
                for segment in wrapped_segments:
                    if not formatted_lines:
                        formatted_lines.append(f"{timestamp_str} {segment}")
                    else:
                        formatted_lines.append(f" {' ' * len(timestamp_str)}{segment}")

            msg_clean = "\n".join(formatted_lines)
            current_loop.call_soon_threadsafe(queue.put_nowait, {"message": msg_clean})

    logs_ref = DB.collection("nodes").document(node_id).collection("logs")
    watch = logs_ref.on_snapshot(on_snapshot)

    async def log_generator():
        try:
            yield "retry: 5000\n\n"
            yield ": init\n\n"
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=2)
                    yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            watch.unsubscribe()

    return StreamingResponse(
        log_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache, no-transform"},
    )


# ============================
# Deleted/failed nodes (last 7 days), paginated
# ============================

@router.get("/v1/cluster/deleted_recent_paginated")
def get_deleted_recent_paginated(
    request: Request,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
):
    _require_auth(request)

    # Accept either (page,page_size) or (offset,limit)
    if offset is None or limit is None:
        p = max(int(page or 0), 0)
        ps = max(int(page_size or 15), 1)
        offset = p * ps
        limit = ps
    else:
        offset = max(int(offset), 0)
        limit = max(int(limit), 1)

    deleted_statuses = {"DELETED", "FAILED"}

    cutoff_ms = int((datetime.utcnow() - timedelta(days=7)).timestamp() * 1000)

    def _sort_ms_from_doc(data: dict) -> int:
        deleted_ms = _to_epoch_ms(data.get("deleted_at"))
        started_ms = _to_epoch_ms(data.get("started_booting_at"))
        return (deleted_ms or started_ms or 0)

    def _within_last_7_days(data: dict) -> bool:
        return _sort_ms_from_doc(data) >= cutoff_ms

    others: list[dict] = []
    last_doc = None
    scanned = 0
    max_scan = 20000

    while True:
        query = DB.collection("nodes").order_by(
            "started_booting_at", direction=firestore.Query.DESCENDING
        )
        if last_doc:
            query = query.start_after(last_doc)

        docs = list(query.limit(500).stream())
        if not docs:
            break

        last_doc = docs[-1]
        scanned += len(docs)

        for doc in docs:
            data = doc.to_dict() or {}
            status = str(data.get("status") or "").upper()
            if status not in deleted_statuses:
                continue

            if not _within_last_7_days(data):
                continue

            deleted_ms = _to_epoch_ms(data.get("deleted_at"))
            started_ms = _to_epoch_ms(data.get("started_booting_at"))
            sort_ms = (deleted_ms or started_ms or 0)

            others.append(
                {
                    "id": doc.id,
                    "name": data.get("instance_name", doc.id),
                    "status": data.get("status"),
                    "type": data.get("machine_type"),
                    "cpus": data.get("num_cpus"),
                    "gpus": data.get("num_gpus"),
                    "memory": data.get("memory"),
                    "deletedAt": deleted_ms if deleted_ms is not None else sort_ms,
                    "started_booting_at": started_ms,
                    "_sort_ms": sort_ms,
                }
            )

        if scanned >= max_scan:
            break

    others.sort(key=lambda n: (n.get("_sort_ms") or 0), reverse=True)

    total = len(others)

    start = offset
    end = offset + limit
    paged = others[start:end]

    for n in paged:
        n.pop("_sort_ms", None)

    return {
        "nodes": paged,
        "total": total,
        "meta": {
            "offset": offset,
            "limit": limit,
            "returned": len(paged),
            "scanned": scanned,
            "max_scan": max_scan,
            "cutoff_days": 7,
        },
    }
