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
    get_logger,
    get_add_background_task_function,
)
from main_service.node import Container, Node
from main_service.helpers import Logger

router = APIRouter()


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
        if v > 2_000_000_000_000:
            dt = datetime.fromtimestamp(v / 1000.0, tz=timezone.utc)
        else:
            dt = datetime.fromtimestamp(v, tz=timezone.utc)
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

    month_keys: list[str] = []
    cur = start_month_dt
    while cur < end_boundary:
        month_keys.append(_month_key(cur))
        cur = _add_months(cur, 1)

    cutoff_sec = start_month_dt.timestamp()
    buckets: dict[str, dict[str, float]] = {mk: {} for mk in month_keys}

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

            group_key = f"{machine_type}|{gcp_region}|{1 if spot_bool else 0}"

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
                m = m_next

        if scanned >= max_scan:
            break

    months_out = []
    grand_total_raw = 0.0

    for mk in month_keys:
        groups_out = []
        month_total_raw = 0.0

        for group_key, raw in buckets[mk].items():
            machine_type, gcp_region, spot_int = group_key.split("|", 2)
            spot_bool = spot_int == "1"

            if raw <= 0:
                continue

            hrs = _quantize_hours(raw)
            month_total_raw += raw
            groups_out.append(
                {
                    "machine_type": machine_type,
                    "gcp_region": gcp_region,
                    "spot": spot_bool,
                    "total_node_hours": hrs,
                }
            )

        groups_out.sort(key=lambda g: g["total_node_hours"], reverse=True)

        grand_total_raw += month_total_raw

        months_out.append(
            {
                "month": mk,
                "total_node_hours": _quantize_hours(month_total_raw),
                "groups": groups_out,
            }
        )

    return {
        "months": months_out,
        "total_node_hours": _quantize_hours(grand_total_raw),
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


@router.get("/v1/nodes/daily_hours")
def nodes_daily_hours(
    request: Request,
    month: Optional[str] = None,  # "YYYY-MM", default current month UTC
):
    _require_auth(request)

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
    buckets: dict[str, dict[str, float]] = {dk: {} for dk in day_keys}

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

                d = d_next

        if scanned >= max_scan:
            break

    days_out = []
    total_raw = 0.0

    for dk in day_keys:
        groups_out = []
        day_total_raw = 0.0

        for group_key, raw in buckets[dk].items():
            machine_type, gcp_region, spot_int = group_key.split("|", 2)
            spot_bool = spot_int == "1"

            if raw <= 0:
                continue

            hrs = _quantize_hours(raw)
            day_total_raw += raw
            groups_out.append(
                {
                    "machine_type": machine_type,
                    "gcp_region": gcp_region,
                    "spot": spot_bool,
                    "total_node_hours": hrs,
                }
            )

        groups_out.sort(key=lambda g: g["total_node_hours"], reverse=True)
        total_raw += day_total_raw

        days_out.append(
            {
                "date": dk,
                "total_node_hours": _quantize_hours(day_total_raw),
                "groups": groups_out,
            }
        )

    return {
        "month": _month_key(month_start),
        "days": days_out,
        "total_node_hours": _quantize_hours(total_raw),
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

        spot_bool = bool(data.get("spot")) if data.get("spot") is not None else False

        nodes_out.append(
            {
                "id": doc.id,
                "instance_name": data.get("instance_name", doc.id),
                "machine_type": data.get("machine_type"),
                "gcp_region": data.get("gcp_region"),
                "spot": spot_bool,
                "started_at_ms": _to_epoch_ms(window_start),
                "ended_at_ms": _to_epoch_ms(window_end),
                "duration_hours": duration_hours,
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
