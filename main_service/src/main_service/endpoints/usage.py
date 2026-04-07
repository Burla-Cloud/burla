import re
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Request, HTTPException
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from google.cloud.firestore_v1.field_path import FieldPath

from main_service import DB

router = APIRouter()


def _require_auth(request: Request) -> dict:
    email = request.session.get("X-User-Email") or request.headers.get("X-User-Email")
    authorization = request.session.get("Authorization") or request.headers.get("Authorization")
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
        value = float(ts)
        if value > 2_000_000_000_000:
            dt = datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
        else:
            dt = datetime.fromtimestamp(value, tz=timezone.utc)
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
    year = dt.year
    month = dt.month + months
    while month > 12:
        year += 1
        month -= 12
    while month < 1:
        year -= 1
        month += 12
    return datetime(year, month, 1, tzinfo=timezone.utc)


def _month_key(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return f"{dt.year:04d}-{dt.month:02d}"


def _date_key(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d}"


def _parse_yyyy_mm(value: str) -> datetime:
    try:
        year, month = value.split("-")
        return datetime(int(year), int(month), 1, tzinfo=timezone.utc)
    except Exception:
        raise HTTPException(status_code=400, detail="Month must be YYYY-MM")


def _quantize_hours(hours: float, decimals: int = 6) -> float:
    if hours is None:
        return 0.0
    value = float(hours)
    if value <= 0:
        return 0.0
    return round(value, decimals)


_N4_RE = re.compile(r"^n4-standard-(\d+)$")


def _cpu_hour_multiplier(machine_type: str) -> int:
    match = _N4_RE.match(str(machine_type or ""))
    if not match:
        return 1
    vcpu = int(match.group(1))
    return vcpu if vcpu > 0 else 1


@router.get("/v1/nodes/monthly_hours")
def nodes_monthly_hours(
    request: Request,
    months_back: int = 3,
    start_month: Optional[str] = None,
    end_month: Optional[str] = None,
):
    _require_auth(request)

    if start_month or end_month:
        if not (start_month and end_month):
            raise HTTPException(
                status_code=400, detail="Provide both start_month and end_month (YYYY-MM)"
            )
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
    compute_buckets: dict[str, dict[str, float]] = {mk: {} for mk in month_keys}

    last_doc = None
    scanned = 0
    max_scan = 20000

    while True:
        query = (
            DB.collection("nodes")
            .where(filter=FieldFilter("ended_at", ">=", cutoff_sec))
            .order_by("ended_at", direction=firestore.Query.DESCENDING)
        )
        if last_doc is not None:
            query = query.start_after(last_doc)

        docs = list(query.limit(500).stream())
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

            month_cursor = _month_start(window_start)
            while month_cursor < window_end:
                next_month = _add_months(month_cursor, 1)
                seg_start = max(window_start, month_cursor)
                seg_end = min(window_end, next_month)
                if seg_end > seg_start:
                    raw_hours = (seg_end - seg_start).total_seconds() / 3600.0
                    month_label = _month_key(month_cursor)
                    if month_label in buckets:
                        buckets[month_label][group_key] = (
                            buckets[month_label].get(group_key, 0.0) + raw_hours
                        )
                    if month_label in compute_buckets:
                        compute_buckets[month_label][group_key] = (
                            compute_buckets[month_label].get(group_key, 0.0) + (raw_hours * mult)
                        )
                month_cursor = next_month

        if scanned >= max_scan:
            break

    months_out = []
    grand_total_raw = 0.0
    grand_total_compute_raw = 0.0

    for month_label in month_keys:
        groups_out = []
        month_total_raw = 0.0
        month_total_compute_raw = 0.0

        for group_key, raw in buckets[month_label].items():
            machine_type, gcp_region, spot_int = group_key.split("|", 2)
            spot_bool = spot_int == "1"
            if raw <= 0:
                continue

            raw_compute = compute_buckets[month_label].get(group_key, 0.0)
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
                "month": month_label,
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


@router.get("/v1/nodes/daily_hours")
def nodes_daily_hours(
    request: Request,
    month: Optional[str] = None,
):
    _require_auth(request)

    now = datetime.now(timezone.utc)
    month_dt = _parse_yyyy_mm(month) if month else _month_start(now)
    month_start = month_dt
    month_end = _add_months(month_dt, 1)

    day_keys: list[str] = []
    day_cursor = _day_start(month_start)
    while day_cursor < month_end:
        day_keys.append(_date_key(day_cursor))
        day_cursor = _add_days(day_cursor, 1)

    cutoff_sec = month_start.timestamp()
    buckets: dict[str, dict[str, float]] = {dk: {} for dk in day_keys}
    compute_buckets: dict[str, dict[str, float]] = {dk: {} for dk in day_keys}

    last_doc = None
    scanned = 0
    max_scan = 20000

    while True:
        query = (
            DB.collection("nodes")
            .where(filter=FieldFilter("ended_at", ">=", cutoff_sec))
            .order_by("ended_at", direction=firestore.Query.DESCENDING)
        )
        if last_doc is not None:
            query = query.start_after(last_doc)

        docs = list(query.limit(500).stream())
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

            day = _day_start(window_start)
            while day < window_end:
                next_day = _add_days(day, 1)
                seg_start = max(window_start, day)
                seg_end = min(window_end, next_day)
                if seg_end > seg_start:
                    raw_hours = (seg_end - seg_start).total_seconds() / 3600.0
                    day_label = _date_key(day)
                    if day_label in buckets:
                        buckets[day_label][group_key] = buckets[day_label].get(group_key, 0.0) + raw_hours
                    if day_label in compute_buckets:
                        compute_buckets[day_label][group_key] = (
                            compute_buckets[day_label].get(group_key, 0.0) + (raw_hours * mult)
                        )
                day = next_day

        if scanned >= max_scan:
            break

    days_out = []
    total_raw = 0.0
    total_compute_raw = 0.0

    for day_label in day_keys:
        groups_out = []
        day_total_raw = 0.0
        day_total_compute_raw = 0.0

        for group_key, raw in buckets[day_label].items():
            machine_type, gcp_region, spot_int = group_key.split("|", 2)
            spot_bool = spot_int == "1"
            if raw <= 0:
                continue

            raw_compute = compute_buckets[day_label].get(group_key, 0.0)
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
                "date": day_label,
                "total_node_hours": _quantize_hours(day_total_raw),
                "total_compute_hours": _quantize_hours(day_total_compute_raw),
                "groups": groups_out,
            }
        )

    return {
        "month": _month_key(month_start),
        "days": days_out,
        "total_node_hours": _quantize_hours(total_raw),
        "total_compute_hours": _quantize_hours(total_compute_raw),
        "meta": {
            "hours_precision_decimals": 6,
            "scanned": scanned,
            "max_scan": max_scan,
        },
    }


@router.get("/v1/nodes/month_nodes")
def nodes_month_nodes(
    request: Request,
    month: Optional[str] = None,
    limit: int = 2000,
    cursor_ended_at: Optional[float] = None,
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

    query = (
        DB.collection("nodes")
        .where(filter=FieldFilter("ended_at", ">=", cutoff_sec))
        .order_by("ended_at", direction=firestore.Query.DESCENDING)
        .order_by(doc_id_field, direction=firestore.Query.DESCENDING)
    )
    if cursor_ended_at is not None and cursor_id is not None:
        query = query.start_after({"ended_at": float(cursor_ended_at), doc_id_field: cursor_id})

    docs = list(query.limit(limit).stream())
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
