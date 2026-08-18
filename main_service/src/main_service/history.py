"""
SQLite-backed history store.

This is the only persistence in Burla. It is NEVER in the live coordination
path - live cluster state lives in memory (see cluster_state.py) and moves
over HTTP. Rows here exist so the dashboard can show jobs / logs / usage
after the fact, and so cluster_config survives head restarts.

All functions are synchronous; call them via `asyncio.to_thread` from async
endpoints. A single WAL-mode connection guarded by a lock is plenty because
high-frequency logs and resource metrics arrive in batches.
"""

import hashlib
import json
import math
import os
import re
import sqlite3
import threading
from pathlib import Path
from time import time

DB_PATH = os.environ.get("HISTORY_DB_PATH", "/var/lib/burla/history.db")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    started_at REAL,
    status TEXT,
    user TEXT,
    function_name TEXT,
    n_inputs INTEGER,
    n_results INTEGER DEFAULT 0,
    data TEXT,
    -- Last on purpose: matches where the ALTER migration below puts it on
    -- pre-existing databases, so import_snapshot's positional SELECT * works
    -- between any two databases on this version.
    ended_at REAL
);
CREATE INDEX IF NOT EXISTS idx_jobs_started_at ON jobs(started_at DESC);

CREATE TABLE IF NOT EXISTS job_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT,
    input_index INTEGER,
    is_error INTEGER DEFAULT 0,
    timestamp REAL,
    logs TEXT
);
CREATE INDEX IF NOT EXISTS idx_job_logs_job ON job_logs(job_id, input_index);
CREATE INDEX IF NOT EXISTS idx_job_logs_error ON job_logs(job_id, is_error);

CREATE TABLE IF NOT EXISTS nodes (
    instance_name TEXT PRIMARY KEY,
    status TEXT,
    machine_type TEXT,
    gcp_region TEXT,
    spot INTEGER DEFAULT 0,
    started_booting_at REAL,
    ended_at REAL,
    data TEXT
);
CREATE INDEX IF NOT EXISTS idx_nodes_ended_at ON nodes(ended_at);
CREATE INDEX IF NOT EXISTS idx_nodes_started_booting_at ON nodes(started_booting_at);

CREATE TABLE IF NOT EXISTS node_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_name TEXT,
    ts REAL,
    msg TEXT
);
CREATE INDEX IF NOT EXISTS idx_node_logs_node ON node_logs(instance_name, ts);

CREATE TABLE IF NOT EXISTS resource_metrics (
    id INTEGER PRIMARY KEY,
    timestamp REAL NOT NULL,
    duration_sec REAL NOT NULL,
    instance_name TEXT NOT NULL,
    scope TEXT NOT NULL,
    job_id TEXT,
    input_index INTEGER,
    worker_id TEXT NOT NULL,
    cpu_seconds REAL NOT NULL,
    cpu_percent REAL NOT NULL,
    memory_bytes INTEGER NOT NULL,
    memory_percent REAL NOT NULL,
    network_rx_bytes INTEGER NOT NULL,
    network_tx_bytes INTEGER NOT NULL,
    disk_read_bytes INTEGER NOT NULL,
    disk_write_bytes INTEGER NOT NULL,
    gpu_percent REAL,
    gpu_memory_bytes INTEGER,
    gpu_memory_percent REAL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_resource_metrics_sample
ON resource_metrics(instance_name, timestamp, scope, worker_id);
CREATE INDEX IF NOT EXISTS idx_resource_metrics_job_task
ON resource_metrics(job_id, scope, input_index, timestamp)
WHERE job_id IS NOT NULL;

-- Exact per-call execution spans, one row per (input, attempt): a worker
-- reports the moment it starts executing an input and the moment that attempt
-- stops (result, error, worker death). The primary key doubles as the
-- (job_id, input_index) index the task-summary aggregation scans.
CREATE TABLE IF NOT EXISTS call_events (
    job_id TEXT NOT NULL,
    input_index INTEGER NOT NULL,
    attempt TEXT NOT NULL,
    started_at REAL,
    ended_at REAL,
    PRIMARY KEY (job_id, input_index, attempt)
) WITHOUT ROWID;

-- Structured debug events (slot accounting, scaling decisions, stall dumps).
-- Never shown to users: node_logs is the end-user story, this is the
-- engineering flight recorder. Pruned by retention, shipped to Burla's
-- telemetry backend when a job fails.
CREATE TABLE IF NOT EXISTS debug_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL,
    instance_name TEXT,
    job_id TEXT,
    event TEXT,
    fields TEXT
);
CREATE INDEX IF NOT EXISTS idx_debug_logs_job ON debug_logs(job_id, ts);
CREATE INDEX IF NOT EXISTS idx_debug_logs_ts ON debug_logs(ts);

CREATE TABLE IF NOT EXISTS cluster_config (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    data TEXT
);

CREATE TABLE IF NOT EXISTS history_imports (
    digest TEXT PRIMARY KEY,
    imported_at REAL
);
"""

# Covering index for the job-utilization charts: lets the whole-job
# aggregation run as an index-only scan of the (small) node-scope subset
# instead of millions of random main-table fetches. Created outside _SCHEMA
# because it references GPU columns the migration below may need to add first.
_NODE_SERIES_INDEX = """
CREATE INDEX IF NOT EXISTS idx_resource_metrics_node_series
ON resource_metrics(job_id, timestamp, instance_name, cpu_percent,
    memory_percent, network_rx_bytes, network_tx_bytes, disk_read_bytes,
    disk_write_bytes, gpu_percent, gpu_memory_percent)
WHERE scope = 'node' AND job_id IS NOT NULL
"""

# Covering index for the per-task summary table: the whole GROUP BY
# input_index aggregation runs as an index-only scan.
_TASK_SUMMARY_INDEX = """
CREATE INDEX IF NOT EXISTS idx_resource_metrics_task_summary
ON resource_metrics(job_id, input_index, timestamp, worker_id, cpu_seconds,
    duration_sec, memory_bytes)
WHERE scope = 'task' AND job_id IS NOT NULL
"""

_lock = threading.Lock()
_conn: sqlite3.Connection | None = None


def _connection() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA synchronous=NORMAL")
        _conn.executescript(_SCHEMA)
        # Metrics tables created before GPU sampling existed lack these columns.
        existing = {row[1] for row in _conn.execute("PRAGMA table_info(resource_metrics)")}
        for column, column_type in (
            ("gpu_percent", "REAL"),
            ("gpu_memory_bytes", "INTEGER"),
            ("gpu_memory_percent", "REAL"),
        ):
            if column not in existing:
                _conn.execute(f"ALTER TABLE resource_metrics ADD COLUMN {column} {column_type}")
        # Jobs tables created before job durations existed lack ended_at.
        existing_job_columns = {row[1] for row in _conn.execute("PRAGMA table_info(jobs)")}
        if "ended_at" not in existing_job_columns:
            _conn.execute("ALTER TABLE jobs ADD COLUMN ended_at REAL")
        _conn.execute(_NODE_SERIES_INDEX)
        _conn.execute(_TASK_SUMMARY_INDEX)
        _conn.commit()
    return _conn


def add_resource_metrics(instance_name: str, samples: list[dict]):
    rows = [
        (
            sample["timestamp"],
            sample["duration_sec"],
            instance_name,
            sample["scope"],
            sample["job_id"],
            sample["input_index"],
            sample["worker_id"],
            sample["cpu_seconds"],
            sample["cpu_percent"],
            sample["memory_bytes"],
            sample["memory_percent"],
            sample["network_rx_bytes"],
            sample["network_tx_bytes"],
            sample["disk_read_bytes"],
            sample["disk_write_bytes"],
            sample["gpu_percent"],
            sample["gpu_memory_bytes"],
            sample["gpu_memory_percent"],
        )
        for sample in samples
    ]
    with _lock:
        conn = _connection()
        conn.executemany(
            "INSERT OR IGNORE INTO resource_metrics "
            "(timestamp, duration_sec, instance_name, scope, job_id, input_index, "
            "worker_id, cpu_seconds, cpu_percent, memory_bytes, memory_percent, "
            "network_rx_bytes, network_tx_bytes, disk_read_bytes, disk_write_bytes, "
            "gpu_percent, gpu_memory_bytes, gpu_memory_percent) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()


def add_call_events(events: list[dict]):
    """`events`: [{"kind": "start"|"end", "job_id", "input_index", "attempt",
    "timestamp"}]. Start inserts the attempt row, end fills ended_at; upserts
    make either arrival order and redelivered batches safe."""
    starts, ends = [], []
    for event in events:
        row = (
            event["job_id"],
            event["input_index"],
            event["attempt"],
            event["timestamp"],
        )
        (starts if event["kind"] == "start" else ends).append(row)
    with _lock:
        conn = _connection()
        conn.executemany(
            "INSERT INTO call_events (job_id, input_index, attempt, started_at) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT DO UPDATE SET started_at = excluded.started_at",
            starts,
        )
        conn.executemany(
            "INSERT INTO call_events (job_id, input_index, attempt, ended_at) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT DO UPDATE SET ended_at = excluded.ended_at",
            ends,
        )
        conn.commit()


# A dedicated read-only connection for dashboard analytics: WAL allows one
# writer plus readers, so a multi-second aggregation over millions of metric
# rows never blocks metric/log ingestion on the main connection.
_read_lock = threading.Lock()
_read_conn: sqlite3.Connection | None = None

# Chart series are downsampled server-side to about this many points so the
# dashboard never receives millions of raw rows.
JOB_SERIES_TARGET_POINTS = 240
# Per-task series stay at 1-second resolution for tasks up to 30 minutes,
# then downsample.
TASK_SERIES_TARGET_POINTS = 1800


def _read_connection() -> sqlite3.Connection:
    global _read_conn
    if _read_conn is None:
        _connection()  # creates the db file + schema on first boot
        _read_conn = sqlite3.connect(
            f"file:{DB_PATH}?mode=ro", uri=True, check_same_thread=False
        )
        # mmap halves large index scans vs pread on multi-GB metric tables.
        _read_conn.execute("PRAGMA mmap_size=4294967296")
    return _read_conn


def job_metrics_series(job_id: str) -> dict:
    """Cluster-wide utilization for one job, bucketed into at most
    ~JOB_SERIES_TARGET_POINTS points. Each node reports one 'node'-scope row
    per second while it works on the job, so COUNT(DISTINCT instance_name)
    per bucket is the node count and SUM(bytes)/bucket_sec is cluster
    throughput."""
    with _read_lock:
        conn = _read_connection()
        first_ts, last_ts = conn.execute(
            "SELECT MIN(timestamp), MAX(timestamp) "
            "FROM resource_metrics INDEXED BY idx_resource_metrics_node_series "
            "WHERE job_id = ? AND scope = 'node'",
            (job_id,),
        ).fetchone()
        if first_ts is None:
            return {"has_metrics": False, "has_gpu": False, "bucket_sec": 0, "points": []}
        bucket_sec = max(1, math.ceil((last_ts - first_ts) / JOB_SERIES_TARGET_POINTS))
        rows = conn.execute(
            "SELECT CAST((timestamp - ?) / ? AS INTEGER) AS bucket, "
            # MAX(x, 0): samples written before the sampler clamped
            # counter-reset deltas can be negative.
            "COUNT(DISTINCT instance_name), AVG(cpu_percent), AVG(memory_percent), "
            "SUM(MAX(network_rx_bytes, 0)), SUM(MAX(network_tx_bytes, 0)), "
            "SUM(MAX(disk_read_bytes, 0)), SUM(MAX(disk_write_bytes, 0)), "
            "AVG(gpu_percent), AVG(gpu_memory_percent), COUNT(gpu_percent) "
            # INDEXED BY: the planner otherwise picks the non-covering
            # job_task index and pays a main-table fetch per row.
            "FROM resource_metrics INDEXED BY idx_resource_metrics_node_series "
            "WHERE job_id = ? AND scope = 'node' "
            "GROUP BY bucket ORDER BY bucket",
            (first_ts, bucket_sec, job_id),
        ).fetchall()
    points = []
    has_gpu = False
    for bucket, nodes, cpu, mem, rx, tx, read, write, gpu, gpu_mem, n_gpu in rows:
        if n_gpu:
            has_gpu = True
        points.append(
            {
                "t": first_ts + bucket * bucket_sec,
                "nodes": nodes,
                "cpu": round(cpu, 2),
                "mem": round(mem, 2),
                "net_rx": round(rx / bucket_sec),
                "net_tx": round(tx / bucket_sec),
                "disk_read": round(read / bucket_sec),
                "disk_write": round(write / bucket_sec),
                "gpu": round(gpu, 2) if gpu is not None else None,
                "gpu_mem": round(gpu_mem, 2) if gpu_mem is not None else None,
            }
        )
    return {
        "has_metrics": True,
        "has_gpu": has_gpu,
        "bucket_sec": bucket_sec,
        "points": points,
    }


def task_metrics_series(job_id: str, input_index: int) -> dict:
    """One task's utilization series plus the nearest input indexes that also
    have samples (for prev/next stepping). vCPUs = cpu_seconds/duration so the
    number is cores, not percent-of-node."""
    with _read_lock:
        conn = _read_connection()
        first_ts, last_ts, n_attempts = conn.execute(
            "SELECT MIN(timestamp), MAX(timestamp), COUNT(DISTINCT worker_id) "
            "FROM resource_metrics "
            "WHERE job_id = ? AND scope = 'task' AND input_index = ?",
            (job_id, input_index),
        ).fetchone()
        prev_index = conn.execute(
            "SELECT MAX(input_index) FROM resource_metrics "
            "WHERE job_id = ? AND scope = 'task' AND input_index < ?",
            (job_id, input_index),
        ).fetchone()[0]
        next_index = conn.execute(
            "SELECT MIN(input_index) FROM resource_metrics "
            "WHERE job_id = ? AND scope = 'task' AND input_index > ?",
            (job_id, input_index),
        ).fetchone()[0]
        if first_ts is None:
            return {
                "has_metrics": False,
                "has_gpu": False,
                "prev_index": prev_index,
                "next_index": next_index,
                "n_attempts": 0,
                "bucket_sec": 0,
                "points": [],
            }
        bucket_sec = max(1, math.ceil((last_ts - first_ts) / TASK_SERIES_TARGET_POINTS))
        rows = conn.execute(
            "SELECT CAST((timestamp - ?) / ? AS INTEGER) AS bucket, "
            "SUM(cpu_seconds) / SUM(duration_sec), AVG(memory_bytes), "
            "SUM(MAX(network_rx_bytes, 0)) / SUM(duration_sec), "
            "SUM(MAX(network_tx_bytes, 0)) / SUM(duration_sec), "
            "SUM(MAX(disk_read_bytes, 0)) / SUM(duration_sec), "
            "SUM(MAX(disk_write_bytes, 0)) / SUM(duration_sec), "
            "AVG(gpu_percent), AVG(gpu_memory_bytes), COUNT(gpu_percent) "
            "FROM resource_metrics "
            "WHERE job_id = ? AND scope = 'task' AND input_index = ? "
            "GROUP BY bucket ORDER BY bucket",
            (first_ts, bucket_sec, job_id, input_index),
        ).fetchall()
    points = []
    has_gpu = False
    for bucket, cpus, mem, rx, tx, read, write, gpu, gpu_mem, n_gpu in rows:
        if n_gpu:
            has_gpu = True
        points.append(
            {
                "t": first_ts + bucket * bucket_sec,
                "cpus": round(cpus, 3),
                "mem": round(mem),
                "net_rx": round(rx),
                "net_tx": round(tx),
                "disk_read": round(read),
                "disk_write": round(write),
                "gpu": round(gpu, 2) if gpu is not None else None,
                "gpu_mem": round(gpu_mem) if gpu_mem is not None else None,
            }
        )
    return {
        "has_metrics": True,
        "has_gpu": has_gpu,
        "prev_index": prev_index,
        "next_index": next_index,
        "n_attempts": n_attempts,
        "bucket_sec": bucket_sec,
        "points": points,
    }


TASK_SUMMARY_SORT_COLUMNS = {
    "index": "input_index",
    "started": "started",
    "ended": "started + duration",
    "duration": "duration",
    "attempts": "attempts",
    "status": "failed",
    "peak_cpus": "peak_cpus",
    "peak_mem": "peak_mem",
}

# Fallback for jobs that predate call events: a call looks running while its
# traces (samples or logs) are still arriving. Nodes batch both every ~5s, so
# the window covers push jitter plus the gap between a call's first log line
# and its first metric sample.
_RUNNING_TRACE_MAX_AGE_SEC = 15

# One row per call, enumerating every index 0..n_inputs-1 so calls with zero
# traces still appear. Exact start/duration/attempts come from call_events;
# jobs that predate events fall back to sample/log-derived values (first
# trace as start, sample span as duration). Filters/sort/pagination stay in
# SQL so 100k+ call jobs never ship the whole set to the dashboard.
_CALLS_BASE_CTE = """
WITH RECURSIVE all_calls(input_index) AS (
    SELECT 0 WHERE :n_inputs > 0
    UNION ALL
    SELECT input_index + 1 FROM all_calls WHERE input_index + 1 < :n_inputs
),
events AS (
    SELECT input_index,
        MIN(started_at) AS started,
        MAX(ended_at) AS ended,
        COUNT(*) AS attempts,
        COUNT(*) - COUNT(ended_at) AS open_attempts
    FROM call_events
    WHERE job_id = :job_id
    GROUP BY input_index
),
logged AS (
    SELECT input_index, MAX(is_error) AS failed,
        MIN(timestamp) AS first_logged, MAX(timestamp) AS last_logged
    FROM job_logs
    WHERE job_id = :job_id AND input_index IS NOT NULL
    GROUP BY input_index
)
"""

_CALLS_FILTERS = """
    WHERE (:failed_only = 0 OR COALESCE(l.failed, 0) = 1)
    AND (:logs_only = 0 OR l.input_index IS NOT NULL)
    AND (:has_metrics = 0 OR EXISTS (
        SELECT 1 FROM resource_metrics
        WHERE job_id = :job_id AND scope = 'task'
        AND input_index = a.input_index
    ))
    AND (:index IS NULL OR a.input_index = :index)
"""

_CALL_STATUS_SQL = """
CASE
    WHEN failed = 1 THEN 'failed'
    WHEN has_events = 1 AND :job_is_running AND open_attempts > 0 THEN 'running'
    WHEN has_events = 0 AND :job_is_running
        AND last_active IS NOT NULL
        AND :now - last_active < 15 THEN 'running'
    WHEN started IS NOT NULL AND :job_is_canceled THEN 'canceled'
    WHEN started IS NOT NULL THEN 'succeeded'
    WHEN :job_is_running THEN 'pending'
    WHEN :job_is_canceled THEN 'not_run'
    ELSE 'unknown'
END
"""

# Fast path, for jobs with call events. Never touches the (potentially tens of
# millions of rows) samples table for page selection: the page is picked from
# the events, then only its ~50 rows get per-input sample lookups for the peak
# columns. An 18M-sample synthetic job pages in ~0.4s this way vs ~5s when the
# full sample aggregation runs.
_EVENT_SUMMARY_CTE = (
    _CALLS_BASE_CTE
    + """,
summarized AS (
    SELECT a.input_index,
        COALESCE(e.started, l.first_logged) AS started,
        CASE
            WHEN e.ended IS NOT NULL THEN e.ended - e.started
            WHEN e.open_attempts > 0 AND :job_is_running THEN :now - e.started
        END AS duration,
        e.attempts AS attempts,
        COALESCE(l.failed, 0) AS failed,
        l.input_index IS NOT NULL AS has_logs,
        e.input_index IS NOT NULL AS has_events,
        COALESCE(e.open_attempts, 0) AS open_attempts,
        l.last_logged AS last_active
    FROM all_calls a
    LEFT JOIN events e ON e.input_index = a.input_index
    LEFT JOIN logged l ON l.input_index = a.input_index
"""
    + _CALLS_FILTERS
    + f"""),
flagged AS (
    SELECT *, {_CALL_STATUS_SQL} AS api_status FROM summarized
)
"""
)

_EVENT_SUMMARY_QUERY = (
    _EVENT_SUMMARY_CTE
    + """
SELECT f.input_index, f.started, f.duration, f.attempts,
    (SELECT MAX(cpu_seconds / duration_sec)
     FROM resource_metrics INDEXED BY idx_resource_metrics_task_summary
     WHERE job_id = :job_id AND scope = 'task'
     AND input_index = f.input_index) AS peak_cpus,
    (SELECT MAX(memory_bytes)
     FROM resource_metrics INDEXED BY idx_resource_metrics_task_summary
     WHERE job_id = :job_id AND scope = 'task'
     AND input_index = f.input_index) AS peak_mem,
    f.failed, f.has_logs, f.has_events, f.open_attempts, f.last_active,
    f.cursor_value
FROM (SELECT *, {sort_column} AS cursor_value FROM flagged {selection_where}
      ORDER BY {order}, input_index {direction}
      LIMIT :limit OFFSET :offset) f
"""
)

# Fallback path: jobs that predate call events (start/duration must come from
# the sample aggregation), and peak-column sorts (which need every input's
# sample aggregate anyway).
_SAMPLE_SUMMARY_CTE = (
    _CALLS_BASE_CTE
    + """,
sampled AS (
    SELECT input_index,
        MIN(timestamp) AS first_seen,
        MAX(timestamp) AS last_seen,
        MAX(timestamp) - MIN(timestamp) + 1 AS duration,
        COUNT(DISTINCT worker_id) AS attempts,
        MAX(cpu_seconds / duration_sec) AS peak_cpus,
        MAX(memory_bytes) AS peak_mem
    FROM resource_metrics INDEXED BY idx_resource_metrics_task_summary
    WHERE job_id = :job_id AND scope = 'task'
    GROUP BY input_index
),
summarized AS (
    SELECT a.input_index,
        COALESCE(e.started,
            MIN(s.first_seen, l.first_logged), s.first_seen, l.first_logged) AS started,
        CASE
            WHEN e.ended IS NOT NULL THEN e.ended - e.started
            WHEN e.open_attempts > 0 AND :job_is_running THEN :now - e.started
            ELSE s.duration
        END AS duration,
        COALESCE(e.attempts, s.attempts) AS attempts,
        s.peak_cpus, s.peak_mem,
        COALESCE(l.failed, 0) AS failed,
        l.input_index IS NOT NULL AS has_logs,
        e.input_index IS NOT NULL AS has_events,
        COALESCE(e.open_attempts, 0) AS open_attempts,
        COALESCE(MAX(s.last_seen, l.last_logged), s.last_seen, l.last_logged) AS last_active
    FROM all_calls a
    LEFT JOIN events e ON e.input_index = a.input_index
    LEFT JOIN sampled s ON s.input_index = a.input_index
    LEFT JOIN logged l ON l.input_index = a.input_index
"""
    + _CALLS_FILTERS
    + f"""),
flagged AS (
    SELECT *, {_CALL_STATUS_SQL} AS api_status FROM summarized
)
"""
)

_SAMPLE_SUMMARY_QUERY = (
    _SAMPLE_SUMMARY_CTE
    + """
SELECT input_index, started, duration, attempts, peak_cpus, peak_mem,
    failed, has_logs, has_events, open_attempts, last_active,
    {sort_column} AS cursor_value
FROM flagged {selection_where}
ORDER BY {order}, input_index {direction} LIMIT :limit OFFSET :offset
"""
)

# Counting filtered rows never needs the samples table: the filters only
# reference the log aggregate and the index search.
_COUNT_QUERY = (
    _CALLS_BASE_CTE
    + """
SELECT COUNT(*) FROM all_calls a
LEFT JOIN logged l ON l.input_index = a.input_index
"""
    + _CALLS_FILTERS
)


def job_task_summaries(
    job_id: str,
    n_inputs: int,
    sort: str,
    descending: bool,
    failed_only: bool,
    logs_only: bool,
    index: int | None,
    offset: int,
    limit: int,
    job_is_running: bool,
    has_metrics: bool = False,
    after_key: tuple | None = None,
    status: str | None = None,
    job_is_canceled: bool = False,
) -> dict:
    direction = "DESC" if descending else "ASC"
    sort_column = TASK_SUMMARY_SORT_COLUMNS[sort]
    order = f"{sort_column} {direction} NULLS LAST"
    selection_conditions = []
    cursor_params = {}
    if after_key is not None:
        null_rank, value, input_index = after_key
        comparator = "<" if descending else ">"
        selection_conditions.append(
            f"(({sort_column} IS NULL) > :cursor_null_rank OR ("
            f"({sort_column} IS NULL) = :cursor_null_rank AND ("
            f"(:cursor_null_rank = 0 AND ({sort_column} {comparator} :cursor_value "
            f"OR ({sort_column} = :cursor_value AND input_index "
            f"{comparator} :cursor_index))) OR "
            f"(:cursor_null_rank = 1 AND input_index "
            f"{comparator} :cursor_index))))"
        )
        cursor_params = {
            "cursor_null_rank": null_rank,
            "cursor_value": value,
            "cursor_index": input_index,
        }
    if status is not None:
        selection_conditions.append("api_status = :status")
    selection_where = "WHERE " + " AND ".join(selection_conditions) if selection_conditions else ""
    now = time()
    params = {
        "job_id": job_id,
        "n_inputs": n_inputs,
        "job_is_running": 1 if job_is_running else 0,
        "job_is_canceled": 1 if job_is_canceled else 0,
        "now": now,
        "failed_only": 1 if failed_only else 0,
        "logs_only": 1 if logs_only else 0,
        "has_metrics": 1 if has_metrics else 0,
        "index": index,
        "status": status,
        "limit": limit,
        "offset": offset,
        **cursor_params,
    }
    unfiltered = not failed_only and not logs_only and not has_metrics and index is None
    with _read_lock:
        conn = _read_connection()
        has_events = conn.execute(
            "SELECT EXISTS(SELECT 1 FROM call_events WHERE job_id = ?)", (job_id,)
        ).fetchone()[0]
        if unfiltered and status is None:
            total = n_inputs
        elif status is not None:
            count_query = (
                _EVENT_SUMMARY_CTE
                if has_events and sort not in ("peak_cpus", "peak_mem")
                else _SAMPLE_SUMMARY_CTE
            )
            total = conn.execute(
                count_query + "SELECT COUNT(*) FROM flagged WHERE api_status = :status",
                params,
            ).fetchone()[0]
        else:
            total = conn.execute(_COUNT_QUERY, params).fetchone()[0]
        query = (
            _EVENT_SUMMARY_QUERY
            if has_events and sort not in ("peak_cpus", "peak_mem")
            else _SAMPLE_SUMMARY_QUERY
        )
        rows = conn.execute(
            query.format(
                order=order,
                sort_column=sort_column,
                selection_where=selection_where,
                direction=direction,
            ),
            params,
        ).fetchall()
    tasks = []
    for row in rows:
        (
            input_index,
            started,
            duration,
            attempts,
            peak_cpus,
            peak_mem,
            failed,
            has_logs,
            has_events,
            open_attempts,
            last_active,
            cursor_value,
        ) = row
        if failed:
            status = "failed"
        elif has_events:
            status = "running" if job_is_running and open_attempts > 0 else "done"
        elif (
            job_is_running
            and last_active is not None
            and now - last_active < _RUNNING_TRACE_MAX_AGE_SEC
        ):
            status = "running"
        else:
            status = "done"
        tasks.append(
            {
                "index": input_index,
                "started_at": started,
                "duration_sec": round(duration, 3) if duration is not None else None,
                "attempts": attempts,
                "peak_cpus": round(peak_cpus, 3) if peak_cpus is not None else None,
                "peak_mem_bytes": peak_mem,
                "has_logs": bool(has_logs),
                "has_error": bool(failed),
                "status": status,
                "_cursor_key": [
                    1 if cursor_value is None else 0,
                    cursor_value,
                    input_index,
                ],
            }
        )
    return {"total": total, "tasks": tasks}


def last_job_metrics_timestamp(job_id: str) -> float | None:
    """Most recent node-scope sample for a job: the backfill source for
    ended_at when a job is finalized without a live end (head died mid-job)."""
    with _read_lock:
        conn = _read_connection()
        row = conn.execute(
            "SELECT MAX(timestamp) "
            "FROM resource_metrics INDEXED BY idx_resource_metrics_node_series "
            "WHERE job_id = ? AND scope = 'node'",
            (job_id,),
        ).fetchone()
    return row[0]


# ---------------------------------------------------------------- debug logs

# Bounded so a laptop-hosted head can't bloat: a week of events, hard row cap.
DEBUG_LOG_RETENTION_SEC = 7 * 24 * 3600
DEBUG_LOG_MAX_ROWS = 200_000


def add_debug_logs(instance_name: str, entries: list[dict]):
    """`entries`: [{"ts", "job_id", "event", "fields"}]. Prunes on every batch
    (cheap: indexed deletes, batches arrive at most ~1/sec per node)."""
    now = time()
    rows = [
        (
            entry.get("ts") or now,
            instance_name,
            entry.get("job_id"),
            entry.get("event"),
            json.dumps(entry.get("fields") or {}),
        )
        for entry in entries
    ]
    with _lock:
        conn = _connection()
        conn.executemany(
            "INSERT INTO debug_logs (ts, instance_name, job_id, event, fields) "
            "VALUES (?, ?, ?, ?, ?)",
            rows,
        )
        conn.execute(
            "DELETE FROM debug_logs WHERE ts < ?", (now - DEBUG_LOG_RETENTION_SEC,)
        )
        conn.execute(
            "DELETE FROM debug_logs WHERE id <= "
            "(SELECT id FROM debug_logs ORDER BY id DESC LIMIT 1 OFFSET ?)",
            (DEBUG_LOG_MAX_ROWS,),
        )
        conn.commit()


def debug_logs_for_job(job_id: str, max_bytes: int = 2_000_000) -> list[dict]:
    """The job's debug-event trail, newest events kept when the byte cap
    truncates, returned oldest-first."""
    with _lock:
        rows = (
            _connection()
            .execute(
                "SELECT ts, instance_name, event, fields FROM debug_logs "
                "WHERE job_id = ? ORDER BY ts DESC",
                (job_id,),
            )
            .fetchall()
        )
    entries = []
    total_bytes = 0
    for ts, instance_name, event, fields in rows:
        total_bytes += len(fields) + len(event or "") + len(instance_name or "") + 24
        if total_bytes > max_bytes and entries:
            break
        entries.append(
            {
                "ts": ts,
                "node": instance_name,
                "event": event,
                "fields": json.loads(fields),
            }
        )
    entries.reverse()
    return entries


# ---------------------------------------------------------------- cluster_config


def get_cluster_config() -> dict | None:
    with _lock:
        row = (
            _connection()
            .execute("SELECT data FROM cluster_config WHERE id = 1")
            .fetchone()
        )
    return json.loads(row[0]) if row else None


def save_cluster_config(config: dict):
    with _lock:
        conn = _connection()
        conn.execute(
            "INSERT INTO cluster_config (id, data) VALUES (1, ?) "
            "ON CONFLICT(id) DO UPDATE SET data = excluded.data",
            (json.dumps(config),),
        )
        conn.commit()


# ---------------------------------------------------------------- jobs


def _upsert_job(conn: sqlite3.Connection, job_id: str, job: dict):
    n_results = sum(
        node.get("current_num_results", 0)
        for node in job.get("assigned_nodes", {}).values()
    )
    data = {k: v for k, v in job.items() if k != "assigned_nodes"}
    conn.execute(
        "INSERT INTO jobs (job_id, started_at, ended_at, status, user, function_name, "
        "n_inputs, n_results, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(job_id) DO UPDATE SET started_at = excluded.started_at, "
        "ended_at = excluded.ended_at, status = excluded.status, user = excluded.user, "
        "function_name = excluded.function_name, n_inputs = excluded.n_inputs, "
        "n_results = MAX(jobs.n_results, excluded.n_results), data = excluded.data",
        (
            job_id,
            job.get("started_at"),
            job.get("ended_at"),
            job.get("status"),
            job.get("user"),
            job.get("function_name"),
            job.get("n_inputs"),
            n_results,
            json.dumps(data),
        ),
    )


def get_job(job_id: str) -> dict | None:
    with _lock:
        row = (
            _connection()
            .execute("SELECT data, n_results FROM jobs WHERE job_id = ?", (job_id,))
            .fetchone()
        )
    if row is None:
        return None
    job = json.loads(row[0])
    job["n_results"] = row[1]
    return job


def list_jobs(offset: int, limit: int) -> list[dict]:
    with _lock:
        rows = (
            _connection()
            .execute(
                "SELECT job_id, data, n_results FROM jobs "
                "ORDER BY started_at DESC LIMIT ? OFFSET ?",
                (limit, offset),
            )
            .fetchall()
        )
    jobs = []
    for job_id, data, n_results in rows:
        job = json.loads(data)
        job["job_id"] = job_id
        job["n_results"] = n_results
        jobs.append(job)
    return jobs


def count_jobs() -> int:
    with _lock:
        return _connection().execute("SELECT COUNT(*) FROM jobs").fetchone()[0]


def running_jobs() -> list[tuple[str, dict]]:
    with _lock:
        rows = (
            _connection()
            .execute("SELECT job_id, data FROM jobs WHERE status = 'RUNNING'")
            .fetchall()
        )
    return [(job_id, json.loads(data)) for job_id, data in rows]


# ---------------------------------------------------------------- job logs


def add_job_logs(job_id: str, documents: list[dict]):
    """`documents` use the same shape JobLogWriter batches:
    {"logs": [{"timestamp": epoch, "message": str}], "timestamp": epoch,
     "input_index": int | absent, "is_error": bool | absent}
    """
    rows = [
        (
            job_id,
            document.get("input_index"),
            1 if document.get("is_error") else 0,
            document.get("timestamp"),
            json.dumps(document.get("logs", [])),
        )
        for document in documents
    ]
    with _lock:
        conn = _connection()
        conn.executemany(
            "INSERT INTO job_logs (job_id, input_index, is_error, timestamp, logs) "
            "VALUES (?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()


def job_error_count(job_id: str) -> int:
    # Failed inputs, not error rows: an input can log several errors, and
    # index-less system notices (e.g. "Job canceled by user") are not inputs.
    with _lock:
        row = (
            _connection()
            .execute(
                "SELECT COUNT(DISTINCT input_index) FROM job_logs "
                "WHERE job_id = ? AND is_error = 1 AND input_index IS NOT NULL",
                (job_id,),
            )
            .fetchone()
        )
    return row[0]


def job_notices(job_id: str) -> list[dict]:
    """Index-less log rows: job-level notices like 'Job canceled by user'.
    These are not function calls, so they live outside the call table."""
    with _lock:
        rows = (
            _connection()
            .execute(
                "SELECT logs FROM job_logs WHERE job_id = ? AND input_index IS NULL",
                (job_id,),
            )
            .fetchall()
        )
    entries = []
    for (logs_json,) in rows:
        for log in json.loads(logs_json):
            timestamp = log.get("timestamp")
            if timestamp is None:
                continue
            entries.append({"message": log.get("message", ""), "timestamp": float(timestamp)})
    entries.sort(key=lambda entry: entry["timestamp"])
    return entries


def job_logs_for_input(job_id: str, input_index: int) -> list[dict]:
    """Flattened log entries for one input index: {message, log_timestamp, is_error}."""
    with _lock:
        rows = (
            _connection()
            .execute(
                "SELECT logs, is_error FROM job_logs WHERE job_id = ? AND input_index = ?",
                (job_id, input_index),
            )
            .fetchall()
        )
    entries = []
    for logs_json, doc_is_error in rows:
        for log in json.loads(logs_json):
            timestamp = log.get("timestamp")
            if timestamp is None:
                continue
            entries.append(
                {
                    "message": log.get("message", ""),
                    "log_timestamp": float(timestamp),
                    "is_error": bool(log.get("is_error", False) or doc_is_error),
                }
            )
    return entries


# ---------------------------------------------------------------- nodes


def _upsert_node(conn: sqlite3.Connection, instance_name: str, node: dict):
    conn.execute(
        "INSERT INTO nodes (instance_name, status, machine_type, gcp_region, spot, "
        "started_booting_at, ended_at, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(instance_name) DO UPDATE SET status = excluded.status, "
        "machine_type = excluded.machine_type, gcp_region = excluded.gcp_region, "
        "spot = excluded.spot, started_booting_at = excluded.started_booting_at, "
        "ended_at = excluded.ended_at, data = excluded.data",
        (
            instance_name,
            node.get("status"),
            node.get("machine_type"),
            node.get("gcp_region"),
            1 if node.get("spot") else 0,
            node.get("started_booting_at"),
            node.get("ended_at"),
            json.dumps(node),
        ),
    )


def upsert_node(instance_name: str, node: dict):
    with _lock:
        conn = _connection()
        _upsert_node(conn, instance_name, node)
        conn.commit()


def upsert_job_and_nodes(job_id: str, job: dict, nodes: list[dict]):
    with _lock:
        conn = _connection()
        _upsert_job(conn, job_id, job)
        for node in nodes:
            _upsert_node(conn, node["instance_name"], node)
        conn.commit()


def active_nodes() -> list[dict]:
    with _lock:
        rows = (
            _connection()
            .execute(
                "SELECT data FROM nodes WHERE status IN ('BOOTING', 'READY', 'RUNNING', 'FAILED')"
            )
            .fetchall()
        )
    return [json.loads(row[0]) for row in rows]


def nodes_ended_after(cutoff_sec: float) -> list[dict]:
    with _lock:
        rows = (
            _connection()
            .execute(
                "SELECT data FROM nodes WHERE ended_at >= ? ORDER BY ended_at DESC",
                (cutoff_sec,),
            )
            .fetchall()
        )
    return [json.loads(row[0]) for row in rows]


def ended_nodes_page(statuses: tuple[str, ...], cutoff_sec: float) -> list[dict]:
    placeholders = ",".join("?" for _ in statuses)
    query = (
        f"SELECT data FROM nodes WHERE status IN ({placeholders}) "
        "AND COALESCE(ended_at, started_booting_at, 0) >= ? "
        "ORDER BY COALESCE(ended_at, started_booting_at, 0) DESC"
    )
    with _lock:
        rows = _connection().execute(query, (*statuses, cutoff_sec)).fetchall()
    return [json.loads(row[0]) for row in rows]


# ---------------------------------------------------------------- node logs


def add_node_logs(instance_name: str, logs: list[dict]):
    rows = [(instance_name, log.get("ts"), log.get("msg", "")) for log in logs]
    with _lock:
        conn = _connection()
        conn.executemany(
            "INSERT INTO node_logs (instance_name, ts, msg) VALUES (?, ?, ?)", rows
        )
        conn.commit()


def node_logs_after(
    instance_name: str, after_id: int, limit: int = 1000
) -> list[tuple[int, float, str]]:
    """Log rows with id > after_id, oldest first. Pass 0 for a full replay."""
    with _lock:
        rows = (
            _connection()
            .execute(
                "SELECT id, ts, msg FROM node_logs "
                "WHERE instance_name = ? AND id > ? ORDER BY id LIMIT ?",
                (instance_name, after_id, limit),
            )
            .fetchall()
        )
    return rows


def first_failure_log(instance_name: str, tokens: tuple[str, ...]) -> str | None:
    with _lock:
        rows = (
            _connection()
            .execute(
                "SELECT msg FROM node_logs WHERE instance_name = ? ORDER BY ts",
                (instance_name,),
            )
            .fetchall()
        )
    for (msg,) in rows:
        msg = (msg or "").strip()
        if msg and any(token in msg for token in tokens):
            return msg
    return None


# ---------------------------------------------------------------- deploy migration

# Only ended rows migrate: RUNNING jobs and BOOTING/READY/RUNNING nodes in a
# snapshot are another head's live state, and loading them here would make the
# reaper "fail" jobs and show phantom nodes that never belonged to this head.
_ENDED_JOB_STATUSES = ("COMPLETED", "FAILED", "CANCELED")
_ENDED_NODE_STATUSES = ("DELETED", "FAILED")
_IMPORT_TABLES = (
    "jobs",
    "job_logs",
    "nodes",
    "node_logs",
    "resource_metrics",
    "call_events",
    "debug_logs",
    "cluster_config",
    "history_imports",
)
_EMPTY_IMPORT_TABLES = tuple(
    table for table in _IMPORT_TABLES if table != "cluster_config"
)


def _validate_attached_snapshot(conn: sqlite3.Connection):
    integrity = conn.execute("PRAGMA snapshot.integrity_check").fetchall()
    if integrity != [("ok",)]:
        raise ValueError(f"history snapshot integrity check failed: {integrity}")
    for table in _IMPORT_TABLES:
        expected = [
            tuple(row[1:]) for row in conn.execute(f"PRAGMA main.table_info({table})")
        ]
        actual = [
            tuple(row[1:])
            for row in conn.execute(f"PRAGMA snapshot.table_info({table})")
        ]
        if actual != expected:
            raise ValueError(f"history snapshot has an incompatible {table} schema")


def snapshot_cluster_config(snapshot_path: str) -> dict | None:
    conn = sqlite3.connect(f"file:{snapshot_path}?mode=ro", uri=True)
    try:
        row = conn.execute("SELECT data FROM cluster_config WHERE id = 1").fetchone()
    finally:
        conn.close()
    return json.loads(row[0]) if row else None


def import_snapshot(
    snapshot_path: str, digest: str, cluster_config: dict | None
) -> bool:
    """Import a first-deploy history snapshot into an otherwise-empty head.

    The digest row makes retrying the same upload a no-op. Returns False if
    this exact snapshot was already imported.
    """
    job_marks = ", ".join("?" for _ in _ENDED_JOB_STATUSES)
    node_marks = ", ".join("?" for _ in _ENDED_NODE_STATUSES)
    with _lock:
        conn = _connection()
        already = conn.execute(
            "SELECT 1 FROM history_imports WHERE digest = ?", (digest,)
        ).fetchone()
        if already:
            return False
        nonempty_tables = [
            table
            for table in _EMPTY_IMPORT_TABLES
            if conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
        ]
        if nonempty_tables:
            raise ValueError(
                "history can only be imported into an empty head; "
                f"found rows in {', '.join(nonempty_tables)}"
            )
        conn.execute("ATTACH DATABASE ? AS snapshot", (snapshot_path,))
        try:
            _validate_attached_snapshot(conn)
            conn.execute(
                f"INSERT INTO jobs SELECT * FROM snapshot.jobs "
                f"WHERE status IN ({job_marks})",
                _ENDED_JOB_STATUSES,
            )
            conn.execute(
                "INSERT INTO job_logs (job_id, input_index, is_error, timestamp, logs) "
                "SELECT job_id, input_index, is_error, timestamp, logs "
                "FROM snapshot.job_logs WHERE job_id IN (SELECT job_id FROM jobs)"
            )
            conn.execute(
                f"INSERT INTO nodes SELECT * FROM snapshot.nodes "
                f"WHERE status IN ({node_marks})",
                _ENDED_NODE_STATUSES,
            )
            conn.execute(
                "INSERT INTO node_logs (instance_name, ts, msg) "
                "SELECT instance_name, ts, msg FROM snapshot.node_logs "
                "WHERE instance_name IN (SELECT instance_name FROM nodes)"
            )
            conn.execute(
                "INSERT INTO resource_metrics "
                "(timestamp, duration_sec, instance_name, scope, job_id, input_index, "
                "worker_id, cpu_seconds, cpu_percent, memory_bytes, memory_percent, "
                "network_rx_bytes, network_tx_bytes, disk_read_bytes, disk_write_bytes, "
                "gpu_percent, gpu_memory_bytes, gpu_memory_percent) "
                "SELECT timestamp, duration_sec, instance_name, scope, job_id, "
                "input_index, worker_id, cpu_seconds, cpu_percent, memory_bytes, "
                "memory_percent, network_rx_bytes, network_tx_bytes, disk_read_bytes, "
                "disk_write_bytes, gpu_percent, gpu_memory_bytes, gpu_memory_percent "
                "FROM snapshot.resource_metrics "
                "WHERE job_id IN (SELECT job_id FROM jobs) "
                "OR (scope = 'node' AND instance_name IN (SELECT instance_name FROM nodes))"
            )
            if cluster_config is not None:
                conn.execute(
                    "INSERT INTO cluster_config (id, data) VALUES (1, ?) "
                    "ON CONFLICT(id) DO UPDATE SET data = excluded.data",
                    (json.dumps(cluster_config),),
                )
            conn.execute(
                "INSERT INTO history_imports (digest, imported_at) VALUES (?, ?)",
                (digest, time()),
            )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.execute("DETACH DATABASE snapshot")
    return True


# ---------------------------------------------------------------- management reads


def management_jobs_page(
    *,
    status: str | None,
    user: str | None,
    function_name: str | None,
    started_after: float | None,
    started_before: float | None,
    sort: str,
    descending: bool,
    after_key: tuple | None,
    limit: int,
) -> dict:
    sort_expressions = {
        "started_at": "j.started_at",
        "ended_at": "j.ended_at",
        "duration": "COALESCE(j.ended_at, ?) - j.started_at",
        "status": "LOWER(j.status)",
        "input_count": "j.n_inputs",
        "result_count": "j.n_results",
        "failed_count": (
            "(SELECT COUNT(DISTINCT input_index) FROM job_logs "
            "WHERE job_id = j.job_id AND input_index IS NOT NULL AND is_error = 1)"
        ),
    }
    where = []
    params = []
    if status is not None:
        where.append("LOWER(j.status) = ?")
        params.append(status)
    if user is not None:
        where.append("j.user = ?")
        params.append(user)
    if function_name is not None:
        where.append("j.function_name = ?")
        params.append(function_name)
    if started_after is not None:
        where.append("j.started_at >= ?")
        params.append(started_after)
    if started_before is not None:
        where.append("j.started_at <= ?")
        params.append(started_before)
    where_sql = "WHERE " + " AND ".join(where) if where else ""
    direction = "DESC" if descending else "ASC"
    sort_expression = sort_expressions[sort]
    sort_params = [time()] if sort == "duration" else []
    page_where = ""
    page_params = []
    if after_key is not None:
        null_rank, value, job_id = after_key
        comparator = "<" if descending else ">"
        page_where = (
            "WHERE (sort_value IS NULL) > ? OR ("
            "(sort_value IS NULL) = ? AND ("
            "(? = 0 AND (sort_value "
            f"{comparator} ? OR (sort_value = ? AND job_id {comparator} ?))) "
            f"OR (? = 1 AND job_id {comparator} ?)))"
        )
        page_params = [
            null_rank,
            null_rank,
            null_rank,
            value,
            value,
            job_id,
            null_rank,
            job_id,
        ]
    with _lock:
        conn = _connection()
        total = conn.execute(
            f"SELECT COUNT(*) FROM jobs j {where_sql}", params
        ).fetchone()[0]
        rows = conn.execute(
            f"WITH filtered AS ("
            f"SELECT j.job_id, j.data, j.n_results, "
            f"{sort_expression} AS sort_value FROM jobs j {where_sql}"
            f") SELECT job_id, data, n_results, sort_value FROM filtered "
            f"{page_where} ORDER BY sort_value {direction} NULLS LAST, "
            f"job_id {direction} LIMIT ?",
            sort_params + params + page_params + [limit],
        ).fetchall()
    jobs = []
    for job_id, data, n_results, sort_value in rows:
        job = json.loads(data)
        job["job_id"] = job_id
        job["n_results"] = n_results
        job["_cursor_key"] = [1 if sort_value is None else 0, sort_value, job_id]
        jobs.append(job)
    return {"total": total, "items": jobs}


def management_job(job_id: str) -> dict | None:
    with _lock:
        row = (
            _connection()
            .execute(
                "SELECT job_id, data, n_results FROM jobs WHERE job_id = ?",
                (job_id,),
            )
            .fetchone()
        )
    if row is None:
        return None
    job = json.loads(row[1])
    job["job_id"] = row[0]
    job["n_results"] = row[2]
    return job


def management_nodes_page(
    *,
    status: str,
    region: str | None,
    job_id: str | None,
    started_after: float | None,
    ended_after: float | None,
    sort: str,
    descending: bool,
    after_key: tuple | None,
    limit: int,
) -> dict:
    sort_expressions = {
        "started_at": "started_booting_at",
        "ended_at": "ended_at",
        "status": "LOWER(status)",
        "machine_type": "machine_type",
    }
    where = []
    params = []
    if status == "active":
        where.append("UPPER(status) IN ('BOOTING', 'READY', 'RUNNING')")
    elif status != "all":
        where.append("LOWER(status) = ?")
        params.append(status)
    if region is not None:
        where.append("gcp_region = ?")
        params.append(region)
    if job_id is not None:
        where.append(
            "(json_extract(data, '$.current_job') = ? "
            "OR json_extract(data, '$.reserved_for_job') = ?)"
        )
        params.extend((job_id, job_id))
    if started_after is not None:
        where.append("started_booting_at >= ?")
        params.append(started_after)
    if ended_after is not None:
        where.append("ended_at >= ?")
        params.append(ended_after)
    where_sql = "WHERE " + " AND ".join(where) if where else ""
    direction = "DESC" if descending else "ASC"
    page_where = ""
    page_params = []
    if after_key is not None:
        null_rank, value, instance_name = after_key
        comparator = "<" if descending else ">"
        page_where = (
            "WHERE (sort_value IS NULL) > ? OR ("
            "(sort_value IS NULL) = ? AND ("
            "(? = 0 AND (sort_value "
            f"{comparator} ? OR (sort_value = ? AND instance_name "
            f"{comparator} ?))) OR (? = 1 AND instance_name "
            f"{comparator} ?)))"
        )
        page_params = [
            null_rank,
            null_rank,
            null_rank,
            value,
            value,
            instance_name,
            null_rank,
            instance_name,
        ]
    with _lock:
        conn = _connection()
        total = conn.execute(
            f"SELECT COUNT(*) FROM nodes {where_sql}", params
        ).fetchone()[0]
        rows = conn.execute(
            f"WITH filtered AS ("
            f"SELECT instance_name, status, machine_type, gcp_region, spot, "
            f"started_booting_at, ended_at, data, "
            f"{sort_expressions[sort]} AS sort_value "
            f"FROM nodes {where_sql}"
            f") SELECT instance_name, status, machine_type, gcp_region, spot, "
            f"started_booting_at, ended_at, data, sort_value "
            f"FROM filtered {page_where} "
            f"ORDER BY sort_value {direction} NULLS LAST, "
            f"instance_name {direction} LIMIT ?",
            params + page_params + [limit],
        ).fetchall()
    items = []
    for (
        instance_name,
        status,
        machine_type,
        gcp_region,
        spot,
        started_booting_at,
        ended_at,
        data,
        sort_value,
    ) in rows:
        node = json.loads(data)
        node.update(
            {
                "instance_name": instance_name,
                "status": status,
                "machine_type": machine_type,
                "gcp_region": gcp_region,
                "spot": spot,
                "started_booting_at": started_booting_at,
                "ended_at": ended_at,
            }
        )
        node["_cursor_key"] = [
            1 if sort_value is None else 0,
            sort_value,
            instance_name,
        ]
        items.append(node)
    return {"total": total, "items": items}


def management_node(instance_name: str) -> dict | None:
    with _lock:
        row = (
            _connection()
            .execute("SELECT data FROM nodes WHERE instance_name = ?", (instance_name,))
            .fetchone()
        )
    return json.loads(row[0]) if row else None


def management_node_logs(
    instance_name: str,
    *,
    before_id: int | None = None,
    after_id: int | None = None,
    limit: int = 501,
) -> list[dict]:
    where = "instance_name = ?"
    params: list = [instance_name]
    descending = before_id is not None
    if before_id is not None:
        where += " AND id < ?"
        params.append(before_id)
    elif after_id is not None:
        where += " AND id > ?"
        params.append(after_id)
    params.append(limit)
    with _lock:
        rows = (
            _connection()
            .execute(
                f"SELECT id, ts, msg FROM node_logs WHERE {where} "
                f"ORDER BY id {'DESC' if descending else 'ASC'} LIMIT ?",
                params,
            )
            .fetchall()
        )
    if descending:
        rows.reverse()
    return [
        {"id": row_id, "timestamp": timestamp, "message": message}
        for row_id, timestamp, message in rows
    ]


def management_job_logs(
    job_id: str,
    input_index: int | None,
    *,
    include_notices: bool = False,
    row_limit: int | None = None,
    before_row_id: int | None = None,
    after_row_id: int | None = None,
    errors_only: bool = False,
) -> list[dict]:
    if include_notices:
        where = "job_id = ? AND input_index IS NULL"
        params = (job_id,)
    else:
        where = "job_id = ? AND input_index = ?"
        params = (job_id, input_index)
    if errors_only:
        where += " AND is_error = 1"
    descending = before_row_id is not None
    if before_row_id is not None:
        where += " AND id <= ?"
        params = (*params, before_row_id)
    elif after_row_id is not None:
        where += " AND id >= ?"
        params = (*params, after_row_id)
    limit_sql = " LIMIT ?" if row_limit is not None else ""
    if row_limit is not None:
        params = (*params, row_limit)
    with _lock:
        rows = (
            _connection()
            .execute(
                f"SELECT id, input_index, is_error, logs FROM job_logs "
                f"WHERE {where} ORDER BY id {'DESC' if descending else 'ASC'}"
                f"{limit_sql}",
                params,
            )
            .fetchall()
        )
    if descending:
        rows.reverse()
    entries = []
    for row_id, row_input_index, document_is_error, logs_json in rows:
        for offset, log in enumerate(json.loads(logs_json)):
            timestamp = log.get("timestamp")
            if timestamp is None:
                continue
            entries.append(
                {
                    "row_id": row_id,
                    "offset": offset,
                    "input_index": row_input_index,
                    "timestamp": float(timestamp),
                    "message": log.get("message", ""),
                    "is_error": bool(log.get("is_error") or document_is_error),
                }
            )
    return entries


def _management_error_signature(message: str) -> str:
    normalized = re.sub(r"\bline \d+\b", "line", message)
    normalized = re.sub(r"0x[0-9a-fA-F]+", "0x", normalized)
    return hashlib.sha256(normalized.encode()).hexdigest()[:24]


def management_error_groups(
    job_id: str, after_key: tuple | None, limit: int
) -> dict:
    entries = """
        SELECT input_index,
            json_extract(entry.value, '$.message') AS message
        FROM job_logs, json_each(job_logs.logs) AS entry
        WHERE job_id = ? AND input_index IS NOT NULL AND is_error = 1
        AND json_extract(entry.value, '$.timestamp') IS NOT NULL
    """
    grouped = f"""
        SELECT management_error_signature(message) AS signature,
            COUNT(*) AS count, MIN(message) AS representative_error,
            MIN(input_index) AS sample_input_index
        FROM ({entries})
        GROUP BY signature
    """
    page_where = ""
    page_params = []
    if after_key is not None:
        count, signature = after_key
        page_where = "WHERE count < ? OR (count = ? AND signature < ?)"
        page_params = [count, count, signature]
    with _lock:
        conn = _connection()
        conn.create_function(
            "management_error_signature", 1, _management_error_signature
        )
        total = conn.execute(f"SELECT COUNT(*) FROM ({grouped})", (job_id,)).fetchone()[0]
        rows = conn.execute(
            f"SELECT signature, count, representative_error, sample_input_index "
            f"FROM ({grouped}) {page_where} "
            f"ORDER BY count DESC, signature DESC LIMIT ?",
            (job_id, *page_params, limit),
        ).fetchall()
    return {
        "total": total,
        "items": [
            {
                "signature": signature,
                "count": count,
                "representative_error": representative_error,
                "sample_input_indexes": [sample_input_index],
            }
            for signature, count, representative_error, sample_input_index in rows
        ],
    }


def management_raw_metrics(
    job_id: str,
    scope: str,
    input_index: int | None,
    after_timestamp: float,
    after_id: int,
    limit: int,
) -> list[dict]:
    where = (
        "job_id = ? AND scope = ? "
        "AND (timestamp > ? OR (timestamp = ? AND id > ?))"
    )
    params: list = [
        job_id,
        scope,
        after_timestamp,
        after_timestamp,
        after_id,
    ]
    if input_index is not None:
        where += " AND input_index = ?"
        params.append(input_index)
    params.append(limit)
    with _read_lock:
        rows = (
            _read_connection()
            .execute(
                "SELECT id, timestamp, duration_sec, instance_name, scope, input_index, "
                "worker_id, cpu_seconds, cpu_percent, memory_bytes, memory_percent, "
                "network_rx_bytes, network_tx_bytes, disk_read_bytes, disk_write_bytes, "
                "gpu_percent, gpu_memory_bytes, gpu_memory_percent "
                f"FROM resource_metrics WHERE {where} ORDER BY timestamp, id LIMIT ?",
                params,
            )
            .fetchall()
        )
    fields = (
        "id",
        "timestamp",
        "duration_seconds",
        "node_id",
        "scope",
        "input_index",
        "worker_id",
        "cpu_seconds",
        "cpu_percent",
        "memory_bytes",
        "memory_percent",
        "network_rx_bytes",
        "network_tx_bytes",
        "disk_read_bytes",
        "disk_write_bytes",
        "gpu_percent",
        "gpu_memory_bytes",
        "gpu_memory_percent",
    )
    return [dict(zip(fields, row)) for row in rows]
