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

import json
import os
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
    data TEXT
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
    disk_write_bytes INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_resource_metrics_sample
ON resource_metrics(instance_name, timestamp, scope, worker_id);
CREATE INDEX IF NOT EXISTS idx_resource_metrics_job_task
ON resource_metrics(job_id, scope, input_index, timestamp)
WHERE job_id IS NOT NULL;

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
        )
        for sample in samples
    ]
    with _lock:
        conn = _connection()
        conn.executemany(
            "INSERT OR IGNORE INTO resource_metrics "
            "(timestamp, duration_sec, instance_name, scope, job_id, input_index, "
            "worker_id, cpu_seconds, cpu_percent, memory_bytes, memory_percent, "
            "network_rx_bytes, network_tx_bytes, disk_read_bytes, disk_write_bytes) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()


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
        "INSERT INTO jobs (job_id, started_at, status, user, function_name, n_inputs, "
        "n_results, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(job_id) DO UPDATE SET started_at = excluded.started_at, "
        "status = excluded.status, user = excluded.user, "
        "function_name = excluded.function_name, n_inputs = excluded.n_inputs, "
        "n_results = MAX(jobs.n_results, excluded.n_results), data = excluded.data",
        (
            job_id,
            job.get("started_at"),
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
    with _lock:
        row = (
            _connection()
            .execute(
                "SELECT COUNT(*) FROM job_logs WHERE job_id = ? AND is_error = 1",
                (job_id,),
            )
            .fetchone()
        )
    return row[0]


def job_logged_input_indexes(job_id: str) -> tuple[list[int], list[int]]:
    """Returns (all indexes with logs, indexes with error logs)."""
    with _lock:
        rows = (
            _connection()
            .execute(
                "SELECT DISTINCT input_index, MAX(is_error) FROM job_logs "
                "WHERE job_id = ? AND input_index IS NOT NULL GROUP BY input_index",
                (job_id,),
            )
            .fetchall()
        )
    indexes = sorted(int(index) for index, _ in rows)
    failed = sorted(int(index) for index, is_error in rows if is_error)
    return indexes, failed


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


def node_logs_after(instance_name: str, after_id: int) -> list[tuple[int, float, str]]:
    """Log rows with id > after_id, oldest first. Pass 0 for a full replay."""
    with _lock:
        rows = (
            _connection()
            .execute(
                "SELECT id, ts, msg FROM node_logs WHERE instance_name = ? AND id > ? ORDER BY id",
                (instance_name, after_id),
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


def snapshot_cluster_config(snapshot_path: str) -> dict | None:
    conn = sqlite3.connect(f"file:{snapshot_path}?mode=ro", uri=True)
    try:
        row = conn.execute("SELECT data FROM cluster_config WHERE id = 1").fetchone()
    finally:
        conn.close()
    return json.loads(row[0]) if row else None


def import_snapshot(snapshot_path: str, digest: str, cluster_config: dict | None) -> bool:
    """Merge a client-hosted head's history snapshot into this database
    (first `burla deploy`). Existing rows always win; log rows are copied only
    for jobs/nodes this import inserted, so two unrelated histories can't mix.
    The digest row makes retrying the same upload a no-op. Returns False if
    this snapshot was already imported."""
    job_marks = ", ".join("?" for _ in _ENDED_JOB_STATUSES)
    node_marks = ", ".join("?" for _ in _ENDED_NODE_STATUSES)
    with _lock:
        conn = _connection()
        already = conn.execute(
            "SELECT 1 FROM history_imports WHERE digest = ?", (digest,)
        ).fetchone()
        if already:
            return False
        conn.execute("ATTACH DATABASE ? AS snapshot", (snapshot_path,))
        try:
            conn.execute("CREATE TEMP TABLE old_jobs AS SELECT job_id FROM jobs")
            conn.execute(
                "CREATE TEMP TABLE old_nodes AS SELECT instance_name FROM nodes"
            )
            conn.execute(
                f"INSERT OR IGNORE INTO jobs SELECT * FROM snapshot.jobs "
                f"WHERE status IN ({job_marks})",
                _ENDED_JOB_STATUSES,
            )
            conn.execute(
                "INSERT INTO job_logs (job_id, input_index, is_error, timestamp, logs) "
                "SELECT job_id, input_index, is_error, timestamp, logs "
                "FROM snapshot.job_logs WHERE job_id IN (SELECT job_id FROM jobs) "
                "AND job_id NOT IN (SELECT job_id FROM old_jobs)"
            )
            conn.execute(
                f"INSERT OR IGNORE INTO nodes SELECT * FROM snapshot.nodes "
                f"WHERE status IN ({node_marks})",
                _ENDED_NODE_STATUSES,
            )
            conn.execute(
                "INSERT INTO node_logs (instance_name, ts, msg) "
                "SELECT instance_name, ts, msg FROM snapshot.node_logs "
                "WHERE instance_name IN (SELECT instance_name FROM nodes) "
                "AND instance_name NOT IN (SELECT instance_name FROM old_nodes)"
            )
            conn.execute(
                "INSERT OR IGNORE INTO resource_metrics "
                "(timestamp, duration_sec, instance_name, scope, job_id, input_index, "
                "worker_id, cpu_seconds, cpu_percent, memory_bytes, memory_percent, "
                "network_rx_bytes, network_tx_bytes, disk_read_bytes, disk_write_bytes) "
                "SELECT timestamp, duration_sec, instance_name, scope, job_id, "
                "input_index, worker_id, cpu_seconds, cpu_percent, memory_bytes, "
                "memory_percent, network_rx_bytes, network_tx_bytes, disk_read_bytes, "
                "disk_write_bytes FROM snapshot.resource_metrics "
                "WHERE job_id IN ("
                "SELECT job_id FROM jobs WHERE job_id NOT IN (SELECT job_id FROM old_jobs)"
                ") OR (scope = 'node' AND instance_name IN ("
                "SELECT instance_name FROM nodes "
                "WHERE instance_name NOT IN (SELECT instance_name FROM old_nodes)"
                "))"
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
            conn.execute("DROP TABLE IF EXISTS old_jobs")
            conn.execute("DROP TABLE IF EXISTS old_nodes")
            conn.execute("DETACH DATABASE snapshot")
    return True
