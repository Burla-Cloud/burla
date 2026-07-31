"""
SQLite-backed history store.

This is the only persistence in Burla. It is NEVER in the live coordination
path - live cluster state lives in memory (see cluster_state.py) and moves
over HTTP. Rows here exist so the dashboard can show jobs / logs / usage
after the fact, and so cluster_config survives head restarts.

All functions are synchronous; call them via `asyncio.to_thread` from async
endpoints. A single WAL-mode connection guarded by a lock is plenty for the
write volume (log batches flush at most ~1/sec per worker).
"""

import json
import os
import sqlite3
import threading
from pathlib import Path

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

CREATE TABLE IF NOT EXISTS cluster_config (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    data TEXT
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
