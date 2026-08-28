"""
Persistence layer.

Why SQLite instead of Redis/RabbitMQ for this build?
This project is meant to run standalone with zero external infra. SQLite
gives us the same properties we actually need from a broker for this
demo: durability (survives a crash/restart), atomic claim-and-lock of a
task ("SELECT ... WHERE status='pending' ... " + UPDATE inside a
transaction), and ordering by next_retry_at. In production this class
is the piece you'd swap for a Redis-backed queue (e.g. RQ/Celery) or
RabbitMQ consumer -- the rest of the system (worker pool, backoff
policy, REST API) does not need to change.
"""
import json
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager

DB_PATH = None  # set by init_db()
_local = threading.local()

SCHEMA = """
CREATE TABLE IF NOT EXISTS endpoints (
    id TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    secret TEXT NOT NULL,
    event_types TEXT NOT NULL,      -- JSON list, "*" = all
    failure_rate REAL DEFAULT 0.0,  -- only used by the mock receiver for simulation
    created_at REAL NOT NULL,
    active INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS deliveries (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    endpoint_id TEXT NOT NULL,
    status TEXT NOT NULL,            -- pending | processing | success | retrying | dead_letter
    attempt_count INTEGER DEFAULT 0,
    max_attempts INTEGER DEFAULT 6,
    next_retry_at REAL NOT NULL,
    last_error TEXT,
    last_response_code INTEGER,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    FOREIGN KEY(event_id) REFERENCES events(id),
    FOREIGN KEY(endpoint_id) REFERENCES endpoints(id)
);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id TEXT PRIMARY KEY,
    delivery_id TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    status TEXT NOT NULL,           -- success | failed
    response_code INTEGER,
    latency_ms REAL,
    error TEXT,
    attempted_at REAL NOT NULL,
    FOREIGN KEY(delivery_id) REFERENCES deliveries(id)
);

CREATE INDEX IF NOT EXISTS idx_deliveries_status_retry
    ON deliveries(status, next_retry_at);
"""


def init_db(path: str):
    global DB_PATH
    DB_PATH = path
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()


def get_conn() -> sqlite3.Connection:
    # one connection per thread; WAL mode lets readers/writers overlap
    if not hasattr(_local, "conn"):
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.row_factory = sqlite3.Row
        _local.conn = conn
    return _local.conn


@contextmanager
def tx():
    conn = get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def new_id() -> str:
    return uuid.uuid4().hex[:16]


# ---------------------------------------------------------------- endpoints
def create_endpoint(url, secret, event_types=None, failure_rate=0.0):
    eid = new_id()
    with tx() as conn:
        conn.execute(
            "INSERT INTO endpoints (id, url, secret, event_types, failure_rate, created_at, active) "
            "VALUES (?, ?, ?, ?, ?, ?, 1)",
            (eid, url, secret, json.dumps(event_types or ["*"]), failure_rate, time.time()),
        )
    return eid


def list_active_endpoints(event_type: str):
    conn = get_conn()
    rows = conn.execute("SELECT * FROM endpoints WHERE active = 1").fetchall()
    out = []
    for r in rows:
        types = json.loads(r["event_types"])
        if "*" in types or event_type in types:
            out.append(r)
    return out


def get_endpoint(endpoint_id):
    conn = get_conn()
    return conn.execute("SELECT * FROM endpoints WHERE id = ?", (endpoint_id,)).fetchone()


# ------------------------------------------------------------------ events
def create_event(event_type, payload: dict):
    eid = new_id()
    with tx() as conn:
        conn.execute(
            "INSERT INTO events (id, event_type, payload, created_at) VALUES (?, ?, ?, ?)",
            (eid, event_type, json.dumps(payload), time.time()),
        )
    return eid


# -------------------------------------------------------------- deliveries
def create_delivery(event_id, endpoint_id, max_attempts=6):
    did = new_id()
    now = time.time()
    with tx() as conn:
        conn.execute(
            "INSERT INTO deliveries (id, event_id, endpoint_id, status, attempt_count, "
            "max_attempts, next_retry_at, created_at, updated_at) "
            "VALUES (?, ?, ?, 'pending', 0, ?, ?, ?, ?)",
            (did, event_id, endpoint_id, max_attempts, now, now, now),
        )
    return did


def claim_due_deliveries(limit=10):
    """
    Atomically claim up to `limit` deliveries that are due for (re)attempt.
    This is the equivalent of a worker doing BLPOP/BRPOP on a Redis list --
    but with a WHERE clause on next_retry_at so retries only fire after
    their backoff window has elapsed.
    """
    now = time.time()
    conn = get_conn()
    # BEGIN IMMEDIATE grabs the write lock *before* the SELECT, so a second
    # worker thread/connection racing us blocks (and retries, via the
    # connection's busy timeout) instead of reading the same "pending" rows
    # before our UPDATE commits -- that race is what caused duplicate
    # attempts on the same delivery in an earlier version of this function.
    conn.execute("BEGIN IMMEDIATE")
    try:
        rows = conn.execute(
            "SELECT * FROM deliveries WHERE status IN ('pending','retrying') "
            "AND next_retry_at <= ? ORDER BY next_retry_at ASC LIMIT ?",
            (now, limit),
        ).fetchall()
        ids = [r["id"] for r in rows]
        if ids:
            q_marks = ",".join("?" * len(ids))
            conn.execute(
                f"UPDATE deliveries SET status='processing', updated_at=? WHERE id IN ({q_marks})",
                (now, *ids),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return rows


def record_attempt_result(delivery, success, response_code, latency_ms, error, backoff_seconds):
    now = time.time()
    attempt_number = delivery["attempt_count"] + 1
    with tx() as conn:
        conn.execute(
            "INSERT INTO delivery_attempts (id, delivery_id, attempt_number, status, "
            "response_code, latency_ms, error, attempted_at) VALUES (?,?,?,?,?,?,?,?)",
            (
                new_id(), delivery["id"], attempt_number,
                "success" if success else "failed",
                response_code, latency_ms, error, now,
            ),
        )
        if success:
            conn.execute(
                "UPDATE deliveries SET status='success', attempt_count=?, "
                "last_response_code=?, last_error=NULL, updated_at=? WHERE id=?",
                (attempt_number, response_code, now, delivery["id"]),
            )
        elif attempt_number >= delivery["max_attempts"]:
            conn.execute(
                "UPDATE deliveries SET status='dead_letter', attempt_count=?, "
                "last_response_code=?, last_error=?, updated_at=? WHERE id=?",
                (attempt_number, response_code, error, now, delivery["id"]),
            )
        else:
            conn.execute(
                "UPDATE deliveries SET status='retrying', attempt_count=?, "
                "last_response_code=?, last_error=?, next_retry_at=?, updated_at=? WHERE id=?",
                (attempt_number, response_code, error, now + backoff_seconds, now, delivery["id"]),
            )


def stats_snapshot():
    conn = get_conn()
    by_status = conn.execute(
        "SELECT status, COUNT(*) c FROM deliveries GROUP BY status"
    ).fetchall()
    attempts = conn.execute(
        "SELECT status, COUNT(*) c, AVG(latency_ms) avg_latency FROM delivery_attempts GROUP BY status"
    ).fetchall()
    retry_hist = conn.execute(
        "SELECT attempt_count, COUNT(*) c FROM deliveries WHERE status='success' GROUP BY attempt_count"
    ).fetchall()
    return {
        "by_status": {r["status"]: r["c"] for r in by_status},
        "attempts": {r["status"]: {"count": r["c"], "avg_latency_ms": r["avg_latency"]} for r in attempts},
        "success_by_attempts_needed": {r["attempt_count"]: r["c"] for r in retry_hist},
    }
