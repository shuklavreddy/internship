"""Database layer for queuectl using SQLite (qctl copy)"""
import json
import os
import sqlite3
from datetime import datetime, timedelta


def init_db(path="queue.db"):
    need = not os.path.exists(path)
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    cur = conn.cursor()
    if need:
        cur.executescript(
            """
        CREATE TABLE jobs (
            id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            state TEXT NOT NULL,
            attempts INTEGER NOT NULL,
            max_retries INTEGER NOT NULL,
            created_at TEXT,
            updated_at TEXT,
            next_attempt_at TEXT,
            last_error TEXT,
            priority INTEGER DEFAULT 0,
            run_at TEXT,
            log_path TEXT,
            timeout INTEGER
        );

        CREATE TABLE config (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE metrics (
            key TEXT PRIMARY KEY,
            value REAL
        );

        INSERT INTO config(key, value) VALUES('backoff_base', '2');
        INSERT INTO metrics(key, value) VALUES('jobs_processed', 0);
        INSERT INTO metrics(key, value) VALUES('jobs_failed', 0);
        INSERT INTO metrics(key, value) VALUES('jobs_retried', 0);
        """
        )
        conn.commit()
    else:
        # run migrations: ensure columns exist
        cur.execute("PRAGMA table_info(jobs)")
        cols = [r[1] for r in cur.fetchall()]
        # add missing columns
        if 'priority' not in cols:
            cur.execute("ALTER TABLE jobs ADD COLUMN priority INTEGER DEFAULT 0")
        if 'run_at' not in cols:
            cur.execute("ALTER TABLE jobs ADD COLUMN run_at TEXT")
        if 'log_path' not in cols:
            cur.execute("ALTER TABLE jobs ADD COLUMN log_path TEXT")
        if 'timeout' not in cols:
            cur.execute("ALTER TABLE jobs ADD COLUMN timeout INTEGER")
        # ensure metrics table exists
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='metrics'")
        if not cur.fetchone():
            cur.executescript(
                """
            CREATE TABLE metrics (
                key TEXT PRIMARY KEY,
                value REAL
            );
            INSERT INTO metrics(key, value) VALUES('jobs_processed', 0);
            INSERT INTO metrics(key, value) VALUES('jobs_failed', 0);
            INSERT INTO metrics(key, value) VALUES('jobs_retried', 0);
            """
            )
        # ensure config has backoff_base
        cur.execute("SELECT value FROM config WHERE key='backoff_base'")
        if not cur.fetchone():
            cur.execute("INSERT INTO config(key,value) VALUES('backoff_base','2')")
        conn.commit()
    return conn


def enqueue_job(dbpath, job):
    conn = sqlite3.connect(dbpath, timeout=30)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO jobs(id,command,state,attempts,max_retries,created_at,updated_at) VALUES (?,?,?,?,?,?,?)",
        (
            job["id"],
            job["command"],
            job.get("state", "pending"),
            job.get("attempts", 0),
            job.get("max_retries", 3),
            job.get("created_at"),
            job.get("updated_at"),
        ),
    )
    conn.commit()
    conn.close()


def _now_ts():
    return datetime.utcnow().isoformat() + "Z"


def _parse_ts(s):
    if s is None:
        return None
    return datetime.fromisoformat(s.replace("Z", ""))


def fetch_and_lock_job(dbpath):
    conn = sqlite3.connect(dbpath, timeout=30)
    cur = conn.cursor()
    now = _now_ts()
    # Safe claim: select candidate, then atomically update by id if still pending
    while True:
        cur.execute(
            "SELECT id FROM jobs WHERE state='pending' AND (next_attempt_at IS NULL OR next_attempt_at<=?) AND (run_at IS NULL OR run_at<=?) ORDER BY priority DESC, created_at LIMIT 1",
            (now, now),
        )
        row = cur.fetchone()
        if not row:
            conn.close()
            return None
        jid = row[0]
        # try to claim
        cur.execute("UPDATE jobs SET state='processing', updated_at=? WHERE id=? AND state='pending'", (now, jid))
        if cur.rowcount == 1:
            # claimed
            cur.execute(
                "SELECT id,command,state,attempts,max_retries,created_at,updated_at,next_attempt_at,last_error,priority,run_at,log_path,timeout FROM jobs WHERE id=?",
                (jid,),
            )
            row2 = cur.fetchone()
            conn.commit()
            conn.close()
            if not row2:
                return None
            keys = [
                "id",
                "command",
                "state",
                "attempts",
                "max_retries",
                "created_at",
                "updated_at",
                "next_attempt_at",
                "last_error",
                "priority",
                "run_at",
                "log_path",
                "timeout",
            ]
            return dict(zip(keys, row2))
        # else someone else claimed it first; loop and try next


def complete_job(dbpath, job_id):
    conn = sqlite3.connect(dbpath, timeout=30)
    cur = conn.cursor()
    now = _now_ts()
    cur.execute("UPDATE jobs SET state='completed', updated_at=? WHERE id=?", (now, job_id))
    conn.commit()
    conn.close()


def fail_job(dbpath, job_id, attempts, max_retries, error_msg, backoff_base=2):
    conn = sqlite3.connect(dbpath, timeout=30)
    cur = conn.cursor()
    now_dt = datetime.utcnow()
    attempts = attempts + 1
    if attempts > max_retries:
        cur.execute("UPDATE jobs SET state='dead', attempts=?, updated_at=?, last_error=? WHERE id=?", (attempts, now_dt.isoformat() + "Z", error_msg, job_id))
    else:
        delay_seconds = (backoff_base ** attempts)
        next_time = (now_dt + timedelta(seconds=delay_seconds)).isoformat() + "Z"
        cur.execute(
            "UPDATE jobs SET state='pending', attempts=?, updated_at=?, next_attempt_at=?, last_error=? WHERE id=?",
            (attempts, now_dt.isoformat() + "Z", next_time, error_msg, job_id),
        )
    conn.commit()
    conn.close()


def list_jobs(dbpath, state=None):
    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()
    if state:
        cur.execute(
            "SELECT id,command,state,attempts,max_retries,created_at,updated_at,next_attempt_at,last_error FROM jobs WHERE state=? ORDER BY created_at",
            (state,),
        )
    else:
        cur.execute(
            "SELECT id,command,state,attempts,max_retries,created_at,updated_at,next_attempt_at,last_error FROM jobs ORDER BY created_at"
        )
    rows = cur.fetchall()
    conn.close()
    keys = ["id", "command", "state", "attempts", "max_retries", "created_at", "updated_at", "next_attempt_at", "last_error"]
    return [dict(zip(keys, r)) for r in rows]


def get_stats(dbpath):
    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()
    cur.execute("SELECT state, COUNT(*) FROM jobs GROUP BY state")
    rows = cur.fetchall()
    conn.close()
    d = {r[0]: r[1] for r in rows}
    return {"states": d}


def set_config(dbpath, key, value):
    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO config(key,value) VALUES(?,?)", (key, str(value)))
    conn.commit()
    conn.close()


def get_config(dbpath, key):
    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()
    cur.execute("SELECT value FROM config WHERE key=?", (key,))
    r = cur.fetchone()
    conn.close()
    return r[0] if r else None


def retry_dead_job(dbpath, job_id):
    conn = sqlite3.connect(dbpath)
    cur = conn.cursor()
    now = _now_ts()
    cur.execute("UPDATE jobs SET state='pending', attempts=0, updated_at=?, next_attempt_at=NULL, last_error=NULL WHERE id=? AND state='dead'", (now, job_id))
    conn.commit()
    conn.close()
