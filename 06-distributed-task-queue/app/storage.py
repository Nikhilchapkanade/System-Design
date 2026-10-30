"""Task State Persistence Layer with SQLite WAL.

System Design Analysis:
Task metadata, execution timelines, and outputs must be durably persisted so that
task state survives worker crashes, provides audit trails, and enables idempotency checks.
"""

import sqlite3
import json
import threading
from typing import Optional, List, Dict, Any

from app.models import Task, TaskState


class TaskStorage:
    """Thread-safe SQLite store for task state and execution audit trails."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._local = threading.local()
        self.init_db()

    def _get_connection(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            conn.execute("PRAGMA busy_timeout = 5000;")
            self._local.conn = conn
        return self._local.conn

    def init_db(self) -> None:
        conn = self._get_connection()
        with conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    task_name TEXT NOT NULL,
                    args_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    retry_count INTEGER NOT NULL,
                    max_retries INTEGER NOT NULL,
                    execute_at REAL NOT NULL,
                    created_at REAL NOT NULL,
                    started_at REAL,
                    completed_at REAL,
                    result_json TEXT,
                    error_text TEXT,
                    worker_id TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_tasks_state 
                ON tasks(state);

                CREATE INDEX IF NOT EXISTS idx_tasks_created 
                ON tasks(created_at DESC);
            """)

    def save_task(self, task: Task) -> None:
        conn = self._get_connection()
        with conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO tasks (
                    task_id, task_name, args_json, state, retry_count, max_retries,
                    execute_at, created_at, started_at, completed_at, result_json,
                    error_text, worker_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task.task_id,
                    task.task_name,
                    json.dumps(task.args),
                    task.state.value,
                    task.retry_count,
                    task.retry_policy.max_retries,
                    task.execute_at,
                    task.created_at,
                    task.started_at,
                    task.completed_at,
                    json.dumps(task.result) if task.result is not None else None,
                    task.error,
                    task.worker_id,
                ),
            )

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        conn = self._get_connection()
        cur = conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,))
        row = cur.fetchone()
        if not row:
            return None
        data = dict(row)
        data["args"] = json.loads(data["args_json"])
        data["result"] = json.loads(data["result_json"]) if data["result_json"] else None
        return data

    def list_tasks(self, state: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        conn = self._get_connection()
        if state:
            cur = conn.execute(
                "SELECT * FROM tasks WHERE state = ? ORDER BY created_at DESC LIMIT ?",
                (state, limit),
            )
        else:
            cur = conn.execute(
                "SELECT * FROM tasks ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
        rows = []
        for r in cur.fetchall():
            d = dict(r)
            d["args"] = json.loads(d["args_json"])
            d["result"] = json.loads(d["result_json"]) if d["result_json"] else None
            rows.append(d)
        return rows

    def get_stats(self) -> Dict[str, Any]:
        conn = self._get_connection()
        cur = conn.execute("SELECT state, COUNT(*) as count FROM tasks GROUP BY state")
        counts = {r["state"]: r["count"] for r in cur.fetchall()}
        return {
            "pending": counts.get("PENDING", 0),
            "scheduled": counts.get("SCHEDULED", 0),
            "running": counts.get("RUNNING", 0),
            "completed": counts.get("COMPLETED", 0),
            "failed": counts.get("FAILED", 0),
            "retrying": counts.get("RETRYING", 0),
            "dead_letter": counts.get("DEAD_LETTER", 0),
            "total": sum(counts.values()),
        }
