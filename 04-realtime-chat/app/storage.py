"""Message History Storage Layer with Monotonic Sequencing and SQLite WAL.

System Design Analysis (Message Ordering & History):
In distributed chat architectures, clock skew across server nodes can corrupt
temporal ordering if raw timestamps alone are relied upon.
We assign an atomic, monotonic sequence ID (seq_id) per room. This ensures:
1. Strict message ordering within a channel.
2. Gap detection: clients can detect if they missed message #42 when #43 arrives.
3. Efficient history pagination (cursor-based: 'give me 50 messages before seq 100').
"""

import sqlite3
import time
import threading
from typing import Optional, List, Dict, Any


class ChatStorage:
    """Thread-safe SQLite storage for chat message history."""

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
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    room_id TEXT NOT NULL,
                    seq_id INTEGER NOT NULL,
                    username TEXT NOT NULL,
                    content TEXT NOT NULL,
                    timestamp REAL NOT NULL
                );

                CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_room_seq 
                ON messages(room_id, seq_id);

                CREATE INDEX IF NOT EXISTS idx_messages_room_time 
                ON messages(room_id, timestamp);
            """)

    def save_message(self, room_id: str, username: str, content: str) -> Dict[str, Any]:
        """Atomically calculate next room sequence ID and persist message."""
        conn = self._get_connection()
        now = time.time()
        with conn:
            # Atomic fetch of max seq_id in room
            cur = conn.execute(
                "SELECT COALESCE(MAX(seq_id), 0) + 1 FROM messages WHERE room_id = ?",
                (room_id,),
            )
            next_seq = cur.fetchone()[0]

            cursor = conn.execute(
                """
                INSERT INTO messages (room_id, seq_id, username, content, timestamp)
                VALUES (?, ?, ?, ?, ?)
                """,
                (room_id, next_seq, username, content, now),
            )
            msg_id = cursor.lastrowid

        return {
            "id": msg_id,
            "room_id": room_id,
            "seq_id": next_seq,
            "username": username,
            "content": content,
            "timestamp": now,
        }

    def get_history(
        self,
        room_id: str,
        limit: int = 50,
        before_seq: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Cursor-based pagination of room message history."""
        conn = self._get_connection()
        if before_seq is not None:
            cursor = conn.execute(
                """
                SELECT id, room_id, seq_id, username, content, timestamp
                FROM messages
                WHERE room_id = ? AND seq_id < ?
                ORDER BY seq_id DESC
                LIMIT ?
                """,
                (room_id, before_seq, limit),
            )
        else:
            cursor = conn.execute(
                """
                SELECT id, room_id, seq_id, username, content, timestamp
                FROM messages
                WHERE room_id = ?
                ORDER BY seq_id DESC
                LIMIT ?
                """,
                (room_id, limit),
            )

        rows = [dict(r) for r in cursor.fetchall()]
        rows.reverse()  # Chronological order
        return rows

    def get_stats(self) -> Dict[str, Any]:
        conn = self._get_connection()
        total_msgs = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        total_rooms = conn.execute("SELECT COUNT(DISTINCT room_id) FROM messages").fetchone()[0]
        return {
            "total_messages": total_msgs,
            "active_rooms": total_rooms,
        }
