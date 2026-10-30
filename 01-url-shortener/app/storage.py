"""Database Storage Layer with SQLite WAL Mode and B-Tree Indexing.

System Design Concept:
Relational databases with B-Tree indexes provide O(log N) lookup time for unique
keys like `short_code`. Enabling Write-Ahead Logging (WAL) decouples reads from writes:
readers never block writers, and writers never block readers.
"""

import sqlite3
import time
import threading
from typing import Optional, Dict, Any, List


class URLStorage:
    """Thread-safe SQLite storage with connection-per-thread pooling."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._local = threading.local()
        self.init_db()

    def _get_connection(self) -> sqlite3.Connection:
        """Get or initialize thread-local connection with WAL mode enabled."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            conn.row_factory = sqlite3.Row
            # System Design Optimization: WAL mode for high concurrent throughput
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            conn.execute("PRAGMA busy_timeout = 5000;")
            self._local.conn = conn
        return self._local.conn

    def init_db(self) -> None:
        """Initialize tables and indexes."""
        conn = self._get_connection()
        with conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS urls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    short_code TEXT UNIQUE NOT NULL,
                    original_url TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    expires_at REAL,
                    click_count INTEGER DEFAULT 0
                );

                CREATE UNIQUE INDEX IF NOT EXISTS idx_urls_short_code 
                ON urls(short_code);

                CREATE INDEX IF NOT EXISTS idx_urls_expires_at 
                ON urls(expires_at);

                CREATE TABLE IF NOT EXISTS click_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    short_code TEXT NOT NULL,
                    clicked_at REAL NOT NULL,
                    ip_address TEXT,
                    user_agent TEXT,
                    referer TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_click_events_code 
                ON click_events(short_code);
            """)

    def insert_url(
        self,
        short_code: str,
        original_url: str,
        ttl_seconds: Optional[int] = None
    ) -> bool:
        """Insert a newly shortened URL. Returns True if inserted, False if conflict."""
        conn = self._get_connection()
        now = time.time()
        expires_at = (now + ttl_seconds) if ttl_seconds and ttl_seconds > 0 else None

        try:
            with conn:
                conn.execute(
                    """
                    INSERT INTO urls (short_code, original_url, created_at, expires_at, click_count)
                    VALUES (?, ?, ?, ?, 0)
                    """,
                    (short_code, original_url, now, expires_at),
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def get_url(self, short_code: str) -> Optional[Dict[str, Any]]:
        """Query URL metadata by short code, filtering expired records."""
        conn = self._get_connection()
        cursor = conn.execute(
            """
            SELECT id, short_code, original_url, created_at, expires_at, click_count
            FROM urls
            WHERE short_code = ?
            """,
            (short_code,),
        )
        row = cursor.fetchone()
        if not row:
            return None

        data = dict(row)
        # Check TTL expiration
        if data["expires_at"] and time.time() > data["expires_at"]:
            return None

        return data

    def record_click(
        self,
        short_code: str,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        referer: Optional[str] = None,
    ) -> None:
        """Record an asynchronous/batched click event and increment aggregate count."""
        conn = self._get_connection()
        now = time.time()
        try:
            with conn:
                conn.execute(
                    "UPDATE urls SET click_count = click_count + 1 WHERE short_code = ?",
                    (short_code,),
                )
                conn.execute(
                    """
                    INSERT INTO click_events (short_code, clicked_at, ip_address, user_agent, referer)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (short_code, now, ip_address, user_agent, referer),
                )
        except Exception:
            # Analytics recording should not fail the primary redirect request
            pass

    def get_analytics(self, short_code: str) -> Optional[Dict[str, Any]]:
        """Fetch click count and the last 100 click events for a short URL."""
        conn = self._get_connection()
        url_row = conn.execute(
            "SELECT short_code, original_url, created_at, expires_at, click_count FROM urls WHERE short_code = ?",
            (short_code,),
        ).fetchone()

        if not url_row:
            return None

        events_cursor = conn.execute(
            """
            SELECT clicked_at, ip_address, user_agent, referer
            FROM click_events
            WHERE short_code = ?
            ORDER BY clicked_at DESC
            LIMIT 100
            """,
            (short_code,),
        )
        recent_events = [dict(r) for r in events_cursor.fetchall()]

        result = dict(url_row)
        result["recent_clicks"] = recent_events
        return result

    def get_stats(self) -> Dict[str, Any]:
        """Aggregate system-level statistics."""
        conn = self._get_connection()
        total_urls = conn.execute("SELECT COUNT(*) FROM urls").fetchone()[0]
        total_clicks = conn.execute("SELECT COALESCE(SUM(click_count), 0) FROM urls").fetchone()[0]
        return {
            "total_urls": total_urls,
            "total_clicks": total_clicks,
        }
