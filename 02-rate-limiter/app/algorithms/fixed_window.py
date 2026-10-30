"""Fixed Window Counter Rate Limiting Algorithm.

System Design Analysis:
- Mechanism: Time is divided into discrete fixed windows of length W. Each window
  maintains an atomic counter.
- Pros: Simple to implement, minimal memory footprint (O(1)).
- The Classic Flaw (Border Spike): A client can send full quota at the tail end of
  window 1 and another full quota at the start of window 2, effectively serving 2x
  traffic across the window boundary.
"""

import time
import threading
from typing import Optional, Dict, Any, Tuple
from app.algorithms.base import RateLimiter, RateLimitResult


class FixedWindowRateLimiter(RateLimiter):
    """Thread-safe Fixed Window Counter rate limiter."""

    def __init__(self, limit: int = 10, window_seconds: int = 60):
        self.limit = limit
        self.window_seconds = window_seconds
        # key -> (window_id, counter)
        self._windows: Dict[str, Tuple[int, int]] = {}
        self._lock = threading.Lock()

    def _get_window_id(self, now: float) -> int:
        return int(now // self.window_seconds)

    def acquire(self, key: str, cost: int = 1) -> RateLimitResult:
        now = time.time()
        current_window = self._get_window_id(now)
        window_end = (current_window + 1) * self.window_seconds
        reset_after = max(0.0, window_end - now)

        with self._lock:
            stored_window, count = self._windows.get(key, (current_window, 0))

            if stored_window != current_window:
                # New window boundary reached
                stored_window = current_window
                count = 0

            if count + cost <= self.limit:
                count += cost
                self._windows[key] = (stored_window, count)
                remaining = self.limit - count
                return RateLimitResult(
                    allowed=True,
                    limit=self.limit,
                    remaining=remaining,
                    reset_after=reset_after,
                    retry_after=0.0,
                )
            else:
                remaining = max(0, self.limit - count)
                return RateLimitResult(
                    allowed=False,
                    limit=self.limit,
                    remaining=remaining,
                    reset_after=reset_after,
                    retry_after=reset_after,
                )

    def reset(self, key: Optional[str] = None) -> None:
        with self._lock:
            if key:
                self._windows.pop(key, None)
            else:
                self._windows.clear()

    def get_state(self, key: str) -> Dict[str, Any]:
        now = time.time()
        current_window = self._get_window_id(now)
        with self._lock:
            stored_window, count = self._windows.get(key, (current_window, 0))
            active_count = count if stored_window == current_window else 0
            return {
                "count": active_count,
                "limit": self.limit,
                "window_seconds": self.window_seconds,
                "seconds_until_reset": round(((current_window + 1) * self.window_seconds) - now, 2),
            }
