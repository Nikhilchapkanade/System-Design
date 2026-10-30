"""Sliding Window Log Rate Limiting Algorithm.

System Design Analysis:
- Mechanism: Keeps an exact record of request timestamps. On each request, timestamps
  older than (now - window_seconds) are evicted.
- Pros: 100% mathematically exact; completely eliminates the Fixed Window boundary burst flaw.
- Cons: Memory consumption scales linearly with request volume O(N), making it impractical
  for high-throughput multi-tenant systems with wide time windows.
"""

import time
import threading
from typing import Optional, Dict, List, Any
from app.algorithms.base import RateLimiter, RateLimitResult


class SlidingWindowLogRateLimiter(RateLimiter):
    """Thread-safe Sliding Window Log rate limiter."""

    def __init__(self, limit: int = 10, window_seconds: int = 60):
        self.limit = limit
        self.window_seconds = window_seconds
        self._logs: Dict[str, List[float]] = {}
        self._lock = threading.Lock()

    def acquire(self, key: str, cost: int = 1) -> RateLimitResult:
        now = time.time()
        window_start = now - self.window_seconds

        with self._lock:
            timestamps = self._logs.get(key, [])
            # Evict timestamps outside active sliding window
            timestamps = [t for t in timestamps if t > window_start]

            if len(timestamps) + cost <= self.limit:
                for _ in range(cost):
                    timestamps.append(now)
                self._logs[key] = timestamps
                remaining = self.limit - len(timestamps)
                oldest = timestamps[0]
                reset_after = max(0.0, (oldest + self.window_seconds) - now)
                return RateLimitResult(
                    allowed=True,
                    limit=self.limit,
                    remaining=remaining,
                    reset_after=reset_after,
                    retry_after=0.0,
                )
            else:
                self._logs[key] = timestamps
                remaining = max(0, self.limit - len(timestamps))
                oldest = timestamps[0]
                retry_after = max(0.1, (oldest + self.window_seconds) - now)
                return RateLimitResult(
                    allowed=False,
                    limit=self.limit,
                    remaining=remaining,
                    reset_after=retry_after,
                    retry_after=retry_after,
                )

    def reset(self, key: Optional[str] = None) -> None:
        with self._lock:
            if key:
                self._logs.pop(key, None)
            else:
                self._logs.clear()

    def get_state(self, key: str) -> Dict[str, Any]:
        now = time.time()
        window_start = now - self.window_seconds
        with self._lock:
            timestamps = [t for t in self._logs.get(key, []) if t > window_start]
            return {
                "active_requests_in_window": len(timestamps),
                "limit": self.limit,
                "window_seconds": self.window_seconds,
            }
