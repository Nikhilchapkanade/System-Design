"""Sliding Window Counter (Approximation) Rate Limiting Algorithm.

System Design Analysis (Cloudflare / Redis Pattern):
- Mechanism: Combines the current window counter and previous window counter using
  a weighted overlap formula:
      estimated_requests = prev_count * (1 - progress_in_window) + curr_count
- Pros:
  1. O(1) memory footprint (only stores 2 integers: prev_count, curr_count).
  2. Smooths out window boundary spikes (prevents 2x border attacks).
  3. Extremely fast O(1) execution with < 0.05% error rate against true sliding log.
- Cons: Approximated assumption that requests in previous window were uniformly distributed.
"""

import time
import threading
from typing import Optional, Dict, Any, Tuple
from app.algorithms.base import RateLimiter, RateLimitResult


class _WindowPair:
    __slots__ = ("curr_window_id", "curr_count", "prev_count")

    def __init__(self, window_id: int):
        self.curr_window_id = window_id
        self.curr_count = 0
        self.prev_count = 0


class SlidingWindowCounterRateLimiter(RateLimiter):
    """Thread-safe Sliding Window Counter (Weighted Approximation) rate limiter."""

    def __init__(self, limit: int = 10, window_seconds: int = 60):
        self.limit = limit
        self.window_seconds = window_seconds
        self._windows: Dict[str, _WindowPair] = {}
        self._lock = threading.Lock()

    def _get_window_id(self, now: float) -> int:
        return int(now // self.window_seconds)

    def acquire(self, key: str, cost: int = 1) -> RateLimitResult:
        now = time.time()
        curr_window_id = self._get_window_id(now)
        window_start = curr_window_id * self.window_seconds
        progress = (now - window_start) / self.window_seconds  # 0.0 to 1.0
        overlap_weight = 1.0 - progress

        with self._lock:
            pair = self._windows.get(key)
            if pair is None:
                pair = _WindowPair(curr_window_id)
                self._windows[key] = pair
            elif pair.curr_window_id != curr_window_id:
                # Rotate windows
                if pair.curr_window_id == curr_window_id - 1:
                    pair.prev_count = pair.curr_count
                else:
                    # More than 1 full window elapsed; previous count expired
                    pair.prev_count = 0
                pair.curr_window_id = curr_window_id
                pair.curr_count = 0

            # Calculate weighted rate estimation
            estimated_requests = (pair.prev_count * overlap_weight) + pair.curr_count

            window_end = (curr_window_id + 1) * self.window_seconds
            reset_after = max(0.0, window_end - now)

            if estimated_requests + cost <= self.limit:
                pair.curr_count += cost
                remaining = max(0, int(self.limit - (estimated_requests + cost)))
                return RateLimitResult(
                    allowed=True,
                    limit=self.limit,
                    remaining=remaining,
                    reset_after=reset_after,
                    retry_after=0.0,
                )
            else:
                remaining = max(0, int(self.limit - estimated_requests))
                # Approximate time when enough weight decays to allow 1 request
                decay_needed = (estimated_requests + cost) - self.limit
                if pair.prev_count > 0:
                    time_to_decay = (decay_needed / pair.prev_count) * self.window_seconds
                    retry_after = min(reset_after, max(0.1, time_to_decay))
                else:
                    retry_after = reset_after

                return RateLimitResult(
                    allowed=False,
                    limit=self.limit,
                    remaining=remaining,
                    reset_after=reset_after,
                    retry_after=retry_after,
                )

    def reset(self, key: Optional[str] = None) -> None:
        with self._lock:
            if key:
                self._windows.pop(key, None)
            else:
                self._windows.clear()

    def get_state(self, key: str) -> Dict[str, Any]:
        now = time.time()
        curr_window_id = self._get_window_id(now)
        window_start = curr_window_id * self.window_seconds
        progress = (now - window_start) / self.window_seconds

        with self._lock:
            pair = self._windows.get(key)
            if not pair:
                return {"estimated_requests": 0.0, "limit": self.limit}
            
            p_count = pair.prev_count if pair.curr_window_id == curr_window_id else (
                pair.curr_count if pair.curr_window_id == curr_window_id - 1 else 0
            )
            c_count = pair.curr_count if pair.curr_window_id == curr_window_id else 0
            est = (p_count * (1.0 - progress)) + c_count
            return {
                "estimated_requests": round(est, 2),
                "curr_count": c_count,
                "prev_count": p_count,
                "limit": self.limit,
            }
