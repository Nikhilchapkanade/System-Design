"""Leaky Bucket Rate Limiting Algorithm.

System Design Analysis:
- Mechanism: Incoming requests fill a buffer queue (capacity C). The bucket leaks
  at a strictly constant rate (leak_rate req/sec).
- Pros: Traffic shaping: completely smooths out bursts, protecting downstream
  services that require a strictly constant processing rate.
- Cons: Bursts cannot be processed immediately even if system was idle; requests
  overflowing the capacity are dropped.
"""

import time
import threading
from typing import Optional, Dict, Any
from app.algorithms.base import RateLimiter, RateLimitResult


class _LeakyBucketState:
    __slots__ = ("water", "last_leak")

    def __init__(self):
        self.water = 0.0
        self.last_leak = time.time()


class LeakyBucketRateLimiter(RateLimiter):
    """Thread-safe Leaky Bucket rate limiter."""

    def __init__(self, capacity: int = 10, leak_rate: float = 2.0):
        """
        Args:
            capacity: Maximum bucket buffer capacity (queued requests).
            leak_rate: Requests leaked/processed per second.
        """
        self.capacity = float(capacity)
        self.leak_rate = float(leak_rate)
        self._buckets: Dict[str, _LeakyBucketState] = {}
        self._lock = threading.Lock()

    def _leak(self, bucket: _LeakyBucketState, now: float) -> None:
        elapsed = now - bucket.last_leak
        if elapsed > 0:
            leaked = elapsed * self.leak_rate
            bucket.water = max(0.0, bucket.water - leaked)
            bucket.last_leak = now

    def acquire(self, key: str, cost: int = 1) -> RateLimitResult:
        now = time.time()
        with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = _LeakyBucketState()
                self._buckets[key] = bucket

            self._leak(bucket, now)

            if bucket.water + cost <= self.capacity:
                bucket.water += cost
                remaining = int(self.capacity - bucket.water)
                # Time until bucket is completely empty
                reset_after = bucket.water / self.leak_rate if self.leak_rate > 0 else 0
                return RateLimitResult(
                    allowed=True,
                    limit=int(self.capacity),
                    remaining=remaining,
                    reset_after=reset_after,
                    retry_after=0.0,
                )
            else:
                remaining = int(self.capacity - bucket.water)
                overflow = (bucket.water + cost) - self.capacity
                retry_after = overflow / self.leak_rate if self.leak_rate > 0 else 1.0
                reset_after = bucket.water / self.leak_rate if self.leak_rate > 0 else 0
                return RateLimitResult(
                    allowed=False,
                    limit=int(self.capacity),
                    remaining=remaining,
                    reset_after=reset_after,
                    retry_after=retry_after,
                )

    def reset(self, key: Optional[str] = None) -> None:
        with self._lock:
            if key:
                self._buckets.pop(key, None)
            else:
                self._buckets.clear()

    def get_state(self, key: str) -> Dict[str, Any]:
        with self._lock:
            bucket = self._buckets.get(key)
            if not bucket:
                return {"water": 0.0, "capacity": self.capacity}
            self._leak(bucket, time.time())
            return {
                "water": round(bucket.water, 2),
                "capacity": self.capacity,
                "leak_rate": self.leak_rate,
            }
