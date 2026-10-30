"""Token Bucket Rate Limiting Algorithm.

System Design Analysis:
- Mechanism: A bucket of capacity C refills at R tokens/sec.
- Pros: Extremely memory efficient (O(1)), highly performant (O(1)), natively supports
  controlled traffic bursts up to capacity C while bounding long-term average throughput.
- Cons: Sudden bursts can temporarily flood downstream services if capacity C is set too high.
"""

import time
import threading
from typing import Optional, Dict, Any
from app.algorithms.base import RateLimiter, RateLimitResult


class _Bucket:
    __slots__ = ("tokens", "last_refill")

    def __init__(self, capacity: float):
        self.tokens = capacity
        self.last_refill = time.time()


class TokenBucketRateLimiter(RateLimiter):
    """Thread-safe Token Bucket rate limiter."""

    def __init__(self, capacity: int = 10, refill_rate: float = 2.0):
        """
        Args:
            capacity: Maximum burst capacity (tokens).
            refill_rate: Tokens added per second.
        """
        self.capacity = float(capacity)
        self.refill_rate = float(refill_rate)
        self._buckets: Dict[str, _Bucket] = {}
        self._lock = threading.Lock()

    def _refill(self, bucket: _Bucket, now: float) -> None:
        elapsed = now - bucket.last_refill
        if elapsed > 0:
            added = elapsed * self.refill_rate
            bucket.tokens = min(self.capacity, bucket.tokens + added)
            bucket.last_refill = now

    def acquire(self, key: str, cost: int = 1) -> RateLimitResult:
        now = time.time()
        with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = _Bucket(self.capacity)
                self._buckets[key] = bucket

            self._refill(bucket, now)

            if bucket.tokens >= cost:
                bucket.tokens -= cost
                remaining = int(bucket.tokens)
                # Seconds until bucket would be completely full again
                reset_after = (self.capacity - bucket.tokens) / self.refill_rate if self.refill_rate > 0 else 0
                return RateLimitResult(
                    allowed=True,
                    limit=int(self.capacity),
                    remaining=remaining,
                    reset_after=reset_after,
                    retry_after=0.0,
                )
            else:
                remaining = int(bucket.tokens)
                needed = cost - bucket.tokens
                retry_after = needed / self.refill_rate if self.refill_rate > 0 else 1.0
                reset_after = (self.capacity - bucket.tokens) / self.refill_rate if self.refill_rate > 0 else 0
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
                return {"tokens": self.capacity, "capacity": self.capacity}
            self._refill(bucket, time.time())
            return {
                "tokens": round(bucket.tokens, 2),
                "capacity": self.capacity,
                "refill_rate": self.refill_rate,
            }
