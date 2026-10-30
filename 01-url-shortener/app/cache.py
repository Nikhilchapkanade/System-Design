"""Cache-Aside Layer for Distributed URL Shortener.

System Design Concept:
URL shorteners exhibit an 80-20 Pareto distribution: 20% of the short URLs
generate 80% of total redirection traffic. Caching these hot links in an
in-memory LRU (Least Recently Used) cache with TTL reduces database read load
by 80-95% and drops p99 latency to under 2ms.
"""

import time
import threading
from collections import OrderedDict
from typing import Optional, Tuple, Any, Dict


class CacheEntry:
    __slots__ = ("value", "expires_at")

    def __init__(self, value: Any, expires_at: Optional[float]):
        self.value = value
        self.expires_at = expires_at

    def is_expired(self) -> bool:
        return self.expires_at is not None and time.time() > self.expires_at


class LRUCacheWithTTL:
    """Thread-safe LRU Cache with TTL expiration and metrics instrumentation."""

    def __init__(self, capacity: int = 10000, default_ttl_seconds: int = 86400):
        self._capacity = max(1, capacity)
        self._default_ttl = default_ttl_seconds
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = threading.Lock()

        # Telemetry metrics
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    def get(self, key: str) -> Optional[Any]:
        """Fetch item from cache. Refreshes LRU position on hit."""
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                self._misses += 1
                return None

            if entry.is_expired():
                del self._cache[key]
                self._misses += 1
                return None

            # Move accessed key to the end (most recently used)
            self._cache.move_to_end(key)
            self._hits += 1
            return entry.value

    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        """Store key-value with TTL and enforce LRU eviction on overflow."""
        ttl = ttl_seconds if ttl_seconds is not None else self._default_ttl
        expires_at = time.time() + ttl if ttl > 0 else None

        with self._lock:
            if key in self._cache:
                del self._cache[key]
            elif len(self._cache) >= self._capacity:
                # Evict oldest entry (least recently used)
                self._cache.popitem(last=False)
                self._evictions += 1

            self._cache[key] = CacheEntry(value=value, expires_at=expires_at)

    def delete(self, key: str) -> bool:
        """Invalidate a key from cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self._hits = 0
            self._misses = 0
            self._evictions = 0

    def get_metrics(self) -> Dict[str, Any]:
        """Return cache health and telemetry stats."""
        with self._lock:
            total = self._hits + self._misses
            hit_ratio = (self._hits / total) if total > 0 else 0.0
            return {
                "size": len(self._cache),
                "capacity": self._capacity,
                "hits": self._hits,
                "misses": self._misses,
                "evictions": self._evictions,
                "hit_ratio": round(hit_ratio, 4),
            }
