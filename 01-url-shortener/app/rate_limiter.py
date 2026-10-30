"""Sliding Window Rate Limiter.

System Design Concept:
Rate limiters protect ingestion endpoints against DDoS, scrapers, and bot storms.
The Sliding Window algorithm prevents border-condition traffic spikes that
plague Fixed Window counters, ensuring steady throughput over rolling intervals.
"""

import time
import threading
from typing import Dict, List, Tuple


class SlidingWindowRateLimiter:
    """Thread-safe sliding window log rate limiter per client key (e.g., IP address)."""

    def __init__(self, max_requests: int = 20, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._history: Dict[str, List[float]] = {}
        self._lock = threading.Lock()

    def is_allowed(self, client_id: str) -> Tuple[bool, int]:
        """Check if request is allowed for given client_id.
        
        Returns:
            (allowed: bool, retry_after_seconds: int)
        """
        now = time.time()
        window_start = now - self.window_seconds

        with self._lock:
            timestamps = self._history.get(client_id, [])
            
            # Filter out timestamps outside the active sliding window
            timestamps = [t for t in timestamps if t > window_start]

            if len(timestamps) >= self.max_requests:
                # Oldest timestamp in window determines retry delay
                oldest = timestamps[0]
                retry_after = max(1, int(oldest + self.window_seconds - now))
                self._history[client_id] = timestamps
                return False, retry_after

            # Allow request and append timestamp
            timestamps.append(now)
            self._history[client_id] = timestamps
            return True, 0

    def reset(self) -> None:
        """Reset all rate limit counters."""
        with self._lock:
            self._history.clear()
