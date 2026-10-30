"""Backend Server Representation and State Machine.

System Design Analysis:
Tracks live state per upstream server:
- Weight: Capacity proportion for Weighted Round Robin.
- Active Connections: In-flight requests for Least Connections.
- Health State Machine: Flapping protection via consecutive failure/success thresholds.
"""

import threading
from typing import Dict, Any


class Backend:
    """Represents a single upstream backend server."""

    def __init__(self, url: str, weight: int = 1):
        self.url = url.rstrip("/")
        self.weight = max(1, weight)
        self.is_healthy = True
        self.active_connections = 0
        self.total_requests = 0
        self.consecutive_failures = 0
        self.consecutive_successes = 0

        # Current weight for smooth weighted round robin algorithm
        self.current_weight = 0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Increment active in-flight connection count."""
        with self._lock:
            self.active_connections += 1
            self.total_requests += 1

    def release(self) -> None:
        """Decrement active in-flight connection count."""
        with self._lock:
            self.active_connections = max(0, self.active_connections - 1)

    def record_success(self, healthy_threshold: int = 2) -> bool:
        """Register successful probe. Returns True if state changed to healthy."""
        with self._lock:
            self.consecutive_failures = 0
            self.consecutive_successes += 1
            if not self.is_healthy and self.consecutive_successes >= healthy_threshold:
                self.is_healthy = True
                return True
            return False

    def record_failure(self, unhealthy_threshold: int = 2) -> bool:
        """Register failed probe/request. Returns True if state changed to unhealthy."""
        with self._lock:
            self.consecutive_successes = 0
            self.consecutive_failures += 1
            if self.is_healthy and self.consecutive_failures >= unhealthy_threshold:
                self.is_healthy = False
                return True
            return False

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "url": self.url,
                "weight": self.weight,
                "is_healthy": self.is_healthy,
                "active_connections": self.active_connections,
                "total_requests": self.total_requests,
                "consecutive_failures": self.consecutive_failures,
            }
