"""Round Robin Load Balancing Algorithm.

System Design Analysis:
Evenly distributes requests across healthy backends in circular order.
Best when backends have homogeneous hardware specs and requests have uniform
processing requirements.
"""

import threading
from typing import List, Optional
from app.backend import Backend
from app.algorithms.base import LoadBalancingStrategy


class RoundRobinStrategy(LoadBalancingStrategy):
    """Thread-safe Round Robin strategy."""

    name: str = "round-robin"

    def __init__(self):
        self._index = 0
        self._lock = threading.Lock()

    def select_backend(self, backends: List[Backend], client_key: Optional[str] = None) -> Optional[Backend]:
        healthy = [b for b in backends if b.is_healthy]
        if not healthy:
            return None

        with self._lock:
            selected = healthy[self._index % len(healthy)]
            self._index = (self._index + 1) % 1_000_000_000
            return selected
