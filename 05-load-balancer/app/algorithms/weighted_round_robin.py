"""Smooth Weighted Round Robin Algorithm (NGINX Standard).

System Design Analysis:
Naive weighted round robin (e.g. weights 3:1:1 -> {A, A, A, B, C}) sends consecutive
bursts to the highest-weight node.
NGINX's Smooth Weighted Round Robin algorithm mathematically interleaves requests
({A, B, A, C, A}), preventing micro-bursts on beefier instances while preserving
exact capacity ratios over time.
"""

import threading
from typing import List, Optional
from app.backend import Backend
from app.algorithms.base import LoadBalancingStrategy


class WeightedRoundRobinStrategy(LoadBalancingStrategy):
    """Thread-safe Smooth Weighted Round Robin strategy."""

    name: str = "weighted-round-robin"

    def __init__(self):
        self._lock = threading.Lock()

    def select_backend(self, backends: List[Backend], client_key: Optional[str] = None) -> Optional[Backend]:
        healthy = [b for b in backends if b.is_healthy]
        if not healthy:
            return None

        with self._lock:
            total_weight = 0
            best_backend: Optional[Backend] = None

            for backend in healthy:
                backend.current_weight += backend.weight
                total_weight += backend.weight

                if best_backend is None or backend.current_weight > best_backend.current_weight:
                    best_backend = backend

            if best_backend is not None:
                best_backend.current_weight -= total_weight

            return best_backend
