"""Least Connections Load Balancing Algorithm.

System Design Analysis:
Routes traffic dynamically to the backend currently processing the fewest active
in-flight requests.
Ideal for workloads with high variance in request execution times (e.g., long-polling,
complex database queries, media rendering), preventing slow requests from piling up
on a single instance.
"""

from typing import List, Optional
from app.backend import Backend
from app.algorithms.base import LoadBalancingStrategy


class LeastConnectionsStrategy(LoadBalancingStrategy):
    """Least Connections strategy with weight consideration."""

    name: str = "least-connections"

    def select_backend(self, backends: List[Backend], client_key: Optional[str] = None) -> Optional[Backend]:
        healthy = [b for b in backends if b.is_healthy]
        if not healthy:
            return None

        # Pick backend with minimum active_connections
        # Ties broken by highest weight
        return min(healthy, key=lambda b: (b.active_connections, -b.weight))
