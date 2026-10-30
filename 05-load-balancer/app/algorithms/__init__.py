"""Load balancing routing algorithms package."""

from app.algorithms.base import LoadBalancingStrategy
from app.algorithms.round_robin import RoundRobinStrategy
from app.algorithms.weighted_round_robin import WeightedRoundRobinStrategy
from app.algorithms.least_connections import LeastConnectionsStrategy
from app.algorithms.consistent_hashing import ConsistentHashingStrategy

__all__ = [
    "LoadBalancingStrategy",
    "RoundRobinStrategy",
    "WeightedRoundRobinStrategy",
    "LeastConnectionsStrategy",
    "ConsistentHashingStrategy",
]
