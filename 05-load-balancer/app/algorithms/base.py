"""Base interface for Load Balancing Strategies."""

from abc import ABC, abstractmethod
from typing import List, Optional
from app.backend import Backend


class LoadBalancingStrategy(ABC):
    """Abstract Strategy interface for picking the optimal backend server."""

    name: str = "base"

    @abstractmethod
    def select_backend(self, backends: List[Backend], client_key: Optional[str] = None) -> Optional[Backend]:
        """Select a healthy backend from the pool."""
        pass
