"""Consistent Hashing with Virtual Nodes Load Balancing Algorithm.

System Design Analysis:
Standard modulo hashing (hash(key) % N) causes catastrophic cache invalidation
when N changes (adding or removing a server remaps ~99% of all keys).

Consistent Hashing maps servers and keys onto a 2^32 cyclic ring:
1. Virtual Nodes: Each physical backend is replicated V times (e.g., 100 virtual nodes)
   along the ring to guarantee uniform distribution and eliminate hotspot skews.
2. Minimal Remapping: When a node fails, only its 1/N partition is reassigned to its
   clockwise neighbor; all other keys remain mapped to their original servers.
3. Cache Affinity: Guarantees that requests from the same user/session consistently
   reach the same backend node, maximizing in-memory L1/L2 cache hit ratios.
"""

import bisect
import hashlib
from typing import List, Optional, Dict
from app.backend import Backend
from app.algorithms.base import LoadBalancingStrategy


class ConsistentHashingStrategy(LoadBalancingStrategy):
    """Consistent Hash Ring with virtual nodes."""

    name: str = "consistent-hashing"

    def __init__(self, virtual_nodes_per_weight: int = 50):
        self.virtual_nodes_per_weight = virtual_nodes_per_weight
        self._ring: List[int] = []
        self._ring_map: Dict[int, Backend] = {}

    def _hash(self, key: str) -> int:
        """Hash string to 32-bit unsigned integer using MD5."""
        digest = hashlib.md5(key.encode("utf-8")).digest()
        return int.from_bytes(digest[:4], byteorder="big")

    def _rebuild_ring(self, healthy_backends: List[Backend]) -> None:
        """Reconstruct the hash ring with virtual nodes for all healthy servers."""
        ring = []
        ring_map = {}

        for backend in healthy_backends:
            num_vnodes = self.virtual_nodes_per_weight * backend.weight
            for i in range(num_vnodes):
                vnode_key = f"{backend.url}#vnode-{i}"
                vnode_hash = self._hash(vnode_key)
                ring.append(vnode_hash)
                ring_map[vnode_hash] = backend

        ring.sort()
        self._ring = ring
        self._ring_map = ring_map

    def select_backend(self, backends: List[Backend], client_key: Optional[str] = None) -> Optional[Backend]:
        healthy = [b for b in backends if b.is_healthy]
        if not healthy:
            return None

        self._rebuild_ring(healthy)
        if not self._ring:
            return None

        # Use client_key or default fallback
        key = client_key or "default_client"
        key_hash = self._hash(key)

        # Binary search for the first virtual node clockwise of key_hash
        idx = bisect.bisect_right(self._ring, key_hash)
        if idx >= len(self._ring):
            idx = 0  # Wrap around the circular ring

        target_hash = self._ring[idx]
        return self._ring_map[target_hash]
