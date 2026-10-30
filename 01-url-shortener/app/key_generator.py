"""Key Generation Service (KGS) and Base62 Encoding.

System Design Concept:
Instead of computing MD5/SHA256 hashes of the URL (which suffer from collisions
and require sequential database probing), production distributed systems use a
centralized Range Allocator (e.g., Apache ZooKeeper or Raft cluster) to assign
non-overlapping integer ranges to each worker node.

Each worker increments an atomic in-memory counter within its assigned range.
Converting that integer into Base62 guarantees:
1. Zero collisions across the entire distributed system.
2. Sub-millisecond generation time (pure in-memory O(1) operation).
3. Compact, URL-friendly strings ([0-9a-zA-Z]).
"""

import threading
import string
import hashlib

BASE62_ALPHABET = string.digits + string.ascii_lowercase + string.ascii_uppercase
BASE = len(BASE62_ALPHABET)  # 62


def base62_encode(num: int, min_length: int = 6) -> str:
    """Encode an integer into a Base62 string.
    
    Args:
        num: Positive integer to encode.
        min_length: Minimum string length, zero-padded with '0'.
    
    Returns:
        Base62 encoded string.
    """
    if num == 0:
        return BASE62_ALPHABET[0].rjust(min_length, BASE62_ALPHABET[0])
    
    digits = []
    current = num
    while current > 0:
        digits.append(BASE62_ALPHABET[current % BASE])
        current //= BASE
    
    encoded = "".join(reversed(digits))
    if len(encoded) < min_length:
        encoded = encoded.rjust(min_length, BASE62_ALPHABET[0])
    return encoded


def base62_decode(encoded_str: str) -> int:
    """Decode a Base62 string back to its original integer."""
    num = 0
    for char in encoded_str:
        num = num * BASE + BASE62_ALPHABET.index(char)
    return num


class RangeKeyGenerator:
    """Thread-safe Key Generator using Range Partitioning.
    
    Simulates a worker node that receives a block of unique IDs
    (e.g., from ZooKeeper or an orchestration service).
    """

    def __init__(self, start_id: int, block_size: int):
        self._lock = threading.Lock()
        self._start_id = start_id
        self._block_size = block_size
        self._current_id = start_id
        self._max_id = start_id + block_size

    def next_id(self) -> int:
        """Atomically fetch the next ID in the allocated block."""
        with self._lock:
            if self._current_id >= self._max_id:
                # In production, request next block from ZooKeeper / central coordinator.
                # Here we simulate allocation of the subsequent range.
                self._start_id = self._max_id
                self._current_id = self._start_id
                self._max_id = self._start_id + self._block_size

            allocated_id = self._current_id
            self._current_id += 1
            return allocated_id

    def generate_short_code(self, min_length: int = 6) -> str:
        """Generate a guaranteed unique short code via Base62."""
        unique_id = self.next_id()
        return base62_encode(unique_id, min_length=min_length)


def hash_fallback(url: str, length: int = 7) -> str:
    """Alternative approach: Hash truncation with SHA-256.
    
    Included for educational comparison in system design reviews.
    Produces collisions at scale requiring collision-resolution loops.
    """
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
    num = int(digest[:12], 16)
    return base62_encode(num)[:length]
