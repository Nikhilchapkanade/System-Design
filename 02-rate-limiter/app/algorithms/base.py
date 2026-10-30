"""Base interfaces and data structures for Rate Limiting."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Dict, Any


@dataclass(frozen=True)
class RateLimitResult:
    """Outcome of a rate limit check with RFC-compliant header values."""
    allowed: bool
    limit: int
    remaining: int
    reset_after: float  # Seconds until full bucket replenishment / window reset
    retry_after: float  # Seconds client must wait before retrying (0.0 if allowed)

    def to_headers(self) -> Dict[str, str]:
        """Convert result to standard rate limit HTTP headers."""
        headers = {
            "X-RateLimit-Limit": str(self.limit),
            "X-RateLimit-Remaining": str(max(0, self.remaining)),
            "X-RateLimit-Reset": str(max(0, int(self.reset_after))),
        }
        if not self.allowed:
            headers["Retry-After"] = str(max(1, int(self.retry_after)))
        return headers


class RateLimiter(ABC):
    """Abstract Base Class for Rate Limiting implementations."""

    @abstractmethod
    def acquire(self, key: str, cost: int = 1) -> RateLimitResult:
        """Attempt to acquire permission for a request under client `key`."""
        pass

    @abstractmethod
    def reset(self, key: Optional[str] = None) -> None:
        """Reset rate limiter state for a specific key or all keys."""
        pass

    @abstractmethod
    def get_state(self, key: str) -> Dict[str, Any]:
        """Inspect internal state for telemetry/monitoring."""
        pass
