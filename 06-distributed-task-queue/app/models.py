"""Task Models, Lifecycle State Machine, and Retry Policies.

System Design Analysis:
Task queues require explicit, deterministic lifecycle states to guarantee
at-least-once processing, avoid ghost executions, and support Dead Letter routing.
"""

import time
import uuid
import random
from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, Any, Optional


class TaskState(str, Enum):
    PENDING = "PENDING"          # In ready queue, awaiting worker pickup
    SCHEDULED = "SCHEDULED"      # In delayed queue, awaiting execution timestamp
    RUNNING = "RUNNING"          # Currently executing on a worker thread
    COMPLETED = "COMPLETED"      # Executed successfully
    RETRYING = "RETRYING"        # Transient failure, waiting for backoff window
    FAILED = "FAILED"            # Final failure
    DEAD_LETTER = "DEAD_LETTER"  # Poisoned task moved to DLQ


@dataclass
class RetryPolicy:
    """Configures exponential backoff with jitter."""
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    backoff_factor: float = 2.0
    enable_jitter: bool = True

    def calculate_delay(self, retry_count: int) -> float:
        """Calculate exponential backoff delay with jitter: min(max, base * factor^count) + jitter."""
        delay = self.base_delay * (self.backoff_factor ** retry_count)
        capped = min(self.max_delay, delay)
        jitter = random.uniform(0, 0.5 * capped) if self.enable_jitter else 0.0
        return capped + jitter


@dataclass
class Task:
    """Represents a job payload traversing the queue lifecycle."""
    task_name: str
    args: Dict[str, Any] = field(default_factory=dict)
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    state: TaskState = TaskState.PENDING
    retry_count: int = 0
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    execute_at: float = field(default_factory=time.time)
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    worker_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "task_name": self.task_name,
            "args": self.args,
            "state": self.state.value,
            "retry_count": self.retry_count,
            "max_retries": self.retry_policy.max_retries,
            "execute_at": self.execute_at,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "result": self.result,
            "error": self.error,
            "worker_id": self.worker_id,
        }
