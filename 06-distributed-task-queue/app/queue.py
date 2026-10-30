"""Broker Queues: Ready Queue (FIFO), Delayed Queue (Priority Min-Heap), and DLQ.

System Design Analysis:
- Ready Queue: O(1) lock-free/synchronized FIFO queue for immediate consumption.
- Delayed Queue: O(log N) min-heap sorted by execution timestamp (simulates Redis ZSET
  or RabbitMQ Delayed Message Exchange).
- Dead Letter Queue: Isolated storage for failed tasks, protecting worker throughput.
"""

import heapq
import queue
import time
import threading
from typing import Optional, List, Dict, Any, Tuple

from app.models import Task, TaskState
from app.storage import TaskStorage


class TaskBroker:
    """Orchestrates Ready, Delayed, and Dead-Letter task queues."""

    def __init__(self, storage: TaskStorage):
        self.storage = storage

        # Thread-safe FIFO for immediate execution
        self._ready_queue: queue.Queue[Task] = queue.Queue()

        # Min-heap priority queue for delayed & retry tasks: stores (execute_at, counter, task)
        self._delayed_heap: List[Tuple[float, int, Task]] = []
        self._heap_counter = 0
        self._heap_lock = threading.Lock()

        # Dead Letter Queue
        self._dlq: Dict[str, Task] = {}
        self._dlq_lock = threading.Lock()

    def enqueue(self, task: Task) -> None:
        """Enqueue task according to its target execution timestamp."""
        now = time.time()
        if task.execute_at <= now:
            task.state = TaskState.PENDING
            self.storage.save_task(task)
            self._ready_queue.put(task)
        else:
            task.state = TaskState.SCHEDULED
            self.storage.save_task(task)
            with self._heap_lock:
                self._heap_counter += 1
                heapq.heappush(self._delayed_heap, (task.execute_at, self._heap_counter, task))

    def dequeue(self, timeout: float = 1.0) -> Optional[Task]:
        """Fetch next ready task for worker execution (blocking with timeout)."""
        try:
            return self._ready_queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def route_to_dlq(self, task: Task, error: str) -> None:
        """Isolate poisoned task into Dead Letter Queue."""
        task.state = TaskState.DEAD_LETTER
        task.error = error
        task.completed_at = time.time()
        self.storage.save_task(task)

        with self._dlq_lock:
            self._dlq[task.task_id] = task

    def replay_dlq_task(self, task_id: str) -> bool:
        """Re-queue a dead-letter task back into the ready queue for reprocessing."""
        with self._dlq_lock:
            task = self._dlq.pop(task_id, None)

        if not task:
            # Fallback to storage
            raw = self.storage.get_task(task_id)
            if not raw or raw["state"] != TaskState.DEAD_LETTER.value:
                return False
            task = Task(
                task_id=raw["task_id"],
                task_name=raw["task_name"],
                args=raw["args"],
                retry_count=0,
            )

        task.retry_count = 0
        task.error = None
        task.execute_at = time.time()
        self.enqueue(task)
        return True

    def promote_due_tasks(self) -> int:
        """Promote due delayed/scheduled tasks from min-heap to ready queue."""
        now = time.time()
        promoted = 0

        with self._heap_lock:
            while self._delayed_heap and self._delayed_heap[0][0] <= now:
                _, _, task = heapq.heappop(self._delayed_heap)
                task.state = TaskState.PENDING
                self.storage.save_task(task)
                self._ready_queue.put(task)
                promoted += 1

        return promoted

    def get_queue_sizes(self) -> Dict[str, int]:
        with self._heap_lock:
            delayed_size = len(self._delayed_heap)
        with self._dlq_lock:
            dlq_size = len(self._dlq)
        return {
            "ready_count": self._ready_queue.qsize(),
            "delayed_count": delayed_size,
            "dlq_count": dlq_size,
        }
