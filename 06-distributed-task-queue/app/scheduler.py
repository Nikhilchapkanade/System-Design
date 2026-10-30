"""Background Scheduler for Delayed Task Promotion.

System Design Analysis:
Delayed tasks (scheduled execution and exponential backoff retries) reside in a
timestamp-ordered min-heap.
The Scheduler runs a polling loop to promote due tasks into the active
FIFO Ready Queue the exact millisecond their execution window arrives.
"""

import time
import threading
from typing import Optional
from app.queue import TaskBroker


class TaskScheduler:
    """Background manager promoting due delayed tasks to the ready queue."""

    def __init__(self, broker: TaskBroker, interval_seconds: float = 0.25):
        self.broker = broker
        self.interval = interval_seconds
        self._thread: Optional[threading.Thread] = None
        self._running = False

    def _loop(self):
        while self._running:
            try:
                self.broker.promote_due_tasks()
            except Exception:
                pass
            time.sleep(self.interval)

    def start(self):
        if not self._running:
            self._running = True
            self._thread = threading.Thread(target=self._loop, name="TaskScheduler", daemon=True)
            self._thread.start()

    def stop(self):
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)
            self._thread = None
