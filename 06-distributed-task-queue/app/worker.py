"""Worker Pool and Task Execution Engine.

System Design Analysis:
Workers dequeue tasks, execute handler logic, manage transient failures with
exponential backoff, and isolate poisoned pills into the Dead Letter Queue.
"""

import time
import threading
from typing import Callable, Dict, Any, Optional, List

from app.models import Task, TaskState
from app.queue import TaskBroker
from app.storage import TaskStorage

# Built-in handler registry
HANDLERS: Dict[str, Callable[[Dict[str, Any]], Any]] = {}


def register_handler(name: str):
    """Decorator to register task handler functions."""
    def decorator(fn: Callable[[Dict[str, Any]], Any]):
        HANDLERS[name] = fn
        return fn
    return decorator


# Sample system design tasks
@register_handler("send_email")
def handle_send_email(args: Dict[str, Any]) -> str:
    recipient = args.get("to", "user@example.com")
    subject = args.get("subject", "Notice")
    return f"Email successfully delivered to {recipient} with subject '{subject}'"


@register_handler("resize_image")
def handle_resize_image(args: Dict[str, Any]) -> Dict[str, Any]:
    width = args.get("width", 800)
    height = args.get("height", 600)
    return {"status": "optimized", "dimensions": f"{width}x{height}", "bytes_saved": 40960}


@register_handler("flaky_task")
def handle_flaky_task(args: Dict[str, Any]) -> str:
    """Simulates a task that fails transiently twice before succeeding."""
    attempt = args.get("attempts_count", 0)
    args["attempts_count"] = attempt + 1
    if args["attempts_count"] < 3:
        raise ConnectionResetError(f"Transient network glitch on attempt {args['attempts_count']}")
    return f"Succeeded on attempt {args['attempts_count']} after retries!"


@register_handler("poison_pill")
def handle_poison_pill(args: Dict[str, Any]):
    """Simulates an unrecoverable failure that routes to DLQ."""
    raise ValueError("Fatal corrupt payload: schema validation failed!")


class WorkerThread(threading.Thread):
    """Single worker thread executing jobs from the Ready Queue."""

    def __init__(self, worker_id: str, broker: TaskBroker, storage: TaskStorage):
        super().__init__(name=worker_id, daemon=True)
        self.worker_id = worker_id
        self.broker = broker
        self.storage = storage
        self._running = True

    def run(self):
        while self._running:
            task = self.broker.dequeue(timeout=0.25)
            if not task:
                continue

            self._process_task(task)

    def _process_task(self, task: Task):
        task.state = TaskState.RUNNING
        task.started_at = time.time()
        task.worker_id = self.worker_id
        self.storage.save_task(task)

        handler = HANDLERS.get(task.task_name)
        if not handler:
            self.broker.route_to_dlq(task, f"No handler registered for task '{task.task_name}'")
            return

        try:
            result = handler(task.args)
            task.state = TaskState.COMPLETED
            task.result = result
            task.completed_at = time.time()
            task.error = None
            self.storage.save_task(task)

        except Exception as ex:
            error_msg = f"{type(ex).__name__}: {str(ex)}"
            if task.retry_count < task.retry_policy.max_retries:
                # Exponential backoff retry
                delay = task.retry_policy.calculate_delay(task.retry_count)
                task.retry_count += 1
                task.state = TaskState.RETRYING
                task.error = error_msg
                task.execute_at = time.time() + delay
                # Re-enqueue: will be placed into the DelayedQueue
                self.broker.enqueue(task)
            else:
                # Max retries exhausted -> Move to DLQ
                self.broker.route_to_dlq(task, error_msg)

    def stop(self):
        self._running = False


class WorkerPool:
    """Manages pool of concurrent worker threads."""

    def __init__(self, concurrency: int, broker: TaskBroker, storage: TaskStorage):
        self.concurrency = concurrency
        self.broker = broker
        self.storage = storage
        self._workers: List[WorkerThread] = []

    def start(self):
        for i in range(self.concurrency):
            w = WorkerThread(f"worker-{i+1}", self.broker, self.storage)
            self._workers.append(w)
            w.start()

    def stop(self):
        for w in self._workers:
            w.stop()
        for w in self._workers:
            w.join(timeout=1.0)
        self._workers.clear()

    def get_status(self) -> Dict[str, Any]:
        return {
            "configured_concurrency": self.concurrency,
            "active_workers": [w.worker_id for w in self._workers if w.is_alive()],
        }
