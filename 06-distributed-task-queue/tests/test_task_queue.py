"""Unit and Integration Tests for Distributed Task Queue & Scheduler."""

import time
import os
import shutil
import tempfile
import unittest

from fastapi.testclient import TestClient

from app.models import Task, TaskState, RetryPolicy
from app.storage import TaskStorage
from app.queue import TaskBroker
from app.worker import WorkerPool
from app.scheduler import TaskScheduler
from app.main import app, broker, storage


class TestTaskQueueCore(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "test_tasks.db")
        self.store = TaskStorage(self.db_path)
        self.broker = TaskBroker(self.store)
        self.worker_pool = WorkerPool(concurrency=2, broker=self.broker, storage=self.store)
        self.scheduler = TaskScheduler(broker=self.broker, interval_seconds=0.05)

        self.worker_pool.start()
        self.scheduler.start()

    def tearDown(self):
        self.worker_pool.stop()
        self.scheduler.stop()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_immediate_task_execution(self):
        task = Task(task_name="send_email", args={"to": "test@example.com", "subject": "Welcome"})
        self.broker.enqueue(task)

        # Wait for completion
        for _ in range(20):
            t = self.store.get_task(task.task_id)
            if t and t["state"] == TaskState.COMPLETED.value:
                break
            time.sleep(0.05)

        record = self.store.get_task(task.task_id)
        self.assertEqual(record["state"], TaskState.COMPLETED.value)
        self.assertIn("successfully delivered", record["result"])

    def test_delayed_task_scheduling(self):
        # 0.3 second delay
        task = Task(
            task_name="send_email",
            args={"to": "delayed@example.com"},
            execute_at=time.time() + 0.3,
        )
        self.broker.enqueue(task)

        # Immediately should be SCHEDULED
        t0 = self.store.get_task(task.task_id)
        self.assertEqual(t0["state"], TaskState.SCHEDULED.value)

        # Wait 0.5s for scheduler to promote and worker to finish
        time.sleep(0.55)
        t1 = self.store.get_task(task.task_id)
        self.assertEqual(t1["state"], TaskState.COMPLETED.value)

    def test_exponential_backoff_and_retry(self):
        # Flaky task succeeds on 3rd attempt
        policy = RetryPolicy(max_retries=3, base_delay=0.1, backoff_factor=1.5, enable_jitter=False)
        task = Task(task_name="flaky_task", args={}, retry_policy=policy)
        self.broker.enqueue(task)

        # Wait for retries
        for _ in range(40):
            t = self.store.get_task(task.task_id)
            if t and t["state"] == TaskState.COMPLETED.value:
                break
            time.sleep(0.05)

        final_task = self.store.get_task(task.task_id)
        self.assertEqual(final_task["state"], TaskState.COMPLETED.value)
        self.assertEqual(final_task["retry_count"], 2)

    def test_dead_letter_queue_routing(self):
        # Poison pill fails and exhausts retries immediately
        policy = RetryPolicy(max_retries=1, base_delay=0.05, enable_jitter=False)
        task = Task(task_name="poison_pill", args={}, retry_policy=policy)
        self.broker.enqueue(task)

        for _ in range(30):
            t = self.store.get_task(task.task_id)
            if t and t["state"] == TaskState.DEAD_LETTER.value:
                break
            time.sleep(0.05)

        t = self.store.get_task(task.task_id)
        self.assertEqual(t["state"], TaskState.DEAD_LETTER.value)
        self.assertIn("ValueError", t["error_text"])

        # Test Replay from DLQ
        replayed = self.broker.replay_dlq_task(task.task_id)
        self.assertTrue(replayed)


class TestAPIEndpoints(unittest.TestCase):
    def setUp(self):
        self._ctx = TestClient(app)
        self.client = self._ctx.__enter__()

    def tearDown(self):
        self._ctx.__exit__(None, None, None)

    def test_health(self):
        res = self.client.get("/health")
        self.assertEqual(res.status_code, 200)

    def test_enqueue_and_poll(self):
        res = self.client.post("/api/v1/tasks/enqueue", json={
            "task_name": "resize_image",
            "args": {"width": 1024, "height": 768},
            "delay_seconds": 0.0,
            "max_retries": 2
        })
        self.assertEqual(res.status_code, 202)
        task_id = res.json()["task_id"]

        # Poll status
        for _ in range(20):
            status_res = self.client.get(f"/api/v1/tasks/{task_id}")
            if status_res.status_code == 200 and status_res.json()["state"] == "COMPLETED":
                break
            time.sleep(0.05)

        status_res = self.client.get(f"/api/v1/tasks/{task_id}")
        self.assertEqual(status_res.status_code, 200)
        self.assertIn(status_res.json()["state"], ["PENDING", "RUNNING", "COMPLETED"])


if __name__ == "__main__":
    unittest.main()
