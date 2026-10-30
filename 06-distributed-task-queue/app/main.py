"""Distributed Task Queue and Job Scheduler API Service.

Exposes REST endpoints for task submission, lifecycle monitoring, DLQ management,
and an interactive operations dashboard.
"""

import time
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional, List
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from app import config
from app.models import Task, TaskState, RetryPolicy
from app.storage import TaskStorage
from app.queue import TaskBroker
from app.worker import WorkerPool, HANDLERS
from app.scheduler import TaskScheduler

# Initialize core queue infrastructure
storage = TaskStorage(config.DATABASE_PATH)
broker = TaskBroker(storage)
worker_pool = WorkerPool(config.DEFAULT_WORKER_CONCURRENCY, broker, storage)
scheduler = TaskScheduler(broker, config.SCHEDULER_INTERVAL_SECONDS)


class EnqueueTaskPayload(BaseModel):
    task_name: str = Field(..., description="Registered task name (e.g. send_email, resize_image, flaky_task, poison_pill)")
    args: Dict[str, Any] = Field(default_factory=dict, description="Input parameters passed to the handler")
    delay_seconds: float = Field(0.0, ge=0.0, description="Optional delay in seconds before task becomes eligible for execution")
    max_retries: int = Field(3, ge=0, le=10, description="Maximum retry attempts on failure")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    worker_pool.start()
    scheduler.start()
    yield
    # Shutdown
    worker_pool.stop()
    scheduler.stop()


app = FastAPI(
    title="Asynchronous Distributed Task Queue",
    description="Distributed background worker engine with exponential backoff retries, delayed scheduling, and Dead Letter Queue.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
def health_check():
    return {"status": "ok", "service": "distributed-task-queue"}


@app.post("/api/v1/tasks/enqueue", status_code=status.HTTP_202_ACCEPTED)
def enqueue_task(payload: EnqueueTaskPayload):
    """Enqueue a new task for immediate or delayed asynchronous execution."""
    now = time.time()
    execute_at = now + payload.delay_seconds

    policy = RetryPolicy(
        max_retries=payload.max_retries,
        base_delay=config.BACKOFF_BASE_SECONDS,
        max_delay=config.BACKOFF_MAX_SECONDS,
    )

    task = Task(
        task_name=payload.task_name,
        args=payload.args,
        retry_policy=policy,
        execute_at=execute_at,
        created_at=now,
    )

    broker.enqueue(task)

    return {
        "status": "accepted",
        "task_id": task.task_id,
        "task_name": task.task_name,
        "initial_state": task.state.value,
        "execute_at": execute_at,
        "is_delayed": payload.delay_seconds > 0,
    }


@app.get("/api/v1/tasks/{task_id}")
def get_task_status(task_id: str):
    """Query current execution state, retry count, and output result for a task."""
    task = storage.get_task(task_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task '{task_id}' not found.",
        )
    return task


@app.get("/api/v1/tasks")
def list_tasks(
    state: Optional[str] = Query(None, description="Filter by state (PENDING, RUNNING, COMPLETED, RETRYING, DEAD_LETTER)"),
    limit: int = Query(50, ge=1, le=100),
):
    """List recent tasks with optional state filter."""
    return storage.list_tasks(state=state, limit=limit)


@app.get("/api/v1/dlq")
def list_dlq_tasks():
    """Inspect tasks currently quarantined in the Dead Letter Queue."""
    return storage.list_tasks(state=TaskState.DEAD_LETTER.value, limit=50)


@app.post("/api/v1/dlq/{task_id}/retry")
def retry_dlq_task(task_id: str):
    """Replay a dead-letter task back into the active queue."""
    success = broker.replay_dlq_task(task_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dead letter task '{task_id}' not found or not in DEAD_LETTER state.",
        )
    return {"status": "requeued", "task_id": task_id, "message": "Task re-enqueued into ReadyQueue."}


@app.get("/api/v1/stats")
def get_system_stats():
    """Observability: Queue depths, worker states, and task lifecycle metrics."""
    return {
        "queues": broker.get_queue_sizes(),
        "workers": worker_pool.get_status(),
        "tasks": storage.get_stats(),
        "available_handlers": list(HANDLERS.keys()),
    }


@app.get("/", response_class=HTMLResponse)
def index():
    """Interactive task operations dashboard."""
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Distributed Task Queue Dashboard</title>
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 1100px; margin: 30px auto; padding: 0 20px; color: #212529; background: #f8f9fa; }
            h1 { color: #0d6efd; margin-bottom: 5px; }
            .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 14px; margin: 20px 0; }
            .stat-card { background: white; border: 1px solid #dee2e6; border-radius: 8px; padding: 14px; box-shadow: 0 2px 4px rgba(0,0,0,0.04); }
            .stat-num { font-size: 26px; font-weight: bold; margin-top: 5px; }
            .form-card { background: white; border: 1px solid #dee2e6; border-radius: 8px; padding: 18px; margin-bottom: 25px; }
            .form-row { display: flex; gap: 12px; margin-top: 10px; }
            select, input { padding: 9px 12px; border: 1px solid #ced4da; border-radius: 6px; font-size: 14px; }
            button { padding: 9px 18px; background: #0d6efd; color: white; border: none; border-radius: 6px; font-weight: bold; cursor: pointer; }
            button:hover { background: #0b5ed7; }
            table { width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.04); }
            th, td { padding: 10px 14px; text-align: left; border-bottom: 1px solid #eee; font-size: 13px; }
            th { background: #f1f3f5; }
            .badge { padding: 3px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; }
            .badge-COMPLETED { background: #d1e7dd; color: #0f5132; }
            .badge-RUNNING { background: #cff4fc; color: #055160; }
            .badge-PENDING { background: #fff3cd; color: #664d03; }
            .badge-SCHEDULED { background: #e2d9f3; color: #432874; }
            .badge-RETRYING { background: #ffe5d0; color: #7a3e00; }
            .badge-DEAD_LETTER { background: #f8d7da; color: #842029; }
        </style>
    </head>
    <body>
        <h1>⚙️ Distributed Task Queue & Scheduler</h1>
        <p style="color: #6c757d; margin-top: 0;">Producer-Consumer workers, Exponential Backoff, Delayed Scheduling, & Dead Letter Queue (DLQ).</p>

        <div class="grid" id="statsGrid">Loading metrics...</div>

        <div class="form-card">
            <h3>Enqueue New Background Task</h3>
            <div class="form-row">
                <select id="taskSelect">
                    <option value="send_email">send_email (Fast success)</option>
                    <option value="resize_image">resize_image (Data processing)</option>
                    <option value="flaky_task">flaky_task (Fails twice, then succeeds via backoff)</option>
                    <option value="poison_pill">poison_pill (Fails max retries -> moves to DLQ)</option>
                </select>
                <input type="number" id="delayInput" placeholder="Delay (seconds)" value="0" style="width: 130px;" />
                <button onclick="submitTask()">Enqueue Job</button>
            </div>
        </div>

        <h3>Recent Tasks Audit Trail</h3>
        <table>
            <thead>
                <tr>
                    <th>Task ID</th>
                    <th>Name</th>
                    <th>State</th>
                    <th>Retries</th>
                    <th>Worker</th>
                    <th>Result / Error</th>
                </tr>
            </thead>
            <tbody id="taskTableBody">
                <tr><td colspan="6">Loading recent tasks...</td></tr>
            </tbody>
        </table>

        <script>
            async function loadData() {
                const [statsRes, tasksRes] = await Promise.all([
                    fetch('/api/v1/stats'),
                    fetch('/api/v1/tasks?limit=15')
                ]);
                const stats = await statsRes.json();
                const tasks = await tasksRes.json();

                // Render metrics
                document.getElementById('statsGrid').innerHTML = `
                    <div class="stat-card"><div>Ready Queue</div><div class="stat-num" style="color:#0d6efd;">${stats.queues.ready_count}</div></div>
                    <div class="stat-card"><div>Delayed Queue</div><div class="stat-num" style="color:#6f42c1;">${stats.queues.delayed_count}</div></div>
                    <div class="stat-card"><div>Dead Letter Queue</div><div class="stat-num" style="color:#dc3545;">${stats.queues.dlq_count}</div></div>
                    <div class="stat-card"><div>Completed Tasks</div><div class="stat-num" style="color:#198754;">${stats.tasks.completed || 0}</div></div>
                    <div class="stat-card"><div>Active Workers</div><div class="stat-num">${stats.workers.active_workers.length}</div></div>
                `;

                // Render tasks table
                document.getElementById('taskTableBody').innerHTML = tasks.map(t => `
                    <tr>
                        <td style="font-family: monospace; font-size: 11px;">${t.task_id.slice(0, 8)}...</td>
                        <td><b>${t.task_name}</b></td>
                        <td><span class="badge badge-${t.state}">${t.state}</span></td>
                        <td>${t.retry_count} / ${t.max_retries}</td>
                        <td>${t.worker_id || '-'}</td>
                        <td style="max-width: 250px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
                            ${t.error ? '<span style="color:#dc3545;">' + t.error + '</span>' : (t.result ? JSON.stringify(t.result) : '-')}
                            ${t.state === 'DEAD_LETTER' ? ` <button style="padding:2px 6px; font-size:10px; background:#dc3545;" onclick="replayDlq('${t.task_id}')">Replay</button>` : ''}
                        </td>
                    </tr>
                `).join('');
            }

            async function submitTask() {
                const taskName = document.getElementById('taskSelect').value;
                const delay = parseFloat(document.getElementById('delayInput').value) || 0.0;
                await fetch('/api/v1/tasks/enqueue', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ task_name: taskName, delay_seconds: delay })
                });
                loadData();
            }

            async function replayDlq(id) {
                await fetch(`/api/v1/dlq/${id}/retry`, { method: 'POST' });
                loadData();
            }

            setInterval(loadData, 1000);
            loadData();
        </script>
    </body>
    </html>
    """
