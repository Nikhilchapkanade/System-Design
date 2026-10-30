"""Active Health Checking Engine.

System Design Analysis:
Active health checks probe upstream backends periodically to detect hardware,
network, or application crashes BEFORE real user traffic hits them.
Consecutive success/failure thresholds prevent flapping (rapid state oscillations).
"""

import asyncio
from typing import List
import httpx
from app.backend import Backend


class HealthChecker:
    """Background asynchronous health monitor for the backend pool."""

    def __init__(
        self,
        backends: List[Backend],
        interval_seconds: float = 2.0,
        timeout_seconds: float = 1.0,
        unhealthy_threshold: int = 2,
        healthy_threshold: int = 2,
    ):
        self.backends = backends
        self.interval = interval_seconds
        self.timeout = timeout_seconds
        self.unhealthy_threshold = unhealthy_threshold
        self.healthy_threshold = healthy_threshold
        self._task: asyncio.Task = None
        self._running = False

    async def probe_backend(self, client: httpx.AsyncClient, backend: Backend) -> None:
        """Ping single backend /health endpoint."""
        url = f"{backend.url}/health"
        try:
            res = await client.get(url)
            if res.status_code == 200:
                recovered = backend.record_success(self.healthy_threshold)
                if recovered:
                    print(f"[*] HEALTH CHECK: Backend '{backend.url}' recovered -> HEALTHY")
            else:
                tripped = backend.record_failure(self.unhealthy_threshold)
                if tripped:
                    print(f"[!] HEALTH CHECK: Backend '{backend.url}' probe failed (HTTP {res.status_code}) -> UNHEALTHY")
        except Exception:
            tripped = backend.record_failure(self.unhealthy_threshold)
            if tripped:
                print(f"[!] HEALTH CHECK: Backend '{backend.url}' unreachable -> UNHEALTHY")

    async def _run_loop(self):
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            while self._running:
                tasks = [self.probe_backend(client, b) for b in self.backends]
                await asyncio.gather(*tasks, return_exceptions=True)
                await asyncio.sleep(self.interval)

    def start(self):
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._run_loop())

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
