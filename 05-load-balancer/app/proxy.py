"""Reverse Proxy Forwarding Engine with Automatic Retry and Failover.

System Design Analysis:
Layer 7 Reverse Proxies inspect the HTTP application layer:
1. Header Decoration: Appends standard RFC forwarding headers (X-Forwarded-For,
   X-Forwarded-Proto, X-Forwarded-Host).
2. Concurrency Tracking: Increments in-flight connection counters before I/O and
   decrements in a finally block.
3. Passive Failover: Catches upstream connection drops and automatically retries on
   the next healthy backend before returning an error to the user.
"""

from typing import List, Optional
import httpx
from fastapi import Request, Response
from fastapi.responses import JSONResponse

from app.backend import Backend
from app.algorithms.base import LoadBalancingStrategy


class ReverseProxy:
    """HTTP Reverse Proxy dispatching to backend pools."""

    def __init__(self, backends: List[Backend], strategy: LoadBalancingStrategy, max_retries: int = 3):
        self.backends = backends
        self.strategy = strategy
        self.max_retries = max_retries
        self._client: Optional[httpx.AsyncClient] = None

    async def get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=10.0)
        return self._client

    async def forward_request(self, request: Request, path: str, client_key: Optional[str] = None) -> Response:
        """Forward an incoming HTTP request to an optimal backend."""
        client = await self.get_client()
        tried_backends = set()

        body = await request.body()
        query_string = request.url.query

        for attempt in range(self.max_retries):
            # Select backend candidate
            available = [b for b in self.backends if b.url not in tried_backends and b.is_healthy]
            if not available:
                break

            target_backend = self.strategy.select_backend(available, client_key=client_key)
            if target_backend is None:
                break

            tried_backends.add(target_backend.url)
            target_url = f"{target_backend.url}/{path}"
            if query_string:
                target_url = f"{target_url}?{query_string}"

            # Prepare proxy headers
            headers = dict(request.headers)
            # Remove host header to allow target host resolution
            headers.pop("host", None)
            headers.pop("content-length", None)

            # Standard Forwarded headers
            client_ip = request.client.host if request.client else "127.0.0.1"
            prev_x_forwarded = headers.get("x-forwarded-for")
            headers["x-forwarded-for"] = f"{prev_x_forwarded}, {client_ip}" if prev_x_forwarded else client_ip
            headers["x-forwarded-proto"] = request.url.scheme
            headers["x-forwarded-host"] = request.headers.get("host", "localhost")
            headers["x-lb-backend"] = target_backend.url

            target_backend.acquire()
            try:
                upstream_res = await client.request(
                    method=request.method,
                    url=target_url,
                    headers=headers,
                    content=body,
                )
                target_backend.record_success()
                
                # Build downstream response
                downstream_headers = dict(upstream_res.headers)
                downstream_headers["x-served-by"] = target_backend.url
                downstream_headers["x-lb-strategy"] = self.strategy.name

                return Response(
                    content=upstream_res.content,
                    status_code=upstream_res.status_code,
                    headers=downstream_headers,
                )

            except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError):
                # Upstream server failed -> register failure strike and retry immediately
                target_backend.record_failure(unhealthy_threshold=1)
                continue
            finally:
                target_backend.release()

        # All retries exhausted
        return JSONResponse(
            status_code=503,
            content={
                "error": "Service Unavailable",
                "detail": "No healthy upstream backends could serve the request.",
                "strategy": self.strategy.name,
                "backends_checked": len(tried_backends),
            },
        )

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()
