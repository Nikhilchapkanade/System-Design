"""API Gateway Rate Limiting Middleware.

System Design Analysis:
RFC 6585 (Additional HTTP Status Codes) specifies HTTP 429 Too Many Requests.
Standard rate limiting headers communicate quota status to clients, allowing
well-behaved SDKs to implement automatic backoff before hitting limits:
- X-RateLimit-Limit: Quota permitted in window.
- X-RateLimit-Remaining: Requests left.
- X-RateLimit-Reset: Seconds until replenishment.
- Retry-After: Seconds to pause before next attempt.
"""

from typing import Callable
from fastapi import Request, Response, HTTPException, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from app.algorithms.base import RateLimiter


def get_client_identifier(request: Request) -> str:
    """Extract client key based on API Key or IP address."""
    api_key = request.headers.get("x-api-key")
    if api_key:
        return f"apikey:{api_key}"

    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return f"ip:{forwarded.split(',')[0].strip()}"

    client_host = request.client.host if request.client else "127.0.0.1"
    return f"ip:{client_host}"


class RateLimitMiddleware(BaseHTTPMiddleware):
    """FastAPI Middleware to enforce rate limits across endpoints."""

    def __init__(self, app, limiter_provider: Callable[[Request], RateLimiter]):
        super().__init__(app)
        self.limiter_provider = limiter_provider

    async def dispatch(self, request: Request, call_next):
        # Exclude documentation and root routes from rate limiting
        if request.url.path in ("/", "/docs", "/openapi.json", "/health"):
            return await call_next(request)

        limiter = self.limiter_provider(request)
        if limiter is None:
            return await call_next(request)

        client_key = get_client_identifier(request)
        route_key = f"{client_key}:{request.url.path}"

        result = limiter.acquire(route_key, cost=1)
        headers = result.to_headers()

        if not result.allowed:
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={
                    "error": "Too Many Requests",
                    "detail": f"Rate limit exceeded. Try again in {headers.get('Retry-After', '1')} seconds.",
                    "client": client_key,
                },
                headers=headers,
            )

        response = await call_next(request)
        # Inject standard rate limiting headers into the downstream response
        for k, v in headers.items():
            response.headers[k] = v
        return response
