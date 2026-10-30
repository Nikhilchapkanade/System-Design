"""Distributed URL Shortener API Service.

System Design Core Architecture:
- Key Generation Service: Distributed range partitioning + Base62
- Cache-Aside Layer: In-memory LRU with TTL and telemetry
- Persistence Layer: SQLite in WAL mode with B-Tree indexes
- Non-blocking Telemetry: Asynchronous background tasks for click tracking
- Redirect Strategy: HTTP 307 Temporary Redirect to preserve click analytics
"""

import time
import re
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks, status
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse
from pydantic import BaseModel, Field, HttpUrl

from app import config
from app.key_generator import RangeKeyGenerator, base62_encode
from app.cache import LRUCacheWithTTL
from app.storage import URLStorage
from app.rate_limiter import SlidingWindowRateLimiter

# Initialize system design components
storage = URLStorage(config.DATABASE_PATH)
cache = LRUCacheWithTTL(
    capacity=config.CACHE_MAX_ENTRIES,
    default_ttl_seconds=config.CACHE_DEFAULT_TTL_SECONDS,
)
key_generator = RangeKeyGenerator(
    start_id=config.KGS_RANGE_START,
    block_size=config.KGS_RANGE_SIZE,
)
rate_limiter = SlidingWindowRateLimiter(
    max_requests=config.RATE_LIMIT_REQUESTS,
    window_seconds=config.RATE_LIMIT_WINDOW_SECONDS,
)

ALIAS_REGEX = re.compile(r"^[a-zA-Z0-9_-]{3,30}$")


class ShortenRequest(BaseModel):
    url: str = Field(..., description="Target long URL to shorten")
    custom_alias: Optional[str] = Field(None, description="Optional custom vanity alias (3-30 chars)")
    ttl_seconds: Optional[int] = Field(None, ge=60, description="Optional time-to-live in seconds (min 60s)")


class ShortenResponse(BaseModel):
    short_code: str
    short_url: str
    original_url: str
    expires_at: Optional[float] = None
    created_via: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: ensure tables exist
    storage.init_db()
    yield
    # Shutdown hook if needed


app = FastAPI(
    title="Distributed URL Shortener",
    description="High-throughput URL shortener implementing distributed KGS, Cache-Aside, and WAL indexing.",
    version="1.0.0",
    lifespan=lifespan,
)


def _get_client_ip(request: Request) -> str:
    """Extract client IP handling reverse proxy headers."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "127.0.0.1"


@app.get("/", response_class=HTMLResponse)
def index():
    """Simple interactive test dashboard for manual testing in browser."""
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Distributed URL Shortener</title>
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 750px; margin: 40px auto; padding: 0 20px; color: #222; }
            h1 { color: #1a73e8; }
            input, button { padding: 10px 14px; font-size: 15px; margin: 5px 0; border: 1px solid #ccc; border-radius: 6px; }
            input[type="text"] { width: 100%; box-sizing: border-box; }
            button { background: #1a73e8; color: white; border: none; cursor: pointer; font-weight: bold; }
            button:hover { background: #1557b0; }
            .card { background: #f8f9fa; border: 1px solid #e9ecef; border-radius: 8px; padding: 18px; margin-top: 20px; }
            pre { background: #282c34; color: #abb2bf; padding: 12px; border-radius: 6px; overflow-x: auto; }
            .badge { display: inline-block; padding: 3px 8px; background: #e8f0fe; color: #1a73e8; border-radius: 4px; font-size: 12px; font-weight: bold; }
        </style>
    </head>
    <body>
        <h1>🔗 Distributed URL Shortener</h1>
        <p>A production-grade system design project demonstrating <b>Distributed KGS</b>, <b>Cache-Aside (LRU)</b>, and <b>WAL persistence</b>.</p>
        
        <div class="card">
            <h3>Shorten a URL</h3>
            <input type="text" id="targetUrl" placeholder="https://example.com/very/long/url/path..." value="https://news.ycombinator.com" />
            <input type="text" id="customAlias" placeholder="Optional Custom Alias (e.g., 'my-alias')" />
            <button onclick="shortenUrl()">Create Short Link</button>
            <div id="shortenResult" style="margin-top: 15px;"></div>
        </div>

        <div class="card">
            <h3>System Telemetry & Metrics</h3>
            <button onclick="loadMetrics()">Refresh Metrics</button>
            <pre id="metricsPre">Click 'Refresh Metrics' to view live cache hit ratios and total traffic.</pre>
        </div>

        <script>
            async function shortenUrl() {
                const url = document.getElementById('targetUrl').value.trim();
                const alias = document.getElementById('customAlias').value.trim();
                const resDiv = document.getElementById('shortenResult');
                if (!url) return alert('Enter a URL');
                
                try {
                    const res = await fetch('/api/v1/shorten', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ url: url, custom_alias: alias || null })
                    });
                    const data = await res.json();
                    if (res.ok) {
                        resDiv.innerHTML = `<p><b>Short URL:</b> <a href="${data.short_url}" target="_blank">${data.short_url}</a></p>
                                            <p><span class="badge">Code: ${data.short_code}</span> <span class="badge">Type: ${data.created_via}</span></p>
                                            <p><a href="/api/v1/analytics/${data.short_code}" target="_blank">View Analytics JSON</a></p>`;
                        loadMetrics();
                    } else {
                        resDiv.innerHTML = `<p style="color: red;">Error: ${JSON.stringify(data.detail)}</p>`;
                    }
                } catch (e) {
                    resDiv.innerHTML = `<p style="color: red;">Network error: ${e}</p>`;
                }
            }

            async function loadMetrics() {
                const res = await fetch('/api/v1/metrics');
                const data = await res.json();
                document.getElementById('metricsPre').textContent = JSON.stringify(data, null, 2);
            }
            loadMetrics();
        </script>
    </body>
    </html>
    """


@app.post("/api/v1/shorten", response_model=ShortenResponse, status_code=status.HTTP_201_CREATED)
def shorten_url(payload: ShortenRequest, request: Request):
    """Create a shortened URL using distributed KGS or custom alias.
    
    Protected by Sliding Window Rate Limiting.
    """
    client_ip = _get_client_ip(request)
    allowed, retry_after = rate_limiter.is_allowed(client_ip)
    if not allowed:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded. Please wait {retry_after} seconds.",
            headers={"Retry-After": str(retry_after)},
        )

    # Basic URL sanitation
    url = payload.url.strip()
    if not (url.startswith("http://") or url.startswith("https://")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="URL must start with http:// or https://",
        )

    # Handle custom vanity alias vs KGS generation
    if payload.custom_alias:
        alias = payload.custom_alias.strip()
        if not ALIAS_REGEX.match(alias):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Custom alias must be 3-30 alphanumeric characters, hyphens or underscores.",
            )
        short_code = alias
        created_via = "custom_alias"
    else:
        short_code = key_generator.generate_short_code()
        created_via = "distributed_kgs"

    # Persist in storage layer
    inserted = storage.insert_url(
        short_code=short_code,
        original_url=url,
        ttl_seconds=payload.ttl_seconds,
    )

    if not inserted:
        if payload.custom_alias:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Custom alias '{short_code}' is already taken. Please choose another.",
            )
        # In rare case KGS range collided with an existing alias, retry with next ID
        short_code = key_generator.generate_short_code()
        storage.insert_url(short_code=short_code, original_url=url, ttl_seconds=payload.ttl_seconds)

    # Cache-Aside pattern: Warm cache on write to eliminate cold-start miss
    ttl = payload.ttl_seconds or config.CACHE_DEFAULT_TTL_SECONDS
    cache.set(short_code, url, ttl_seconds=ttl)

    # Calculate expiration timestamp for response
    expires_at = (time.time() + payload.ttl_seconds) if payload.ttl_seconds else None

    return ShortenResponse(
        short_code=short_code,
        short_url=f"{config.BASE_URL}/{short_code}",
        original_url=url,
        expires_at=expires_at,
        created_via=created_via,
    )


@app.get("/health")
def health_check():
    """Kubernetes / Load Balancer Liveness & Readiness probe."""
    return {"status": "ok", "service": "url-shortener"}


@app.get("/api/v1/analytics/{short_code}")
def get_analytics(short_code: str):
    """Retrieve click counts, creation timestamps, and recent access logs."""
    data = storage.get_analytics(short_code)
    if not data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No analytics available for '{short_code}'.",
        )
    return data


@app.get("/api/v1/metrics")
def get_system_metrics():
    """System Design observability: Cache hit ratios, storage stats, and capacity."""
    cache_metrics = cache.get_metrics()
    db_stats = storage.get_stats()
    return {
        "status": "healthy",
        "cache": cache_metrics,
        "database": db_stats,
        "kgs": {
            "current_worker_range_start": config.KGS_RANGE_START,
            "block_size": config.KGS_RANGE_SIZE,
        },
    }


@app.get("/{short_code}")
def redirect_to_url(
    short_code: str,
    request: Request,
    background_tasks: BackgroundTasks,
):
    """HTTP 307 Temporary Redirect with Cache-Aside query.
    
    1. Check in-memory LRU Cache (p99 < 1ms).
    2. On miss, query indexed SQLite database and populate cache.
    3. Asynchronously record click analytics without blocking the client.
    """
    client_ip = _get_client_ip(request)
    user_agent = request.headers.get("user-agent", "")
    referer = request.headers.get("referer", "")

    # Step 1: Check Cache (Fast path)
    cached_url = cache.get(short_code)
    if cached_url:
        background_tasks.add_task(
            storage.record_click,
            short_code=short_code,
            ip_address=client_ip,
            user_agent=user_agent,
            referer=referer,
        )
        return RedirectResponse(url=cached_url, status_code=status.HTTP_307_TEMPORARY_REDIRECT)

    # Step 2: Cache Miss -> Read Database
    record = storage.get_url(short_code)
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Short code '{short_code}' not found or has expired.",
        )

    # Step 3: Populate Cache for subsequent reads
    remaining_ttl = None
    if record["expires_at"]:
        remaining_ttl = max(1, int(record["expires_at"] - time.time()))

    cache.set(short_code, record["original_url"], ttl_seconds=remaining_ttl)

    # Step 4: Record analytics asynchronously
    background_tasks.add_task(
        storage.record_click,
        short_code=short_code,
        ip_address=client_ip,
        user_agent=user_agent,
        referer=referer,
    )

    return RedirectResponse(url=record["original_url"], status_code=status.HTTP_307_TEMPORARY_REDIRECT)

