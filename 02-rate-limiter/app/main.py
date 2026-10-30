"""API Gateway Rate Limiter Simulator Application.

Exposes endpoints governed by different rate limiting strategies, allowing
real-time side-by-side comparison of algorithm behavior and header responses.
"""

from typing import Dict, Any
from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse

from app.algorithms import (
    TokenBucketRateLimiter,
    LeakyBucketRateLimiter,
    FixedWindowRateLimiter,
    SlidingWindowLogRateLimiter,
    SlidingWindowCounterRateLimiter,
    RateLimiter,
)
from app.middleware import RateLimitMiddleware, get_client_identifier

# Initialize individual limiters for each strategy
limiters: Dict[str, RateLimiter] = {
    "token-bucket": TokenBucketRateLimiter(capacity=10, refill_rate=2.0),
    "leaky-bucket": LeakyBucketRateLimiter(capacity=10, leak_rate=2.0),
    "fixed-window": FixedWindowRateLimiter(limit=10, window_seconds=10),
    "sliding-window-log": SlidingWindowLogRateLimiter(limit=10, window_seconds=10),
    "sliding-window-counter": SlidingWindowCounterRateLimiter(limit=10, window_seconds=10),
}


def route_limiter_resolver(request: Request) -> RateLimiter:
    """Resolve which rate limiter applies based on URL path."""
    path = request.url.path
    for key, limiter in limiters.items():
        if f"/api/v1/{key}/" in path:
            return limiter
    return None


app = FastAPI(
    title="Distributed Rate Limiter Service",
    description="Interactive API gateway showcasing Token Bucket, Leaky Bucket, and Sliding Window variants.",
    version="1.0.0",
)

# Attach rate limit middleware
app.add_middleware(RateLimitMiddleware, limiter_provider=route_limiter_resolver)


@app.get("/health")
def health_check():
    return {"status": "ok", "service": "rate-limiter"}


@app.get("/api/v1/token-bucket/resource")
def get_token_bucket_resource(request: Request):
    return {"message": "Success", "algorithm": "Token Bucket", "desc": "Allows bursts up to capacity 10, refills at 2/s"}


@app.get("/api/v1/leaky-bucket/resource")
def get_leaky_bucket_resource(request: Request):
    return {"message": "Success", "algorithm": "Leaky Bucket", "desc": "Smooths traffic at steady 2/s leak rate"}


@app.get("/api/v1/fixed-window/resource")
def get_fixed_window_resource(request: Request):
    return {"message": "Success", "algorithm": "Fixed Window Counter", "desc": "Resets every 10s window (watch for border burst)"}


@app.get("/api/v1/sliding-window-log/resource")
def get_sliding_window_log_resource(request: Request):
    return {"message": "Success", "algorithm": "Sliding Window Log", "desc": "100% precise sliding log with O(N) memory"}


@app.get("/api/v1/sliding-window-counter/resource")
def get_sliding_window_counter_resource(request: Request):
    return {"message": "Success", "algorithm": "Sliding Window Counter", "desc": "Weighted boundary approximation with O(1) memory"}


@app.post("/api/v1/reset")
def reset_all_limiters():
    for limiter in limiters.values():
        limiter.reset()
    return {"status": "reset_complete", "message": "All rate limiter buckets reset to initial capacity."}


@app.get("/api/v1/status")
def get_all_status(request: Request):
    client_key = get_client_identifier(request)
    status_data = {}
    for name, limiter in limiters.items():
        route_key = f"{client_key}:/api/v1/{name}/resource"
        status_data[name] = limiter.get_state(route_key)
    return {"client": client_key, "algorithms": status_data}


@app.get("/", response_class=HTMLResponse)
def index():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Distributed Rate Limiter Gateway</title>
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 900px; margin: 30px auto; padding: 0 20px; color: #222; }
            h1 { color: #0d6efd; }
            .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 16px; margin-top: 20px; }
            .card { background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 8px; padding: 16px; }
            button { width: 100%; padding: 10px; font-weight: bold; background: #0d6efd; color: white; border: none; border-radius: 5px; cursor: pointer; margin-top: 10px; }
            button:hover { background: #0b5ed7; }
            .burst-btn { background: #dc3545; margin-top: 5px; }
            .burst-btn:hover { background: #bb2d3b; }
            .reset-btn { background: #6c757d; max-width: 200px; }
            .stat { font-size: 13px; color: #555; margin-top: 8px; min-height: 48px; }
            pre { background: #212529; color: #f8f9fa; padding: 10px; border-radius: 6px; font-size: 12px; overflow-x: auto; }
            .badge-200 { background: #d1e7dd; color: #0f5132; padding: 3px 6px; border-radius: 4px; font-weight: bold; }
            .badge-429 { background: #f8d7da; color: #842029; padding: 3px 6px; border-radius: 4px; font-weight: bold; }
        </style>
    </head>
    <body>
        <h1>🛡️ Distributed Rate Limiter Gateway</h1>
        <p>Interactive demonstration of the <b>5 fundamental rate limiting algorithms</b> in system design.</p>
        <button class="reset-btn" onclick="resetAll()">Reset All Buckets</button>

        <div class="grid">
            <div class="card">
                <h3>Token Bucket</h3>
                <p>Capacity: 10 | Refill: 2/sec</p>
                <button onclick="sendReq('token-bucket')">Send Single (1)</button>
                <button class="burst-btn" onclick="sendBurst('token-bucket', 8)">Send Burst (8)</button>
                <div class="stat" id="stat-token-bucket">Ready</div>
            </div>

            <div class="card">
                <h3>Leaky Bucket</h3>
                <p>Buffer: 10 | Leak: 2/sec</p>
                <button onclick="sendReq('leaky-bucket')">Send Single (1)</button>
                <button class="burst-btn" onclick="sendBurst('leaky-bucket', 8)">Send Burst (8)</button>
                <div class="stat" id="stat-leaky-bucket">Ready</div>
            </div>

            <div class="card">
                <h3>Fixed Window</h3>
                <p>Limit: 10 per 10s</p>
                <button onclick="sendReq('fixed-window')">Send Single (1)</button>
                <button class="burst-btn" onclick="sendBurst('fixed-window', 8)">Send Burst (8)</button>
                <div class="stat" id="stat-fixed-window">Ready</div>
            </div>

            <div class="card">
                <h3>Sliding Window Log</h3>
                <p>Limit: 10 per 10s (Exact)</p>
                <button onclick="sendReq('sliding-window-log')">Send Single (1)</button>
                <button class="burst-btn" onclick="sendBurst('sliding-window-log', 8)">Send Burst (8)</button>
                <div class="stat" id="stat-sliding-window-log">Ready</div>
            </div>

            <div class="card">
                <h3>Sliding Window Counter</h3>
                <p>Limit: 10 per 10s (Weighted)</p>
                <button onclick="sendReq('sliding-window-counter')">Send Single (1)</button>
                <button class="burst-btn" onclick="sendBurst('sliding-window-counter', 8)">Send Burst (8)</button>
                <div class="stat" id="stat-sliding-window-counter">Ready</div>
            </div>
        </div>

        <h3 style="margin-top: 30px;">Last Request Headers (RFC Inspection)</h3>
        <pre id="headerLog">Click any button above to inspect X-RateLimit headers and Retry-After values.</pre>

        <script>
            async function sendReq(algo) {
                const res = await fetch(`/api/v1/${algo}/resource`);
                const statDiv = document.getElementById(`stat-${algo}`);
                const badge = res.status === 200 ? '<span class="badge-200">200 OK</span>' : '<span class="badge-429">429 Throttled</span>';
                statDiv.innerHTML = `${badge} | Remaining: ${res.headers.get('x-ratelimit-remaining') || '0'} | Reset: ${res.headers.get('x-ratelimit-reset') || '0'}s`;
                
                const headerMap = {};
                for (let [k, v] of res.headers.entries()) {
                    if (k.toLowerCase().includes('ratelimit') || k.toLowerCase().includes('retry')) {
                        headerMap[k] = v;
                    }
                }
                document.getElementById('headerLog').textContent = JSON.stringify({
                    status: res.status,
                    algorithm: algo,
                    headers: headerMap
                }, null, 2);
            }

            async function sendBurst(algo, count) {
                for (let i = 0; i < count; i++) {
                    sendReq(algo);
                }
            }

            async function resetAll() {
                await fetch('/api/v1/reset', { method: 'POST' });
                alert('All rate limiter states cleared.');
                location.reload();
            }
        </script>
    </body>
    </html>
    """
