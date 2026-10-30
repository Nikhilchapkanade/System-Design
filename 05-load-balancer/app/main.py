"""Layer 7 Reverse Proxy & Load Balancer Application.

Exposes proxy routing endpoints, dynamic algorithm switching, and a real-time
cluster observability dashboard.
"""

from contextlib import asynccontextmanager
from typing import Dict, Optional, List
from fastapi import FastAPI, Request, Response, HTTPException, status
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from app import config
from app.backend import Backend
from app.algorithms import (
    LoadBalancingStrategy,
    RoundRobinStrategy,
    WeightedRoundRobinStrategy,
    LeastConnectionsStrategy,
    ConsistentHashingStrategy,
)
from app.health_checker import HealthChecker
from app.proxy import ReverseProxy

# Initialize backend pool
backends: List[Backend] = [
    Backend(url=url, weight=weight) for url, weight in config.DEFAULT_BACKENDS
]

# Available routing strategies
strategies: Dict[str, LoadBalancingStrategy] = {
    "round-robin": RoundRobinStrategy(),
    "weighted-round-robin": WeightedRoundRobinStrategy(),
    "least-connections": LeastConnectionsStrategy(),
    "consistent-hashing": ConsistentHashingStrategy(),
}

active_strategy_name = "round-robin"
proxy = ReverseProxy(backends=backends, strategy=strategies[active_strategy_name])
health_checker = HealthChecker(backends=backends, interval_seconds=config.HEALTH_CHECK_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    health_checker.start()
    yield
    # Shutdown
    health_checker.stop()
    await proxy.close()


app = FastAPI(
    title="Layer 7 Reverse Proxy & Load Balancer",
    description="Software-defined load balancer implementing Round Robin, WRR, Least Connections, and Consistent Hashing.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
def lb_health_check():
    """Load balancer's own liveness probe."""
    healthy_count = sum(1 for b in backends if b.is_healthy)
    return {
        "status": "healthy" if healthy_count > 0 else "degraded",
        "healthy_backends": healthy_count,
        "total_backends": len(backends),
        "active_strategy": active_strategy_name,
    }


@app.get("/api/v1/cluster")
def get_cluster_status():
    """Telemetry endpoint reporting backend pool health and connection states."""
    return {
        "active_strategy": active_strategy_name,
        "available_strategies": list(strategies.keys()),
        "backends": [b.to_dict() for b in backends],
    }


@app.post("/api/v1/strategy/{strategy_name}")
def switch_strategy(strategy_name: str):
    """Switch active load balancing strategy dynamically at runtime."""
    global active_strategy_name
    strategy_name = strategy_name.lower()
    if strategy_name not in strategies:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid strategy '{strategy_name}'. Choose from: {list(strategies.keys())}",
        )
    active_strategy_name = strategy_name
    proxy.strategy = strategies[strategy_name]
    return {
        "status": "success",
        "active_strategy": active_strategy_name,
        "message": f"Switched strategy to {active_strategy_name}.",
    }


@app.get("/", response_class=HTMLResponse)
def index():
    """Cluster observability dashboard and request tester."""
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Layer 7 Load Balancer Dashboard</title>
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 1000px; margin: 30px auto; padding: 0 20px; color: #212529; }
            h1 { color: #0d6efd; }
            .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; margin: 20px 0; }
            .card { background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 8px; padding: 18px; }
            .badge-healthy { background: #d1e7dd; color: #0f5132; padding: 4px 8px; border-radius: 4px; font-weight: bold; }
            .badge-unhealthy { background: #f8d7da; color: #842029; padding: 4px 8px; border-radius: 4px; font-weight: bold; }
            button { padding: 10px 16px; background: #0d6efd; color: white; border: none; border-radius: 6px; font-weight: bold; cursor: pointer; }
            button:hover { background: #0b5ed7; }
            select { padding: 8px; border-radius: 6px; border: 1px solid #ccc; font-size: 14px; }
            pre { background: #212529; color: #f8f9fa; padding: 12px; border-radius: 6px; overflow-x: auto; }
            .ctrl-bar { display: flex; gap: 15px; align-items: center; margin: 20px 0; background: #e9ecef; padding: 15px; border-radius: 8px; }
        </style>
    </head>
    <body>
        <h1>⚖️ Layer 7 Reverse Proxy & Load Balancer</h1>
        <p>Real-time cluster topology, health checking, and routing strategy controller.</p>

        <div class="ctrl-bar">
            <label><b>Active Routing Algorithm:</b></label>
            <select id="strategySelect" onchange="changeStrategy()">
                <option value="round-robin">Round Robin</option>
                <option value="weighted-round-robin">Smooth Weighted Round Robin</option>
                <option value="least-connections">Least Connections</option>
                <option value="consistent-hashing">Consistent Hashing (Virtual Nodes)</option>
            </select>
            <button onclick="sendProxyRequest()">Send Test Request</button>
            <button style="background: #198754;" onclick="sendBatch(10)">Send Batch (10)</button>
        </div>

        <h3>Upstream Backend Server Pool</h3>
        <div class="grid" id="backendGrid">Loading cluster status...</div>

        <h3>Last Routed Response</h3>
        <pre id="responseLog">Click 'Send Test Request' to view which backend serves the traffic.</pre>

        <script>
            async function loadCluster() {
                const res = await fetch('/api/v1/cluster');
                const data = await res.json();
                document.getElementById('strategySelect').value = data.active_strategy;

                const grid = document.getElementById('backendGrid');
                grid.innerHTML = data.backends.map(b => `
                    <div class="card">
                        <h3>${b.url}</h3>
                        <p>Status: ${b.is_healthy ? '<span class="badge-healthy">HEALTHY</span>' : '<span class="badge-unhealthy">UNHEALTHY</span>'}</p>
                        <p>Configured Weight: <b>${b.weight}</b></p>
                        <p>Active In-Flight Connections: <b>${b.active_connections}</b></p>
                        <p>Total Requests Served: <b>${b.total_requests}</b></p>
                    </div>
                `).join('');
            }

            async function changeStrategy() {
                const sel = document.getElementById('strategySelect').value;
                await fetch(`/api/v1/strategy/${sel}`, { method: 'POST' });
                loadCluster();
            }

            async function sendProxyRequest() {
                const res = await fetch('/proxy/api/hello');
                const servedBy = res.headers.get('x-served-by') || 'None (503)';
                const strategy = res.headers.get('x-lb-strategy') || 'N/A';
                const body = await res.text();

                document.getElementById('responseLog').textContent = JSON.stringify({
                    status: res.status,
                    served_by_backend: servedBy,
                    algorithm_used: strategy,
                    response_body: body
                }, null, 2);
                loadCluster();
            }

            async function sendBatch(n) {
                for (let i = 0; i < n; i++) {
                    await sendProxyRequest();
                }
            }

            setInterval(loadCluster, 2000);
            loadCluster();
        </script>
    </body>
    </html>
    """


@app.api_route("/proxy/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy_gateway_route(request: Request, path: str):
    """Primary reverse proxy forwarding endpoint."""
    client_ip = request.client.host if request.client else "127.0.0.1"
    return await proxy.forward_request(request=request, path=path, client_key=client_ip)
