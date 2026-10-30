# 05 - Layer 7 Reverse Proxy & Load Balancer

A software-defined Layer 7 HTTP reverse proxy and load balancer built from first principles demonstrating the foundational routing mechanics behind **NGINX, Envoy, HAProxy, and AWS ALB**.

---

## 1. High-Level Architecture

```mermaid
graph TD
    Client([Clients / Web Browsers])
    LB[Layer 7 Reverse Proxy & Load Balancer<br/>Port 8000]

    subgraph Health Monitor
        HC[Active Health Probe Loop<br/>Probes /health every 2s]
    end

    subgraph Backend Server Pool
        B1[Backend Server 1<br/>Weight: 2 | Port 9001]
        B2[Backend Server 2<br/>Weight: 1 | Port 9002]
        B3[Backend Server 3<br/>Weight: 1 | Port 9003]
    end

    Client -->|HTTP Request| LB
    HC -.->|Background Ping| B1
    HC -.->|Background Ping| B2
    HC -.->|Background Ping| B3

    LB -->|1. Smooth WRR / Least Connections| B1
    LB -->|2. Consistent Hashing| B2
    LB -.->|3. Failover if unreachable| B3
```

---

## 2. Layer 4 vs. Layer 7 Load Balancing

| Dimension | Layer 4 (Transport Layer) | Layer 7 (Application Layer - Implemented) |
|---|---|---|
| **OSI Layer** | TCP / UDP (IP & Port only) | HTTP / HTTPS / WebSockets / gRPC |
| **Inspection** | Cannot read HTTP headers, paths, cookies | Can inspect URL path, headers, cookies, payloads |
| **TLS Termination** | Passes raw packets or terminates TCP | Decrypts TLS (SSL Termination) & modifies headers |
| **Routing Decisions** | IP hash, Port round robin | Path-based routing, header routing, Consistent Hashing |
| **Hardware Cost** | Ultra low CPU / high throughput | Higher CPU (requires parsing HTTP protocol) |

---

## 3. Supported Routing Algorithms

### 1. Round Robin
* Sequentially distributes requests across healthy backends in circular order.
* Best when all backend instances have identical CPU/RAM and requests have uniform complexity.

### 2. Smooth Weighted Round Robin (NGINX Algorithm)
* Distributes traffic according to backend capacity weights (e.g. weights `2:1:1` -> 50%, 25%, 25%).
* **Smooth Dispersion:** Unlike naive WRR which dumps consecutive bursts on the highest-weight node (`{A, A, B, C}`), Smooth WRR mathematically interleaves requests (`{A, B, A, C}`) to avoid micro-spikes.

### 3. Least Connections
* Dynamically routes traffic to the backend currently handling the fewest active in-flight requests.
* Ties are broken by backend weight.
* Ideal for workloads with high execution variance (e.g., long-polling, heavy reporting, file conversions).

### 4. Consistent Hashing (Virtual Nodes)
* Maps server nodes and client keys onto a $2^{32}-1$ cyclic hash ring.
* **Virtual Nodes:** Each physical node is replicated 50–100 times along the ring to eliminate cluster hot spots.
* **Cache Affinity:** Guarantees that requests from the same user/session consistently reach the same backend, maximizing local L1/L2 cache efficiency.
* **Minimal Disruption:** If a node dies, only $1/N$ of keys are remapped to its clockwise neighbor.

---

## 4. Fault Tolerance: Active Probes vs. Passive Retries

1. **Active Health Checking (`app/health_checker.py`):**
   * Background asyncio task pings `GET /health` on backends every 2 seconds.
   * **Flapping Protection:** Requires 2 consecutive failures to mark a node `UNHEALTHY`, and 2 consecutive successes to restore it to `HEALTHY`.
2. **Passive Failover & Retry (`app/proxy.py`):**
   * If an in-flight request fails with a connection drop or timeout, the proxy intercepts the exception, marks a failure strike on that backend, and **instantly retries the request on an alternate healthy backend**.
   * **Result:** Zero client-visible 502/503 errors during server crashes.

---

## 5. Project Structure

```
05-load-balancer/
├── app/
│   ├── config.py             # Port, backend list, & health check timings
│   ├── backend.py            # Backend model with in-flight counters & health state
│   ├── algorithms/
│   │   ├── base.py                   # LoadBalancingStrategy ABC
│   │   ├── round_robin.py            # Round Robin strategy
│   │   ├── weighted_round_robin.py   # Smooth Weighted Round Robin
│   │   ├── least_connections.py      # Least Connections strategy
│   │   └── consistent_hashing.py     # Consistent Hashing with virtual nodes
│   ├── health_checker.py     # Background active polling loop
│   ├── proxy.py              # Reverse proxy engine with passive retry
│   └── main.py               # Load balancer server & cluster dashboard
├── tests/
│   └── test_load_balancer.py # Unit tests for algorithms, failover, & health checks
├── benchmark.py              # Chaos failover benchmark (3 backends, kills 1 mid-traffic)
├── requirements.txt          # Dependencies
└── README.md                 # System design case study & comparison matrix
```

---

## 6. Running the Project

### Setup Environment
```bash
# Activate virtual environment
.\.venv\Scripts\activate   # Windows
# or: source .venv/bin/activate (macOS/Linux)
```

### Start the Load Balancer
```bash
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload
```
* **Cluster Dashboard:** Open [http://127.0.0.1:8000/](http://127.0.0.1:8000/) in your browser. Switch routing algorithms live, trigger single or batch test requests, and watch backend traffic counters update in real time!
* **API Documentation (Swagger UI):** Open [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs).

### Run Test Suite
```bash
python -m unittest discover -s tests -v
```

### Run Chaos Failover Benchmark
```bash
python benchmark.py
```
Spawns 3 mock backends, validates Weighted Round Robin ratios, terminates Backend 2 mid-traffic, and proves 100% successful request completion via passive retry!
