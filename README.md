# System Design 

A production-grade, zero-external-dependency learning portfolio containing 6 end-to-end system design implementations built with Python and FastAPI. Each project includes complete source code, architectural documentation, capacity estimations, unit/integration test suites, chaos benchmarks, and interactive browser dashboards.

---

## 📚 Project Index

| # | Project | Key Concepts | Benchmark Highlights | Port |
|---|---|---|---|---|
| **01** | [**URL Shortener (TinyURL)**](./01-url-shortener/) | Base62 encoding, MD5/SHA256, LRU Cache, Bloom Filter, 301 vs 302 redirects | **100% cache hit ratio**, 1.00ms latency, ~600 req/s | `8001` |
| **02** | [**API Rate Limiter**](./02-rate-limiter/) | Token Bucket, Leaky Bucket, Fixed Window, Sliding Window Log & Counter | **50 concurrent threads**, zero race conditions | `8002` |
| **03** | [**LSM-Tree Key-Value Store**](./03-key-value-store/) | Append-only WAL, In-Memory MemTable, Sparse Index, Segment Compaction, Crash Recovery | **74k writes/s**, **2.3M reads/s**, 90% compaction savings | `8003` |
| **04** | [**Real-Time Chat Engine**](./04-realtime-chat/) | WebSockets, In-Memory Pub/Sub, Channel Multiplexing, Ephemeral & Persistent history | **100% delivery rate**, 3,795 msgs/s, 4ms latency | `8004` |
| **05** | [**Layer-7 Load Balancer**](./05-load-balancer/) | Round Robin, Weighted RR, Least Connections, Consistent Hashing, Active/Passive Health Checks | **100% success rate** under backend chaos | `8005` |
| **06** | [**Distributed Task Queue**](./06-distributed-task-queue/) | Priority Queue, Exponential Backoff, Dead-Letter Queue (DLQ), Delayed Execution | **1,659 tasks/s**, 100% DLQ isolation accuracy | `8006` |

---

## 🚀 Quick Start

You can run any project independently using its dedicated virtual environment or use the central runner script:

```bash
# Launch interactive project manager
python run.py
```

### Running Individual Projects

Each folder is completely self-contained:

```bash
# 1. URL Shortener
cd 01-url-shortener
.venv\Scripts\activate
uvicorn app.main:app --port 8001 --reload

# 2. Rate Limiter
cd 02-rate-limiter
.venv\Scripts\activate
uvicorn app.main:app --port 8002 --reload

# 3. Key-Value Store
cd 03-key-value-store
.venv\Scripts\activate
python cli.py

# 4. Real-Time Chat Engine
cd 04-realtime-chat
.venv\Scripts\activate
uvicorn app.main:app --port 8004 --reload

# 5. Load Balancer & Reverse Proxy
cd 05-load-balancer
.venv\Scripts\activate
uvicorn app.main:app --port 8005 --reload

# 6. Distributed Task Queue
cd 06-distributed-task-queue
.venv\Scripts\activate
uvicorn app.main:app --port 8006 --reload
```

---

## 🧪 Running All Tests and Benchmarks

Each project folder contains automated tests and benchmark scripts:

```bash
# Example: Run tests
pytest 01-url-shortener/tests -v
pytest 02-rate-limiter/tests -v
pytest 03-key-value-store/tests -v
pytest 04-realtime-chat/tests -v
pytest 05-load-balancer/tests -v
pytest 06-distributed-task-queue/tests -v

# Example: Run performance benchmarks
python 01-url-shortener/benchmark.py
python 02-rate-limiter/benchmark.py
python 03-key-value-store/benchmark.py
python 04-realtime-chat/benchmark.py
python 05-load-balancer/benchmark.py
python 06-distributed-task-queue/benchmark.py
```

---

## 🏗️ Architecture Diagrams

Detailed architectural schematics, capacity estimations, and trade-off tables can be found in each project's respective `README.md`.
