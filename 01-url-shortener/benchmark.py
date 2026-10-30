"""High-Concurrency System Design Benchmark for URL Shortener.

Simulates real-world traffic patterns:
- Ingestion phase: Shorten N unique URLs.
- Read phase: Dispatch concurrent read requests following an 80/20 Pareto distribution
  (20% of hot URLs generate 80% of redirection requests).
- Measures: QPS, latency percentiles (p50, p95, p99), and cache hit ratios.
"""

import asyncio
import time
import random
from typing import List
import httpx


async def run_benchmark(
    base_url: str = "http://127.0.0.1:8000",
    num_urls: int = 50,
    total_reads: int = 1000,
    concurrency: int = 25,
):
    print("=" * 65)
    print(">> DISTRIBUTED URL SHORTENER - SYSTEM DESIGN BENCHMARK")
    print("=" * 65)
    print(f"Target: {base_url}")
    print(f"Concurrency: {concurrency} workers | Total Reads: {total_reads}")
    print("-" * 65)

    transport = None
    try:
        async with httpx.AsyncClient(base_url=base_url, timeout=1.0) as test_client:
            res = await test_client.get("/health")
            if res.status_code == 200:
                print(f"[*] Connected to live server at {base_url}")
    except Exception:
        from app.main import app
        print(f"[*] Live server not detected on {base_url}. Running in-process ASGI benchmark.")
        transport = httpx.ASGITransport(app=app)
        base_url = "http://testserver"

    async with httpx.AsyncClient(transport=transport, base_url=base_url, timeout=10.0) as client:
        # 1. Ingestion Phase
        print(f"[*] Ingesting {num_urls} URLs across simulated clients...")
        short_codes: List[str] = []
        for i in range(num_urls):
            target = f"https://example.com/system-design/article-{i}"
            headers = {"x-forwarded-for": f"10.0.{i // 10}.{i % 10}"}
            res = await client.post("/api/v1/shorten", json={"url": target}, headers=headers)
            if res.status_code == 201:
                short_codes.append(res.json()["short_code"])
            else:
                print(f"Warning: shorten failed with {res.status_code}: {res.text}")

        print(f"[+] Ingested {len(short_codes)} URLs successfully.")

        # Partition URLs into Hot (20%) and Cold (80%)
        hot_count = max(1, int(len(short_codes) * 0.2))
        hot_urls = short_codes[:hot_count]
        cold_urls = short_codes[hot_count:]

        def get_weighted_url() -> str:
            # 80% probability -> choose from hot URLs
            if random.random() < 0.8:
                return random.choice(hot_urls)
            return random.choice(cold_urls)

        # 2. Concurrency Read Phase
        print(f"[*] Dispatching {total_reads} reads with {concurrency} async workers...")
        latencies: List[float] = []
        status_counts = {}
        semaphore = asyncio.Semaphore(concurrency)

        async def worker():
            code = get_weighted_url()
            async with semaphore:
                start = time.perf_counter()
                try:
                    # follow_redirects=False to measure our server's direct response time
                    res = await client.get(f"/{code}", follow_redirects=False)
                    elapsed = (time.perf_counter() - start) * 1000.0  # in ms
                    latencies.append(elapsed)
                    status_counts[res.status_code] = status_counts.get(res.status_code, 0) + 1
                except Exception as ex:
                    status_counts["ERROR"] = status_counts.get("ERROR", 0) + 1

        overall_start = time.perf_counter()
        tasks = [asyncio.create_task(worker()) for _ in range(total_reads)]
        await asyncio.gather(*tasks)
        total_time = time.perf_counter() - overall_start

        # 3. Fetch final cache metrics
        metrics_res = await client.get("/api/v1/metrics")
        metrics_data = metrics_res.json() if metrics_res.status_code == 200 else {}

        # 4. Compute statistics
        latencies.sort()
        count = len(latencies)
        qps = count / total_time if total_time > 0 else 0
        p50 = latencies[int(count * 0.50)] if count else 0
        p90 = latencies[int(count * 0.90)] if count else 0
        p95 = latencies[int(count * 0.95)] if count else 0
        p99 = latencies[int(count * 0.99)] if count else 0
        avg_lat = sum(latencies) / count if count else 0

        print("\n" + "=" * 65)
        print("BENCHMARK RESULTS")
        print("=" * 65)
        print(f"Total Requests Completed: {count} in {total_time:.3f}s")
        print(f"Throughput:               {qps:.2f} req/sec (QPS)")
        print(f"Status Codes:             {status_counts}")
        print("-" * 65)
        print("LATENCY DISTRIBUTION (ms):")
        print(f"  Min:                    {latencies[0]:.2f} ms")
        print(f"  Avg:                    {avg_lat:.2f} ms")
        print(f"  p50 (Median):           {p50:.2f} ms")
        print(f"  p90:                    {p90:.2f} ms")
        print(f"  p95:                    {p95:.2f} ms")
        print(f"  p99:                    {p99:.2f} ms")
        print(f"  Max:                    {latencies[-1]:.2f} ms")
        print("-" * 65)
        if "cache" in metrics_data:
            cache_info = metrics_data["cache"]
            print("CACHE-ASIDE TELEMETRY:")
            print(f"  Cache Hits:             {cache_info.get('hits', 0)}")
            print(f"  Cache Misses:           {cache_info.get('misses', 0)}")
            print(f"  Hit Ratio:              {cache_info.get('hit_ratio', 0) * 100:.2f}%")
            print(f"  Evictions:              {cache_info.get('evictions', 0)}")
        print("=" * 65)


if __name__ == "__main__":
    asyncio.run(run_benchmark())
