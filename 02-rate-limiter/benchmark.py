"""Comparative Burst and Sustained Load Benchmark for Rate Limiting Algorithms.

Simulates two distinct real-world traffic profiles:
1. Sudden Spike Burst: 30 requests sent instantaneously in under 50ms against a quota of 10.
2. Sustained Flow: Requests spaced across 2 seconds to measure refill and smoothing rates.
"""

import asyncio
import time
from typing import Dict, List, Tuple
import httpx

from app.main import app

ALGORITHMS = [
    ("token-bucket", "Token Bucket (Burst-friendly)"),
    ("leaky-bucket", "Leaky Bucket (Traffic-shaping)"),
    ("fixed-window", "Fixed Window Counter"),
    ("sliding-window-log", "Sliding Window Log (Exact)"),
    ("sliding-window-counter", "Sliding Window Counter (Weighted)"),
]


async def run_burst_test(client: httpx.AsyncClient, algo_key: str, burst_count: int = 30) -> Tuple[int, int, float]:
    """Fire `burst_count` requests with minimal delay."""
    path = f"/api/v1/{algo_key}/resource"
    headers = {"x-api-key": f"bench-{algo_key}-{time.time()}"}

    start = time.perf_counter()
    tasks = [client.get(path, headers=headers) for _ in range(burst_count)]
    responses = await asyncio.gather(*tasks)
    elapsed = (time.perf_counter() - start) * 1000.0

    passed = sum(1 for r in responses if r.status_code == 200)
    throttled = sum(1 for r in responses if r.status_code == 429)
    return passed, throttled, elapsed


async def run_sustained_test(client: httpx.AsyncClient, algo_key: str, total_req: int = 20, delay_sec: float = 0.1) -> Tuple[int, int]:
    """Send requests spaced over time to test replenishment dynamics."""
    path = f"/api/v1/{algo_key}/resource"
    headers = {"x-api-key": f"sustained-{algo_key}-{time.time()}"}

    passed = 0
    throttled = 0
    for _ in range(total_req):
        res = await client.get(path, headers=headers)
        if res.status_code == 200:
            passed += 1
        else:
            throttled += 1
        await asyncio.sleep(delay_sec)
    return passed, throttled


async def main():
    print("=" * 72)
    print(">> DISTRIBUTED RATE LIMITER - MULTI-ALGORITHM BENCHMARK")
    print("=" * 72)

    # Use in-process ASGITransport for fast, reproducible standalone runs
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:

        # Phase 1: Burst Spike Simulation
        print("\n[*] PHASE 1: SUDDEN BURST SPIKE (30 requests fired in < 50ms, Quota = 10)")
        print("-" * 72)
        print(f"{'Algorithm':<35} | {'Passed (200)':<12} | {'Throttled (429)':<15} | {'Time (ms)':<8}")
        print("-" * 72)

        for key, label in ALGORITHMS:
            passed, throttled, elapsed = await run_burst_test(client, key, burst_count=30)
            print(f"{label:<35} | {passed:<12} | {throttled:<15} | {elapsed:<8.2f}")

        # Phase 2: Sustained Paced Ingestion
        print("\n[*] PHASE 2: SUSTAINED PACED TRAFFIC (20 requests spaced over 2.0 seconds)")
        print("-" * 72)
        print(f"{'Algorithm':<35} | {'Passed (200)':<12} | {'Throttled (429)':<15} | {'Refill Note'}")
        print("-" * 72)

        for key, label in ALGORITHMS:
            passed, throttled = await run_sustained_test(client, key, total_req=20, delay_sec=0.1)
            refill_note = "Continuous token regeneration" if "Token" in label or "Leaky" in label else "Window boundary bound"
            print(f"{label:<35} | {passed:<12} | {throttled:<15} | {refill_note}")

        print("=" * 72)
        print("SUMMARY OF SYSTEM DESIGN TAKEAWAYS:")
        print("1. Token Bucket: Best for general API tiers (absorbs sudden bursts cleanly).")
        print("2. Leaky Bucket: Best for downstream message brokers / DB writes (strictly smooth).")
        print("3. Fixed Window: Simple but vulnerable to 2x traffic bursts at window edges.")
        print("4. Sliding Window Log: 100% precise but consumes O(N) memory per client.")
        print("5. Sliding Window Counter: Cloudflare/Redis industry favorite (O(1) memory, smooth).")
        print("=" * 72)


if __name__ == "__main__":
    asyncio.run(main())
