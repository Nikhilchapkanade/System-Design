"""High-Concurrency WebSocket Fan-Out Benchmark.

Simulates:
- 25 concurrent WebSocket clients subscribed to a single room (#benchmark-room).
- 50 messages broadcast through the distributed Pub/Sub layer.
- Total messages received: 25 * 50 = 1,250 message deliveries.
- Measures: Fan-out latency percentiles (p50, p95, p99) and delivery rate.
"""

import asyncio
import time
import json
import threading
import uvicorn
import websockets

from app.main import app


def start_background_server(port: int = 8008):
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    return server


async def run_benchmark(port: int = 8008, num_clients: int = 25, num_messages: int = 50):
    print("=" * 70)
    print(">> REAL-TIME CHAT & PUBSUB FAN-OUT BENCHMARK")
    print("=" * 70)
    print(f"Target: ws://127.0.0.1:{port}")
    print(f"Room: #benchmark-room | Clients: {num_clients} | Messages: {num_messages}")
    print(f"Expected Deliveries: {num_clients * num_messages}")
    print("-" * 70)

    # Launch local server for benchmark
    print("[*] Launching transient benchmark server on port", port)
    server = start_background_server(port)
    await asyncio.sleep(1.0)  # Wait for startup

    base_ws_url = f"ws://127.0.0.1:{port}/ws/benchmark-room"
    connected_sockets = []
    received_counts = [0] * num_clients
    delivery_latencies = []

    try:
        # Phase 1: Connect all clients
        print(f"[*] Establishing {num_clients} concurrent WebSocket connections...")
        for i in range(num_clients):
            ws = await websockets.connect(f"{base_ws_url}/user_{i}")
            # Consume initial join event
            _ = await ws.recv()
            connected_sockets.append(ws)

        # Drain any residual join notifications
        for ws in connected_sockets:
            while True:
                try:
                    await asyncio.wait_for(ws.recv(), timeout=0.05)
                except asyncio.TimeoutError:
                    break

        print(f"[+] All {num_clients} clients connected successfully.")

        # Phase 2: Concurrent Fan-Out Testing
        print(f"[*] Dispatching {num_messages} broadcast messages...")
        sender_ws = connected_sockets[0]

        async def client_listener(idx: int, ws):
            count = 0
            while count < num_messages:
                msg_raw = await ws.recv()
                data = json.loads(msg_raw)
                if data.get("type") == "chat":
                    recv_time = time.time()
                    latency_ms = (recv_time - data["timestamp"]) * 1000.0
                    delivery_latencies.append(latency_ms)
                    count += 1
            received_counts[idx] = count

        listener_tasks = [
            asyncio.create_task(client_listener(i, ws))
            for i, ws in enumerate(connected_sockets)
        ]

        overall_start = time.perf_counter()
        for m in range(num_messages):
            await sender_ws.send(f"Benchmark payload message #{m}")
            await asyncio.sleep(0.01)  # 10ms pacing between broadcasts

        await asyncio.gather(*listener_tasks)
        total_time = time.perf_counter() - overall_start

        # Phase 3: Compute Statistics
        total_delivered = sum(received_counts)
        delivery_latencies.sort()
        count = len(delivery_latencies)

        p50 = delivery_latencies[int(count * 0.50)] if count else 0
        p90 = delivery_latencies[int(count * 0.90)] if count else 0
        p95 = delivery_latencies[int(count * 0.95)] if count else 0
        p99 = delivery_latencies[int(count * 0.99)] if count else 0
        avg_lat = sum(delivery_latencies) / count if count else 0

        print("\n" + "=" * 70)
        print("BENCHMARK RESULTS")
        print("=" * 70)
        print(f"Total Broadcasts Sent:     {num_messages}")
        print(f"Total Message Deliveries:  {total_delivered} / {num_clients * num_messages} (100%)")
        print(f"Elapsed Time:              {total_time:.3f} s")
        print(f"Effective Delivery Rate:   {total_delivered / total_time:.2f} delivered_msgs/sec")
        print("-" * 70)
        print("FAN-OUT LATENCY (Origin to Subscriber delivery):")
        print(f"  Min:                     {delivery_latencies[0]:.2f} ms")
        print(f"  Avg:                     {avg_lat:.2f} ms")
        print(f"  p50 (Median):            {p50:.2f} ms")
        print(f"  p90:                     {p90:.2f} ms")
        print(f"  p95:                     {p95:.2f} ms")
        print(f"  p99:                     {p99:.2f} ms")
        print(f"  Max:                     {delivery_latencies[-1]:.2f} ms")
        print("=" * 70)

    finally:
        for ws in connected_sockets:
            await ws.close()
        server.should_exit = True


if __name__ == "__main__":
    asyncio.run(run_benchmark())
