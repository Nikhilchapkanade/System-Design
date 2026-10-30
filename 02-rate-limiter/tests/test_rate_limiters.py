"""Unit and Integration Tests for Distributed Rate Limiter."""

import time
import unittest
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from app.algorithms import (
    TokenBucketRateLimiter,
    LeakyBucketRateLimiter,
    FixedWindowRateLimiter,
    SlidingWindowLogRateLimiter,
    SlidingWindowCounterRateLimiter,
)
from app.main import app, limiters


class TestTokenBucket(unittest.TestCase):
    def test_burst_and_refill(self):
        limiter = TokenBucketRateLimiter(capacity=3, refill_rate=2.0)
        client = "user_1"

        # 3 initial tokens should succeed
        for _ in range(3):
            self.assertTrue(limiter.acquire(client).allowed)

        # 4th should fail
        res = limiter.acquire(client)
        self.assertFalse(res.allowed)
        self.assertGreater(res.retry_after, 0.0)

        # Wait 0.6s -> should refill at least 1 token (refill_rate = 2/s)
        time.sleep(0.6)
        self.assertTrue(limiter.acquire(client).allowed)

    def test_concurrency_race_condition(self):
        """50 concurrent threads racing for 10 tokens: exactly 10 must succeed."""
        limiter = TokenBucketRateLimiter(capacity=10, refill_rate=0.0)
        client = "concurrency_client"

        with ThreadPoolExecutor(max_workers=20) as executor:
            results = list(executor.map(lambda _: limiter.acquire(client).allowed, range(50)))

        success_count = sum(1 for allowed in results if allowed)
        self.assertEqual(success_count, 10, "Race condition detected! Allowed tokens exceeded capacity.")


class TestLeakyBucket(unittest.TestCase):
    def test_buffer_and_leak(self):
        limiter = LeakyBucketRateLimiter(capacity=3, leak_rate=2.0)
        client = "user_leaky"

        for _ in range(3):
            self.assertTrue(limiter.acquire(client).allowed)

        # Bucket full
        self.assertFalse(limiter.acquire(client).allowed)

        # Wait for leak
        time.sleep(0.6)
        self.assertTrue(limiter.acquire(client).allowed)


class TestFixedWindow(unittest.TestCase):
    def test_fixed_window_limit(self):
        limiter = FixedWindowRateLimiter(limit=3, window_seconds=1)
        client = "user_fw"

        for _ in range(3):
            self.assertTrue(limiter.acquire(client).allowed)

        self.assertFalse(limiter.acquire(client).allowed)

        # Wait for window boundary reset
        time.sleep(1.1)
        self.assertTrue(limiter.acquire(client).allowed)


class TestSlidingWindowLog(unittest.TestCase):
    def test_exact_sliding_log(self):
        limiter = SlidingWindowLogRateLimiter(limit=3, window_seconds=1)
        client = "user_swl"

        for _ in range(3):
            self.assertTrue(limiter.acquire(client).allowed)

        self.assertFalse(limiter.acquire(client).allowed)

        time.sleep(1.1)
        self.assertTrue(limiter.acquire(client).allowed)


class TestSlidingWindowCounter(unittest.TestCase):
    def test_weighted_counter(self):
        limiter = SlidingWindowCounterRateLimiter(limit=4, window_seconds=2)
        client = "user_swc"

        for _ in range(4):
            self.assertTrue(limiter.acquire(client).allowed)

        self.assertFalse(limiter.acquire(client).allowed)


class TestAPIMiddlewareIntegration(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        for lim in limiters.values():
            lim.reset()

    def test_health_unrestricted(self):
        res = self.client.get("/health")
        self.assertEqual(res.status_code, 200)

    def test_token_bucket_headers_and_throttling(self):
        headers = {"x-api-key": "test-key-alpha"}

        # First request should succeed and return RFC headers
        res = self.client.get("/api/v1/token-bucket/resource", headers=headers)
        self.assertEqual(res.status_code, 200)
        self.assertIn("x-ratelimit-limit", res.headers)
        self.assertIn("x-ratelimit-remaining", res.headers)
        self.assertIn("x-ratelimit-reset", res.headers)

        # Exhaust capacity (10 tokens)
        for _ in range(9):
            res = self.client.get("/api/v1/token-bucket/resource", headers=headers)

        # 11th request must receive HTTP 429 Too Many Requests
        res_throttled = self.client.get("/api/v1/token-bucket/resource", headers=headers)
        self.assertEqual(res_throttled.status_code, 429)
        self.assertIn("retry-after", res_throttled.headers)

    def test_multi_client_isolation(self):
        """Client A being throttled must never impact Client B."""
        headers_a = {"x-api-key": "client-A"}
        headers_b = {"x-api-key": "client-B"}

        # Exhaust Client A
        for _ in range(15):
            self.client.get("/api/v1/token-bucket/resource", headers=headers_a)

        # Client A is throttled
        res_a = self.client.get("/api/v1/token-bucket/resource", headers=headers_a)
        self.assertEqual(res_a.status_code, 429)

        # Client B should still have full quota
        res_b = self.client.get("/api/v1/token-bucket/resource", headers=headers_b)
        self.assertEqual(res_b.status_code, 200)


if __name__ == "__main__":
    unittest.main()
