"""Unit and Integration Tests for Distributed URL Shortener."""

import time
import unittest
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from app.key_generator import base62_encode, base62_decode, RangeKeyGenerator
from app.cache import LRUCacheWithTTL
from app.rate_limiter import SlidingWindowRateLimiter
from app.main import app, cache, storage, rate_limiter


class TestKeyGenerator(unittest.TestCase):
    """Test Base62 encoding and Range Partitioning."""

    def test_base62_roundtrip(self):
        numbers = [0, 1, 61, 62, 1000, 10000000, 9999999999]
        for n in numbers:
            encoded = base62_encode(n, min_length=1)
            decoded = base62_decode(encoded)
            self.assertEqual(n, decoded, f"Failed for {n}: encoded as {encoded}")

    def test_base62_padding(self):
        encoded = base62_encode(5, min_length=6)
        self.assertEqual(len(encoded), 6)
        self.assertEqual(base62_decode(encoded), 5)

    def test_range_key_generator_concurrency(self):
        """Ensure thread-safety: no duplicate IDs across 1,000 concurrent threads."""
        generator = RangeKeyGenerator(start_id=1, block_size=500)
        num_requests = 1000

        with ThreadPoolExecutor(max_workers=20) as executor:
            results = list(executor.map(lambda _: generator.next_id(), range(num_requests)))

        self.assertEqual(len(results), num_requests)
        self.assertEqual(len(set(results)), num_requests, "Duplicate IDs were generated!")


class TestCacheLayer(unittest.TestCase):
    """Test LRU Cache eviction and TTL expiration."""

    def test_lru_eviction(self):
        cache_instance = LRUCacheWithTTL(capacity=3, default_ttl_seconds=60)
        cache_instance.set("a", "1")
        cache_instance.set("b", "2")
        cache_instance.set("c", "3")

        # Access 'a' to make it most recently used; 'b' is now oldest
        _ = cache_instance.get("a")

        # Insert 'd', which should evict 'b'
        cache_instance.set("d", "4")

        self.assertEqual(cache_instance.get("b"), None)
        self.assertEqual(cache_instance.get("a"), "1")
        self.assertEqual(cache_instance.get("c"), "3")
        self.assertEqual(cache_instance.get("d"), "4")

    def test_ttl_expiration(self):
        cache_instance = LRUCacheWithTTL(capacity=10, default_ttl_seconds=1)
        cache_instance.set("temp", "val", ttl_seconds=1)
        self.assertEqual(cache_instance.get("temp"), "val")
        time.sleep(1.1)
        self.assertEqual(cache_instance.get("temp"), None)


class TestRateLimiter(unittest.TestCase):
    """Test Sliding Window Rate Limiting."""

    def test_rate_limiter_window(self):
        limiter = SlidingWindowRateLimiter(max_requests=3, window_seconds=2)
        ip = "192.168.1.100"

        # First 3 should pass
        for _ in range(3):
            allowed, _ = limiter.is_allowed(ip)
            self.assertTrue(allowed)

        # 4th should be blocked
        allowed, retry_after = limiter.is_allowed(ip)
        self.assertFalse(allowed)
        self.assertGreater(retry_after, 0)

        # Another client should pass
        allowed, _ = limiter.is_allowed("192.168.1.101")
        self.assertTrue(allowed)


class TestAPIEndpoints(unittest.TestCase):
    """Integration tests for HTTP endpoints."""

    def setUp(self):
        self.client = TestClient(app)
        rate_limiter.reset()

    def test_health_check(self):
        res = self.client.get("/health")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["status"], "ok")

    def test_shorten_and_redirect(self):
        # 1. Shorten
        target = "https://example.com/system-design-tutorial"
        res = self.client.post("/api/v1/shorten", json={"url": target})
        self.assertEqual(res.status_code, 201)
        data = res.json()
        short_code = data["short_code"]
        self.assertIn("short_url", data)

        # 2. Redirect (follow_redirects=False to inspect 307)
        redirect_res = self.client.get(f"/{short_code}", follow_redirects=False)
        self.assertEqual(redirect_res.status_code, 307)
        self.assertEqual(redirect_res.headers["location"], target)

        # 3. Analytics
        time.sleep(0.1)  # allow background task to write
        analytics_res = self.client.get(f"/api/v1/analytics/{short_code}")
        self.assertEqual(analytics_res.status_code, 200)
        analytics_data = analytics_res.json()
        self.assertGreaterEqual(analytics_data["click_count"], 1)

    def test_custom_alias_conflict(self):
        alias = f"alias-{int(time.time() * 1000)}"
        url1 = "https://google.com"
        url2 = "https://github.com"

        # First should succeed
        res1 = self.client.post("/api/v1/shorten", json={"url": url1, "custom_alias": alias})
        self.assertEqual(res1.status_code, 201)

        # Second with same alias should fail with 409 Conflict
        res2 = self.client.post("/api/v1/shorten", json={"url": url2, "custom_alias": alias})
        self.assertEqual(res2.status_code, 409)

    def test_cache_telemetry_metrics(self):
        metrics_res = self.client.get("/api/v1/metrics")
        self.assertEqual(metrics_res.status_code, 200)
        data = metrics_res.json()
        self.assertIn("cache", data)
        self.assertIn("database", data)
        self.assertIn("hit_ratio", data["cache"])


if __name__ == "__main__":
    unittest.main()
