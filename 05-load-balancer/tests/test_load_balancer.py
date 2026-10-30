"""Unit Tests for Load Balancing Algorithms, Health Checking, and Routing."""

import unittest
from fastapi.testclient import TestClient

from app.backend import Backend
from app.algorithms import (
    RoundRobinStrategy,
    WeightedRoundRobinStrategy,
    LeastConnectionsStrategy,
    ConsistentHashingStrategy,
)
from app.main import app, backends, strategies


class TestLoadBalancingAlgorithms(unittest.TestCase):
    def test_round_robin(self):
        b1 = Backend("http://node1:8001")
        b2 = Backend("http://node2:8002")
        b3 = Backend("http://node3:8003")
        pool = [b1, b2, b3]

        rr = RoundRobinStrategy()
        picks = [rr.select_backend(pool).url for _ in range(6)]
        expected = [
            "http://node1:8001",
            "http://node2:8002",
            "http://node3:8003",
            "http://node1:8001",
            "http://node2:8002",
            "http://node3:8003",
        ]
        self.assertEqual(picks, expected)

    def test_weighted_round_robin_ratios(self):
        """Weights 3:1:1 across 10 requests must yield 6:2:2."""
        b1 = Backend("http://heavy:8001", weight=3)
        b2 = Backend("http://light1:8002", weight=1)
        b3 = Backend("http://light2:8003", weight=1)
        pool = [b1, b2, b3]

        wrr = WeightedRoundRobinStrategy()
        picks = [wrr.select_backend(pool).url for _ in range(10)]

        self.assertEqual(picks.count("http://heavy:8001"), 6)
        self.assertEqual(picks.count("http://light1:8002"), 2)
        self.assertEqual(picks.count("http://light2:8003"), 2)

    def test_least_connections(self):
        b1 = Backend("http://node1:8001")
        b2 = Backend("http://node2:8002")
        b1.active_connections = 5
        b2.active_connections = 1

        lc = LeastConnectionsStrategy()
        selected = lc.select_backend([b1, b2])
        self.assertEqual(selected.url, "http://node2:8002")

    def test_consistent_hashing_affinity(self):
        b1 = Backend("http://node1:8001")
        b2 = Backend("http://node2:8002")
        pool = [b1, b2]

        ch = ConsistentHashingStrategy(virtual_nodes_per_weight=50)

        # 10 requests from client A must all route to the identical backend
        client_a_pick = ch.select_backend(pool, client_key="192.168.1.100").url
        for _ in range(10):
            self.assertEqual(ch.select_backend(pool, client_key="192.168.1.100").url, client_a_pick)


class TestHealthStateMachine(unittest.TestCase):
    def test_state_transitions(self):
        backend = Backend("http://test:8001")
        self.assertTrue(backend.is_healthy)

        # 1st failure: still healthy (flapping protection)
        tripped = backend.record_failure(unhealthy_threshold=2)
        self.assertFalse(tripped)
        self.assertTrue(backend.is_healthy)

        # 2nd failure: trips to unhealthy
        tripped = backend.record_failure(unhealthy_threshold=2)
        self.assertTrue(tripped)
        self.assertFalse(backend.is_healthy)

        # 1st recovery success: still unhealthy
        recovered = backend.record_success(healthy_threshold=2)
        self.assertFalse(recovered)
        self.assertFalse(backend.is_healthy)

        # 2nd recovery success: transitions back to healthy
        recovered = backend.record_success(healthy_threshold=2)
        self.assertTrue(recovered)
        self.assertTrue(backend.is_healthy)


class TestLoadBalancerAPI(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_health_endpoint(self):
        res = self.client.get("/health")
        self.assertEqual(res.status_code, 200)
        self.assertIn("healthy_backends", res.json())

    def test_cluster_status(self):
        res = self.client.get("/api/v1/cluster")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("backends", data)
        self.assertIn("active_strategy", data)

    def test_switch_strategy(self):
        res = self.client.post("/api/v1/strategy/least-connections")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["active_strategy"], "least-connections")

        # Invalid strategy
        res_invalid = self.client.post("/api/v1/strategy/non-existent")
        self.assertEqual(res_invalid.status_code, 400)


if __name__ == "__main__":
    unittest.main()
