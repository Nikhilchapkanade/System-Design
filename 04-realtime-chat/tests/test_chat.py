"""Unit and Integration Tests for Real-Time Chat & Pub/Sub."""

import asyncio
import os
import shutil
import tempfile
import unittest

from fastapi.testclient import TestClient

from app.pubsub import InMemoryPubSubBroker
from app.storage import ChatStorage
from app.main import app, storage


class TestChatStorage(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "test_chat.db")
        self.store = ChatStorage(self.db_path)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_monotonic_sequence_ids(self):
        # Insert 5 messages in room A
        for i in range(1, 6):
            msg = self.store.save_message("room_A", "user1", f"Hello {i}")
            self.assertEqual(msg["seq_id"], i)

        # Room B sequence should start independently at 1
        msg_b = self.store.save_message("room_B", "user2", "Hello room B")
        self.assertEqual(msg_b["seq_id"], 1)

    def test_history_pagination(self):
        for i in range(1, 11):
            self.store.save_message("general", "alice", f"msg_{i}")

        history = self.store.get_history("general", limit=5)
        self.assertEqual(len(history), 5)
        # Verify chronological order of the latest 5 (seq 6 to 10)
        self.assertEqual(history[0]["seq_id"], 6)
        self.assertEqual(history[-1]["seq_id"], 10)


class TestPubSubBroker(unittest.IsolatedAsyncioTestCase):
    async def test_publish_subscribe(self):
        broker = InMemoryPubSubBroker()
        received_messages = []

        async def listener(msg):
            received_messages.append(msg)

        await broker.subscribe("channel_1", listener)
        await broker.publish("channel_1", {"text": "hello pubsub"})
        await broker.publish("other_channel", {"text": "ignore"})

        self.assertEqual(len(received_messages), 1)
        self.assertEqual(received_messages[0]["text"], "hello pubsub")

        # Unsubscribe
        await broker.unsubscribe("channel_1", listener)
        await broker.publish("channel_1", {"text": "after unsubscribe"})
        self.assertEqual(len(received_messages), 1)


class TestWebSocketChatAPI(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_rest_health(self):
        res = self.client.get("/health")
        self.assertEqual(res.status_code, 200)

    def test_websocket_broadcast_flow(self):
        """Simulates Alice and Bob connecting to #engineering and exchanging messages."""
        room = "engineering"

        with self.client.websocket_connect(f"/ws/{room}/Alice") as ws_alice:
            # Alice receives her own join event
            join_alice = ws_alice.receive_json()
            self.assertEqual(join_alice["type"], "system")
            self.assertEqual(join_alice["username"], "Alice")

            with self.client.websocket_connect(f"/ws/{room}/Bob") as ws_bob:
                # Bob receives his join event
                join_bob = ws_bob.receive_json()
                self.assertEqual(join_bob["username"], "Bob")

                # Alice receives Bob's join event
                alice_sees_bob_join = ws_alice.receive_json()
                self.assertEqual(alice_sees_bob_join["username"], "Bob")

                # Alice sends chat message
                ws_alice.send_text("Deploying v2.0 now")

                # Both Alice and Bob should receive the broadcasted message
                msg_alice = ws_alice.receive_json()
                msg_bob = ws_bob.receive_json()

                self.assertEqual(msg_alice["content"], "Deploying v2.0 now")
                self.assertEqual(msg_alice["username"], "Alice")
                self.assertEqual(msg_bob["content"], "Deploying v2.0 now")
                self.assertEqual(msg_bob["username"], "Alice")
                self.assertGreaterEqual(msg_alice["seq_id"], 1)

        # Check REST history endpoint
        history_res = self.client.get(f"/api/v1/rooms/{room}/history")
        self.assertEqual(history_res.status_code, 200)
        data = history_res.json()
        self.assertGreaterEqual(data["count"], 1)


if __name__ == "__main__":
    unittest.main()
