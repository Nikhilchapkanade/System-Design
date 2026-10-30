"""Unit Tests for Key-Value Store Engine and Protocol."""

import io
import os
import shutil
import tempfile
import unittest

from engine.record import Record, OpType
from engine.wal import WriteAheadLog
from engine.store import KeyValueStore
from server.protocol import CommandParser


class TestRecordSerialization(unittest.TestCase):
    def test_record_roundtrip(self):
        rec = Record(op_type=OpType.SET, key="user:100", value="Alice Wonderland")
        data = rec.serialize()

        stream = io.BytesIO(data)
        decoded = Record.deserialize_from_stream(stream)

        self.assertIsNotNone(decoded)
        self.assertEqual(decoded.op_type, OpType.SET)
        self.assertEqual(decoded.key, "user:100")
        self.assertEqual(decoded.value, "Alice Wonderland")

    def test_crc32_corruption_detection(self):
        rec = Record(op_type=OpType.SET, key="secure_key", value="secret_data")
        raw_bytes = bytearray(rec.serialize())

        # Tamper with one byte in the payload (index 20)
        raw_bytes[20] ^= 0xFF

        stream = io.BytesIO(bytes(raw_bytes))
        with self.assertRaises(ValueError) as ctx:
            Record.deserialize_from_stream(stream)
        self.assertIn("CRC32", str(ctx.exception))


class TestKeyValueStoreEngine(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.store = KeyValueStore(data_dir=self.test_dir, sync_always=True)

    def tearDown(self):
        self.store.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_crud_operations(self):
        self.assertIsNone(self.store.get("name"))
        self.assertFalse(self.store.exists("name"))

        self.store.set("name", "Nikhil")
        self.assertTrue(self.store.exists("name"))
        self.assertEqual(self.store.get("name"), "Nikhil")

        deleted = self.store.delete("name")
        self.assertTrue(deleted)
        self.assertIsNone(self.store.get("name"))

        # Second delete should return False
        self.assertFalse(self.store.delete("name"))

    def test_compaction(self):
        # Write "counter" 100 times to create 100 log entries
        for i in range(100):
            self.store.set("counter", str(i))

        # Add other keys and delete one
        self.store.set("temp", "to_be_deleted")
        self.store.set("permanent", "active")
        self.store.delete("temp")

        size_before = self.store._wal.get_size_bytes()
        self.assertGreater(size_before, 2000)

        # Trigger compaction
        old_size, new_size = self.store.compact()
        self.assertLess(new_size, old_size)
        self.assertEqual(self.store.get("counter"), "99")
        self.assertEqual(self.store.get("permanent"), "active")
        self.assertIsNone(self.store.get("temp"))


class TestCommandProtocol(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.store = KeyValueStore(data_dir=self.test_dir)

    def tearDown(self):
        self.store.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_protocol_commands(self):
        # PING
        res, _ = CommandParser.execute(self.store, ["PING"])
        self.assertEqual(res, b"+PONG\r\n")

        # SET
        res, _ = CommandParser.execute(self.store, ["SET", "k1", "v1"])
        self.assertEqual(res, b"+OK\r\n")

        # GET
        res, _ = CommandParser.execute(self.store, ["GET", "k1"])
        self.assertEqual(res, b"$2\r\nv1\r\n")

        # DEL
        res, _ = CommandParser.execute(self.store, ["DEL", "k1"])
        self.assertEqual(res, b":1\r\n")

        # GET non-existent
        res, _ = CommandParser.execute(self.store, ["GET", "k1"])
        self.assertEqual(res, b"$-1\r\n")


if __name__ == "__main__":
    unittest.main()
