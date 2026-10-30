"""Crash Recovery Simulation Tests for Key-Value Store."""

import os
import shutil
import tempfile
import unittest

from engine.store import KeyValueStore


class TestCrashRecovery(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_clean_crash_recovery(self):
        """Simulates writing hundreds of records, an abrupt crash, and full state recovery."""
        # Phase 1: Write initial dataset
        store1 = KeyValueStore(data_dir=self.test_dir, sync_always=True)
        for i in range(200):
            store1.set(f"user:{i}", f"profile_data_{i}")

        # Update 50 keys
        for i in range(0, 50):
            store1.set(f"user:{i}", f"updated_data_{i}")

        # Delete 25 keys
        for i in range(150, 175):
            store1.delete(f"user:{i}")

        # Simulating sudden process termination
        store1.close()
        del store1

        # Phase 2: Start new instance (Cold Restart Replay)
        store2 = KeyValueStore(data_dir=self.test_dir, sync_always=True)
        report = store2.last_recovery_report

        self.assertGreaterEqual(report["records_replayed"], 275)

        # Verify updated keys
        for i in range(0, 50):
            self.assertEqual(store2.get(f"user:{i}"), f"updated_data_{i}")

        # Verify unchanged keys
        for i in range(50, 150):
            self.assertEqual(store2.get(f"user:{i}"), f"profile_data_{i}")

        # Verify deleted keys
        for i in range(150, 175):
            self.assertIsNone(store2.get(f"user:{i}"))

        store2.close()

    def test_torn_write_auto_repair(self):
        """Simulates sudden power failure mid-write leaving a corrupt trailing record."""
        store1 = KeyValueStore(data_dir=self.test_dir, sync_always=True)
        store1.set("key_1", "valid_value_1")
        store1.set("key_2", "valid_value_2")
        wal_path = store1.log_path
        store1.close()

        # Simulate torn write: append 8 bytes of garbage to file tail
        with open(wal_path, "ab") as f:
            f.write(b"\xDE\xAD\xBE\xEF\x01\x02\x03\x04")

        # Restart store: should detect CRC corruption and auto-repair/truncate
        store2 = KeyValueStore(data_dir=self.test_dir, sync_always=True)
        report = store2.last_recovery_report

        self.assertGreater(report["discarded_corrupt_bytes"], 0)
        self.assertEqual(store2.get("key_1"), "valid_value_1")
        self.assertEqual(store2.get("key_2"), "valid_value_2")

        store2.close()


if __name__ == "__main__":
    unittest.main()
