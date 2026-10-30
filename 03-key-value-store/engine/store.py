"""Core KeyValueStore Engine orchestrating MemTable and WAL.

System Design Architecture:
- Read Path: O(1) in-memory lookup directly from MemTable (sub-microsecond latency).
- Write Path: Append-only disk write to WAL (with fsync) -> MemTable update -> Acknowledge.
- Recovery Path: Sequential log replay restoring in-memory MemTable on startup.
"""

import time
import threading
from typing import Optional, Dict, Any, List, Tuple
from engine.record import Record, OpType
from engine.wal import WriteAheadLog
from engine.compactor import Compactor


class KeyValueStore:
    """Thread-safe, crash-resilient Key-Value Store with Write-Ahead Logging."""

    def __init__(self, data_dir: str = "data", log_filename: str = "store.wal", sync_always: bool = True):
        self.data_dir = data_dir
        self.log_path = f"{data_dir}/{log_filename}"
        self.sync_always = sync_always

        self._memtable: Dict[str, str] = {}
        self._lock = threading.Lock()
        
        # Initialize WAL
        self._wal = WriteAheadLog(self.log_path, sync_always=self.sync_always)

        # Telemetry metrics
        self._stats = {
            "total_reads": 0,
            "total_writes": 0,
            "total_deletes": 0,
            "recoveries_run": 0,
            "compactions_run": 0,
        }

        # Run crash recovery replay on startup
        self.last_recovery_report = self.recover()

    def recover(self) -> Dict[str, Any]:
        """Replay WAL from disk to restore in-memory MemTable state."""
        start_time = time.perf_counter()
        with self._lock:
            records, discarded_bytes = self._wal.read_all(auto_repair_torn_writes=True)
            self._memtable.clear()

            for rec in records:
                if rec.op_type == OpType.SET:
                    self._memtable[rec.key] = rec.value
                elif rec.op_type == OpType.DELETE:
                    self._memtable.pop(rec.key, None)

            elapsed_ms = (time.perf_counter() - start_time) * 1000.0
            self._stats["recoveries_run"] += 1

            return {
                "records_replayed": len(records),
                "active_keys_recovered": len(self._memtable),
                "discarded_corrupt_bytes": discarded_bytes,
                "recovery_time_ms": round(elapsed_ms, 2),
            }

    def get(self, key: str) -> Optional[str]:
        """O(1) in-memory read from MemTable."""
        with self._lock:
            self._stats["total_reads"] += 1
            return self._memtable.get(key)

    def set(self, key: str, value: str) -> None:
        """Durable write: append to WAL before in-memory update."""
        rec = Record(op_type=OpType.SET, key=key, value=value)
        with self._lock:
            self._wal.append(rec)
            self._memtable[key] = value
            self._stats["total_writes"] += 1

    def delete(self, key: str) -> bool:
        """Durable delete: append tombstone to WAL before in-memory eviction."""
        rec = Record(op_type=OpType.DELETE, key=key)
        with self._lock:
            if key not in self._memtable:
                return False
            self._wal.append(rec)
            del self._memtable[key]
            self._stats["total_deletes"] += 1
            return True

    def exists(self, key: str) -> bool:
        with self._lock:
            return key in self._memtable

    def keys(self) -> List[str]:
        with self._lock:
            return list(self._memtable.keys())

    def compact(self) -> Tuple[int, int]:
        """Compact WAL by snapshotting active MemTable."""
        with self._lock:
            old_size, new_size = Compactor.compact(self._wal, self._memtable)
            self._stats["compactions_run"] += 1
            return old_size, new_size

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "active_keys": len(self._memtable),
                "wal_size_bytes": self._wal.get_size_bytes(),
                "sync_always": self.sync_always,
                "telemetry": dict(self._stats),
            }

    def close(self) -> None:
        with self._lock:
            self._wal.close()
