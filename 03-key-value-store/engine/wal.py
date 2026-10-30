"""Write-Ahead Log (WAL) Manager.

System Design Analysis (Durability & Disk I/O):
Sequential disk appends are orders of magnitude faster than random disk updates
(even on NVMe SSDs). By appending mutations to a WAL before updating in-memory
state, we achieve both ACID Durability and high write throughput.

fsync vs Page Cache:
- Without fsync(), Python/OS buffers writes in kernel page cache. If OS crashes, data is lost.
- With fsync(), data is physically committed to non-volatile disk blocks.
"""

import os
import threading
from typing import List, Optional, Tuple
from engine.record import Record


class WriteAheadLog:
    """Thread-safe Append-Only Write-Ahead Log with fsync controls."""

    def __init__(self, log_path: str, sync_always: bool = True):
        self.log_path = log_path
        self.sync_always = sync_always
        self._lock = threading.Lock()
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(os.path.abspath(log_path)), exist_ok=True)
        
        # Open in binary append+read mode
        self._file = open(self.log_path, "a+b")
        self._file.seek(0, os.SEEK_END)

    def append(self, record: Record) -> int:
        """Atomically append a record to disk and optionally fsync.
        
        Returns:
            Offset where record was written.
        """
        data = record.serialize()
        with self._lock:
            offset = self._file.tell()
            self._file.write(data)
            self._file.flush()
            if self.sync_always:
                os.fsync(self._file.fileno())
            return offset

    def read_all(self, auto_repair_torn_writes: bool = True) -> Tuple[List[Record], int]:
        """Read and validate all records from the log from the beginning.
        
        Args:
            auto_repair_torn_writes: If True, truncates any trailing corrupted
            record caused by a sudden process termination.
            
        Returns:
            (records: List[Record], corrupted_bytes_discarded: int)
        """
        records: List[Record] = []
        discarded_bytes = 0

        with self._lock:
            self._file.flush()
            self._file.seek(0)
            last_valid_offset = 0

            try:
                while True:
                    curr_offset = self._file.tell()
                    record = Record.deserialize_from_stream(self._file)
                    if record is None:
                        break  # Clean end of file
                    records.append(record)
                    last_valid_offset = self._file.tell()
            except ValueError as ex:
                # Corruption or torn write detected at current offset
                self._file.seek(0, os.SEEK_END)
                total_size = self._file.tell()
                discarded_bytes = total_size - last_valid_offset

                if auto_repair_torn_writes:
                    # Truncate corrupted tail to preserve clean state
                    self._file.seek(last_valid_offset)
                    self._file.truncate()
                    self._file.flush()
                    if self.sync_always:
                        os.fsync(self._file.fileno())

            # Return file pointer to end for new writes
            self._file.seek(0, os.SEEK_END)
            return records, discarded_bytes

    def get_size_bytes(self) -> int:
        """Return total size of the log file on disk."""
        with self._lock:
            self._file.flush()
            curr = self._file.tell()
            self._file.seek(0, os.SEEK_END)
            size = self._file.tell()
            self._file.seek(curr)
            return size

    def close(self) -> None:
        with self._lock:
            if not self._file.closed:
                self._file.flush()
                if self.sync_always:
                    os.fsync(self._file.fileno())
                self._file.close()
