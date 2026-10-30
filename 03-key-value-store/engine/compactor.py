"""Log Compactor and Snapshot Engine.

System Design Analysis (Compaction & Bitcask Pattern):
Append-only logs accumulate stale updates and deleted tombstones over time.
Compaction reads the active in-memory state, writes only current active keys to
a fresh temporary segment, and uses atomic file replacement (os.replace) to
swap the log file with zero data loss and minimal disk footprint.
"""

import os
import time
from typing import Dict, Tuple
from engine.record import Record, OpType
from engine.wal import WriteAheadLog


class Compactor:
    """Manages snapshot generation and WAL truncation."""

    @staticmethod
    def compact(wal: WriteAheadLog, memtable: Dict[str, str]) -> Tuple[int, int]:
        """Perform snapshot compaction of the active MemTable.
        
        Args:
            wal: Current WriteAheadLog instance.
            memtable: Current active key-value mappings.
            
        Returns:
            (bytes_before: int, bytes_after: int)
        """
        old_size = wal.get_size_bytes()
        log_path = wal.log_path
        temp_path = f"{log_path}.compact.{int(time.time() * 1000)}.tmp"

        # 1. Write active keys to temporary file
        temp_wal = WriteAheadLog(temp_path, sync_always=True)
        try:
            for key, val in memtable.items():
                rec = Record(op_type=OpType.SET, key=key, value=val)
                temp_wal.append(rec)
            temp_wal.close()
        except Exception:
            temp_wal.close()
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

        # 2. Close active WAL
        wal.close()

        # 3. Atomically replace old WAL with compacted WAL
        # os.replace is atomic across both POSIX and modern Windows NTFS
        os.replace(temp_path, log_path)

        # 4. Reopen active WAL pointing to compacted log
        wal._file = open(log_path, "a+b")
        wal._file.seek(0, os.SEEK_END)
        new_size = wal.get_size_bytes()

        return old_size, new_size
