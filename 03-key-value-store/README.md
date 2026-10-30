# 03 - Key-Value Store with Write-Ahead Log (WAL) & Crash Recovery

A crash-resilient storage engine built from first principles demonstrating the core architectural patterns powering **Redis (AOF)**, **PostgreSQL (WAL)**, **RocksDB (LSM)**, and **Bitcask**.

---

## 1. High-Level Architecture

```mermaid
graph TD
    Client([TCP Clients / CLI])
    Server[Asyncio TCP Server Protocol]

    subgraph Storage Engine
        MemTable[In-Memory MemTable<br/>O(1) Hash Map]
        WAL[Append-Only WAL Disk File<br/>Sequential I/O + CRC32]
        Compactor[Snapshot Compactor]
    end

    Client -->|SET key value| Server
    Server -->|1. Append & fsync| WAL
    WAL -->|2. Update memory| MemTable
    Server -->|3. Acknowledge +OK| Client

    Client -->|GET key| Server
    Server -->|Direct O(1) in-memory read| MemTable

    Compactor -.->|Scans active keys & snapshots to fresh WAL| WAL
```

### Read vs. Write Paths
* **Write Path:** Every mutation (`SET`, `DEL`) appends a binary record to the Write-Ahead Log with a CRC32 checksum before modifying the in-memory MemTable.
* **Read Path:** Reads are served directly from the in-memory MemTable in sub-microsecond time ($O(1)$) with zero disk I/O.
* **Recovery Path:** On startup or after an unexpected server crash, the engine sequentially replays the WAL from the beginning, reconstructing the exact MemTable state.

---

## 2. Binary Frame Format

Each entry appended to the WAL uses a deterministic 19-byte header:

```
+----------------+--------------------+---------------+----------------+----------------+---------+-----------+
| CRC32 (4B)     | Timestamp (8B)     | OpType (1B)   | KeyLen (2B)    | ValLen (4B)    | Key     | Value     |
| Big-Endian u32 | Unix Epoch (double)| 1=SET, 2=DEL  | Big-Endian u16 | Big-Endian u32 | Raw UTF8| Raw UTF8  |
+----------------+--------------------+---------------+----------------+----------------+---------+-----------+
```

### CRC32 Corruption & Torn-Write Protection
If the host server loses power mid-write, the trailing bytes on disk will be incomplete. During cold recovery, the parser detects a CRC32 mismatch or truncated header, **auto-repairs the log by truncating the corrupt tail**, and preserves all previous valid transactions without crashing.

---

## 3. Log Compaction (Bitcask Pattern)

Because the WAL is append-only, repeated updates to the same key or deletions (tombstones) accumulate dead records on disk.

```
Uncompacted WAL: [SET a 1] [SET b 1] [SET a 2] [DEL b] [SET a 3] -> 5 records
Compacted WAL:   [SET a 3]                                        -> 1 record
```

### Zero-Downtime Atomic Replacement
1. Scans the current active MemTable in memory.
2. Writes active keys to a temporary segment file (`store.wal.compact.tmp`).
3. Closes the bloated WAL and calls `os.replace()`, which is an **atomic file operation** on both POSIX and Windows NTFS.
4. Reopens the newly compacted WAL.

---

## 4. Project Structure

```
03-key-value-store/
├── engine/
│   ├── record.py          # Binary serialization format & CRC32 verification
│   ├── wal.py             # Append-only WAL manager with fsync controls
│   ├── compactor.py       # Snapshot engine with atomic os.replace swapping
│   └── store.py           # KeyValueStore orchestrating MemTable, WAL, & recovery
├── server/
│   ├── protocol.py        # Redis-compatible RESP/inline command parser
│   └── tcp_server.py      # Asyncio TCP socket server (port 6380)
├── cli.py                 # Interactive REPL / command-line client
├── tests/
│   ├── test_engine.py     # Unit tests for CRUD, CRC32 tampering, & compaction
│   └── test_recovery.py   # Crash simulation & torn-write auto-repair tests
├── benchmark.py           # Throughput, latency, & compaction benchmark
├── requirements.txt       # Dependencies
└── README.md              # System design writeup & architecture analysis
```

---

## 5. Running the Project

### Setup Environment
```bash
# Activate virtual environment
.\.venv\Scripts\activate   # Windows
# or: source .venv/bin/activate (macOS/Linux)
```

### Start the TCP Server
```bash
python server/tcp_server.py
```
*Listens on `127.0.0.1:6380`.*

### Connect with the Interactive CLI
In another terminal:
```bash
python cli.py
```
Try running commands:
```text
kv-store:6380> PING
PONG
kv-store:6380> SET username "nikhil"
OK
kv-store:6380> GET username
nikhil
kv-store:6380> STATS
{"active_keys": 1, "wal_size_bytes": 55, ...}
kv-store:6380> COMPACT
COMPACTED before=55B after=55B reduced=0.0%
kv-store:6380> QUIT
```

### Run Test Suite
```bash
python -m unittest discover -s tests -v
```

### Run Storage Engine Benchmark
```bash
python benchmark.py
```
Measures pure write QPS, in-memory read latency (microseconds), disk space reclaimed by compaction, and cold crash recovery speed.
