"""Binary Frame Record Format and CRC32 Checksum Validation.

System Design Analysis (Bitcask / WAL Record Format):
To guarantee durability and crash resilience, every disk record is framed with
a strict binary header including a CRC32 checksum. If a process terminates
abruptly during disk write, the trailing torn record will fail CRC32 and be
safely rejected without corrupting preceding data.

Binary Layout (19-byte Header + Variable Payload):
+----------------+--------------------+---------------+----------------+----------------+---------+-----------+
| CRC32 (4B)     | Timestamp (8B)     | OpType (1B)   | KeyLen (2B)    | ValLen (4B)    | Key     | Value     |
| Big-Endian uint| Unix Epoch (double)| 1=SET, 2=DEL  | Big-Endian u16 | Big-Endian u32 | Raw UTF8| Raw UTF8  |
+----------------+--------------------+---------------+----------------+----------------+---------+-----------+
"""

import struct
import time
import zlib
from enum import IntEnum
from typing import Optional, Tuple


class OpType(IntEnum):
    SET = 1
    DELETE = 2  # Tombstone record


# struct format without CRC: Timestamp(d:8), OpType(B:1), KeyLen(H:2), ValLen(I:4)
HEADER_PAYLOAD_STRUCT = struct.Struct(">d B H I")
HEADER_SIZE = 4 + HEADER_PAYLOAD_STRUCT.size  # 4 (CRC) + 15 = 19 bytes


class Record:
    __slots__ = ("op_type", "key", "value", "timestamp")

    def __init__(
        self,
        op_type: OpType,
        key: str,
        value: Optional[str] = None,
        timestamp: Optional[float] = None,
    ):
        self.op_type = op_type
        self.key = key
        self.value = value if value is not None else ""
        self.timestamp = timestamp if timestamp is not None else time.time()

    def serialize(self) -> bytes:
        """Encode record to binary bytes with CRC32."""
        key_bytes = self.key.encode("utf-8")
        val_bytes = self.value.encode("utf-8")

        key_len = len(key_bytes)
        val_len = len(val_bytes)

        if key_len > 65535:
            raise ValueError(f"Key length exceeds maximum 65535 bytes: {key_len}")

        # Pack payload metadata
        meta = HEADER_PAYLOAD_STRUCT.pack(
            self.timestamp,
            int(self.op_type),
            key_len,
            val_len,
        )
        payload = meta + key_bytes + val_bytes

        # Calculate CRC32 over the payload
        checksum = zlib.crc32(payload) & 0xFFFFFFFF
        crc_bytes = struct.pack(">I", checksum)

        return crc_bytes + payload

    @classmethod
    def deserialize_from_stream(cls, stream) -> Optional["Record"]:
        """Read and validate a record from a file stream.
        
        Returns None on clean EOF, or raises ValueError if corrupted.
        """
        crc_raw = stream.read(4)
        if not crc_raw or len(crc_raw) < 4:
            return None  # Clean EOF

        expected_crc = struct.unpack(">I", crc_raw)[0]

        meta_raw = stream.read(HEADER_PAYLOAD_STRUCT.size)
        if len(meta_raw) < HEADER_PAYLOAD_STRUCT.size:
            raise ValueError("Corrupt WAL: Incomplete header detected (torn write).")

        timestamp, op_val, key_len, val_len = HEADER_PAYLOAD_STRUCT.unpack(meta_raw)

        body_raw = stream.read(key_len + val_len)
        if len(body_raw) < (key_len + val_len):
            raise ValueError("Corrupt WAL: Truncated payload body (torn write).")

        # Verify CRC32
        payload = meta_raw + body_raw
        actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
        if actual_crc != expected_crc:
            raise ValueError(f"CRC32 Checksum mismatch! Expected {expected_crc:#x}, got {actual_crc:#x}.")

        key = body_raw[:key_len].decode("utf-8")
        value = body_raw[key_len:].decode("utf-8")

        return cls(
            op_type=OpType(op_val),
            key=key,
            value=value if op_val == OpType.SET else None,
            timestamp=timestamp,
        )
