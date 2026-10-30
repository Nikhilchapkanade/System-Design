"""Command Protocol Parser and Formatter for Key-Value Store.

Lightweight Redis-compatible text protocol:
- +<str>\\r\\n : Simple string (e.g. +OK, +PONG)
- -<err>\\r\\n : Error message
- :<int>\\r\\n : Integer (e.g. :1, :0)
- $<len>\\r\\n<val>\\r\\n : Bulk string
- $-1\\r\\n : Null response (key not found)
"""

import json
from typing import List, Optional, Tuple
from engine.store import KeyValueStore


class CommandParser:
    """Parses raw text command lines into action invocations."""

    @staticmethod
    def parse_line(line: str) -> List[str]:
        """Split line respecting quotes for multi-word values."""
        parts = []
        current = []
        in_quote = False
        quote_char = ""

        for char in line.strip():
            if char in ('"', "'"):
                if in_quote and char == quote_char:
                    in_quote = False
                elif not in_quote:
                    in_quote = True
                    quote_char = char
                else:
                    current.append(char)
            elif char.isspace() and not in_quote:
                if current:
                    parts.append("".join(current))
                    current = []
            else:
                current.append(char)

        if current:
            parts.append("".join(current))
        return parts

    @staticmethod
    def execute(store: KeyValueStore, parts: List[str]) -> Tuple[bytes, bool]:
        """Execute command against store. Returns (response_bytes, should_close)."""
        if not parts:
            return b"-ERR empty command\r\n", False

        cmd = parts[0].upper()

        if cmd == "PING":
            msg = parts[1] if len(parts) > 1 else "PONG"
            return f"+{msg}\r\n".encode("utf-8"), False

        elif cmd == "SET":
            if len(parts) < 3:
                return b"-ERR wrong number of arguments for 'set' command\r\n", False
            key = parts[1]
            val = " ".join(parts[2:])  # Support multi-word values
            store.set(key, val)
            return b"+OK\r\n", False

        elif cmd == "GET":
            if len(parts) != 2:
                return b"-ERR wrong number of arguments for 'get' command\r\n", False
            val = store.get(parts[1])
            if val is None:
                return b"$-1\r\n", False
            val_bytes = val.encode("utf-8")
            return f"${len(val_bytes)}\r\n{val}\r\n".encode("utf-8"), False

        elif cmd == "DEL":
            if len(parts) != 2:
                return b"-ERR wrong number of arguments for 'del' command\r\n", False
            deleted = store.delete(parts[1])
            return f":{1 if deleted else 0}\r\n".encode("utf-8"), False

        elif cmd == "EXISTS":
            if len(parts) != 2:
                return b"-ERR wrong number of arguments for 'exists' command\r\n", False
            exists = store.exists(parts[1])
            return f":{1 if exists else 0}\r\n".encode("utf-8"), False

        elif cmd == "KEYS":
            keys = store.keys()
            body = " ".join(keys)
            return f"${len(body.encode('utf-8'))}\r\n{body}\r\n".encode("utf-8"), False

        elif cmd == "COMPACT":
            old_size, new_size = store.compact()
            ratio = round((1 - (new_size / old_size)) * 100, 1) if old_size > 0 else 0.0
            return f"+COMPACTED before={old_size}B after={new_size}B reduced={ratio}%\r\n".encode("utf-8"), False

        elif cmd == "STATS":
            stats = store.get_stats()
            raw_json = json.dumps(stats)
            return f"${len(raw_json.encode('utf-8'))}\r\n{raw_json}\r\n".encode("utf-8"), False

        elif cmd == "QUIT":
            return b"+OK Bye\r\n", True

        else:
            return f"-ERR unknown command '{cmd}'\r\n".encode("utf-8"), False
