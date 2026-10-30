"""Asynchronous TCP Server for Key-Value Store.

System Design Analysis (Async I/O Concurrency):
Uses an event-driven non-blocking socket model (asyncio / epoll / IOCP on Windows)
capable of handling thousands of concurrent client TCP connections on a single thread.
"""

import asyncio
import os
from typing import Optional
from engine.store import KeyValueStore
from server.protocol import CommandParser


class TCPServer:
    """Asyncio TCP Server serving the Key-Value Store protocol."""

    def __init__(self, store: KeyValueStore, host: str = "127.0.0.1", port: int = 6380):
        self.store = store
        self.host = host
        self.port = port
        self.server: Optional[asyncio.Server] = None

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info("peername")
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break  # Client disconnected

                text = line.decode("utf-8", errors="replace")
                parts = CommandParser.parse_line(text)
                if not parts:
                    continue

                response_bytes, should_close = CommandParser.execute(self.store, parts)
                writer.write(response_bytes)
                await writer.drain()

                if should_close:
                    break
        except Exception:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def start(self):
        self.server = await asyncio.start_server(self.handle_client, self.host, self.port)
        print(f"[*] KV-Store TCP Server listening on {self.host}:{self.port}")
        async with self.server:
            await self.server.serve_forever()

    def stop(self):
        if self.server:
            self.server.close()


def run_server(data_dir: str = "data", host: str = "127.0.0.1", port: int = 6380, sync_always: bool = True):
    """Entrypoint to launch the standalone TCP server."""
    store = KeyValueStore(data_dir=data_dir, sync_always=sync_always)
    server = TCPServer(store=store, host=host, port=port)
    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        print("\n[*] Shutting down server...")
    finally:
        store.close()


if __name__ == "__main__":
    run_server()
