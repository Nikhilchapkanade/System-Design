"""Interactive REPL and CLI Client for Key-Value Store."""

import socket
import sys


class KVClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 6380):
        self.host = host
        self.port = port
        self.sock = None

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))

    def send_cmd(self, command: str) -> str:
        if not self.sock:
            self.connect()

        self.sock.sendall(f"{command.strip()}\r\n".encode("utf-8"))
        
        # Read response
        response = self.sock.recv(4096).decode("utf-8", errors="replace")
        return response

    def close(self):
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass


def start_repl(host: str = "127.0.0.1", port: int = 6380):
    client = KVClient(host, port)
    try:
        client.connect()
        print(f"Connected to Key-Value Store at {host}:{port}")
        print("Type commands (e.g. SET foo bar, GET foo, COMPACT, STATS, QUIT)\n")

        while True:
            try:
                user_input = input(f"kv-store:{port}> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break

            if not user_input:
                continue

            if user_input.upper() in ("QUIT", "EXIT"):
                break

            res = client.send_cmd(user_input)
            print(res.strip())

    except ConnectionRefusedError:
        print(f"Error: Could not connect to server at {host}:{port}. Is the server running?")
    finally:
        client.close()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # One-off command mode: python cli.py "GET mykey"
        cmd = " ".join(sys.argv[1:])
        c = KVClient()
        try:
            print(c.send_cmd(cmd).strip())
        finally:
            c.close()
    else:
        start_repl()
