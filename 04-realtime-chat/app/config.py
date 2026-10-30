"""Configuration settings for Real-Time Chat Service."""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

PORT = int(os.getenv("PORT", "8000"))
NODE_ID = os.getenv("NODE_ID", "node-alpha")
DATABASE_PATH = os.getenv("DATABASE_PATH", str(BASE_DIR / "chat_history.db"))
HEARTBEAT_INTERVAL_SECONDS = int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "30"))
