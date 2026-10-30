"""Configuration settings for Distributed Task Queue."""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

PORT = int(os.getenv("PORT", "8000"))
DATABASE_PATH = os.getenv("DATABASE_PATH", str(BASE_DIR / "tasks.db"))

# Worker Pool configuration
DEFAULT_WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "4"))

# Retry policy defaults
DEFAULT_MAX_RETRIES = int(os.getenv("DEFAULT_MAX_RETRIES", "3"))
BACKOFF_BASE_SECONDS = float(os.getenv("BACKOFF_BASE_SECONDS", "1.0"))
BACKOFF_MAX_SECONDS = float(os.getenv("BACKOFF_MAX_SECONDS", "60.0"))

# Scheduler loop intervals
SCHEDULER_INTERVAL_SECONDS = float(os.getenv("SCHEDULER_INTERVAL", "0.25"))
