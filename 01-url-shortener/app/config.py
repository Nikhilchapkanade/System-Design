"""Configuration settings for Distributed URL Shortener."""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# Service settings
BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000")
DATABASE_PATH = os.getenv("DATABASE_PATH", str(BASE_DIR / "url_shortener.db"))

# Key Generation Service (KGS) Range settings
# In a distributed deployment, each node is provisioned with a distinct range via env vars or Zookeeper/etcd
KGS_RANGE_START = int(os.getenv("KGS_RANGE_START", "10000000"))  # Base62 produces ~5-6 char strings
KGS_RANGE_SIZE = int(os.getenv("KGS_RANGE_SIZE", "1000000"))     # 1 million IDs allocated per worker block

# Cache settings
CACHE_MAX_ENTRIES = int(os.getenv("CACHE_MAX_ENTRIES", "10000"))
CACHE_DEFAULT_TTL_SECONDS = int(os.getenv("CACHE_DEFAULT_TTL_SECONDS", "86400"))  # 24 hours

# Rate limiting settings (Sliding Window)
RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "20"))
RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60"))
