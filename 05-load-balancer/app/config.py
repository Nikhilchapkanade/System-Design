"""Configuration settings for Load Balancer and Backend Pool."""

import os

PORT = int(os.getenv("PORT", "8000"))

# Default Backend Servers (URL, Weight)
DEFAULT_BACKENDS = [
    ("http://127.0.0.1:8001", 3),
    ("http://127.0.0.1:8002", 1),
    ("http://127.0.0.1:8003", 1),
]

# Health checking configuration
HEALTH_CHECK_INTERVAL_SECONDS = float(os.getenv("HEALTH_CHECK_INTERVAL", "2.0"))
HEALTH_CHECK_TIMEOUT_SECONDS = float(os.getenv("HEALTH_CHECK_TIMEOUT", "1.0"))
HEALTH_CHECK_UNHEALTHY_THRESHOLD = int(os.getenv("UNHEALTHY_THRESHOLD", "2"))
HEALTH_CHECK_HEALTHY_THRESHOLD = int(os.getenv("HEALTHY_THRESHOLD", "2"))

# Proxy retry limit on connection failure
PROXY_MAX_RETRIES = int(os.getenv("PROXY_MAX_RETRIES", "3"))
