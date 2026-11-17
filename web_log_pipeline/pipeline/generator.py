"""가짜 웹 서버 access log 이벤트를 만든다."""

import random
import uuid
from datetime import datetime, timezone

# (method, path, 가중치) — 가중치가 클수록 자주 호출되는 API
ENDPOINTS = [
    ("GET", "/", 3),
    ("GET", "/products", 4),
    ("GET", "/products/detail", 3),
    ("GET", "/cart", 2),
    ("POST", "/orders", 2),
    ("POST", "/login", 2),
]

# 장애 모드에서 문제가 생기는 API
FAULTY_PATH = "/orders"

OK_STATUSES = [200] * 12 + [201, 204, 301, 304, 400, 401, 404]
ERROR_STATUSES = [500, 502, 503, 504]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def make_event(rng: random.Random, fault: bool, error_rate: float, fault_error_rate: float) -> dict:
    method, path, _ = rng.choices(ENDPOINTS, weights=[w for *_, w in ENDPOINTS])[0]

    broken = fault and path == FAULTY_PATH
    is_error = rng.random() < (fault_error_rate if broken else error_rate)

    if is_error:
        status = rng.choice(ERROR_STATUSES)
        latency_ms = rng.randint(800, 5000)
    else:
        status = rng.choice(OK_STATUSES)
        latency_ms = rng.randint(20, 300) if not broken else rng.randint(500, 2000)

    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": now_iso(),
        "method": method,
        "path": path,
        "status": status,
        "latency_ms": latency_ms,
        "client_ip": f"10.0.{rng.randint(0, 255)}.{rng.randint(1, 254)}",
    }
