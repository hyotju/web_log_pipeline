"""Kafka 연결 재시도와 종료 신호 처리."""

import signal
import threading
import time

from kafka.errors import NoBrokersAvailable


def connect_with_retry(factory, timeout_seconds: float, name: str):
    """Kafka가 아직 준비되지 않았으면 2초마다 다시 연결한다."""
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            client = factory()
            print(f"[{name}] Kafka 연결 완료", flush=True)
            return client
        except NoBrokersAvailable:
            if time.monotonic() > deadline:
                raise
            print(f"[{name}] Kafka 연결 대기 중...", flush=True)
            time.sleep(2)


def install_stop_handler() -> threading.Event:
    """Ctrl+C, docker compose stop 시 루프를 깔끔하게 끝내기 위한 이벤트."""
    stop = threading.Event()

    def _handle(signum, frame):
        stop.set()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)
    return stop
