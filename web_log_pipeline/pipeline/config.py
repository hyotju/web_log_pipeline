"""환경 변수로 실습 설정을 읽는다. 값은 docker-compose.yml / .env에서 바꾼다."""

import os
from dataclasses import dataclass
from pathlib import Path

TOPICS = {
    "ok": "web.logs.ok",
    "error": "web.logs.error",
}


@dataclass(frozen=True)
class Config:
    bootstrap_servers: str
    output_dir: Path
    producer_interval: float
    max_events: int
    error_rate: float
    fault_error_rate: float
    connect_timeout: float

    @property
    def fault_flag(self) -> Path:
        """이 파일이 있으면 producer가 '주문 API 장애' 상황을 흉내 낸다."""
        return self.output_dir / "control" / "fault_mode"

    @property
    def logs_dir(self) -> Path:
        return self.output_dir / "logs"


def load_config() -> Config:
    return Config(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        output_dir=Path(os.getenv("OUTPUT_DIR", "/workspace/pipeline_output")),
        producer_interval=float(os.getenv("PRODUCER_INTERVAL_SECONDS", "0.5")),
        max_events=int(os.getenv("MAX_EVENTS", "0")),
        error_rate=float(os.getenv("ERROR_RATE", "0.03")),
        fault_error_rate=float(os.getenv("FAULT_ERROR_RATE", "0.8")),
        connect_timeout=float(os.getenv("KAFKA_CONNECT_TIMEOUT_SECONDS", "90")),
    )
