"""CLASSIFICATION(ok / error)에 해당하는 토픽을 구독하고 로그 파일에 쌓는다."""

import json
import os

from kafka import KafkaConsumer

from pipeline.config import TOPICS, load_config
from pipeline.generator import now_iso
from pipeline.kafka_utils import connect_with_retry, install_stop_handler


def main() -> None:
    config = load_config()
    classification = os.environ["CLASSIFICATION"]
    topic = TOPICS[classification]
    name = f"{classification}-consumer"
    stop = install_stop_handler()

    consumer = connect_with_retry(
        lambda: KafkaConsumer(
            topic,
            bootstrap_servers=config.bootstrap_servers,
            group_id=name,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        ),
        config.connect_timeout,
        name,
    )
    print(f"[{name}] '{topic}' 구독 시작", flush=True)

    log_path = config.logs_dir / f"{classification}.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    with log_path.open("a", encoding="utf-8") as f:
        while not stop.is_set():
            batches = consumer.poll(timeout_ms=1000)
            for messages in batches.values():
                for msg in messages:
                    record = {
                        **msg.value,
                        "consumed_at": now_iso(),
                        "partition": msg.partition,
                        "offset": msg.offset,
                    }
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    count += 1
                    print(
                        f"[{name}] offset={msg.offset:<5} {record['status']} {record['path']}",
                        flush=True,
                    )
            f.flush()

    consumer.close()
    print(f"[{name}] 종료 (이번 실행에서 {count}건 저장)", flush=True)


if __name__ == "__main__":
    main()
