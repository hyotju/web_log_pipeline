"""웹 로그 이벤트를 만들고, 판정 결과에 따라 ok / error 토픽으로 보낸다."""

import json
import random

from kafka import KafkaProducer

from pipeline.config import TOPICS, load_config
from pipeline.generator import make_event
from pipeline.kafka_utils import connect_with_retry, install_stop_handler
from pipeline.rules import classify


def main() -> None:
    config = load_config()
    stop = install_stop_handler()

    producer = connect_with_retry(
        lambda: KafkaProducer(
            bootstrap_servers=config.bootstrap_servers,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            acks="all",
        ),
        config.connect_timeout,
        "producer",
    )

    rng = random.Random()
    sent = 0
    while not stop.is_set():
        if config.max_events and sent >= config.max_events:
            break

        fault = config.fault_flag.exists()
        event = make_event(rng, fault, config.error_rate, config.fault_error_rate)
        event["classification"] = classify(event)
        topic = TOPICS[event["classification"]]

        producer.send(topic, key=event["path"], value=event).get(timeout=10)
        sent += 1

        mark = "  ⚠ 장애 모드" if fault else ""
        print(
            f"[producer] #{sent:<5} {event['status']} {event['method']:<4} {event['path']:<17} "
            f"{event['latency_ms']:>5}ms -> {topic}{mark}",
            flush=True,
        )
        stop.wait(config.producer_interval)

    producer.flush()
    producer.close()
    print(f"[producer] 종료 (총 {sent}건 전송)", flush=True)


if __name__ == "__main__":
    main()
