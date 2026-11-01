from __future__ import annotations

from typing import Any

from confluent_kafka import KafkaError, Message, Producer

from kinda.config import Config
from kinda.domain import canonical_key, decode_event, resource, semantic_hash


def publish_sample(config: Config) -> dict[str, Any]:
    raw = resource("sale.example.json")
    envelope = decode_event(raw)
    producer = Producer({"bootstrap.servers": config.brokers, "enable.idempotence": True,
                         "acks": "all", "max.in.flight.requests.per.connection": 5,
                         "delivery.timeout.ms": 120000, "linger.ms": 10})
    delivered: list[dict[str, Any]] = []
    errors: list[str] = []

    def callback(error: KafkaError | None, message: Message) -> None:
        if error:
            errors.append(str(error))
        else:
            delivered.append({"partition": message.partition(), "offset": message.offset()})

    producer.produce(config.topic, value=raw, key=canonical_key(envelope.sale.key).encode(), on_delivery=callback)
    remaining = producer.flush(125)
    if remaining or errors or len(delivered) != 1:
        raise RuntimeError(f"Kafka delivery failed/unknown: remaining={remaining}, errors={errors}")
    return {"acknowledged": 1, "event_id": str(envelope.event_id),
            "semantic_hash": semantic_hash(envelope.sale), **delivered[0]}
