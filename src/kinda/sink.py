from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from kinda.config import Config
from kinda.domain import canonical_key, decode_event, sale_object, semantic_hash


@dataclass(frozen=True)
class KafkaRecord:
    topic: str
    partition: int
    offset: int
    key: bytes
    value: bytes
    timestamp: datetime | None


WRITER_LOCK = (1263092801, 1)


class PostgresSink:
    def __init__(self, config: Config):
        self.config = config
        self.conn = psycopg.connect(autocommit=True)
        locked = self.conn.execute("SELECT pg_try_advisory_lock(%s, %s)", WRITER_LOCK).fetchone()
        if locked != (True,):
            self.conn.close()
            raise ValueError("Another ledger writer is active")
        binding = self.conn.execute("SELECT stream_epoch FROM kinda.stream_sources WHERE source_name='pos-sales-v1'").fetchone()
        if binding != (config.epoch,):
            self.close()
            raise ValueError("Stream epoch mismatch")

    def close(self) -> None:
        self.conn.close()

    def apply(self, records: Iterable[KafkaRecord]) -> dict[str, int]:
        report = {"accepted": 0, "duplicate": 0, "skipped_deliveries": 0}
        with self.conn.transaction():
            for record in records:
                seen = self.conn.execute("SELECT 1 FROM kinda.deliveries WHERE stream_epoch=%s "
                                         "AND topic=%s AND partition_id=%s AND offset_id=%s",
                                         (self.config.epoch, record.topic, record.partition, record.offset)).fetchone()
                if seen:
                    report["skipped_deliveries"] += 1
                    continue
                envelope = decode_event(record.value)
                sale = envelope.sale
                if record.key != canonical_key(sale.key).encode():
                    raise ValueError("INVALID_KAFKA_KEY")
                digest = semantic_hash(sale)
                existing = self.conn.execute("SELECT semantic_hash FROM kinda.sale_events WHERE event_id=%s",
                                             (envelope.event_id,)).fetchone()
                outcome = "duplicate" if existing else "accepted"
                if existing and existing[0] != digest:
                    raise ValueError("IDENTITY_CONFLICT")
                if existing is None:
                    data: dict[str, Any] = sale_object(sale)
                    header = {name: data[name] for name in data if name != "lines"}
                    header.update(event_id=envelope.event_id, semantic_hash=digest, canonical_sale=Jsonb(data))
                    names = list(header)
                    self.conn.execute(sql.SQL("INSERT INTO kinda.sale_events ({}) VALUES ({})").format(
                        sql.SQL(",").join(map(sql.Identifier, names)), sql.SQL(",").join(sql.Placeholder() for _ in names)),
                        [header[name] for name in names])
                    for line in sale.lines:
                        self.conn.execute("INSERT INTO kinda.sale_lines VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                          (envelope.event_id, line.line_id, line.product_id, line.quantity,
                                           line.unit_price, line.gross_amount, line.discount_amount,
                                           line.net_amount, line.tax_amount))
                self.conn.execute("INSERT INTO kinda.deliveries "
                                  "(stream_epoch,topic,partition_id,offset_id,kafka_timestamp,kafka_key,raw_value,outcome,canonical_event_id) "
                                  "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                  (self.config.epoch, record.topic, record.partition, record.offset,
                                   record.timestamp, record.key, record.value, outcome, envelope.event_id))
                report[outcome] += 1
        return report
