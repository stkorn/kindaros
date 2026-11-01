from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any

import psycopg

from kinda.bootstrap import bootstrap, kafka_metadata, migration_checksum
from kinda.config import Config
from kinda.domain import decode_event, resource, semantic_hash
from kinda.publisher import publish_sample


def verify_sample(config: Config, timeout: int, partition: int | None = None,
                  offset: int | None = None) -> dict[str, Any]:
    expected = decode_event(resource("sale.example.json"))
    sale = expected.sale
    expected_header = (semantic_hash(sale), sale.gross_amount, sale.discount_amount, sale.net_amount,
                       len(sale.lines), sum(line.quantity for line in sale.lines))
    expected_lines = [(line.line_id, line.product_id, line.quantity, line.unit_price, line.gross_amount,
                       line.discount_amount, line.net_amount, line.tax_amount) for line in sale.lines]
    deadline = time.monotonic() + timeout
    while True:
        with psycopg.connect() as conn:
            header = conn.execute("SELECT semantic_hash,gross_amount,discount_amount,net_amount,"
                                  "(SELECT count(*) FROM kinda.sale_lines l WHERE l.event_id=e.event_id),"
                                  "(SELECT sum(quantity) FROM kinda.sale_lines l WHERE l.event_id=e.event_id) "
                                  "FROM kinda.sale_events e WHERE event_id=%s", (expected.event_id,)).fetchone()
            lines = conn.execute("SELECT line_id,product_id,quantity,unit_price,gross_amount,discount_amount,"
                                 "net_amount,tax_amount FROM kinda.sale_lines WHERE event_id=%s ORDER BY line_id",
                                 (expected.event_id,)).fetchall()
            delivery = None
            if partition is not None and offset is not None:
                delivery = conn.execute("SELECT outcome,canonical_event_id FROM kinda.deliveries "
                                        "WHERE stream_epoch=%s AND topic=%s AND partition_id=%s AND offset_id=%s",
                                        (config.epoch, config.topic, partition, offset)).fetchone()
        if header and (partition is None or delivery):
            if delivery and (delivery[0] not in ("accepted", "duplicate") or delivery[1] != expected.event_id):
                raise ValueError("Sample delivery reconciliation mismatch")
            if tuple(header) != expected_header or lines != expected_lines:
                raise ValueError("Sample reconciliation mismatch")
            return {"verified": True, "event_id": str(expected.event_id), "semantic_hash": header[0],
                    "gross": str(header[1]), "discount": str(header[2]), "net": str(header[3]),
                    "lines": header[4], "units": header[5], "line_details": lines,
                    "delivery_outcome": delivery[0] if delivery else None}
        if time.monotonic() >= deadline:
            raise TimeoutError("Sample not committed before timeout")
        time.sleep(1)


def doctor(config: Config, *, require_stream: bool = True) -> dict[str, Any]:
    metadata = kafka_metadata(config)
    with psycopg.connect() as conn:
        if conn.execute("SELECT checksum FROM public.schema_migrations WHERE version=1").fetchone() != (migration_checksum(),):
            raise ValueError("Migration missing or checksum mismatch")
        if conn.execute("SELECT count(*) FROM kinda.branches").fetchone() != (3,):
            raise ValueError("Branch catalog incomplete")
        if conn.execute("SELECT count(*) FROM kinda.products").fetchone() != (3,):
            raise ValueError("Product catalog incomplete")
        if conn.execute("SELECT stream_epoch FROM kinda.stream_sources WHERE source_name='pos-sales-v1'").fetchone() != (config.epoch,):
            raise ValueError("Stream binding missing")
    if require_stream:
        status_path = config.checkpoint / "kinda-status.json"
        status = json.loads(status_path.read_text())
        os.kill(status["pid"], 0)
        if not status["active"] or not status["progress"] or time.time() - status_path.stat().st_mtime > 30:
            raise ValueError("Stream is not active")
        probe = config.checkpoint / f".doctor-{os.getpid()}"
        probe.write_text("ready")
        probe.unlink()
    return {"doctor": "ready", "stream": require_stream, **metadata}


def exit_code(command: str, exc: Exception) -> int:
    if isinstance(exc, psycopg.Error):
        return 4
    return {"verify-sample": 5, "publish-sample": 3}.get(command, 4)


def main() -> None:
    parser = argparse.ArgumentParser(prog="kinda")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("bootstrap", "publish-sample", "stream", "offline-selftest", "doctor"):
        sub.add_parser(name)
    verify = sub.add_parser("verify-sample")
    verify.add_argument("--timeout", type=int, default=120)
    verify.add_argument("--partition", type=int)
    verify.add_argument("--offset", type=int)
    args = parser.parse_args()
    config = Config.load()
    try:
        result: dict[str, Any] | None
        if args.command == "bootstrap":
            result = bootstrap(config)
        elif args.command == "publish-sample":
            result = publish_sample(config)
        elif args.command == "verify-sample":
            if (args.partition is None) != (args.offset is None):
                parser.error("--partition and --offset must be supplied together")
            result = verify_sample(config, args.timeout, args.partition, args.offset)
        elif args.command == "doctor":
            result = doctor(config)
        else:
            from kinda.streaming import offline_selftest, run_stream
            if args.command == "offline-selftest":
                result = offline_selftest()
            else:
                run_stream(config)
                result = None
        if result is not None:
            print(json.dumps(result, default=str))
    except Exception as exc:
        print(json.dumps({"error": type(exc).__name__, "message": str(exc)}), file=sys.stderr)
        sys.exit(exit_code(args.command, exc))
