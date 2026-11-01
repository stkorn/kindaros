import json
from decimal import Decimal
from uuid import UUID

import pytest

from kinda.domain import decode_event, event_id_for, resource, semantic_hash

SAMPLE_HASH = "836bdd4d3773dd795ea1d875c3c3335e5298e3abd23db35c0b353306b5de5dad"


def sample() -> dict:
    return json.loads(resource("sale.example.json"))


def encode(event: dict) -> bytes:
    return json.dumps(event).encode()


def test_sample_decodes_to_canonical_identity_and_decimal_totals() -> None:
    envelope = decode_event(resource("sale.example.json"))
    sale = envelope.sale
    assert envelope.event_id == UUID("80cf28a1-0931-5dd4-98fb-0d0374dfb196") == event_id_for(sale.key)
    assert semantic_hash(sale) == SAMPLE_HASH
    assert (sale.gross_amount, sale.discount_amount, sale.net_amount) == (
        Decimal("130.00"), Decimal("10.00"), Decimal("120.00"))
    assert len(sale.lines) == 2
    assert sum(line.quantity for line in sale.lines) == 3


def test_line_order_and_produced_at_do_not_change_identity() -> None:
    event = sample()
    event["sale"]["lines"].reverse()
    event["produced_at"] = "2026-10-31T00:00:00Z"
    envelope = decode_event(encode(event))
    assert envelope.event_id == UUID(sample()["event_id"])
    assert semantic_hash(envelope.sale) == SAMPLE_HASH


def test_event_id_must_match_business_key() -> None:
    event = sample()
    event["sale"]["receipt_id"] = "demo-001-000002"
    with pytest.raises(ValueError, match="INVALID_EVENT_ID"):
        decode_event(encode(event))
