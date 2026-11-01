from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from importlib.resources import files
from typing import Any
from uuid import UUID, uuid5
from zoneinfo import ZoneInfo

from jsonschema import Draft202012Validator, FormatChecker

NAMESPACE = UUID("2b210905-93dd-4c7a-9473-382777ecbb49")
MONEY_FIELDS = ("gross_amount", "discount_amount", "net_amount", "tax_amount")


def resource(name: str) -> bytes:
    return files("kinda").joinpath("resources", name).read_bytes()


@dataclass(frozen=True)
class SaleKey:
    source_system: str
    source_instance: str
    branch_id: str
    business_date: date
    receipt_id: str


@dataclass(frozen=True)
class SaleLine:
    line_id: str
    product_id: str
    quantity: int
    unit_price: Decimal
    gross_amount: Decimal
    discount_amount: Decimal
    net_amount: Decimal
    tax_amount: Decimal


@dataclass(frozen=True)
class CompletedSale:
    key: SaleKey
    occurred_at: datetime
    currency: str
    tax_mode: str
    gross_amount: Decimal
    discount_amount: Decimal
    net_amount: Decimal
    tax_amount: Decimal
    lines: tuple[SaleLine, ...]


@dataclass(frozen=True)
class SaleEnvelope:
    event_id: UUID
    produced_at: datetime
    sale: CompletedSale


def canonical_key(key: SaleKey) -> str:
    return json.dumps([key.source_system, key.source_instance, key.branch_id,
                       key.business_date.isoformat(), key.receipt_id],
                      ensure_ascii=False, separators=(",", ":"))


def event_id_for(key: SaleKey) -> UUID:
    return uuid5(NAMESPACE, canonical_key(key))


def sale_object(sale: CompletedSale) -> dict[str, Any]:
    result = asdict(sale)
    result.update(result.pop("key"))
    result["business_date"] = sale.key.business_date.isoformat()
    result["occurred_at"] = sale.occurred_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    result["lines"] = sorted(result["lines"], key=lambda line: line["line_id"])
    for obj in [result, *result["lines"]]:
        for name in (*MONEY_FIELDS, "unit_price"):
            if name in obj:
                obj[name] = format(obj[name], ".2f")
    return result


def semantic_hash(sale: CompletedSale) -> str:
    raw = json.dumps(sale_object(sale), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


def unique_properties(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, value in pairs:
        if name in result:
            raise ValueError(f"Duplicate JSON property {name}")
        result[name] = value
    return result


def decode_event(raw: bytes) -> SaleEnvelope:
    if len(raw) > 131072:
        raise ValueError("Event exceeds 128 KiB")
    obj = json.loads(raw.decode("utf-8"), object_pairs_hook=unique_properties)
    Draft202012Validator(json.loads(resource("pos-event-v1.schema.json")),
                         format_checker=FormatChecker()).validate(obj)
    data = obj["sale"]
    key = SaleKey(data["source_system"], data["source_instance"], data["branch_id"],
                  date.fromisoformat(data["business_date"]), data["receipt_id"])
    occurred = datetime.fromisoformat(data["occurred_at"])
    if occurred.astimezone(ZoneInfo("Asia/Bangkok")).date() != key.business_date:
        raise ValueError("Business date differs from Bangkok sale date")
    lines = tuple(SaleLine(line["line_id"], line["product_id"], line["quantity"],
                           Decimal(line["unit_price"]),
                           Decimal(line["gross_amount"]), Decimal(line["discount_amount"]),
                           Decimal(line["net_amount"]), Decimal(line["tax_amount"])) for line in data["lines"])
    sale = CompletedSale(key, occurred, data["currency"], data["tax_mode"],
                         Decimal(data["gross_amount"]), Decimal(data["discount_amount"]),
                         Decimal(data["net_amount"]), Decimal(data["tax_amount"]), lines)
    if len({line.line_id for line in lines}) != len(lines):
        raise ValueError("Duplicate line ID")
    for line in lines:
        if line.gross_amount != line.quantity * line.unit_price:
            raise ValueError("Line gross mismatch")
    items: tuple[CompletedSale | SaleLine, ...] = (sale, *lines)
    for item in items:
        if not (Decimal(0) <= item.discount_amount <= item.gross_amount
                and item.net_amount == item.gross_amount - item.discount_amount
                and Decimal(0) <= item.tax_amount <= item.net_amount):
            raise ValueError("Sale arithmetic mismatch")
        if sale.tax_mode == "not_provided" and item.tax_amount != 0:
            raise ValueError("Tax detail not provided")
    for name in MONEY_FIELDS:
        if getattr(sale, name) != sum(getattr(line, name) for line in lines):
            raise ValueError("Header totals differ from lines")
    event_id = UUID(obj["event_id"])
    if event_id != event_id_for(key):
        raise ValueError("INVALID_EVENT_ID")
    return SaleEnvelope(event_id, datetime.fromisoformat(obj["produced_at"]), sale)
