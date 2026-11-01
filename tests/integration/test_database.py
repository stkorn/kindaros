from uuid import UUID, uuid4

import psycopg
import pytest

SAMPLE = UUID("80cf28a1-0931-5dd4-98fb-0d0374dfb196")


def insert_header(conn: psycopg.Connection, event_id: UUID) -> None:
    conn.execute("INSERT INTO kinda.sale_events "
                 "(event_id,source_system,source_instance,branch_id,business_date,receipt_id,occurred_at,"
                 "currency,tax_mode,gross_amount,discount_amount,net_amount,tax_amount,semantic_hash,canonical_sale) "
                 "SELECT %s,source_system,source_instance,branch_id,business_date,%s,occurred_at,"
                 "currency,tax_mode,gross_amount,discount_amount,net_amount,tax_amount,semantic_hash,canonical_sale "
                 "FROM kinda.sale_events WHERE event_id=%s", (event_id, str(event_id), SAMPLE))


def assert_absent(event_id: UUID) -> None:
    with psycopg.connect() as conn:
        assert conn.execute("SELECT count(*) FROM kinda.sale_events WHERE event_id=%s", (event_id,)).fetchone() == (0,)
        assert conn.execute("SELECT count(*) FROM kinda.sale_lines WHERE event_id=%s", (event_id,)).fetchone() == (0,)


def test_header_without_lines_cannot_commit() -> None:
    event_id = uuid4()
    with pytest.raises(psycopg.errors.CheckViolation, match="SALE_TOTAL_MISMATCH"):
        with psycopg.connect() as conn:
            insert_header(conn, event_id)
    assert_absent(event_id)


def test_partial_lines_roll_back_the_entire_receipt() -> None:
    event_id = uuid4()
    with pytest.raises(psycopg.errors.CheckViolation, match="SALE_TOTAL_MISMATCH"):
        with psycopg.connect() as conn:
            insert_header(conn, event_id)
            conn.execute("INSERT INTO kinda.sale_lines SELECT %s,line_id,product_id,quantity,unit_price,"
                         "gross_amount,discount_amount,net_amount,tax_amount FROM kinda.sale_lines "
                         "WHERE event_id=%s AND line_id='1'", (event_id, SAMPLE))
    assert_absent(event_id)


def test_zero_value_line_cannot_be_appended_to_committed_receipt() -> None:
    with pytest.raises(psycopg.errors.CheckViolation, match="IMMUTABLE_RECEIPT"):
        with psycopg.connect() as conn:
            conn.execute("INSERT INTO kinda.sale_lines VALUES (%s,'late-zero','BREAD_VANILLA',1,0,0,0,0,0)", (SAMPLE,))
    with psycopg.connect() as conn:
        assert conn.execute("SELECT count(*) FROM kinda.sale_lines WHERE event_id=%s", (SAMPLE,)).fetchone() == (2,)


@pytest.mark.parametrize("statement", [
    "UPDATE kinda.sale_events SET net_amount=net_amount",
    "DELETE FROM kinda.sale_events",
    "TRUNCATE kinda.sale_events CASCADE",
    "UPDATE kinda.sale_lines SET quantity=quantity",
    "DELETE FROM kinda.sale_lines",
    "UPDATE kinda.deliveries SET outcome=outcome",
    "UPDATE kinda.stream_sources SET topic=topic",
    "INSERT INTO kinda.branches VALUES ('BR999','invalid','Asia/Bangkok')",
    "CREATE TABLE kinda.unwanted (n integer)",
    "CREATE TABLE public.unwanted (n integer)",
])
def test_runtime_role_cannot_mutate_accepted_sales_or_schema(statement: str) -> None:
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        with psycopg.connect() as conn:
            conn.execute(statement)


def test_runtime_role_has_no_admin_attributes() -> None:
    with psycopg.connect() as conn:
        assert conn.execute("SELECT current_user,rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls "
                            "FROM pg_roles WHERE rolname=current_user").fetchone() == (
                                "kinda_app", False, False, False, False, False)
