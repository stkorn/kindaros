"""Drives the public CLI against the running Compose stack from the host."""

import json
import os
import subprocess

import pytest
from confluent_kafka.admin import AdminClient

SAMPLE = "80cf28a1-0931-5dd4-98fb-0d0374dfb196"
TOPIC = "kinda.pos.sales.v1"


def compose(*args: str, env: dict[str, str] | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["docker", "compose", *args], capture_output=True, text=True, check=check,
                          env={**os.environ, **(env or {})}, timeout=300)


def kinda(*args: str) -> dict:
    return json.loads(compose("exec", "-T", "stream", "kinda", *args).stdout)


def admin_sql(query: str) -> str:
    return compose("exec", "-T", "postgres", "psql", "-U", "kinda_admin", "-d", "kinda",
                   "-AtXqc", query).stdout.strip()


def ledger_counts() -> str:
    return admin_sql("SELECT (SELECT count(*) FROM kinda.sale_events)||'/'||(SELECT count(*) FROM kinda.sale_lines)"
                     "||'/'||(SELECT coalesce(sum(net_amount),0) FROM kinda.sale_events)")


def publish_and_verify() -> dict:
    ack = kinda("publish-sample")
    assert ack["acknowledged"] == 1 and ack["event_id"] == SAMPLE
    return kinda("verify-sample", "--partition", str(ack["partition"]), "--offset", str(ack["offset"]),
                 "--timeout", "90")


def test_sample_receipt_is_committed_with_matching_header_and_lines() -> None:
    result = publish_and_verify()
    assert result["verified"] is True
    assert (result["event_id"], result["gross"], result["discount"], result["net"]) == (
        SAMPLE, "130.00", "10.00", "120.00")
    assert (result["lines"], result["units"]) == (2, 3)
    assert result["delivery_outcome"] in ("accepted", "duplicate")


def test_republishing_the_sample_does_not_change_the_ledger() -> None:
    publish_and_verify()
    before = ledger_counts()
    assert publish_and_verify()["delivery_outcome"] == "duplicate"
    assert ledger_counts() == before == "1/2/120.00"


def test_doctor_reports_ready_stack_on_container_listener() -> None:
    report = kinda("doctor")
    assert report["doctor"] == "ready" and report["stream"] is True
    assert report["partitions"] == 3
    assert report["brokers"] == [{"host": "kafka", "port": 29092}]


def test_host_client_uses_host_advertised_listener() -> None:
    metadata = AdminClient({"bootstrap.servers": "localhost:9092"}).list_topics(timeout=15)
    assert [(b.host, b.port) for b in metadata.brokers.values()] == [("localhost", 9092)]
    topic = metadata.topics[TOPIC]
    assert len(topic.partitions) == 3
    assert all(len(p.replicas) == 1 for p in topic.partitions.values())


def test_kafka_is_a_single_combined_kraft_node() -> None:
    status = compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-metadata-quorum.sh",
                     "--bootstrap-server", "localhost:29092", "describe", "--status").stdout
    assert "LeaderId:               1" in status
    assert "CurrentVoters:          [{\"id\": 1" in status
    roles = compose("exec", "-T", "kafka", "grep", "^process.roles=", "/opt/kinda/server.properties").stdout
    assert roles.strip() == "process.roles=broker,controller"


def seed_state() -> str:
    return admin_sql("SELECT (SELECT string_agg(version||':'||checksum, ',') FROM public.schema_migrations)||'/'||"
                     "(SELECT count(*) FROM kinda.branches)||'/'||(SELECT count(*) FROM kinda.products)||'/'||"
                     "(SELECT count(*) FROM kinda.stream_sources)")


def test_bootstrap_rerun_changes_nothing() -> None:
    before = seed_state()
    compose("run", "--rm", "--no-deps", "bootstrap")
    assert seed_state() == before
    assert before.split("/")[1:] == ["3", "3", "1"]


def test_bootstrap_rejects_migration_checksum_mismatch() -> None:
    checksum = admin_sql("SELECT checksum FROM public.schema_migrations WHERE version=1")
    admin_sql("UPDATE public.schema_migrations SET checksum='tampered' WHERE version=1")
    try:
        result = compose("run", "--rm", "--no-deps", "bootstrap", check=False)
    finally:
        admin_sql(f"UPDATE public.schema_migrations SET checksum='{checksum}' WHERE version=1")
    assert result.returncode == 4
    assert "Migration checksum mismatch" in result.stderr


def test_bootstrap_rejects_stream_epoch_mismatch() -> None:
    result = compose("run", "--rm", "--no-deps", "-e", "STREAM_EPOCH=00000000-0000-4000-8000-000000000000",
                     "bootstrap", check=False)
    assert result.returncode == 4
    assert "Kafka source binding mismatch" in result.stderr


@pytest.mark.parametrize("statement", ["UPDATE kinda.sale_events SET net_amount=0",
                                       "DELETE FROM kinda.sale_lines"])
def test_stream_runtime_role_cannot_modify_accepted_sales(statement: str) -> None:
    result = compose("exec", "-T", "stream", "python", "-c",
                     f"import psycopg; psycopg.connect().execute({statement!r})", check=False)
    assert result.returncode != 0 and "InsufficientPrivilege" in result.stderr


def test_doctor_checks_dependencies_before_the_stream_starts() -> None:
    report = json.loads(compose("run", "--rm", "--no-deps", "-T", "stream", "kinda", "doctor", "--skip-stream").stdout)
    assert report["doctor"] == "ready" and report["stream"] is False


def test_serving_views_read_back_the_sample_totals() -> None:
    publish_and_verify()
    daily = admin_sql("SELECT receipt_count,gross_sales,discounts,sales_after_discount_before_returns,"
                      "receipts_without_tax_detail FROM kinda.v_branch_daily_sales WHERE source_system='simulator' "
                      "AND branch_id='BR001' AND business_date=DATE '2026-10-30'")
    assert daily == "1|130.00|10.00|120.00|1"
    products = admin_sql("SELECT product_id,units,sales_after_discount_before_returns "
                         "FROM kinda.v_branch_product_daily_sales WHERE source_system='simulator' "
                         "AND business_date=DATE '2026-10-30' ORDER BY product_id")
    assert products.splitlines() == ["BREAD_VANILLA|2|60.00", "SMOOTHIE_MANGO|1|60.00"]


def test_batch_log_carries_observability_fields() -> None:
    publish_and_verify()
    batches = [json.loads(line.split("|", 1)[-1]) for line in compose("logs", "--no-color", "stream").stdout.splitlines()
               if '"batch_id"' in line]
    assert {"service", "query_name", "batch_id", "stream_epoch", "input_count", "accepted", "duplicate",
            "skipped_deliveries", "commit_duration_ms"} <= batches[-1].keys()
