from __future__ import annotations

import base64
import hashlib
import os
from typing import Any
from uuid import UUID

import psycopg
from confluent_kafka import KafkaException, TopicCollection
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic, ResourceType
from psycopg import sql

from kinda.config import Config
from kinda.domain import resource

TOPIC_CONFIG = {"min.insync.replicas": "1", "cleanup.policy": "delete",
                "compression.type": "uncompressed", "retention.ms": "604800000"}


def kafka_metadata(config: Config, *, create: bool = False) -> dict[str, Any]:
    admin = AdminClient({"bootstrap.servers": config.brokers, "socket.timeout.ms": 10000})
    if create:
        try:
            admin.create_topics([NewTopic(config.topic, num_partitions=3, replication_factor=1,
                                           config=TOPIC_CONFIG)])[config.topic].result(20)
        except KafkaException as exc:
            if exc.args[0].name() != "TOPIC_ALREADY_EXISTS":
                raise
    metadata = admin.list_topics(timeout=15)
    topic = metadata.topics.get(config.topic)
    if topic is None or topic.error is not None:
        raise ValueError("Topic unavailable")
    if len(topic.partitions) != 3 or any(len(p.replicas) != 1 for p in topic.partitions.values()):
        raise ValueError("Topic requires 3 partitions and RF1")
    target = ConfigResource(ResourceType.TOPIC, config.topic)
    actual = admin.describe_configs([target])[target].result(15)
    for name, expected in TOPIC_CONFIG.items():
        if actual[name].value != expected:
            raise ValueError(f"Topic config mismatch {name}: {actual[name].value} != {expected}")
    desc = admin.describe_topics(TopicCollection([config.topic]))[config.topic].result(15)
    return {"cluster_id": metadata.cluster_id, "topic_id": str(UUID(bytes=base64.urlsafe_b64decode(str(desc.topic_id) + "=="))),
            "partitions": len(topic.partitions), "brokers":
            [{"host": b.host, "port": b.port} for b in metadata.brokers.values()]}


def migration_checksum() -> str:
    return hashlib.sha256(resource("001_initial.sql")).hexdigest()


def bootstrap(config: Config) -> dict[str, Any]:
    metadata = kafka_metadata(config, create=True)
    topic_uuid = UUID(metadata["topic_id"])
    with psycopg.connect() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(789021)")
        conn.execute("CREATE TABLE IF NOT EXISTS public.schema_migrations "
                     "(version integer PRIMARY KEY, checksum text NOT NULL)")
        row = conn.execute("SELECT checksum FROM public.schema_migrations WHERE version=1").fetchone()
        if row is None:
            conn.execute(resource("001_initial.sql").decode())
            conn.execute("INSERT INTO public.schema_migrations VALUES (1,%s)", (migration_checksum(),))
        elif row[0] != migration_checksum():
            raise ValueError("Migration checksum mismatch")
        if conn.execute("SELECT 1 FROM pg_roles WHERE rolname='kinda_app'").fetchone() is None:
            conn.execute("CREATE ROLE kinda_app LOGIN")
        conn.execute(sql.SQL("ALTER ROLE kinda_app NOSUPERUSER NOCREATEDB NOCREATEROLE "
                             "NOREPLICATION NOBYPASSRLS PASSWORD {}").format(
                                 sql.Literal(os.environ["APP_DB_PASSWORD"])))
        conn.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
        conn.execute("GRANT USAGE ON SCHEMA kinda TO kinda_app")
        conn.execute("GRANT SELECT ON ALL TABLES IN SCHEMA kinda TO kinda_app")
        conn.execute("GRANT SELECT ON public.schema_migrations TO kinda_app")
        conn.execute("GRANT INSERT ON kinda.sale_events,kinda.sale_lines,kinda.deliveries TO kinda_app")
        conn.execute("INSERT INTO kinda.stream_sources VALUES ('pos-sales-v1',%s,%s,%s,%s) "
                     "ON CONFLICT DO NOTHING", (metadata["cluster_id"], config.topic, topic_uuid, config.epoch))
        binding = conn.execute("SELECT kafka_cluster_id,topic,kafka_topic_id,stream_epoch "
                               "FROM kinda.stream_sources WHERE source_name='pos-sales-v1'").fetchone()
        if binding != (metadata["cluster_id"], config.topic, topic_uuid, config.epoch):
            raise ValueError("Kafka source binding mismatch; preserve topic and checkpoint state")
    return {"bootstrap": "ready", "migration_checksum": migration_checksum(), **metadata}
