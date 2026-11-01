from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from kinda.config import Config
from kinda.sink import KafkaRecord, PostgresSink

JARS = Path("/opt/kinda/ivy/jars")
QUERY_NAME = "kinda-pos-sales-v1"


def spark_session() -> SparkSession:
    jars = sorted(JARS.glob("*.jar"))
    return (SparkSession.builder.master("local[2]").appName(QUERY_NAME)
            .config("spark.jars", ",".join(str(path) for path in jars))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "3")
            .config("spark.ui.enabled", "false").getOrCreate())


def offline_selftest() -> dict[str, Any]:
    spark = spark_session()
    try:
        assert spark.range(3).count() == 3
        # Loading the provider and constructing its plan requires the baked connector, not a broker.
        spark.readStream.format("kafka").option("kafka.bootstrap.servers", "127.0.0.1:1").option("subscribe", "offline").load()
        return {"offline_connector": "ready", "spark": spark.version, "uid": os.getuid(),
                "jars": len(list(JARS.glob("*.jar")))}
    finally:
        spark.stop()


def run_stream(config: Config) -> None:
    config.checkpoint.mkdir(parents=True, exist_ok=True)
    spark = spark_session()
    sink = PostgresSink(config)
    status_path = config.status_path

    def write_batch(frame: DataFrame, batch_id: int) -> None:
        records = (KafkaRecord(row.topic, row.partition, row.offset, bytes(row.key), bytes(row.value), row.timestamp)
                   for row in frame.orderBy("topic", "partition", "offset").toLocalIterator())
        started = time.monotonic()
        report = sink.apply(records)
        print(json.dumps({"service": "stream", "query_name": QUERY_NAME, "batch_id": batch_id,
                          "stream_epoch": str(config.epoch), "input_count": sum(report.values()), **report,
                          "commit_duration_ms": round((time.monotonic() - started) * 1000)}), flush=True)

    try:
        source = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", config.brokers)
                  .option("subscribe", config.topic).option("startingOffsets", "earliest")
                  .option("failOnDataLoss", "true").option("maxOffsetsPerTrigger", "250").load())
        query = (source.writeStream.queryName(QUERY_NAME).outputMode("append")
                 .foreachBatch(write_batch)
                 .option("checkpointLocation", str(config.checkpoint / "spark"))
                 .trigger(processingTime="5 seconds").start())
        while query.isActive:
            progress = query.lastProgress
            temp = status_path.with_suffix(".tmp")
            temp.write_text(json.dumps({"pid": os.getpid(), "active": query.isActive,
                                        "progress": progress}, default=str))
            temp.replace(status_path)
            query.awaitTermination(1)
        if query.exception() is not None:
            raise RuntimeError(str(query.exception()))
    finally:
        status_path.unlink(missing_ok=True)
        sink.close()
        spark.stop()
