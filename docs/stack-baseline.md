# Stack baseline evidence (M1/P1)

ตรวจซ้ำวันที่ 2026-11-01 จาก registry/PyPI จริง หลักฐานชุดแรกเก็บเมื่อ 2026-10-30

## Images

Digest คือ SHA-256 ของ OCI image index ที่ `docker buildx imagetools inspect --raw` คืนมา ไฟล์ index อยู่ใน [evidence/](evidence/)

| Image | Index digest | Platforms ที่ตรวจ |
|---|---|---|
| `apache/kafka:4.4.0` | `sha256:833b867a9585a141da455dc2b57431c65cbccb27f11c91f694444cca9f777e0f` | linux/amd64, linux/arm64 |
| `postgres:18.6-bookworm` | `sha256:5c4783a8cc3f0074912164f94dfa1be4e043438f0f707cf968d7af9a0d31a2c8` | linux/amd64, linux/arm64 |
| `python:3.12.14-slim-trixie` | `sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f` | linux/amd64, linux/arm64 |

`compose.yaml` และ `docker/Dockerfile` pin digest เหล่านี้ JDK pin เป็น Debian package `openjdk-21-jre-headless=21.0.13+9-1~deb13u2` เมื่อ Debian security update ถอด version นี้ออกจาก mirror build จะ fail ชัดเจน ต้องเลือก version ใหม่พร้อมทดสอบ ไม่ fallback เงียบ

## Python

`uv.lock` เป็น source of truth และ `requirements.lock` เป็น hashed export ที่ image ติดตั้งด้วย `pip --require-hashes` (`make lock` สร้างทั้งสองไฟล์) Binary wheels ที่ต้องมีสำหรับ cp312 manylinux ทั้ง x86_64 และ aarch64: `confluent-kafka 2.16.1`, `psycopg-binary 3.3.6`, `rpds-py 2026.9.1`, `librt 0.16.0` มีครบใน lock `pyspark 4.2.1` เป็น pure-Python sdist

## Maven

Image resolve `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1` ครั้งเดียวตอน build แล้ว `scripts/check_dependency_locks.py` ตรวจ SHA-256 ของ 11 jars เทียบ `maven.lock.json` Runtime ใช้ `spark.jars` จาก path ใน image จึงไม่ดาวน์โหลดจาก Maven/PyPI ตอนรัน `make offline` พิสูจน์ด้วย `--network none` เป็น UID 10001

## Runtime architecture

Runtime ที่รันทดสอบบนเครื่อง developer คือ linux/arm64 (Docker Desktop บน Apple Silicon) CI runner `ubuntu-24.04` เป็น linux/amd64
