# Kinda Intelligence

Milestone 1 Engineering Specification สำหรับ MVP 3 สาขา

- [Engineering specification](docs/milestone-1-engineering-spec.md) มี architecture, contracts, sink algorithm, tests, acceptance criteria และ task dependency
- [Reference contracts](docs/reference/) มี JSON Schema, sample event, SQL migration, Compose, Kafka config และ CSV mapping จำลอง

Implementation ปัจจุบันครอบคลุม M1/P1 ([#2](https://github.com/stkorn/kindaros/issues/2)) เท่านั้น: ส่ง completed receipt ตัวอย่างหนึ่งใบผ่าน CLI → Kafka KRaft → Spark Structured Streaming → PostgreSQL แล้วอ่านผลจาก ledger ยังไม่มี simulator, CSV import, durable rejection หรือ recovery tests
ตรวจเวอร์ชันจากแหล่งทางการ ณ 30 ตุลาคม 2026 และแยก documentation compatibility ออกจาก runtime acceptance ที่ Coding Agent ต้องพิสูจน์

Wongnai mapping ใช้ synthetic fixture เท่านั้น ยังไม่รับรองรูปแบบ export จริง และไม่สมมติ public realtime API

## Local stack (M1/P1)

ต้องมี Docker Compose 5.x และ [uv](https://docs.astral.sh/uv/) 0.13 บน host

```bash
make up       # build image, start Kafka/PostgreSQL, run bootstrap, wait until stream is healthy
make doctor   # topic metadata, migration checksum, seed, stream binding, active query, checkpoint write
make smoke    # publish sample, verify PostgreSQL readout, duplicate/bootstrap/listener/role checks
make check    # reference copies, offline Spark connector selftest, mypy strict, unit tests
make down     # stop containers and keep volumes
```

Host client ใช้ `localhost:9092` ส่วน container ใช้ `kafka:29092` Kafka เป็น single node RF1 จึงไม่ทนต่อ disk/node loss `docker compose down -v` ลบ Kafka/PostgreSQL/checkpoint volumes และเป็น destructive reset

Contracts อยู่ที่ `contracts/`, migration อยู่ที่ `migrations/`, Kafka config อยู่ที่ `config/` และถูก package เข้า wheel จากตำแหน่งนี้ `docs/reference/` เป็น copy ที่ `make check` ตรวจว่าตรงกัน Stack baseline และ lock evidence อยู่ที่ [docs/stack-baseline.md](docs/stack-baseline.md)
