# Kinda Intelligence

Milestone 1 Engineering Specification สำหรับ MVP 3 สาขา

- [Engineering specification](docs/milestone-1-engineering-spec.md) มี architecture, contracts, sink algorithm, tests, acceptance criteria และ task dependency
- [Reference contracts](docs/reference/) มี JSON Schema, sample event, SQL migration, Compose, Kafka config และ CSV mapping จำลอง

เอกสารชุดนี้เป็นสเปกสำหรับเริ่ม implementation ยังไม่มี service code หรือ pipeline ที่รันครบแล้ว
ตรวจเวอร์ชันจากแหล่งทางการ ณ 30 ตุลาคม 2026 และแยก documentation compatibility ออกจาก runtime acceptance ที่ Coding Agent ต้องพิสูจน์

Wongnai mapping ใช้ synthetic fixture เท่านั้น ยังไม่รับรองรูปแบบ export จริง และไม่สมมติ public realtime API
