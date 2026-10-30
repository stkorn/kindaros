# Kinda Intelligence: project roadmap และ M1 delivery plan

วันที่สังเคราะห์: 30 ตุลาคม 2026 • Repository: [stkorn/kindaros](https://github.com/stkorn/kindaros)

สถานะ: ผู้ใช้ยืนยัน acceptance boundary แล้ว และ publish เป็น [GitHub issue #1](https://github.com/stkorn/kindaros/issues/1) พร้อม label ready-for-agent; implementation ยังไม่เสร็จ

เอกสารนี้สังเคราะห์ข้อสรุปจากแชต [ไอเดียโปรเจกต์การเงิน realtime](chatgpt-conversation://6ae49166-1394-83ec-86ec-2f7e6214b073) และใช้ [M1 Engineering Specification](milestone-1-engineering-spec.md) ที่ทำต่อใน workspace นี้เป็นรายละเอียดสำหรับเริ่มพัฒนา

Flow ที่เลือกจาก ask-matt คือ `to-spec → to-tickets → implement` เพราะมีข้อสรุปแล้วและงานต้องใช้หลาย session งานรอบนี้เขียนและ publish แผน/spec ผู้ใช้ยืนยันขอบเขตการทดสอบแล้ว ส่วนการแตก implementation tickets และเขียน service code เป็นขั้นถัดไป

## Problem Statement

Kinda เป็นธุรกิจแฟรนไชส์ขนมปังไส้ครีมและน้ำปั่น ครัวกลางผลิตขนมปังเปล่าและไส้ครีมแยกกัน ส่งให้สาขาประกอบขาย ส่วนเครื่องดื่มเตรียมที่สาขา ระบบ POS ที่ระบุคือ Wongnai POS

ข้อมูลขายเพียงอย่างเดียวยังไม่เชื่อมการผลิต การโอนวัตถุดิบ การใช้ไส้ครีม ของเสีย การชำระเงิน และค่าธรรมเนียม Franchise จึงยังตอบไม่ได้ครบว่าสาขาขายอะไร ใช้วัตถุดิบเท่าไร ยอดใดควรเรียกเก็บ และเงินส่วนใดรับจริงแล้ว

โครงการต้องเป็นทั้ง Data Engineering portfolio ที่ทดสอบซ้ำได้ และฐานสำหรับใช้งานจริงในอนาคต จุดเริ่มต้นคือทำให้ยอดขายจาก 3 สาขาเข้า pipeline ได้ถูกต้อง แม้ข้อมูลซ้ำ ระบบล่ม หรือส่งข้อมูลย้อนหลัง โดยไม่สมมติว่ามี public realtime API จาก Wongnai

## Solution

พัฒนา Data Platform เป็น 5 milestone เริ่ม local ทั้งหมด ใช้ simulator พิสูจน์ streaming และใช้ CSV adapter เป็นขอบเขตเชื่อม export ที่ตรวจสอบได้ จากนั้นขยายไปครัวกลาง การเงิน และ forecast ตามลำดับ

| Milestone | ผลลัพธ์หลัก | เกณฑ์ก่อนเริ่มขั้นถัดไป |
| --- | --- | --- |
| M1 Foundation & POS Streaming | Simulator/CSV → Kafka → Spark → PostgreSQL พร้อมยอดขายรายสาขา/สินค้า/วันและ rejection audit | ยอดตรง manifest, import/replay ซ้ำไม่เพิ่มยอด, crash recovery ผ่าน |
| M2 Central Kitchen & Inventory | Production batch, bread/cream lot, shipment, receiving, assembly และ waste | กระทบยอดรับ-ใช้-คงเหลือได้ และแยก estimated recipe consumption จาก actual usage |
| M3 Franchise Financial Intelligence | Payment/settlement reconciliation, cash position, supplier invoices, contract-versioned royalty และ financial marts | อธิบายยอดประเมิน ยอดเรียกเก็บ และยอดรับเงินจริงแยกกันได้ |
| M4 Forecast & Thai Open Data | Bread/cream/smoothie demand forecast และ production planning พร้อม backtesting | เทียบ baseline ด้วยข้อมูลตามลำดับเวลา และแสดงคุณภาพ/ความครอบคลุมของ source |
| M5 Reliability & Portfolio | Load/recovery/isolation tests, monitoring, runbook, reproducible demo และ portfolio evidence | Clean environment รัน demo และตรวจผลซ้ำได้ |

M1 เป็น implementation scope ที่กำหนดรายละเอียดแล้วในแผนนี้ ส่วน M2–M5 เป็น roadmap ที่ต้องทำ spec และ acceptance ของตนก่อนเริ่ม implementation Reliability พื้นฐานและ CI ต้องมีตั้งแต่ M1; M5 เพิ่มการทดสอบและหลักฐานระดับทั้งระบบ

## User Stories

### M1: พร้อมแตกเป็น implementation tickets

1. As a developer, I want to start the local stack from a clean environment, so that setup does not depend on undocumented manual steps.
2. As a developer, I want pinned dependencies and verified runtime compatibility, so that other contributors can reproduce the same pipeline.
3. As a developer, I want exactly three simulated branches, so that I can complete the MVP before expanding the network.
4. As a developer, I want deterministic simulated receipts and an expected-results manifest, so that I can verify every receipt and amount.
5. As a data engineer, I want one canonical completed-sale contract, so that simulator and CSV input follow the same validation rules.
6. As a data engineer, I want stable business identities across imports, so that renamed or overlapping exports do not create duplicate sales.
7. As an operator, I want CSV dry-run and full-file preflight, so that invalid or unsupported exports are rejected before publishing.
8. As an operator, I want explicit branch/product mappings, so that source codes are not matched by guesswork.
9. As an operator, I want import reports that distinguish acknowledged, failed and unknown delivery, so that producer success is not confused with DB completion.
10. As a data engineer, I want duplicate events to have no additional database effect, so that replay keeps the original sales totals.
11. As an operator, I want changed payloads for an accepted receipt to produce a conflict, so that the original amount is not silently overwritten.
12. As an operator, I want durable rejection records with reasons and source coordinates, so that invalid input can be investigated.
13. As a developer, I want receipt headers and lines to commit atomically, so that readers never observe partial sales.
14. As an operator, I want restart recovery before and after DB commit, so that process failure does not lose or duplicate sales.
15. As a branch analyst, I want sales by branch, product and Bangkok calendar date, so that I can reconcile daily activity.
16. As an analyst, I want simulated and imported sources to remain distinguishable, so that demo data is not presented as real business sales.
17. As an analyst, I want exact monetary arithmetic and explicit missing-tax information, so that reports do not imply unsupported financial precision.
18. As a maintainer, I want CI to verify the real pipeline and fault cases, so that successful validation does not depend on one laptop.
19. As an operator, I want a visible failure when retained Kafka offsets are missing, so that data gaps cannot be skipped silently.
20. As a portfolio reviewer, I want repeatable commands and reconciliation evidence, so that I can assess the engineering claims myself.

### M2–M5: roadmap intent, ยังไม่ใช่ implementation scope ของ M1

21. As a kitchen operator, I want production batches for bread shells and cream fillings, so that quantity, cost and expiry can be traced separately.
22. As a branch operator, I want shipment and receiving records, so that sent and received quantities can be reconciled.
23. As an operations analyst, I want assembly and waste records, so that actual usage is distinguishable from recipe-based estimates.
24. As a finance analyst, I want POS payment confirmation separated from bank settlement, so that reported sales are not mistaken for cash received.
25. As a franchisor, I want royalty calculations to use the applicable contract version, so that historical assessments do not change when a rate changes.
26. As a franchisee, I want central-kitchen purchases separated from royalty fees, so that different financial obligations can be reconciled.
27. As a finance analyst, I want late refunds handled through explicit adjustments, so that issued statements retain their history.
28. As a production planner, I want demand forecasts for shells, cream flavors and smoothies, so that purchasing and production can be planned from measured demand.
29. As a franchisee owner, I want financial access limited to authorized entities, so that one franchisee cannot read another's private finances.
30. As a portfolio reviewer, I want load, recovery and forecast results with reproducible methods, so that demonstrations are supported by evidence.

## Implementation Decisions

### Active delivery scope

- ใช้ 3 สาขา BR001, BR002, BR003 ตลอด M1 ไม่เพิ่มขนาดเพื่อทดแทน correctness tests
- Business scenario ระยะยาวเป็นหนึ่งสาขาบริษัทและสองสาขา Franchise แต่ M1 ยังไม่สร้าง tenant/legal-entity model หรือใช้ประเภทสาขาคิดค่าธรรมเนียม
- M1 ใช้ completed SALE เท่านั้น หนึ่ง receipt พร้อม lines ต่อ event ส่วน refund, void และ correction ต้องถูกระบุว่า unsupported ไม่แปลงเป็นยอดขายปกติ
- Simulator เป็น streaming source ที่ควบคุมได้ CSV เป็น file-import source; production mapping ยังต้องพิสูจน์จาก sanitized export จริง
- ไม่รับรองว่า canonical fields ของโครงการตรงกับ export Wongnai ทุก field และไม่สมมติ public API/webhook
- M1 ใช้ Python simulator/adapter/publisher, Kafka KRaft, Spark Structured Streaming และ PostgreSQL แบบ local ผ่าน Compose ไม่เพิ่ม FastAPI โดยไม่มี consumer requirement
- Baseline เวอร์ชันอ้างอิง Engineering Specification ที่ตรวจไว้ก่อนหน้าเป็น candidate จนกว่าจะผ่าน runtime smoke และ dependency resolution ในงานเริ่มต้น แผนนี้ไม่ได้ตรวจ latest releases ใหม่

### Data ownership และ reliability

- Domain module เป็น owner ของ identity, canonicalization, money/date rules; adapter เป็น owner ของ source layout และ completeness
- Business identity ประกอบด้วย source system, stable installation, branch, Bangkok business date และ receipt ID โดยไม่ใช้ชื่อไฟล์หรือเวลา import เป็น identity
- ใช้ deterministic event ID และ canonical semantic hash Key/hash เดิมเป็น duplicate ส่วน key เดิม/hash ต่างเป็น conflict
- PostgreSQL sink เป็น writer ของ accepted sales เพียง owner เดียว ใช้หนึ่ง transaction ต่อ bounded microbatch ให้ ledger, lines และ delivery outcomes commit พร้อมกัน
- ใช้ DB uniqueness, financial constraints, deferred header/line totals checks และ insert guard ป้องกันการเติม line เข้า accepted receipt ภายหลัง
- Session advisory lock จำกัด active sink writer ใช้ explicit DB transaction ที่ commit ก่อน foreachBatch return
- Spark checkpoint และ Kafka source coordinates ใช้ recovery; batch ID ใช้ diagnostics ไม่ใช่ business deduplication
- Contract รับประกัน at-least-once delivery กับ idempotent DB effects ภายใต้ stable identity ไม่ claim distributed exactly-once
- Poison records ต้องมี durable rejection ส่วน DB outage/configuration bug ต้อง fail และ replay ไม่ถูกกลืนเป็น bad data
- เก็บ persistent Kafka/PostgreSQL/checkpoint state และผูก transport epoch กับ cluster/topic identity เพื่อแยก offset ที่ถูกใช้ซ้ำหลัง recreate topic

### Financial/reporting meaning

- ใช้ Decimal/SQL NUMERIC ไม่ใช้ float สำหรับเงิน Quantity ใน M1 เป็นจำนวนชิ้น/แก้วเต็ม
- Metric ของ M1 คือยอด completed sale หลังส่วนลดก่อนคืนเงิน รวมภาษีที่ source รวมไว้แล้ว ไม่ใช่ Eligible Net Sales, bank settlement หรือ franchisor revenue
- Report แยก simulator/imported source และความครบของ tax detail การไม่มี tax detail ไม่ถูกแปลเป็น VAT rate 0
- Source timestamp ใช้ UTC และสรุปวันตาม Bangkok calendar date; POS trading day แบบอื่นต้องมี contract ใหม่
- Readout ของ M1 เป็น SQL views/CLI ไม่มี Dashboard UI หรือ payment status ใน acceptance ของรอบนี้

### Decisions retained for later milestones

- M2 แยก bread shells กับ cream fillings และแยก actual assembly usage จาก recipe estimates การระบุ lot จริงต้องมี source assembly record
- M3 แยก network sales, corporate revenue, royalty assessment/invoice/payment และ central-kitchen wholesale transactions
- Royalty 4% กับ Marketing Fee 1% เป็น proposed simulation policy จากแชตเดิม ไม่ใช่สัญญาจริงหรือ market benchmark ต้องกำหนด Eligible Net Sales และ effective contract version ก่อน implementation
- M4 ตรวจ access, licensing, grain, coverage และ freshness ของ Thai open data ก่อนเลือก connector ไม่อ้างว่า endpoint พร้อมใช้งานจากแผนเก่า
- dbt/Airflow, forecasting, tenant isolation และ cloud extension เข้าตาม milestone ที่มี spec ของตน ไม่เพิ่มเข้า M1 ล่วงหน้า

## Testing Decisions

### จุดทดสอบหลักที่เสนอให้ยืนยัน

ใช้ acceptance boundary หลักหนึ่งเส้นคือ input ที่ผู้ใช้ส่งผ่าน CLI ไปจนถึง readout และ rejection ที่อ่านจาก PostgreSQL ไม่ใช้การ mock Spark/Kafka แล้วสรุปว่า pipeline ใช้งานได้

| Boundary | สิ่งที่พิสูจน์ |
| --- | --- |
| Simulator/import CLI → Kafka → Spark → PostgreSQL | expected receipts, lines, hashes และยอดครบตรง manifest |
| Replay/reimport/restart → PostgreSQL | จำนวนและยอดเดิม แม้ crash ก่อน commit หรือหลัง commit ก่อน checkpoint |
| CSV preflight และ invalid Kafka input → report/rejection | invalid CSV publish ศูนย์; poison event มี durable reason; valid events เดินต่อได้ |

Domain unit tests และ DB integration tests สนับสนุน boundary นี้ โดยทดสอบ observable results ไม่ผูกกับ private helper names หรือจำนวนครั้งที่เรียก method

### Minimum M1 acceptance

1. Simulator 300 receipts ต้องได้ 100 receipts ต่อสาขา ยอด/lines/hashes ตรง manifest ทั้งชุด
2. ส่ง event เดิมซ้ำ, reimport export ที่เปลี่ยนชื่อ/ลำดับแถว และ replay retained log แล้ว accepted totals ไม่เพิ่ม
3. ตัวอย่าง receipt ตามสเปกปัจจุบันมี gross 130.00, discount 10.00 และยอดหลังส่วนลด 120.00 ส่งซ้ำ 10 ครั้งยังได้ receipt เดียวและยอด 120.00
4. Key เดิมที่ amount เปลี่ยนต้องเกิด conflict และยอดเดิมอยู่ครบ
5. Header ไม่มี lines, totals ไม่ตรง หรือ append line ให้ receipt ที่ commit แล้วต้องถูกปฏิเสธ
6. Fault ก่อน commit และหลัง DB commit ก่อน checkpoint ต้อง recovery ได้ครบโดยไม่มี partial receipt หรือ double count
7. Invalid CSV แม้เพียงหนึ่งแถวต้อง publish ศูนย์; unknown layout/refund/void ไม่ผ่าน preflight
8. DB outage ต้อง fail/restart โดยไม่ advance batch completion; Kafka retention gap ต้องแสดง failure ชัดเจน
9. ข้อมูล/checkpoint/topic binding อยู่หลัง down/up ที่เก็บ volumes; checkpoint reset อย่างเดียวไม่เปลี่ยน business totals
10. Warm-up 30 วินาที แล้วทดสอบ 5 receipts/second รวม 3 สาขา 120 วินาที เป้าหมาย acknowledged-to-DB-visible p95 ไม่เกิน 15 วินาที และครบหลัง drain ไม่เกิน 120 วินาที นี่เป็น target ที่ยังไม่ใช่ผลวัดจริง
11. Clean environment ต้อง build/start/doctor ได้ และ runtime ไม่ต้องดาวน์โหลด dependency จาก Maven/PyPI เพิ่ม
12. CI เก็บ reconciliation reports, dependency versions และ logs ที่ตรวจย้อนหลังได้

Prior art ที่มีขณะเขียนคือ reference JSON Schema/sample, SQL migration, Compose configuration และ specification validation เท่านั้น ยังไม่มี service integration suite ที่รันผ่านให้ใช้เป็นหลักฐาน M1 เสร็จ

M2–M5 ต้องกำหนด test boundaries เพิ่มตอนทำ spec ของแต่ละ milestone ได้แก่ material reconciliation, financial reconciliation, temporal forecast backtesting และ authorization/isolation tests ตามลำดับ

## Out of Scope

สำหรับ implementation issue ของ M1 ไม่รวม kitchen/inventory, royalty, payment confirmation, settlement, refunds/voids/corrections, financial marts, forecasting, Dashboard UI, FastAPI service, production HA, full tenant authorization หรือ AWS deployment

การวาง roadmap ไม่อนุญาตให้ Coding Agent เริ่ม milestone ถัดไปโดยข้าม M1 acceptance การตั้ง GitHub tracker ไม่ได้หมายความว่า source files ใน workspace ถูก push แล้ว และการมี reference SQL/Compose ไม่ได้หมายความว่า stack runtime ผ่านแล้ว

ไม่ใช้สเปกนี้ยืนยันข้อเท็จจริงด้านภาษี สัญญาแฟรนไชส์ หรือสิทธิ์เข้าถึงข้อมูลของ vendor อัตราและนโยบายการเงินใน roadmap เป็น simulation assumptions ที่ต้องยืนยันก่อนใช้จริง

## Further Notes

### ลำดับทำ M1

| งาน | Depends on | ผลที่ต้องตรวจได้ |
| --- | --- | --- |
| T0 Verify stack | — | image/dependency availability และ runtime smoke; reconcile baseline ก่อนเดินต่อ |
| T1 Foundation | T0 | repository tooling, locked build, CLI และ CI foundation |
| T2 Contract/domain | T1 | identity/hash/money/date tests และ golden fixtures |
| T3 Database | T2 | migrations, seed, roles, constraints และ atomic receipt tests |
| T4 Infrastructure | T1,T3 | Kafka KRaft/topic, DB readiness, bootstrap และ persistence |
| T5 Publisher/simulator | T2,T4 | delivery reports และ deterministic 300-receipt manifest |
| T6 CSV adapter | T2,T3 | full-file preflight, mapping, dry-run/report และ fixture tests |
| T7 Idempotent sink | T2,T3,T4 | atomic batch, single writer, dedup/conflict/rejection integration tests |
| T8 Spark wiring | T5,T7 | end-to-end readout พร้อม checkpoint/recovery |
| T9 E2E/fault acceptance | T5,T6,T8 | reconciliation และ failure matrix ผ่าน |
| T10 Handoff | T9 | clean-environment runbook, CI evidence และ acceptance ครบ |

ตารางนี้อ้างอิง task IDs จาก Engineering Specification เพื่อ trace งานเดิมได้ เมื่อใช้ to-tickets ให้จัด implementation tickets เป็น tracer-bullet slices ที่แต่ละใบพิสูจน์ behavior ได้ พร้อม blocking edges จริง ไม่จำเป็นต้องแตกหนึ่ง task ในตารางเป็นหนึ่ง GitHub issue แบบ mechanical

### Roadmap timing

| ช่วงตามแผนต้นทาง | Milestone | สถานะการใช้ |
| --- | --- | --- |
| สัปดาห์ 1–3 | M1 | เริ่มได้เมื่อ baseline/runtime พร้อม |
| สัปดาห์ 4–5 | M2 | หลัง M1 ผ่านและ M2 spec พร้อม |
| สัปดาห์ 6–8 | M3 | หลัง sales/material semantics พร้อม และ M3 spec พร้อม |
| สัปดาห์ 9–10 | M4 | หลังข้อมูลย้อนหลังเพียงพอและ source/backtest plan พร้อม |
| สัปดาห์ 11–12 | M5 | หลัง milestone ก่อนหน้ามี behavior ให้พิสูจน์ |

เป็นกรอบ 12 สัปดาห์จากแชตเดิมสำหรับการทำควบคู่งานประจำ ไม่ใช่ estimate ที่ยืนยันหรือวันส่งมอบ เริ่มนับหลังจัด environment และเวลาทำงานจริงแล้ว Re-plan จาก acceptance evidence ไม่บีบ scope ให้พอดีวันที่เดาไว้

### ความต่างระหว่าง roadmap เดิมกับ M1 ที่ลงรายละเอียดแล้ว

| ประเด็น | แผนต้นทาง | M1 specification ใน workspace |
| --- | --- | --- |
| UI/payment | มี Sales & Payment Dashboard ในภาพ MVP | M1 ใช้ SALE-only SQL/CLI; UI/payment เป็น post-M1 ก่อนใช้จริงด้านการเงิน |
| Catalog | ขนมปัง 4 รส น้ำปั่น 3 เมนูใน business scenario | seed ทดสอบเริ่ม 2 bread products + 1 smoothie; ขยาย catalog หลัง correctness ผ่าน |
| Branch code | ตัวอย่าง BKK-001 | ใช้ BR001–BR003 ตาม contract ปัจจุบัน |
| ตัวอย่างราคา | รวม 125 บาทและ QR event | sample ปัจจุบัน net 120 บาท ไม่มี payment event; ไม่ใช่การเปลี่ยนราคาขายจริง |
| Tenant/entity | ระบุว่าต้องออกแบบเผื่อ | ยังไม่เพิ่ม field/authorization ใน M1; ต้องออก contract evolution ก่อน M3 และก่อนรับ confidential multi-entity data |
| Progress | แชตเก่ามีข้อความ checklist completed | ไม่นำข้อความนั้นมาอ้างว่างานเขียน code/build/test เสร็จแล้ว |

แผนนี้รักษา business roadmap จากต้นทางและใช้ M1 specification ที่ทำต่อในแชตนี้เป็น working scope ความต่างข้างต้นต้องอยู่ใน issue ที่ publish เพื่อไม่ให้ agent ใช้ acceptance คนละชุด

### ข้อมูลที่ต้องมีก่อน production integration

- Sanitized Wongnai export ที่ยืนยัน receipt/line identities, status, completeness, timezone และ monetary semantics ได้
- บันทึกวิธีประกอบสินค้าจริงที่สาขา ถ้าต้องการ lot traceability
- นโยบาย wholesale ระหว่าง HQ/Franchisee และนิยาม Eligible Net Sales/contract version สำหรับ M3
- Historical data และ permission ของ external sources สำหรับ M4

### ขั้นตอนถัดไปใน Matt flow

1. ผู้ใช้ยืนยัน acceptance boundary ของ M1 ตาม Testing Decisions แล้ว
2. Publish แล้วเป็น GitHub issue #1 ใน stkorn/kindaros พร้อม ready-for-agent โดยระบุ active scope ว่า M1 เท่านั้น
3. ใช้ to-tickets แตก M1 เป็น self-contained tracer-bullet tickets พร้อม blocking edges; ไม่ triage tickets ที่สร้างจาก approved spec ซ้ำ
4. ใช้ implement ทีละ ticket ใน context ใหม่ ให้ test/review รองรับก่อนปิดงาน
5. เมื่อ M1 acceptance ครบ จึงทำ spec ของ M2 โดยใช้ business roadmap นี้เป็น input
