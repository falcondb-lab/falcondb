# FalconDB

<p align="center">
  <img src="assets/falcondb-logo.png" alt="FalconDB Logo" width="220" />
</p>

<h1 align="center">FalconDB</h1>

<p align="center">
  <strong>متوافق مع PG · موزّع · ذاكرة أولاً · دلالات معاملات حتمية</strong>
</p>

<p align="center">
  <a href="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml">
    <img src="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml/badge.svg" alt="CI" />
  </a>
  <img src="https://img.shields.io/badge/version-1.2.0-blue" alt="الإصدار" />
  <img src="https://img.shields.io/badge/MSRV-1.75-blue" alt="MSRV" />
  <img src="https://img.shields.io/badge/license-Apache--2.0-green" alt="الترخيص" />
</p>

> English | [简体中文](README_zh.md) | [Español](README_es.md) | [Français](README_fr.md) | [한국어](README_ko.md) | [日本語](README_ja.md) | [Deutsch](README_de.md) | **العربية** | [Italiano](README_it.md) | [Bahasa Melayu](README_ms.md) | [Português](README_pt.md)

> FalconDB هي **قاعدة بيانات OLTP متوافقة مع PG، موزّعة، تعتمد الذاكرة أولاً** مع دلالات معاملات حتمية. تمت مقارنتها ببرامج PostgreSQL وVoltDB وSingleStore — انظر **[مصفوفة المعايير](benchmarks/README.md)**.
>
> - ✅ **زمن استجابة منخفض** — تتجاوز عمليات الإيداع السريعة أحادية الشارد 2PC بالكامل
> - ✅ **أداء المسح** — تجميعات بث مدمجة، تكرار MVCC بدون نسخ، تعادل قريب من PG على مليون صف
> - ✅ **الاستقرار** — p99 محدود، معدل الإلغاء < 1%، معايير قابلة للإعادة
> - ✅ **اتساق قابل للإثبات** — MVCC/OCC تحت عزل اللقطة، ACID متحقق منه بـCI
> - ✅ **قابلية التشغيل** — أكثر من 50 أمر SHOW، مقاييس Prometheus، بوابة CI للتجاوز
> - ✅ **الحتمية** — آلة حالة مقوّاة، in-doubt محدود، إعادة محاولة آمنة
> - ❌ ليست HTAP — لا أحمال عمل تحليلية
> - ❌ ليست PG كاملة — [انظر قائمة غير المدعوم](#not-supported)

### المنصات المدعومة

| المنصة | البناء | الاختبار | الحالة |
|--------|:------:|:--------:|--------|
| **Linux** (x86_64, Ubuntu 22.04+) | ✅ | ✅ | هدف CI الرئيسي |
| **Windows** (x86_64, MSVC) | ✅ | ✅ | هدف CI |
| **macOS** (x86_64 / aarch64) | ✅ | ✅ | اختبار المجتمع |

**MSRV**: Rust **1.75** (`rust-version = "1.75"` في `Cargo.toml`)

### توافق بروتوكول PG

| الميزة | الحالة | ملاحظات |
|--------|:------:|---------|
| بروتوكول الاستعلام البسيط | ✅ | جملة واحدة أو متعددة |
| الاستعلام الموسّع (Parse/Bind/Execute) | ✅ | جمل معدّة + بوابات |
| Auth: Trust | ✅ | أي مستخدم مقبول |
| Auth: MD5 | ✅ | نوع auth PG 5 |
| Auth: SCRAM-SHA-256 | ✅ | متوافق مع PG 10+ |
| Auth: كلمة المرور (نص صريح) | ✅ | نوع auth PG 3 |
| TLS/SSL | ✅ | SSLRequest → ترقية عند التهيئة |
| COPY IN/OUT | ✅ | صيغ النص وCSV |
| `psql` 12+ | ✅ | مختبر بالكامل |
| `pgbench` (init + run) | ✅ | السكريبتات المدمجة تعمل |
| JDBC (pgjdbc 42.x) | ✅ | مختبر مع 42.7+ |
| طلب الإلغاء | ✅ | استطلاع AtomicBool، زمن استجابة 50ms |
| LISTEN/NOTIFY | ✅ | مركز البث في الذاكرة |
| بروتوكول النسخ المتماثل المنطقي | ✅ | IDENTIFY_SYSTEM, CREATE/DROP_REPLICATION_SLOT, START_REPLICATION |

### تغطية SQL

| الفئة | المدعوم |
|-------|---------|
| **DDL** | CREATE/DROP/ALTER TABLE، CREATE/DROP INDEX، CREATE/DROP VIEW، CREATE/DROP SEQUENCE، TRUNCATE |
| **DML** | INSERT (شاملاً ON CONFLICT, RETURNING, SELECT)، UPDATE (شاملاً FROM, RETURNING)، DELETE (شاملاً USING, RETURNING)، COPY |
| **الاستعلامات** | WHERE، ORDER BY، LIMIT/OFFSET، DISTINCT، GROUP BY/HAVING، JOINs، الاستعلامات الفرعية، CTEs (شاملاً RECURSIVE)، UNION/INTERSECT/EXCEPT، دوال النوافذ |
| **التجميعات** | COUNT، SUM، AVG، MIN، MAX، STRING_AGG، BOOL_AND/OR، ARRAY_AGG |
| **الأنواع** | INT، BIGINT، FLOAT8، DECIMAL/NUMERIC، TEXT، BOOLEAN، TIMESTAMP، DATE، JSONB، ARRAY، SERIAL/BIGSERIAL |
| **المعاملات** | BEGIN/COMMIT/ROLLBACK، READ ONLY/READ WRITE، مهلة لكل معاملة، Read Committed، Snapshot Isolation |
| **الدوال** | أكثر من 500 دالة قياسية (سلاسل، رياضيات، تاريخ/وقت، تشفير، JSON، مصفوفات) |
| **المراقبة** | SHOW falcon.*، EXPLAIN، EXPLAIN ANALYZE، CHECKPOINT، ANALYZE TABLE |

### <a id="not-supported"></a>غير مدعوم (v1.2)

| الميزة | رمز الخطأ | رسالة الخطأ |
|--------|:---------:|------------|
| الإجراءات المخزّنة / PL/pgSQL | `0A000` | `stored procedures are not supported` |
| المشغّلات (Triggers) | `0A000` | `triggers are not supported` |
| طرق العرض المجسّدة | `0A000` | `materialized views are not supported` |
| أغلفة البيانات الخارجية (FDW) | `0A000` | `foreign data wrappers are not supported` |
| البحث النصي الكامل | `0A000` | `full-text search is not supported` |
| DDL عبر الإنترنت | `0A000` | `concurrent index operations are not supported` |
| HTAP / ColumnStore | — | معطّل في وقت الترجمة |

---

## 1. البناء

```bash
# بناء جميع الحزم (تصحيح)
cargo build --workspace

# بناء الإصدار
cargo build --release --workspace

# تشغيل الاختبارات (أكثر من 2,643 اختبار)
cargo test --workspace

# الفحص
cargo clippy --workspace
```

---

## 2. البدء السريع

```bash
# وضع الذاكرة (بدون WAL، الأسرع)
cargo run -p falcon_server -- --no-wal

# مع استمرارية WAL
cargo run -p falcon_server -- --data-dir ./falcon_data

# الاتصال عبر psql
psql -h 127.0.0.1 -p 5433 -U falcon
```

---

## 3. محركات التخزين

| المحرك | التخزين | الاستمرارية | الأنسب لـ |
|--------|--------|------------|----------|
| **Rowstore** (الافتراضي) | في الذاكرة (سلاسل إصدار MVCC) | WAL فقط | OLTP منخفض الزمن |
| **LSM** | القرص (LSM-Tree + WAL) | استمرارية كاملة على القرص | مجموعات البيانات الكبيرة |

---

## 4. نموذج المعاملات

- **LocalTxn (المسار السريع)**: تُيداع معاملات الشارد الواحد باستخدام OCC تحت عزل اللقطة — بدون حمل 2PC.
- **GlobalTxn (المسار البطيء)**: تستخدم المعاملات متعددة الشاردات XA-2PC مع prepare/commit.

---

## 5. البنية المعمارية

```
┌─────────────────────────────────────────────────────────┐
│  بروتوكول PG Wire (TCP)  │  البروتوكول الأصلي (TCP/TLS) │
├──────────────────────────┴─────────────────────────────┤
│         واجهة SQL الأمامية (sqlparser-rs → Binder)      │
├────────────────────────────────────────────────────────┤
│         المُخطّط / الموجّه                               │
├────────────────────────────────────────────────────────┤
│         المُنفّذ (صف بصف + تجميعات بث مدمجة)            │
├──────────────────┬─────────────────────────────────────┤
│   مدير المعاملات │   محرك التخزين                      │
│   (MVCC, OCC)    │   (MemTable + LSM + WAL + GC)       │
├──────────────────┴─────────────────────────────────────┤
│  الكتلة (ShardMap، النسخ المتماثل، التجاوز، Epoch)     │
└────────────────────────────────────────────────────────┘
```

---

## 6. المقاييس والمراقبة

```sql
-- إحصائيات المعاملات
SHOW falcon.txn_stats;

-- إحصائيات GC
SHOW falcon.gc_stats;

-- إحصائيات النسخ المتماثل
SHOW falcon.replication_stats;
```

---

## الترخيص

Apache-2.0
