# FalconDB

<p align="center">
  <img src="assets/falcondb-logo.png" alt="FalconDB Logo" width="220" />
</p>

<h1 align="center">FalconDB</h1>

<p align="center">
  <strong>Serasi PG · Teragih · Memori Pertama · Semantik Transaksi Deterministik</strong>
</p>

<p align="center">
  <a href="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml">
    <img src="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml/badge.svg" alt="CI" />
  </a>
  <img src="https://img.shields.io/badge/version-1.2.0-blue" alt="Versi" />
  <img src="https://img.shields.io/badge/MSRV-1.75-blue" alt="MSRV" />
  <img src="https://img.shields.io/badge/license-Apache--2.0-green" alt="Lesen" />
</p>

> English | [简体中文](README_zh.md) | [Español](README_es.md) | [Français](README_fr.md) | [한국어](README_ko.md) | [日本語](README_ja.md) | [Deutsch](README_de.md) | [العربية](README_ar.md) | [Italiano](README_it.md) | **Bahasa Melayu** | [Português](README_pt.md)

> FalconDB ialah **pangkalan data OLTP yang serasi PG, teragih dan mengutamakan memori** dengan semantik transaksi deterministik. Penanda aras berbanding PostgreSQL, VoltDB dan SingleStore — lihat **[Matriks Penanda Aras](benchmarks/README.md)**.
>
> - ✅ **Kependaman rendah** — komit laluan pantas shard tunggal memintas 2PC sepenuhnya
> - ✅ **Prestasi imbasan** — agregat penstriman bercantum, lelaran MVCC sifar-salin, pariti hampir PG pada 1M baris
> - ✅ **Kestabilan** — p99 terbatas, kadar pembatalan < 1%, penanda aras boleh dihasilkan semula
> - ✅ **Konsistensi terbukti** — MVCC/OCC di bawah Pengasingan Snapshot, ACID disahkan CI
> - ✅ **Kebolehoperasian** — 50+ perintah SHOW, metrik Prometheus, get CI failover
> - ✅ **Determinisme** — mesin keadaan yang teguh, in-doubt terbatas, cuba semula idempoten
> - ❌ Bukan HTAP — tiada beban kerja analitik
> - ❌ Bukan PG penuh — [lihat senarai tidak disokong](#not-supported)

FalconDB menyediakan OLTP stabil, transaksi laluan pantas/perlahan, replikasi primer–replika berasaskan WAL dengan penstriman gRPC, promote/failover, pengumpulan sampah MVCC dan penanda aras boleh dihasilkan semula.

### Platform Disokong

| Platform | Bina | Ujian | Status |
|----------|:----:|:-----:|--------|
| **Linux** (x86_64, Ubuntu 22.04+) | ✅ | ✅ | Sasaran CI utama |
| **Windows** (x86_64, MSVC) | ✅ | ✅ | Sasaran CI |
| **macOS** (x86_64 / aarch64) | ✅ | ✅ | Diuji komuniti |

**MSRV**: Rust **1.75** (`rust-version = "1.75"` dalam `Cargo.toml`)

### Keserasian Protokol PG

| Ciri | Status | Nota |
|------|:------:|------|
| Protokol pertanyaan mudah | ✅ | Satu + berbilang penyataan |
| Pertanyaan lanjutan (Parse/Bind/Execute) | ✅ | Penyataan tersedia + portal |
| Auth: Trust | ✅ | Mana-mana pengguna diterima |
| Auth: MD5 | ✅ | Jenis auth PG 5 |
| Auth: SCRAM-SHA-256 | ✅ | Serasi PG 10+ |
| Auth: Kata laluan (teks biasa) | ✅ | Jenis auth PG 3 |
| TLS/SSL | ✅ | SSLRequest → naik taraf apabila dikonfigurasi |
| COPY IN/OUT | ✅ | Format teks dan CSV |
| `psql` 12+ | ✅ | Diuji sepenuhnya |
| `pgbench` (init + run) | ✅ | Skrip terbina dalam berfungsi |
| JDBC (pgjdbc 42.x) | ✅ | Diuji dengan 42.7+ |
| Permintaan batal | ✅ | Pengundian AtomicBool, kependaman 50ms |
| LISTEN/NOTIFY | ✅ | Hab siaran dalam memori |
| Protokol replikasi logik | ✅ | IDENTIFY_SYSTEM, CREATE/DROP_REPLICATION_SLOT, START_REPLICATION |

### Liputan SQL

| Kategori | Disokong |
|---------|---------|
| **DDL** | CREATE/DROP/ALTER TABLE, CREATE/DROP INDEX, CREATE/DROP VIEW, CREATE/DROP SEQUENCE, TRUNCATE |
| **DML** | INSERT (termasuk ON CONFLICT, RETURNING, SELECT), UPDATE (termasuk FROM, RETURNING), DELETE (termasuk USING, RETURNING), COPY |
| **Pertanyaan** | WHERE, ORDER BY, LIMIT/OFFSET, DISTINCT, GROUP BY/HAVING, JOIN, subpertanyaan, CTE (termasuk RECURSIVE), UNION/INTERSECT/EXCEPT, fungsi tetingkap |
| **Agregat** | COUNT, SUM, AVG, MIN, MAX, STRING_AGG, BOOL_AND/OR, ARRAY_AGG |
| **Jenis** | INT, BIGINT, FLOAT8, DECIMAL/NUMERIC, TEXT, BOOLEAN, TIMESTAMP, DATE, JSONB, ARRAY, SERIAL/BIGSERIAL |
| **Transaksi** | BEGIN/COMMIT/ROLLBACK, READ ONLY/READ WRITE, tamat masa per transaksi, Read Committed, Snapshot Isolation |
| **Fungsi** | 500+ fungsi skalar (rentetan, matematik, tarikh/masa, kripto, JSON, tatasusunan) |
| **Kebolehmerhatian** | SHOW falcon.*, EXPLAIN, EXPLAIN ANALYZE, CHECKPOINT, ANALYZE TABLE |

### <a id="not-supported"></a>Tidak Disokong (v1.2)

| Ciri | Kod Ralat | Mesej Ralat |
|------|:---------:|------------|
| Prosedur tersimpan / PL/pgSQL | `0A000` | `stored procedures are not supported` |
| Pencetus (Trigger) | `0A000` | `triggers are not supported` |
| Paparan terwujud | `0A000` | `materialized views are not supported` |
| Pembalut data luaran (FDW) | `0A000` | `foreign data wrappers are not supported` |
| Carian teks penuh | `0A000` | `full-text search is not supported` |
| DDL dalam talian | `0A000` | `concurrent index operations are not supported` |
| HTAP / ColumnStore | — | Dilumpuhkan pada masa kompilasi |

---

## 1. Membina

```bash
# Bina semua crate (debug)
cargo build --workspace

# Bina release
cargo build --release --workspace

# Jalankan ujian (2,643+ ujian)
cargo test --workspace

# Lint
cargo clippy --workspace
```

---

## 2. Permulaan Pantas

```bash
# Mod dalam memori (tanpa WAL, paling pantas)
cargo run -p falcon_server -- --no-wal

# Dengan kegigihan WAL
cargo run -p falcon_server -- --data-dir ./falcon_data

# Sambung melalui psql
psql -h 127.0.0.1 -p 5433 -U falcon
```

---

## 3. Enjin Storan

| Enjin | Storan | Kegigihan | Terbaik Untuk |
|-------|--------|-----------|--------------|
| **Rowstore** (lalai) | Dalam memori (rantai versi MVCC) | WAL sahaja | OLTP kependaman rendah |
| **LSM** | Cakera (LSM-Tree + WAL) | Kegigihan cakera penuh | Set data besar |

---

## 4. Model Transaksi

- **LocalTxn (laluan pantas)**: transaksi shard tunggal komit dengan OCC di bawah Snapshot Isolation — tiada overhead 2PC.
- **GlobalTxn (laluan perlahan)**: transaksi merentas shard menggunakan XA-2PC dengan prepare/commit.

---

## 5. Seni Bina

```
┌─────────────────────────────────────────────────────────┐
│  Protokol PG Wire (TCP)  │  Protokol Natif (TCP/TLS)    │
├──────────────────────────┴─────────────────────────────┤
│         Frontend SQL (sqlparser-rs → Binder)            │
├────────────────────────────────────────────────────────┤
│         Perancang / Penghala                            │
├────────────────────────────────────────────────────────┤
│         Pelaksana (baris demi baris + agregat streaming)│
├──────────────────┬─────────────────────────────────────┤
│   Pengurus Txn   │   Enjin Storan                      │
│   (MVCC, OCC)    │   (MemTable + LSM + WAL + GC)       │
├──────────────────┴─────────────────────────────────────┤
│  Kelompok (ShardMap, Replikasi, Failover, Epoch)       │
└────────────────────────────────────────────────────────┘
```

---

## 6. Metrik dan Kebolehmerhatian

```sql
-- Statistik transaksi
SHOW falcon.txn_stats;

-- Statistik GC
SHOW falcon.gc_stats;

-- Statistik replikasi
SHOW falcon.replication_stats;
```

---

## Lesen

Apache-2.0
