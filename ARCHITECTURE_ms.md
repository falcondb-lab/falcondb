# FalconDB — Pangkalan Data OLTP Teragih Dalam Memori, Serasi Protokol PG

> **[English](ARCHITECTURE.md)** | [简体中文](ARCHITECTURE_zh.md) | [日本語](ARCHITECTURE_ja.md) | [한국어](ARCHITECTURE_ko.md) | [Français](ARCHITECTURE_fr.md) | [Deutsch](ARCHITECTURE_de.md) | [Español](ARCHITECTURE_es.md) | [Português](ARCHITECTURE_pt.md) | [Italiano](ARCHITECTURE_it.md) | [العربية](ARCHITECTURE_ar.md) | Bahasa Melayu

## 0. Sifat Teras: Jaminan Komit Deterministik (DCG)

> **Jika FalconDB mengembalikan "committed", transaksi tersebut akan bertahan melalui sebarang kegagalan nod tunggal, sebarang failover dan sebarang pemulihan — tanpa pengecualian.**

**Jaminan Komit Deterministik** dikuatkuasakan oleh model titik komit (§1 dalam `falcon_common::consistency`): di bawah dasar komit aktif, tiada ACK klien dihantar sebelum kekekalan WAL. Empat fasa komit — **WAL direkod → WAL dikekalkan → Kelihatan → Disahkan** — maju secara ketat ke hadapan; sebarang percubaan undur ditolak sebagai `InvariantViolation`.

- Bukti: [docs/consistency_evidence_map.md](docs/consistency_evidence_map.md)
- Rajah jujukan: [docs/commit_sequence.md](docs/commit_sequence.md)
- Bukan-matlamat: [docs/non_goals.md](docs/non_goals.md)

## 1. Gambaran Keseluruhan Seni Bina

### 1.1 Aliran Data (laluan biasa: SELECT)

```
psql ──TCP──▶ [Lapisan Protokol (PG Wire)]
                  │
                  ▼
            [Frontend SQL: Parse → Bind → Analyze]
                  │
                  ▼
            [Perancang / Penghala]
                  │
          ┌───────┴────────┐
          │ Shard tunggal  │ Berbilang shard
          ▼                ▼
    [Pelaksana tempatan] [Penyelaras teragih]
          │                │
          ▼                ▼
    [Pengurus Txn ─── MVCC ─── Enjin Storan]
          │
          ▼
    [Jadual Memori + Indeks]
          │
          ▼ (async)
    [WAL / Titik Semak]
```

### 1.2 Aliran Data (laluan tulis)

```
Klien INSERT ──▶ PG Wire ──▶ Frontend SQL ──▶ Perancang/Penghala
    ──▶ Mula Txn ──▶ Penentuan shard (hash PK)
    ──▶ LocalTxn (laluan pantas, shard tunggal, tanpa 2PC)
        │  atau GlobalTxn (laluan perlahan, berbilang shard, XA-2PC)
    ──▶ Komit ──▶ Gunakan MemTable + WAL fsync
    ──▶ WAL dihantar ke replika (async, skim A)
    ──▶ Pengesahan klien
```

### 1.3 Peranan Nod (M1: Utama / Replika)

M1 menggunakan topologi utama-replika bagi setiap shard:
- **Utama**: Menerima baca/tulis; mengesahkan komit selepas kekekalan WAL.
- **Replika**: Menerima ketulan WAL; baca sahaja sebelum kenaikan pangkat.
- **Kenaikan pangkat**: Failover berasaskan pagar (fence → kejar → tukar → unfence).

---

## 2. Inventori Modul

### 2.1 `protocol_pg` — Protokol Wire PostgreSQL

| Elemen | Butiran |
|--------|---------|
| **Tanggungjawab** | Penerimaan TCP, jabat tangan PG, penghuraian Simple/Extended Query, penyirian respons |
| **Jenis utama** | `PgConnection`, `PgMessage`, `AuthMethod`, `PgSession` |
| **Guna semula OSS** | crate `pgwire` (MIT) untuk kodek mesej |

### 2.2 `sql_frontend` — Penghurai / Penganalisis / Pengikat

| Elemen | Butiran |
|--------|---------|
| **Tanggungjawab** | Teks SQL → AST → Pelan logik diselesaikan dengan rujukan katalog |
| **Jenis utama** | `Statement`, `Expr`, `TableRef`, `ColumnRef`, `BoundStatement` |
| **Guna semula OSS** | `sqlparser-rs` (Apache-2.0) |

### 2.3 `planner_router` — Perancang Pertanyaan & Penghala Teragih

| Elemen | Butiran |
|--------|---------|
| **Tanggungjawab** | Pelan logik → Pelan fizikal; penentuan shard sasaran; tolak turun penapis |
| **Jenis utama** | `LogicalPlan`, `PhysicalPlan`, `ShardTarget`, `PushDown` |

### 2.4 `executor` — Enjin Pelaksanaan

| Elemen | Butiran |
|--------|---------|
| **Tanggungjawab** | Pelaksanaan operator pelan fizikal; gabenor pertanyaan; pengagregatan bercantum secara aliran |
| **Jenis utama** | `Operator` trait, `SeqScan`, `IndexScan`, `Filter`, `Project`, `Sort`, `Agg`, `QueryGovernor` |
| **Laluan pantas** | `exec_fused_aggregate` — WHERE+GROUP BY+agregat dalam satu laluan MVCC (sifar salinan baris) |

### 2.5 `txn` — Pengurus Transaksi

| Elemen | Butiran |
|--------|---------|
| **Tanggungjawab** | Mula/komit/batal transaksi; peruntukan cap masa MVCC; pengesahan OCC (SI) |
| **Jenis utama** | `TxnManager`, `TxnHandle`, `TxnId`, `Timestamp`, `IsolationLevel` |

### 2.6 `storage` — Enjin Storan Dalam Memori

| Elemen | Butiran |
|--------|---------|
| **Tanggungjawab** | Storan tupel dan versi MVCC dalam memori; penyelenggaraan indeks; tulis WAL + pemulihan; GC MVCC |
| **Jenis utama** | `StorageEngine`, `MemTable`, `VersionChain`, `SecondaryIndex`, `WalWriter`, `GcRunner` |

### 2.7 `raft` — Konsensus (stub — bukan laluan pengeluaran)

> **⚠️ Crate ini ialah stub nod tunggal. Ia tidak digunakan dalam laluan data pengeluaran.**

### 2.8 `cluster` — Kluster, Replikasi dan Pelaksanaan Teragih

| Elemen | Butiran |
|--------|---------|
| **Tanggungjawab** | Peta shard; replikasi utama-replika berasaskan WAL; kenaikan pangkat/failover dengan pagar; pelaksanaan teragih scatter/gather |
| **Jenis utama** | `ShardMap`, `WalChunk`, `ReplicationTransport`, `DistributedQueryEngine` |

### 2.9 `observability` — Metrik / Penjejakan / Pengelogan

Metrik Prometheus, penjejakan OpenTelemetry, pengelogan berstruktur.

### 2.10 `common` — Infrastruktur Dikongsi

Jenis dikongsi, hierarki ralat, konfigurasi, sistem jenis PG, perwakilan datum, RBAC.

---

## 3. Skop MVP

### 3.1 Disokong

| Kategori | Skop |
|----------|------|
| **Protokol** | PG wire Simple Query + Extended Query; pengesahan trust |
| **SQL DDL** | `CREATE TABLE`, `DROP TABLE`, `TRUNCATE`, `ALTER TABLE`, `CREATE INDEX`, `CREATE VIEW` |
| **SQL DML** | `INSERT`, `UPDATE`, `DELETE` (dengan `RETURNING`), `SELECT`, `JOIN`, subpertanyaan, CTE, CTE rekursif, fungsi tingkap, 260+ fungsi skalar |
| **Jenis** | `INT`, `BIGINT`, `TEXT`, `BOOL`, `FLOAT8`/`NUMERIC`/`DECIMAL`, `TIMESTAMP`, `JSONB`, `ARRAY`, `DATE` |
| **Transaksi** | `BEGIN`, `COMMIT`, `ROLLBACK`; tahap pengasingan Read Committed |
| **Storan** | Stor baris dalam memori; indeks hash PK; indeks sekunder BTree |
| **Kekekalan** | WAL (segmen 64 MB, komit berkumpulan); pemulihan selepas ranap |
| **Teragih** | Pembahagian hash PK; replikasi WAL gRPC; laluan pantas shard tunggal + 2PC rentas shard |

### 3.2 Tidak disokong (M1)

- Replikasi logik, CDC
- Perubahan skema dalam talian
- Multi-penyewa
- Pengimbangan semula automatik (manual sahaja)

---

## 4. Matriks Guna Semula OSS

| Modul | Calon | Lesen | Sebab |
|-------|-------|-------|-------|
| **PG Wire** | `pgwire` 0.25+ | MIT | Kodek mesej PG lengkap |
| **Penghurai SQL** | `sqlparser-rs` 0.50+ | Apache-2.0 | Dialek PG terbukti |
| **Runtime async** | `tokio` 1.x | MIT | Standard de facto |
| **RPC** | `tonic` (gRPC) | MIT | Matang, penjanaan kod, penstriman |
| **Metrik** | `metrics` + `metrics-exporter-prometheus` | MIT | Ringan, asli Prometheus |

**Dasar fork**: Tiada fork. Semua kebergantungan menggunakan crate yang diterbitkan.

---

## 5. Reka Bentuk Transaksi dan Ketekalan

### 5.2 Dilaksanakan: Laluan Pantas + Laluan Perlahan + OCC

- **LocalTxn (laluan pantas)**: Transaksi shard tunggal, komit oleh pengesahan OCC di bawah pengasingan snapshot. Tanpa beban 2PC.
- **GlobalTxn (laluan perlahan)**: Transaksi rentas shard dengan XA-2PC.
- **Penguatkuasaan invarian keras**: `TxnContext.validate_commit_invariants()` mengembalikan ralat `InvariantViolation`.

### 5.4 GC MVCC

- **Titik selamat**: `gc_safepoint = min(min_active_ts, replica_safe_ts) - 1`
- **Sedar WAL**: Tidak pernah mengumpul versi yang belum dikomit atau dibatalkan
- **Selamat untuk replikasi**: Menghormati cap masa yang digunakan oleh replika

### 5.5 Replikasi — Pengangkutan WAL melalui gRPC

```
Utama ──Ketulan WAL──▶ Replika
         (strim gRPC, tonic, RPC SubscribeWal)
```

> **⚠️ Had keras**: Crate `falcon_raft` ialah stub nod tunggal. Replika pengangkutan WAL **bukan** pengikut Raft.

---

## 6. Struktur Repositori

```
falcon/
├── Cargo.toml                  (akar ruang kerja)
├── crates/
│   ├── falcon_common/           (jenis dikongsi, ralat, konfigurasi, RBAC)
│   ├── falcon_storage/          (jadual, LSM, indeks, WAL, GC)
│   ├── falcon_txn/              (pengurusan txn, MVCC, OCC)
│   ├── falcon_sql_frontend/     (penghurai, pengikat, penganalisis)
│   ├── falcon_planner/          (pelan logik+fizikal)
│   ├── falcon_executor/         (pelaksanaan operator)
│   ├── falcon_protocol_pg/      (protokol PG wire)
│   ├── falcon_cluster/          (replikasi, failover, scatter/gather)
│   ├── falcon_observability/    (metrik, penjejakan, pengelogan)
│   ├── falcon_server/           (binari utama + ujian integrasi)
│   └── falcon_bench/            (harnes penanda aras YCSB)
├── clients/
│   └── falcondb-jdbc/           (pemacu Java JDBC)
└── scripts/
```

---

## 7. Sasaran Prestasi

| Metrik | Sasaran |
|--------|---------|
| QPS baca titik | ≥ 200K |
| Baca titik P50 / P99 | < 0.2ms / < 1ms |
| QPS tulis titik | ≥ 100K |
| QPS campuran (50/50) | ≥ 120K |
| QPS tulis (kluster 3 nod) | ≥ 50K |

---

## 8. Ujian dan Pencapaian

### 8.2 Bilangan Ujian (3,654 ujian)

| Crate | Ujian |
|-------|-------|
| `falcon_cluster` | 1,057 |
| `falcon_storage` | 776 |
| `falcon_server` (integrasi) | 421 |
| `falcon_common` | 254 |
| `falcon_protocol_pg` | 240 |
| `falcon_cli` | 201 |
| `falcon_executor` | 192 |
| `falcon_sql_frontend` | 149 |
| `falcon_txn` | 103 |
| `falcon_planner` | 89 |
| **Jumlah** | **3,654** (lulus) |

### 8.4 Pencapaian

| Fasa | Skop | Status |
|------|------|--------|
| **M1** | OLTP stabil, transaksi laluan pantas/perlahan, replikasi WAL | **Selesai** |
| **M2** | Penstriman WAL gRPC, penempatan berbilang nod | Selesai |
| **M3** | Pengerasan pengeluaran: replika baca sahaja, penutupan anggun | **Selesai** |
| **M4** | Pengoptimuman pertanyaan, EXPLAIN ANALYZE, log perlahan | **Selesai** |
| **M5** | information_schema, paparan, cache pelan | **Selesai** |
| **M6** | JSONB, subpertanyaan berkorelasi, FK CASCADE, CTE rekursif | **Selesai** |
| **M7** | Fungsi jujukan, fungsi tingkap | **Selesai** |
| **M8** | Jenis DATE, arahan COPY | **Selesai** |

---

## 9. Fungsi Skalar Lanjutan

> **Dipindahkan ke [docs/extended_scalar_functions.md](docs/extended_scalar_functions.md)**. Mengandungi spesifikasi 260+ fungsi.
