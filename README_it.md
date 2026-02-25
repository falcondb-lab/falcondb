# FalconDB

<p align="center">
  <img src="assets/falcondb-logo.png" alt="FalconDB Logo" width="220" />
</p>

<h1 align="center">FalconDB</h1>

<p align="center">
  <strong>Compatibile con PG · Distribuito · Memory-First · Semantica Transazionale Deterministica</strong>
</p>

<p align="center">
  <a href="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml">
    <img src="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml/badge.svg" alt="CI" />
  </a>
  <img src="https://img.shields.io/badge/version-1.2.0-blue" alt="Versione" />
  <img src="https://img.shields.io/badge/MSRV-1.75-blue" alt="MSRV" />
  <img src="https://img.shields.io/badge/license-Apache--2.0-green" alt="Licenza" />
</p>

> English | [简体中文](README_zh.md) | [Español](README_es.md) | [Français](README_fr.md) | [한국어](README_ko.md) | [日本語](README_ja.md) | [Deutsch](README_de.md) | [العربية](README_ar.md) | **Italiano** | [Bahasa Melayu](README_ms.md) | [Português](README_pt.md)

> FalconDB è un **database OLTP compatibile con PG, distribuito e memory-first** con semantica transazionale deterministica. Benchmark confrontati con PostgreSQL, VoltDB e SingleStore — vedere la **[Matrice di Benchmark](benchmarks/README.md)**.
>
> - ✅ **Bassa latenza** — i commit sul percorso veloce a singolo shard bypassano completamente il 2PC
> - ✅ **Prestazioni di scansione** — aggregati in streaming fusi, iterazione MVCC senza copia, parità vicina a PG su 1M righe
> - ✅ **Stabilità** — p99 limitato, tasso di interruzione < 1%, benchmark riproducibili
> - ✅ **Coerenza dimostrabile** — MVCC/OCC sotto Snapshot Isolation, ACID verificato da CI
> - ✅ **Operabilità** — 50+ comandi SHOW, metriche Prometheus, gate CI per failover
> - ✅ **Determinismo** — macchina a stati robusta, in-doubt limitato, retry idempotenti
> - ❌ Non è HTAP — nessun carico di lavoro analitico
> - ❌ Non è PG completo — [vedere lista non supportati](#not-supported)

FalconDB fornisce OLTP stabile, transazioni su percorso veloce/lento, replica primario–replica basata su WAL con streaming gRPC, promozione/failover, garbage collection MVCC e benchmark riproducibili.

### Piattaforme Supportate

| Piattaforma | Build | Test | Stato |
|-------------|:-----:|:----:|-------|
| **Linux** (x86_64, Ubuntu 22.04+) | ✅ | ✅ | Target CI principale |
| **Windows** (x86_64, MSVC) | ✅ | ✅ | Target CI |
| **macOS** (x86_64 / aarch64) | ✅ | ✅ | Testato dalla community |

**MSRV**: Rust **1.75** (`rust-version = "1.75"` in `Cargo.toml`)

### Compatibilità Protocollo PG

| Funzionalità | Stato | Note |
|-------------|:-----:|------|
| Protocollo query semplice | ✅ | Singola + multi-istruzione |
| Query estesa (Parse/Bind/Execute) | ✅ | Istruzioni preparate + portali |
| Auth: Trust | ✅ | Qualsiasi utente accettato |
| Auth: MD5 | ✅ | Tipo auth PG 5 |
| Auth: SCRAM-SHA-256 | ✅ | Compatibile PG 10+ |
| Auth: Password (testo in chiaro) | ✅ | Tipo auth PG 3 |
| TLS/SSL | ✅ | SSLRequest → upgrade se configurato |
| COPY IN/OUT | ✅ | Formati testo e CSV |
| `psql` 12+ | ✅ | Completamente testato |
| `pgbench` (init + run) | ✅ | Script integrati funzionanti |
| JDBC (pgjdbc 42.x) | ✅ | Testato con 42.7+ |
| Richiesta di annullamento | ✅ | Polling AtomicBool, latenza 50ms |
| LISTEN/NOTIFY | ✅ | Hub di broadcast in memoria |
| Protocollo di replica logica | ✅ | IDENTIFY_SYSTEM, CREATE/DROP_REPLICATION_SLOT, START_REPLICATION |

### Copertura SQL

| Categoria | Supportato |
|-----------|-----------|
| **DDL** | CREATE/DROP/ALTER TABLE, CREATE/DROP INDEX, CREATE/DROP VIEW, CREATE/DROP SEQUENCE, TRUNCATE |
| **DML** | INSERT (incl. ON CONFLICT, RETURNING, SELECT), UPDATE (incl. FROM, RETURNING), DELETE (incl. USING, RETURNING), COPY |
| **Query** | WHERE, ORDER BY, LIMIT/OFFSET, DISTINCT, GROUP BY/HAVING, JOIN, sottoquery, CTE (incl. RECURSIVE), UNION/INTERSECT/EXCEPT, funzioni finestra |
| **Aggregati** | COUNT, SUM, AVG, MIN, MAX, STRING_AGG, BOOL_AND/OR, ARRAY_AGG |
| **Tipi** | INT, BIGINT, FLOAT8, DECIMAL/NUMERIC, TEXT, BOOLEAN, TIMESTAMP, DATE, JSONB, ARRAY, SERIAL/BIGSERIAL |
| **Transazioni** | BEGIN/COMMIT/ROLLBACK, READ ONLY/READ WRITE, timeout per transazione, Read Committed, Snapshot Isolation |
| **Funzioni** | 500+ funzioni scalari (stringhe, matematica, data/ora, crypto, JSON, array) |
| **Osservabilità** | SHOW falcon.*, EXPLAIN, EXPLAIN ANALYZE, CHECKPOINT, ANALYZE TABLE |

### <a id="not-supported"></a>Non Supportato (v1.2)

| Funzionalità | Codice Errore | Messaggio di Errore |
|-------------|:------------:|-------------------|
| Procedure memorizzate / PL/pgSQL | `0A000` | `stored procedures are not supported` |
| Trigger | `0A000` | `triggers are not supported` |
| Viste materializzate | `0A000` | `materialized views are not supported` |
| Wrapper dati esterni (FDW) | `0A000` | `foreign data wrappers are not supported` |
| Ricerca full-text | `0A000` | `full-text search is not supported` |
| DDL online | `0A000` | `concurrent index operations are not supported` |
| HTAP / ColumnStore | — | Disabilitato in fase di compilazione |

---

## 1. Compilazione

```bash
# Compilare tutti i crate (debug)
cargo build --workspace

# Build release
cargo build --release --workspace

# Eseguire i test (2.643+ test)
cargo test --workspace

# Lint
cargo clippy --workspace
```

---

## 2. Avvio Rapido

```bash
# Modalità in memoria (senza WAL, più veloce)
cargo run -p falcon_server -- --no-wal

# Con persistenza WAL
cargo run -p falcon_server -- --data-dir ./falcon_data

# Connessione via psql
psql -h 127.0.0.1 -p 5433 -U falcon
```

---

## 3. Motori di Storage

| Motore | Storage | Persistenza | Ideale Per |
|--------|--------|------------|-----------|
| **Rowstore** (predefinito) | In memoria (catene di versioni MVCC) | Solo WAL | OLTP a bassa latenza |
| **LSM** | Disco (LSM-Tree + WAL) | Persistenza completa su disco | Dataset di grandi dimensioni |

---

## 4. Modello di Transazione

- **LocalTxn (percorso veloce)**: le transazioni su singolo shard eseguono commit con OCC sotto Snapshot Isolation — nessun overhead 2PC.
- **GlobalTxn (percorso lento)**: le transazioni cross-shard usano XA-2PC con prepare/commit.

---

## 5. Architettura

```
┌─────────────────────────────────────────────────────────┐
│  Protocollo PG Wire (TCP)  │  Protocollo Nativo (TCP/TLS)│
├──────────────────────────┴─────────────────────────────┤
│         Frontend SQL (sqlparser-rs → Binder)            │
├────────────────────────────────────────────────────────┤
│         Pianificatore / Router                          │
├────────────────────────────────────────────────────────┤
│         Executor (riga per riga + aggregati streaming)  │
├──────────────────┬─────────────────────────────────────┤
│   Gestore Txn    │   Motore di Storage                 │
│   (MVCC, OCC)    │   (MemTable + LSM + WAL + GC)       │
├──────────────────┴─────────────────────────────────────┤
│  Cluster (ShardMap, Replica, Failover, Epoch)          │
└────────────────────────────────────────────────────────┘
```

---

## 6. Metriche e Osservabilità

```sql
-- Statistiche transazioni
SHOW falcon.txn_stats;

-- Statistiche GC
SHOW falcon.gc_stats;

-- Statistiche replica
SHOW falcon.replication_stats;
```

---

## Licenza

Apache-2.0
