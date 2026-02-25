# FalconDB

<p align="center">
  <img src="assets/falcondb-logo.png" alt="FalconDB Logo" width="220" />
</p>

<h1 align="center">FalconDB</h1>

<p align="center">
  <strong>PG-Kompatibel · Verteilt · Memory-First · Deterministische Transaktionssemantik</strong>
</p>

<p align="center">
  <a href="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml">
    <img src="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml/badge.svg" alt="CI" />
  </a>
  <img src="https://img.shields.io/badge/version-1.2.0-blue" alt="Version" />
  <img src="https://img.shields.io/badge/MSRV-1.75-blue" alt="MSRV" />
  <img src="https://img.shields.io/badge/license-Apache--2.0-green" alt="Lizenz" />
</p>

> English | [简体中文](README_zh.md) | [Español](README_es.md) | [Français](README_fr.md) | [한국어](README_ko.md) | [日本語](README_ja.md) | **Deutsch** | [العربية](README_ar.md) | [Italiano](README_it.md) | [Bahasa Melayu](README_ms.md) | [Português](README_pt.md)

> FalconDB ist eine **PG-kompatible, verteilte, Memory-First-OLTP-Datenbank** mit deterministischer Transaktionssemantik. Benchmarked gegen PostgreSQL, VoltDB und SingleStore — siehe **[Benchmark-Matrix](benchmarks/README.md)**.
>
> - ✅ **Geringe Latenz** — Single-Shard-Fast-Path-Commits umgehen 2PC vollständig
> - ✅ **Scan-Leistung** — fusionierte Streaming-Aggregate, Zero-Copy-MVCC-Iteration, nahe PG-Parität bei 1M Zeilen
> - ✅ **Stabilität** — p99 begrenzt, Abbruchrate < 1%, reproduzierbare Benchmarks
> - ✅ **Nachweisbare Konsistenz** — MVCC/OCC unter Snapshot Isolation, CI-verifiziertes ACID
> - ✅ **Betreibbarkeit** — 50+ SHOW-Befehle, Prometheus-Metriken, Failover-CI-Gate
> - ✅ **Determinismus** — robuste Zustandsmaschine, begrenztes In-Doubt, idempotente Wiederholungen
> - ❌ Kein HTAP — keine analytischen Workloads
> - ❌ Kein vollständiges PG — [siehe nicht unterstützte Liste](#not-supported)

FalconDB bietet stabiles OLTP, Fast-/Slow-Path-Transaktionen, WAL-basierte Primär-Replikat-Replikation mit gRPC-Streaming, Promote/Failover, MVCC-Garbage-Collection und reproduzierbare Benchmarks.

### Unterstützte Plattformen

| Plattform | Build | Test | Status |
|-----------|:-----:|:----:|--------|
| **Linux** (x86_64, Ubuntu 22.04+) | ✅ | ✅ | Primäres CI-Ziel |
| **Windows** (x86_64, MSVC) | ✅ | ✅ | CI-Ziel |
| **macOS** (x86_64 / aarch64) | ✅ | ✅ | Community-getestet |

**MSRV**: Rust **1.75** (`rust-version = "1.75"` in `Cargo.toml`)

### PG-Protokoll-Kompatibilität

| Funktion | Status | Hinweise |
|---------|:------:|---------|
| Einfaches Abfrageprotokoll | ✅ | Einzel- + Mehrfachanweisungen |
| Erweitertes Abfrageprotokoll (Parse/Bind/Execute) | ✅ | Vorbereitete Anweisungen + Portale |
| Auth: Trust | ✅ | Jeder Benutzer akzeptiert |
| Auth: MD5 | ✅ | PG-Auth-Typ 5 |
| Auth: SCRAM-SHA-256 | ✅ | PG 10+ kompatibel |
| Auth: Passwort (Klartext) | ✅ | PG-Auth-Typ 3 |
| TLS/SSL | ✅ | SSLRequest → Upgrade bei Konfiguration |
| COPY IN/OUT | ✅ | Text- und CSV-Formate |
| `psql` 12+ | ✅ | Vollständig getestet |
| `pgbench` (init + run) | ✅ | Eingebaute Skripte funktionieren |
| JDBC (pgjdbc 42.x) | ✅ | Getestet mit 42.7+ |
| Abbruchanforderung | ✅ | AtomicBool-Polling, 50ms Latenz |
| LISTEN/NOTIFY | ✅ | In-Memory-Broadcast-Hub |
| Logisches Replikationsprotokoll | ✅ | IDENTIFY_SYSTEM, CREATE/DROP_REPLICATION_SLOT, START_REPLICATION |

### SQL-Abdeckung

| Kategorie | Unterstützt |
|-----------|------------|
| **DDL** | CREATE/DROP/ALTER TABLE, CREATE/DROP INDEX, CREATE/DROP VIEW, CREATE/DROP SEQUENCE, TRUNCATE |
| **DML** | INSERT (inkl. ON CONFLICT, RETURNING, SELECT), UPDATE (inkl. FROM, RETURNING), DELETE (inkl. USING, RETURNING), COPY |
| **Abfragen** | WHERE, ORDER BY, LIMIT/OFFSET, DISTINCT, GROUP BY/HAVING, JOINs, Unterabfragen, CTEs (inkl. RECURSIVE), UNION/INTERSECT/EXCEPT, Fensterfunktionen |
| **Aggregate** | COUNT, SUM, AVG, MIN, MAX, STRING_AGG, BOOL_AND/OR, ARRAY_AGG |
| **Typen** | INT, BIGINT, FLOAT8, DECIMAL/NUMERIC, TEXT, BOOLEAN, TIMESTAMP, DATE, JSONB, ARRAY, SERIAL/BIGSERIAL |
| **Transaktionen** | BEGIN/COMMIT/ROLLBACK, READ ONLY/READ WRITE, Timeout pro Transaktion, Read Committed, Snapshot Isolation |
| **Funktionen** | 500+ skalare Funktionen (Zeichenketten, Mathematik, Datum/Zeit, Krypto, JSON, Arrays) |
| **Beobachtbarkeit** | SHOW falcon.*, EXPLAIN, EXPLAIN ANALYZE, CHECKPOINT, ANALYZE TABLE |

### <a id="not-supported"></a>Nicht Unterstützt (v1.2)

| Funktion | Fehlercode | Fehlermeldung |
|---------|:----------:|--------------|
| Gespeicherte Prozeduren / PL/pgSQL | `0A000` | `stored procedures are not supported` |
| Trigger | `0A000` | `triggers are not supported` |
| Materialisierte Views | `0A000` | `materialized views are not supported` |
| Externe Datenwrapper (FDW) | `0A000` | `foreign data wrappers are not supported` |
| Volltextsuche | `0A000` | `full-text search is not supported` |
| Online-DDL | `0A000` | `concurrent index operations are not supported` |
| HTAP / ColumnStore | — | Zur Kompilierzeit deaktiviert |

---

## 1. Kompilieren

```bash
# Alle Crates kompilieren (debug)
cargo build --workspace

# Release-Build
cargo build --release --workspace

# Tests ausführen (2.643+ Tests)
cargo test --workspace

# Lint
cargo clippy --workspace
```

---

## 2. Schnellstart

```bash
# In-Memory-Modus (ohne WAL, am schnellsten)
cargo run -p falcon_server -- --no-wal

# Mit WAL-Persistenz
cargo run -p falcon_server -- --data-dir ./falcon_data

# Verbindung via psql
psql -h 127.0.0.1 -p 5433 -U falcon
```

---

## 3. Storage-Engines

| Engine | Speicher | Persistenz | Am besten für |
|--------|---------|-----------|--------------|
| **Rowstore** (Standard) | In-Memory (MVCC-Versionsketten) | Nur WAL | Niedrig-Latenz-OLTP |
| **LSM** | Festplatte (LSM-Tree + WAL) | Vollständige Festplattenpersistenz | Große Datensätze |

---

## 4. Transaktionsmodell

- **LocalTxn (Fast Path)**: Single-Shard-Transaktionen committen mit OCC unter Snapshot Isolation — kein 2PC-Overhead.
- **GlobalTxn (Slow Path)**: Cross-Shard-Transaktionen verwenden XA-2PC mit prepare/commit.

---

## 5. Architektur

```
┌─────────────────────────────────────────────────────────┐
│  PG-Wire-Protokoll (TCP)  │  Natives Protokoll (TCP/TLS)│
├──────────────────────────┴─────────────────────────────┤
│         SQL-Frontend (sqlparser-rs → Binder)            │
├────────────────────────────────────────────────────────┤
│         Planer / Router                                 │
├────────────────────────────────────────────────────────┤
│         Executor (zeilenweise + fusionierte Streaming-  │
│         Aggregate)                                      │
├──────────────────┬─────────────────────────────────────┤
│   Transaktions-  │   Storage-Engine                    │
│   Manager        │   (MemTable + LSM + WAL + GC)       │
├──────────────────┴─────────────────────────────────────┤
│  Cluster (ShardMap, Replikation, Failover, Epoch)      │
└────────────────────────────────────────────────────────┘
```

---

## 6. Metriken und Beobachtbarkeit

```sql
-- Transaktionsstatistiken
SHOW falcon.txn_stats;

-- GC-Statistiken
SHOW falcon.gc_stats;

-- Replikationsstatistiken
SHOW falcon.replication_stats;
```

---

## Lizenz

Apache-2.0
