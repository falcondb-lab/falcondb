# FalconDB — PG-protokollkompatible verteilte In-Memory-OLTP-Datenbank

> **[English](ARCHITECTURE.md)** | [简体中文](ARCHITECTURE_zh.md) | [日本語](ARCHITECTURE_ja.md) | [한국어](ARCHITECTURE_ko.md) | [Français](ARCHITECTURE_fr.md) | Deutsch | [Español](ARCHITECTURE_es.md) | [Português](ARCHITECTURE_pt.md) | [Italiano](ARCHITECTURE_it.md) | [العربية](ARCHITECTURE_ar.md) | [Bahasa Melayu](ARCHITECTURE_ms.md)

## 0. Kerneigenschaft: Deterministische Commit-Garantie (DCG)

> **Wenn FalconDB „committed" zurückgibt, überlebt die Transaktion jeden einzelnen Knotenausfall, jedes Failover und jede Wiederherstellung — ohne Ausnahme.**

Die **Deterministische Commit-Garantie** wird durch das Commit-Point-Modell (§1 in `falcon_common::consistency`) durchgesetzt: Unter der aktiven Commit-Policy wird kein Client-ACK vor der WAL-Persistierung gesendet. Die vier Commit-Phasen — **WAL aufgezeichnet → WAL persistiert → Sichtbar → Bestätigt** — schreiten strikt vorwärts; jeder Rücknahmeversuch wird als `InvariantViolation` abgelehnt.

- Nachweise: [docs/consistency_evidence_map.md](docs/consistency_evidence_map.md)
- Sequenzdiagramm: [docs/commit_sequence.md](docs/commit_sequence.md)
- Nicht-Ziele: [docs/non_goals.md](docs/non_goals.md)

## 1. Architekturübersicht

### 1.1 Datenfluss (Normalpfad: SELECT)

```
psql ──TCP──▶ [Protokollschicht (PG Wire)]
                  │
                  ▼
            [SQL-Frontend: Parse → Bind → Analyze]
                  │
                  ▼
            [Planer / Router]
                  │
          ┌───────┴────────┐
          │ Einzelshard    │ Multi-Shard
          ▼                ▼
    [Lokale Ausführung] [Verteilter Koordinator]
          │                │
          ▼                ▼
    [Transaktions-Mgr ─── MVCC ─── Speicher-Engine]
          │
          ▼
    [Speichertabelle + Index]
          │
          ▼ (async)
    [WAL / Checkpoint]
```

### 1.2 Datenfluss (Schreibpfad)

```
Client INSERT ──▶ PG Wire ──▶ SQL-Frontend ──▶ Planer/Router
    ──▶ Txn starten ──▶ Shard-Lokalisierung (PK-Hash)
    ──▶ LocalTxn (Schnellpfad, Einzelshard, kein 2PC)
        │  oder GlobalTxn (Langsamerpfad, Multi-Shard, XA-2PC)
    ──▶ Commit ──▶ MemTable-Anwendung + WAL fsync
    ──▶ WAL an Replikate senden (async, Schema A)
    ──▶ Client-Bestätigung
```

### 1.3 Knotenrollen (M1: Primär / Replikat)

M1 verwendet eine Primär-Replikat-Topologie pro Shard:
- **Primär**: Akzeptiert Lese-/Schreibzugriffe; bestätigt Commit nach WAL-Persistierung.
- **Replikat**: Empfängt WAL-Chunks; schreibgeschützt vor Promotion.
- **Promotion**: Fencing-basiertes Failover (fence → Aufholen → Umschaltung → unfence).

---

## 2. Modulinventar

### 2.1 `protocol_pg` — PostgreSQL Wire-Protokoll

| Element | Details |
|---------|---------|
| **Verantwortung** | TCP-Verbindungsannahme, PG-Handshake, Simple/Extended Query Parsing, Antwort-Serialisierung |
| **Eingabe** | Roher TCP-Bytestrom |
| **Ausgabe** | Geparstes `Statement` + Sitzungskontext; serialisierte PG-Antwortrahmen |
| **Kerntypen** | `PgConnection`, `PgMessage`, `AuthMethod`, `PgSession` |
| **OSS-Wiederverwendung** | `pgwire` Crate (MIT) für Nachrichtencodec |

### 2.2 `sql_frontend` — Parser / Analysator / Binder

| Element | Details |
|---------|---------|
| **Verantwortung** | SQL-Text → AST → Aufgelöster logischer Plan mit Katalogreferenzen |
| **Kerntypen** | `Statement`, `Expr`, `TableRef`, `ColumnRef`, `BoundStatement` |
| **OSS-Wiederverwendung** | `sqlparser-rs` (Apache-2.0) |

### 2.3 `planner_router` — Abfrageplaner & Verteilter Router

| Element | Details |
|---------|---------|
| **Verantwortung** | Logischer Plan → Physischer Plan; Shard-Zielbestimmung; Filter-Pushdown |
| **Kerntypen** | `LogicalPlan`, `PhysicalPlan`, `ShardTarget`, `PushDown` |

### 2.4 `executor` — Ausführungs-Engine

| Element | Details |
|---------|---------|
| **Verantwortung** | Ausführung physischer Plan-Operatoren; Query-Governor; fusionierte Streaming-Aggregation |
| **Kerntypen** | `Operator` Trait, `SeqScan`, `IndexScan`, `Filter`, `Project`, `Sort`, `Agg`, `QueryGovernor` |
| **Schnellpfad** | `exec_fused_aggregate` — WHERE+GROUP BY+Aggregation in einem MVCC-Kettendurchlauf (null Zeilenkopien) |

### 2.5 `txn` — Transaktionsmanager

| Element | Details |
|---------|---------|
| **Verantwortung** | Transaktionsstart/-commit/-abbruch; MVCC-Zeitstempelvergabe; OCC-Validierung (SI) |
| **Kerntypen** | `TxnManager`, `TxnHandle`, `TxnId`, `Timestamp`, `IsolationLevel` |

### 2.6 `storage` — In-Memory-Speicher-Engine

| Element | Details |
|---------|---------|
| **Verantwortung** | In-Memory-Speicherung von Tupeln und MVCC-Versionen; Indexwartung; WAL-Schreiben + Wiederherstellung; MVCC-GC |
| **Kerntypen** | `StorageEngine`, `MemTable`, `VersionChain`, `SecondaryIndex`, `WalWriter`, `GcRunner` |

### 2.7 `raft` — Konsens (Stub — nicht im Produktionspfad)

> **⚠️ Dieses Crate ist ein Einzelknoten-Stub. Es wird nicht im Produktionsdatenpfad verwendet.**

### 2.8 `cluster` — Cluster, Replikation und verteilte Ausführung

| Element | Details |
|---------|---------|
| **Verantwortung** | Shard-Map; WAL-basierte Primär-Replikat-Replikation; Fencing-basierte Promotion/Failover; Scatter/Gather verteilte Ausführung |
| **Kerntypen** | `ShardMap`, `WalChunk`, `ReplicationTransport`, `DistributedQueryEngine` |

### 2.9 `observability` — Metriken / Tracing / Protokollierung

Prometheus-Metriken, OpenTelemetry-Tracing, strukturierte Protokollierung.

### 2.10 `common` — Gemeinsame Infrastruktur

Gemeinsame Typen, Fehlerhierarchie, Konfiguration, PG-Typsystem, Datum-Darstellung, RBAC.

---

## 3. MVP-Umfang

### 3.1 Unterstützt

| Kategorie | Umfang |
|-----------|--------|
| **Protokoll** | PG Wire Simple Query + Extended Query; Trust-Authentifizierung |
| **SQL DDL** | `CREATE TABLE`, `DROP TABLE`, `TRUNCATE`, `ALTER TABLE`, `CREATE INDEX`, `CREATE VIEW` |
| **SQL DML** | `INSERT`, `UPDATE`, `DELETE` (mit `RETURNING`), `SELECT`, `JOIN`, Unterabfragen, CTE, rekursive CTE, Fensterfunktionen, 260+ Skalarfunktionen |
| **Typen** | `INT`, `BIGINT`, `TEXT`, `BOOL`, `FLOAT8`/`NUMERIC`/`DECIMAL`, `TIMESTAMP`, `JSONB`, `ARRAY`, `DATE` |
| **Transaktionen** | `BEGIN`, `COMMIT`, `ROLLBACK`; Isolationsstufe Read Committed |
| **Speicher** | In-Memory-Zeilenspeicher; PK-Hash-Index; BTree-Sekundärindex |
| **Persistenz** | WAL (Segmente 64 MB, Gruppencommit); Crash-Recovery |
| **Verteilt** | PK-Hash-Sharding; gRPC-WAL-Replikation; Einzelshard-Schnellpfad + Cross-Shard-2PC |

### 3.2 Nicht unterstützt (M1)

- Logische Replikation, CDC
- Online-Schemaänderung
- Mandantenfähigkeit
- Automatisches Rebalancing (nur manuell)

---

## 4. OSS-Wiederverwendungsmatrix

| Modul | Kandidat | Lizenz | Begründung |
|-------|----------|--------|------------|
| **PG Wire** | `pgwire` 0.25+ | MIT | Vollständiger PG-Nachrichtencodec |
| **SQL-Parser** | `sqlparser-rs` 0.50+ | Apache-2.0 | Bewährter PG-Dialekt |
| **Async-Runtime** | `tokio` 1.x | MIT | De-facto-Standard |
| **RPC** | `tonic` (gRPC) | MIT | Ausgereift, Codegenerierung, Streaming |
| **Metriken** | `metrics` + `metrics-exporter-prometheus` | MIT | Leichtgewichtig, Prometheus-nativ |

**Fork-Richtlinie**: Kein Fork. Alle Abhängigkeiten verwenden veröffentlichte Crates.

---

## 5. Transaktions- und Konsistenzdesign

### 5.2 Implementiert: Schnellpfad + Langsamerpfad + OCC

- **LocalTxn (Schnellpfad)**: Einzelshard-Transaktion, Commit durch OCC-Validierung unter Snapshot-Isolation. Kein 2PC-Overhead.
- **GlobalTxn (Langsamerpfad)**: Cross-Shard-Transaktion mit XA-2PC.
- **Harte Invariantenerzwingung**: `TxnContext.validate_commit_invariants()` gibt `InvariantViolation`-Fehler zurück.

### 5.4 MVCC-GC

- **Sicherungspunkt**: `gc_safepoint = min(min_active_ts, replica_safe_ts) - 1`
- **WAL-bewusst**: Sammelt niemals nicht committete oder abgebrochene Versionen
- **Replikationssicher**: Respektiert Replikat-angewendete Zeitstempel

### 5.5 Replikation — gRPC-WAL-Transport

```
Primär ──WAL-Chunks──▶ Replikat
         (gRPC-Stream, tonic, SubscribeWal-RPC)
```

> **⚠️ Harte Grenze**: Das `falcon_raft` Crate ist ein Einzelknoten-Stub. WAL-Transport-Replikate sind **keine** Raft-Follower.

---

## 6. Repository-Struktur

```
falcon/
├── Cargo.toml                  (Workspace-Root)
├── crates/
│   ├── falcon_common/           (Gemeinsame Typen, Fehler, Konfiguration, RBAC)
│   ├── falcon_storage/          (Tabellen, LSM, Index, WAL, GC)
│   ├── falcon_txn/              (Transaktionsverwaltung, MVCC, OCC)
│   ├── falcon_sql_frontend/     (Parser, Binder, Analysator)
│   ├── falcon_planner/          (Logische+physische Pläne)
│   ├── falcon_executor/         (Operatorausführung)
│   ├── falcon_protocol_pg/      (PG Wire-Protokoll)
│   ├── falcon_cluster/          (Replikation, Failover, Scatter/Gather)
│   ├── falcon_observability/    (Metriken, Tracing, Protokollierung)
│   ├── falcon_server/           (Hauptbinär + Integrationstests)
│   └── falcon_bench/            (YCSB-Benchmark-Harness)
├── clients/
│   └── falcondb-jdbc/           (Java JDBC-Treiber)
└── scripts/
```

---

## 7. Leistungsziele

| Metrik | Ziel |
|--------|------|
| Punktlese-QPS | ≥ 200K |
| Punktlese P50 / P99 | < 0,2ms / < 1ms |
| Punktschreib-QPS | ≥ 100K |
| Gemischt (50/50) QPS | ≥ 120K |
| Schreib-QPS (3-Knoten-Cluster) | ≥ 50K |

---

## 8. Tests und Meilensteine

### 8.2 Testanzahl (3.654 Tests)

| Crate | Tests |
|-------|-------|
| `falcon_cluster` | 1.057 |
| `falcon_storage` | 776 |
| `falcon_server` (Integration) | 421 |
| `falcon_common` | 254 |
| `falcon_protocol_pg` | 240 |
| `falcon_cli` | 201 |
| `falcon_executor` | 192 |
| `falcon_sql_frontend` | 149 |
| `falcon_txn` | 103 |
| `falcon_planner` | 89 |
| **Gesamt** | **3.654** (bestanden) |

### 8.4 Meilensteine

| Phase | Umfang | Status |
|-------|--------|--------|
| **M1** | Stabiles OLTP, Schnell-/Langsamerpfad-Transaktionen, WAL-Replikation | **Abgeschlossen** |
| **M2** | gRPC WAL-Streaming, Multi-Knoten-Deployment | Abgeschlossen |
| **M3** | Produktionshärtung: Nur-Lese-Replikate, Graceful Shutdown | **Abgeschlossen** |
| **M4** | Abfrageoptimierung, EXPLAIN ANALYZE, Slow Log | **Abgeschlossen** |
| **M5** | information_schema, Views, Abfrageplan-Cache | **Abgeschlossen** |
| **M6** | JSONB, korrelierte Unterabfragen, FK CASCADE, rekursive CTE | **Abgeschlossen** |
| **M7** | Sequenzfunktionen, Fensterfunktionen | **Abgeschlossen** |
| **M8** | DATE-Typ, COPY-Befehl | **Abgeschlossen** |

---

## 9. Erweiterte Skalarfunktionen

> **Verschoben nach [docs/extended_scalar_functions.md](docs/extended_scalar_functions.md)**. Enthält Spezifikationen für 260+ Funktionen.
