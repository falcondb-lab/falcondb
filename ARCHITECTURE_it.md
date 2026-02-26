# FalconDB — Database OLTP distribuito in memoria, compatibile con protocollo PG

> **[English](ARCHITECTURE.md)** | [简体中文](ARCHITECTURE_zh.md) | [日本語](ARCHITECTURE_ja.md) | [한국어](ARCHITECTURE_ko.md) | [Français](ARCHITECTURE_fr.md) | [Deutsch](ARCHITECTURE_de.md) | [Español](ARCHITECTURE_es.md) | [Português](ARCHITECTURE_pt.md) | Italiano | [العربية](ARCHITECTURE_ar.md) | [Bahasa Melayu](ARCHITECTURE_ms.md)

## 0. Proprietà fondamentale: Garanzia di Commit Deterministico (DCG)

> **Se FalconDB restituisce "committed", la transazione sopravvive a qualsiasi guasto di singolo nodo, qualsiasi failover e qualsiasi ripristino — senza eccezioni.**

La **Garanzia di Commit Deterministico** è applicata dal modello del punto di commit (§1 in `falcon_common::consistency`): sotto la politica di commit attiva, nessun ACK client viene inviato prima della persistenza WAL. Le quattro fasi di commit — **WAL registrato → WAL persistito → Visibile → Confermato** — avanzano strettamente in avanti; qualsiasi tentativo di rollback viene rifiutato come `InvariantViolation`.

- Evidenze: [docs/consistency_evidence_map.md](docs/consistency_evidence_map.md)
- Diagramma di sequenza: [docs/commit_sequence.md](docs/commit_sequence.md)
- Non-obiettivi: [docs/non_goals.md](docs/non_goals.md)

## 1. Panoramica dell'architettura

### 1.1 Flusso dati (percorso normale: SELECT)

```
psql ──TCP──▶ [Livello protocollo (PG Wire)]
                  │
                  ▼
            [Frontend SQL: Parse → Bind → Analyze]
                  │
                  ▼
            [Pianificatore / Router]
                  │
          ┌───────┴────────┐
          │ Singolo shard  │ Multi-shard
          ▼                ▼
    [Esecutore locale] [Coordinatore distribuito]
          │                │
          ▼                ▼
    [Gestore Txn ─── MVCC ─── Motore di storage]
          │
          ▼
    [Tabella in memoria + Indice]
          │
          ▼ (async)
    [WAL / Checkpoint]
```

### 1.2 Flusso dati (percorso di scrittura)

```
Client INSERT ──▶ PG Wire ──▶ Frontend SQL ──▶ Pianificatore/Router
    ──▶ Inizio Txn ──▶ Localizzazione shard (hash PK)
    ──▶ LocalTxn (percorso veloce, singolo shard, senza 2PC)
        │  o GlobalTxn (percorso lento, multi-shard, XA-2PC)
    ──▶ Commit ──▶ Applicazione MemTable + WAL fsync
    ──▶ WAL inviato alle repliche (async, schema A)
    ──▶ Conferma al client
```

### 1.3 Ruoli dei nodi (M1: Primario / Replica)

M1 usa topologia primario-replica per shard:
- **Primario**: Accetta letture/scritture; conferma commit dopo persistenza WAL.
- **Replica**: Riceve chunk WAL; solo lettura prima della promozione.
- **Promozione**: Failover basato su fencing (fence → recupero → switchover → unfence).

---

## 2. Inventario dei moduli

### 2.1 `protocol_pg` — Protocollo Wire PostgreSQL

| Elemento | Dettagli |
|----------|----------|
| **Responsabilità** | Accettazione TCP, handshake PG, parsing Simple/Extended Query, serializzazione risposte |
| **Tipi chiave** | `PgConnection`, `PgMessage`, `AuthMethod`, `PgSession` |
| **Riuso OSS** | crate `pgwire` (MIT) per codec messaggi |

### 2.2 `sql_frontend` — Parser / Analizzatore / Binder

| Elemento | Dettagli |
|----------|----------|
| **Responsabilità** | Testo SQL → AST → Piano logico risolto con riferimenti catalogo |
| **Tipi chiave** | `Statement`, `Expr`, `TableRef`, `ColumnRef`, `BoundStatement` |
| **Riuso OSS** | `sqlparser-rs` (Apache-2.0) |

### 2.3 `planner_router` — Pianificatore query & Router distribuito

| Elemento | Dettagli |
|----------|----------|
| **Responsabilità** | Piano logico → Piano fisico; determinazione shard target; pushdown filtri |
| **Tipi chiave** | `LogicalPlan`, `PhysicalPlan`, `ShardTarget`, `PushDown` |

### 2.4 `executor` — Motore di esecuzione

| Elemento | Dettagli |
|----------|----------|
| **Responsabilità** | Esecuzione operatori piano fisico; governatore query; aggregazione fusa in streaming |
| **Tipi chiave** | `Operator` trait, `SeqScan`, `IndexScan`, `Filter`, `Project`, `Sort`, `Agg`, `QueryGovernor` |
| **Percorso veloce** | `exec_fused_aggregate` — WHERE+GROUP BY+aggregazione in singola passata MVCC (zero copie righe) |

### 2.5 `txn` — Gestore transazioni

| Elemento | Dettagli |
|----------|----------|
| **Responsabilità** | Inizio/commit/abort transazioni; assegnazione timestamp MVCC; validazione OCC (SI) |
| **Tipi chiave** | `TxnManager`, `TxnHandle`, `TxnId`, `Timestamp`, `IsolationLevel` |

### 2.6 `storage` — Motore di storage in memoria

| Elemento | Dettagli |
|----------|----------|
| **Responsabilità** | Storage in memoria di tuple e versioni MVCC; manutenzione indici; scrittura WAL + ripristino; GC MVCC |
| **Tipi chiave** | `StorageEngine`, `MemTable`, `VersionChain`, `SecondaryIndex`, `WalWriter`, `GcRunner` |

### 2.7 `raft` — Consenso (stub — fuori dal percorso di produzione)

> **⚠️ Questo crate è uno stub a nodo singolo. Non viene utilizzato nel percorso dati di produzione.**

### 2.8 `cluster` — Cluster, replica e esecuzione distribuita

| Elemento | Dettagli |
|----------|----------|
| **Responsabilità** | Mappa shard; replica primario-replica basata su WAL; promozione/failover con fencing; esecuzione distribuita scatter/gather |
| **Tipi chiave** | `ShardMap`, `WalChunk`, `ReplicationTransport`, `DistributedQueryEngine` |

### 2.9 `observability` — Metriche / Tracciamento / Logging

Metriche Prometheus, tracciamento OpenTelemetry, logging strutturato.

### 2.10 `common` — Infrastruttura condivisa

Tipi condivisi, gerarchia errori, configurazione, sistema tipi PG, rappresentazione datum, RBAC.

---

## 3. Ambito MVP

### 3.1 Supportato

| Categoria | Ambito |
|-----------|--------|
| **Protocollo** | PG wire Simple Query + Extended Query; autenticazione trust |
| **SQL DDL** | `CREATE TABLE`, `DROP TABLE`, `TRUNCATE`, `ALTER TABLE`, `CREATE INDEX`, `CREATE VIEW` |
| **SQL DML** | `INSERT`, `UPDATE`, `DELETE` (con `RETURNING`), `SELECT`, `JOIN`, sottoquery, CTE, CTE ricorsive, funzioni finestra, 260+ funzioni scalari |
| **Tipi** | `INT`, `BIGINT`, `TEXT`, `BOOL`, `FLOAT8`/`NUMERIC`/`DECIMAL`, `TIMESTAMP`, `JSONB`, `ARRAY`, `DATE` |
| **Transazioni** | `BEGIN`, `COMMIT`, `ROLLBACK`; livello di isolamento Read Committed |
| **Storage** | Store righe in memoria; indice hash PK; indice secondario BTree |
| **Persistenza** | WAL (segmenti 64 MB, commit di gruppo); ripristino da crash |
| **Distribuito** | Sharding hash PK; replica WAL gRPC; percorso veloce singolo shard + 2PC cross-shard |

### 3.2 Non supportato (M1)

- Replica logica, CDC
- Modifica schema online
- Multi-tenancy
- Ribilanciamento automatico (solo manuale)

---

## 4. Matrice di riuso OSS

| Modulo | Candidato | Licenza | Motivazione |
|--------|-----------|---------|-------------|
| **PG Wire** | `pgwire` 0.25+ | MIT | Codec completo messaggi PG |
| **Parser SQL** | `sqlparser-rs` 0.50+ | Apache-2.0 | Dialetto PG collaudato |
| **Runtime async** | `tokio` 1.x | MIT | Standard de facto |
| **RPC** | `tonic` (gRPC) | MIT | Maturo, generazione codice, streaming |
| **Metriche** | `metrics` + `metrics-exporter-prometheus` | MIT | Leggero, nativo Prometheus |

**Politica di fork**: Nessun fork. Tutte le dipendenze usano crate pubblicati.

---

## 5. Design transazioni e consistenza

### 5.2 Implementato: Percorso veloce + Percorso lento + OCC

- **LocalTxn (percorso veloce)**: Transazione singolo shard, commit per validazione OCC sotto isolamento snapshot. Senza overhead 2PC.
- **GlobalTxn (percorso lento)**: Transazione cross-shard con XA-2PC.
- **Applicazione invarianti rigide**: `TxnContext.validate_commit_invariants()` restituisce errore `InvariantViolation`.

### 5.4 GC MVCC

- **Punto sicuro**: `gc_safepoint = min(min_active_ts, replica_safe_ts) - 1`
- **Consapevole del WAL**: Non raccoglie mai versioni non committate o abortate
- **Sicuro per replica**: Rispetta i timestamp applicati dalle repliche

### 5.5 Replica — Trasporto WAL via gRPC

```
Primario ──Chunk WAL──▶ Replica
         (stream gRPC, tonic, RPC SubscribeWal)
```

> **⚠️ Limite rigido**: Il crate `falcon_raft` è uno stub a nodo singolo. Le repliche di trasporto WAL **non** sono follower Raft.

---

## 6. Struttura del repository

```
falcon/
├── Cargo.toml                  (radice workspace)
├── crates/
│   ├── falcon_common/           (tipi condivisi, errori, config, RBAC)
│   ├── falcon_storage/          (tabelle, LSM, indici, WAL, GC)
│   ├── falcon_txn/              (gestione txn, MVCC, OCC)
│   ├── falcon_sql_frontend/     (parser, binder, analizzatore)
│   ├── falcon_planner/          (piani logico+fisico)
│   ├── falcon_executor/         (esecuzione operatori)
│   ├── falcon_protocol_pg/      (protocollo PG wire)
│   ├── falcon_cluster/          (replica, failover, scatter/gather)
│   ├── falcon_observability/    (metriche, tracciamento, logging)
│   ├── falcon_server/           (binario principale + test di integrazione)
│   └── falcon_bench/            (harness benchmark YCSB)
├── clients/
│   └── falcondb-jdbc/           (driver Java JDBC)
└── scripts/
```

---

## 7. Obiettivi di prestazione

| Metrica | Obiettivo |
|---------|-----------|
| QPS lettura puntuale | ≥ 200K |
| Lettura puntuale P50 / P99 | < 0,2ms / < 1ms |
| QPS scrittura puntuale | ≥ 100K |
| QPS misto (50/50) | ≥ 120K |
| QPS scrittura (cluster 3 nodi) | ≥ 50K |

---

## 8. Test e milestone

### 8.2 Conteggio test (3.654 test)

| Crate | Test |
|-------|------|
| `falcon_cluster` | 1.057 |
| `falcon_storage` | 776 |
| `falcon_server` (integrazione) | 421 |
| `falcon_common` | 254 |
| `falcon_protocol_pg` | 240 |
| `falcon_cli` | 201 |
| `falcon_executor` | 192 |
| `falcon_sql_frontend` | 149 |
| `falcon_txn` | 103 |
| `falcon_planner` | 89 |
| **Totale** | **3.654** (superati) |

### 8.4 Milestone

| Fase | Ambito | Stato |
|------|--------|-------|
| **M1** | OLTP stabile, transazioni percorso veloce/lento, replica WAL | **Completato** |
| **M2** | Streaming WAL gRPC, deployment multi-nodo | Completato |
| **M3** | Indurimento produzione: repliche solo lettura, spegnimento grazioso | **Completato** |
| **M4** | Ottimizzazione query, EXPLAIN ANALYZE, log lento | **Completato** |
| **M5** | information_schema, viste, cache piani | **Completato** |
| **M6** | JSONB, sottoquery correlate, FK CASCADE, CTE ricorsive | **Completato** |
| **M7** | Funzioni sequenza, funzioni finestra | **Completato** |
| **M8** | Tipo DATE, comando COPY | **Completato** |

---

## 9. Funzioni scalari estese

> **Spostato in [docs/extended_scalar_functions.md](docs/extended_scalar_functions.md)**. Contiene specifiche per 260+ funzioni.
