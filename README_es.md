# FalconDB

<p align="center">
  <img src="assets/falcondb-logo.png" alt="FalconDB Logo" width="220" />
</p>

<h1 align="center">FalconDB</h1>

<p align="center">
  <strong>Compatible con PG · Distribuido · Memoria Primaria · Semántica de Transacciones Determinista</strong>
</p>

<p align="center">
  <a href="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml">
    <img src="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml/badge.svg" alt="CI" />
  </a>
  <img src="https://img.shields.io/badge/version-1.2.0-blue" alt="Versión" />
  <img src="https://img.shields.io/badge/MSRV-1.75-blue" alt="MSRV" />
  <img src="https://img.shields.io/badge/license-Apache--2.0-green" alt="Licencia" />
</p>

> English | [简体中文](README_zh.md) | **Español** | [Français](README_fr.md) | [한국어](README_ko.md) | [日本語](README_ja.md) | [Deutsch](README_de.md) | [العربية](README_ar.md) | [Italiano](README_it.md) | [Bahasa Melayu](README_ms.md) | [Português](README_pt.md)

> FalconDB es una **base de datos OLTP compatible con PG, distribuida y con memoria primaria** con semántica de transacciones determinista. Evaluada frente a PostgreSQL, VoltDB y SingleStore — consulta la **[Matriz de Benchmarks](benchmarks/README.md)**.
>
> - ✅ **Baja latencia** — las confirmaciones de ruta rápida en un solo fragmento evitan completamente el 2PC
> - ✅ **Rendimiento de escaneo** — agregados en streaming fusionados, iteración MVCC sin copia, paridad cercana a PG en 1M filas
> - ✅ **Estabilidad** — p99 acotado, tasa de aborto < 1%, benchmarks reproducibles
> - ✅ **Consistencia demostrable** — MVCC/OCC bajo Aislamiento de Instantáneas, ACID verificado por CI
> - ✅ **Operabilidad** — más de 50 comandos SHOW, métricas Prometheus, puerta de CI para failover
> - ✅ **Determinismo** — máquina de estados robusta, en-duda acotado, reintentos idempotentes
> - ❌ No es HTAP — sin cargas de trabajo analíticas
> - ❌ No es PG completo — [ver lista de no soportados](#not-supported)

FalconDB ofrece OLTP estable, transacciones de ruta rápida/lenta, replicación primaria–réplica basada en WAL con streaming gRPC, promoción/failover, recolección de basura MVCC y benchmarks reproducibles.

### Plataformas Soportadas

| Plataforma | Compilación | Pruebas | Estado |
|------------|:-----------:|:-------:|--------|
| **Linux** (x86_64, Ubuntu 22.04+) | ✅ | ✅ | Objetivo CI principal |
| **Windows** (x86_64, MSVC) | ✅ | ✅ | Objetivo CI |
| **macOS** (x86_64 / aarch64) | ✅ | ✅ | Probado por la comunidad |

**MSRV**: Rust **1.75** (`rust-version = "1.75"` en `Cargo.toml`)

### Compatibilidad con el Protocolo PG

| Característica | Estado | Notas |
|----------------|:------:|-------|
| Protocolo de consulta simple | ✅ | Una o múltiples sentencias |
| Consulta extendida (Parse/Bind/Execute) | ✅ | Sentencias preparadas + portales |
| Auth: Trust | ✅ | Cualquier usuario aceptado |
| Auth: MD5 | ✅ | Tipo de auth PG 5 |
| Auth: SCRAM-SHA-256 | ✅ | Compatible con PG 10+ |
| Auth: Contraseña (texto claro) | ✅ | Tipo de auth PG 3 |
| TLS/SSL | ✅ | SSLRequest → actualización cuando está configurado |
| COPY IN/OUT | ✅ | Formatos texto y CSV |
| `psql` 12+ | ✅ | Completamente probado |
| `pgbench` (init + run) | ✅ | Los scripts incorporados funcionan |
| JDBC (pgjdbc 42.x) | ✅ | Probado con 42.7+ |
| Solicitud de cancelación | ✅ | Sondeo AtomicBool, latencia 50ms |
| LISTEN/NOTIFY | ✅ | Hub de difusión en memoria |
| Protocolo de replicación lógica | ✅ | IDENTIFY_SYSTEM, CREATE/DROP_REPLICATION_SLOT, START_REPLICATION |

### Cobertura SQL

| Categoría | Soportado |
|-----------|-----------|
| **DDL** | CREATE/DROP/ALTER TABLE, CREATE/DROP INDEX, CREATE/DROP VIEW, CREATE/DROP SEQUENCE, TRUNCATE |
| **DML** | INSERT (incl. ON CONFLICT, RETURNING, SELECT), UPDATE (incl. FROM, RETURNING), DELETE (incl. USING, RETURNING), COPY |
| **Consultas** | WHERE, ORDER BY, LIMIT/OFFSET, DISTINCT, GROUP BY/HAVING, JOINs, subconsultas, CTEs (incl. RECURSIVE), UNION/INTERSECT/EXCEPT, funciones de ventana |
| **Agregados** | COUNT, SUM, AVG, MIN, MAX, STRING_AGG, BOOL_AND/OR, ARRAY_AGG |
| **Tipos** | INT, BIGINT, FLOAT8, DECIMAL/NUMERIC, TEXT, BOOLEAN, TIMESTAMP, DATE, JSONB, ARRAY, SERIAL/BIGSERIAL |
| **Transacciones** | BEGIN/COMMIT/ROLLBACK, READ ONLY/READ WRITE, timeout por transacción, Read Committed, Snapshot Isolation |
| **Funciones** | Más de 500 funciones escalares (cadenas, matemáticas, fecha/hora, crypto, JSON, arrays) |
| **Observabilidad** | SHOW falcon.*, EXPLAIN, EXPLAIN ANALYZE, CHECKPOINT, ANALYZE TABLE |

### <a id="not-supported"></a>No Soportado (v1.2)

| Característica | Código de Error | Mensaje de Error |
|----------------|:---------------:|------------------|
| Procedimientos almacenados / PL/pgSQL | `0A000` | `stored procedures are not supported` |
| Triggers | `0A000` | `triggers are not supported` |
| Vistas materializadas | `0A000` | `materialized views are not supported` |
| Wrappers de datos externos (FDW) | `0A000` | `foreign data wrappers are not supported` |
| Búsqueda de texto completo | `0A000` | `full-text search is not supported` |
| DDL en línea | `0A000` | `concurrent index operations are not supported` |
| HTAP / ColumnStore | — | Desactivado en tiempo de compilación |

---

## 1. Compilación

```bash
# Compilar todos los crates (debug)
cargo build --workspace

# Compilar release
cargo build --release --workspace

# Ejecutar pruebas (2.643+ pruebas)
cargo test --workspace

# Lint
cargo clippy --workspace
```

---

## 2. Inicio Rápido

```bash
# Modo en memoria (sin WAL, más rápido)
cargo run -p falcon_server -- --no-wal

# Con persistencia WAL
cargo run -p falcon_server -- --data-dir ./falcon_data

# Conectar via psql
psql -h 127.0.0.1 -p 5433 -U falcon
```

---

## 3. Motores de Almacenamiento

| Motor | Almacenamiento | Persistencia | Mejor Para |
|-------|---------------|--------------|------------|
| **Rowstore** (por defecto) | En memoria (cadenas de versiones MVCC) | Solo WAL | OLTP de baja latencia |
| **LSM** | Disco (LSM-Tree + WAL) | Persistencia completa en disco | Conjuntos de datos grandes |

---

## 4. Modelo de Transacciones

- **LocalTxn (ruta rápida)**: transacciones de un solo fragmento confirman con OCC bajo Snapshot Isolation — sin overhead 2PC.
- **GlobalTxn (ruta lenta)**: transacciones entre fragmentos usan XA-2PC con prepare/commit.

---

## 5. Arquitectura

```
┌─────────────────────────────────────────────────────────┐
│  Protocolo PG Wire (TCP)  │  Protocolo Nativo (TCP/TLS) │
├──────────────────────────┴─────────────────────────────┤
│         Frontend SQL (sqlparser-rs → Binder)            │
├────────────────────────────────────────────────────────┤
│         Planificador / Router                           │
├────────────────────────────────────────────────────────┤
│         Ejecutor (fila a fila + agregados en streaming) │
├──────────────────┬─────────────────────────────────────┤
│   Gestor de Txn  │   Motor de Almacenamiento           │
│   (MVCC, OCC)    │   (MemTable + LSM + WAL + GC)       │
├──────────────────┴─────────────────────────────────────┤
│  Cluster (ShardMap, Replicación, Failover, Epoch)      │
└────────────────────────────────────────────────────────┘
```

---

## 6. Métricas y Observabilidad

```sql
-- Estadísticas de transacciones
SHOW falcon.txn_stats;

-- Estadísticas de GC
SHOW falcon.gc_stats;

-- Estadísticas de replicación
SHOW falcon.replication_stats;
```

---

## Licencia

Apache-2.0
