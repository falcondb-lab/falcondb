# FalconDB — Base de datos OLTP distribuida en memoria, compatible con protocolo PG

> **[English](ARCHITECTURE.md)** | [简体中文](ARCHITECTURE_zh.md) | [日本語](ARCHITECTURE_ja.md) | [한국어](ARCHITECTURE_ko.md) | [Français](ARCHITECTURE_fr.md) | [Deutsch](ARCHITECTURE_de.md) | Español | [Português](ARCHITECTURE_pt.md) | [Italiano](ARCHITECTURE_it.md) | [العربية](ARCHITECTURE_ar.md) | [Bahasa Melayu](ARCHITECTURE_ms.md)

## 0. Propiedad fundamental: Garantía de Commit Determinista (DCG)

> **Si FalconDB devuelve "committed", la transacción sobrevive a cualquier fallo de nodo único, cualquier conmutación por error y cualquier recuperación — sin excepciones.**

La **Garantía de Commit Determinista** se aplica mediante el modelo de punto de commit (§1 en `falcon_common::consistency`): bajo la política de commit activa, no se envía ningún ACK al cliente antes de la persistencia WAL. Las cuatro fases de commit — **WAL registrado → WAL persistido → Visible → Confirmado** — avanzan estrictamente hacia adelante; cualquier intento de retroceso se rechaza como `InvariantViolation`.

- Evidencia: [docs/consistency_evidence_map.md](docs/consistency_evidence_map.md)
- Diagrama de secuencia: [docs/commit_sequence.md](docs/commit_sequence.md)
- No-objetivos: [docs/non_goals.md](docs/non_goals.md)

## 1. Visión general de la arquitectura

### 1.1 Flujo de datos (ruta normal: SELECT)

```
psql ──TCP──▶ [Capa de protocolo (PG Wire)]
                  │
                  ▼
            [Frontend SQL: Parse → Bind → Analyze]
                  │
                  ▼
            [Planificador / Enrutador]
                  │
          ┌───────┴────────┐
          │ Shard único    │ Multi-shard
          ▼                ▼
    [Ejecutor local]   [Coordinador distribuido]
          │                │
          ▼                ▼
    [Gestor de Txn ─── MVCC ─── Motor de almacenamiento]
          │
          ▼
    [Tabla en memoria + Índice]
          │
          ▼ (async)
    [WAL / Checkpoint]
```

### 1.2 Flujo de datos (ruta de escritura)

```
Cliente INSERT ──▶ PG Wire ──▶ Frontend SQL ──▶ Planificador/Enrutador
    ──▶ Inicio Txn ──▶ Localización de shard (hash PK)
    ──▶ LocalTxn (ruta rápida, shard único, sin 2PC)
        │  o GlobalTxn (ruta lenta, multi-shard, XA-2PC)
    ──▶ Commit ──▶ Aplicar MemTable + WAL fsync
    ──▶ WAL enviado a réplicas (async, esquema A)
    ──▶ Confirmación al cliente
```

### 1.3 Roles de nodos (M1: Primario / Réplica)

M1 usa topología primario-réplica por shard:
- **Primario**: Acepta lecturas/escrituras; confirma commit tras persistencia WAL.
- **Réplica**: Recibe chunks WAL; solo lectura antes de promoción.
- **Promoción**: Conmutación basada en fencing (fence → recuperación → conmutación → unfence).

---

## 2. Inventario de módulos

### 2.1 `protocol_pg` — Protocolo Wire PostgreSQL

| Elemento | Detalles |
|----------|----------|
| **Responsabilidad** | Aceptación TCP, handshake PG, parsing Simple/Extended Query, serialización de respuestas |
| **Entrada** | Flujo de bytes TCP sin procesar |
| **Salida** | `Statement` parseado + contexto de sesión; marcos de respuesta PG serializados |
| **Tipos clave** | `PgConnection`, `PgMessage`, `AuthMethod`, `PgSession` |
| **Reutilización OSS** | crate `pgwire` (MIT) para codec de mensajes |

### 2.2 `sql_frontend` — Parser / Analizador / Vinculador

| Elemento | Detalles |
|----------|----------|
| **Responsabilidad** | Texto SQL → AST → Plan lógico resuelto con referencias de catálogo |
| **Tipos clave** | `Statement`, `Expr`, `TableRef`, `ColumnRef`, `BoundStatement` |
| **Reutilización OSS** | `sqlparser-rs` (Apache-2.0) |

### 2.3 `planner_router` — Planificador de consultas y enrutador distribuido

| Elemento | Detalles |
|----------|----------|
| **Responsabilidad** | Plan lógico → Plan físico; determinación de shard destino; pushdown de filtros |
| **Tipos clave** | `LogicalPlan`, `PhysicalPlan`, `ShardTarget`, `PushDown` |

### 2.4 `executor` — Motor de ejecución

| Elemento | Detalles |
|----------|----------|
| **Responsabilidad** | Ejecución de operadores del plan físico; gobernador de consultas; agregación fusionada en streaming |
| **Tipos clave** | `Operator` trait, `SeqScan`, `IndexScan`, `Filter`, `Project`, `Sort`, `Agg`, `QueryGovernor` |
| **Ruta rápida** | `exec_fused_aggregate` — WHERE+GROUP BY+agregación en un solo paso MVCC (cero copias de filas) |

### 2.5 `txn` — Gestor de transacciones

| Elemento | Detalles |
|----------|----------|
| **Responsabilidad** | Inicio/commit/aborto de transacciones; asignación de timestamps MVCC; validación OCC (SI) |
| **Tipos clave** | `TxnManager`, `TxnHandle`, `TxnId`, `Timestamp`, `IsolationLevel` |

### 2.6 `storage` — Motor de almacenamiento en memoria

| Elemento | Detalles |
|----------|----------|
| **Responsabilidad** | Almacenamiento en memoria de tuplas y versiones MVCC; mantenimiento de índices; escritura WAL + recuperación; GC MVCC |
| **Tipos clave** | `StorageEngine`, `MemTable`, `VersionChain`, `SecondaryIndex`, `WalWriter`, `GcRunner` |

### 2.7 `raft` — Consenso (stub — fuera de la ruta de producción)

> **⚠️ Este crate es un stub de nodo único. No se utiliza en la ruta de datos de producción.**

### 2.8 `cluster` — Clúster, replicación y ejecución distribuida

| Elemento | Detalles |
|----------|----------|
| **Responsabilidad** | Mapa de shards; replicación primario-réplica basada en WAL; promoción/conmutación con fencing; ejecución distribuida scatter/gather |
| **Tipos clave** | `ShardMap`, `WalChunk`, `ReplicationTransport`, `DistributedQueryEngine` |

### 2.9 `observability` — Métricas / Trazado / Registro

Métricas Prometheus, trazado OpenTelemetry, registro estructurado.

### 2.10 `common` — Infraestructura compartida

Tipos compartidos, jerarquía de errores, configuración, sistema de tipos PG, representación datum, RBAC.

---

## 3. Alcance MVP

### 3.1 Soportado

| Categoría | Alcance |
|-----------|---------|
| **Protocolo** | PG wire Simple Query + Extended Query; autenticación trust |
| **SQL DDL** | `CREATE TABLE`, `DROP TABLE`, `TRUNCATE`, `ALTER TABLE`, `CREATE INDEX`, `CREATE VIEW` |
| **SQL DML** | `INSERT`, `UPDATE`, `DELETE` (con `RETURNING`), `SELECT`, `JOIN`, subconsultas, CTE, CTE recursivos, funciones ventana, 260+ funciones escalares |
| **Tipos** | `INT`, `BIGINT`, `TEXT`, `BOOL`, `FLOAT8`/`NUMERIC`/`DECIMAL`, `TIMESTAMP`, `JSONB`, `ARRAY`, `DATE` |
| **Transacciones** | `BEGIN`, `COMMIT`, `ROLLBACK`; nivel de aislamiento Read Committed |
| **Almacenamiento** | Almacén de filas en memoria; índice hash PK; índice secundario BTree |
| **Persistencia** | WAL (segmentos 64 MB, commit grupal); recuperación tras fallo |
| **Distribuido** | Sharding por hash PK; replicación WAL gRPC; ruta rápida shard único + 2PC cross-shard |

### 3.2 No soportado (M1)

- Replicación lógica, CDC
- Cambio de esquema en línea
- Multi-tenencia
- Rebalanceo automático (solo manual)

---

## 4. Matriz de reutilización OSS

| Módulo | Candidato | Licencia | Razón |
|--------|-----------|----------|-------|
| **PG Wire** | `pgwire` 0.25+ | MIT | Codec completo de mensajes PG |
| **Parser SQL** | `sqlparser-rs` 0.50+ | Apache-2.0 | Dialecto PG probado |
| **Runtime async** | `tokio` 1.x | MIT | Estándar de facto |
| **RPC** | `tonic` (gRPC) | MIT | Maduro, generación de código, streaming |
| **Métricas** | `metrics` + `metrics-exporter-prometheus` | MIT | Ligero, nativo Prometheus |

**Política de fork**: Sin forks. Todas las dependencias usan crates publicados.

---

## 5. Diseño de transacciones y consistencia

### 5.2 Implementado: Ruta rápida + Ruta lenta + OCC

- **LocalTxn (ruta rápida)**: Transacción de shard único, commit por validación OCC bajo aislamiento snapshot. Sin overhead de 2PC.
- **GlobalTxn (ruta lenta)**: Transacción cross-shard con XA-2PC.
- **Aplicación de invariantes estrictas**: `TxnContext.validate_commit_invariants()` devuelve error `InvariantViolation`.

### 5.4 GC MVCC

- **Punto seguro**: `gc_safepoint = min(min_active_ts, replica_safe_ts) - 1`
- **Consciente de WAL**: Nunca recolecta versiones no comprometidas o abortadas
- **Seguro para replicación**: Respeta timestamps aplicados por réplicas

### 5.5 Replicación — Transporte WAL por gRPC

```
Primario ──Chunks WAL──▶ Réplica
         (stream gRPC, tonic, RPC SubscribeWal)
```

> **⚠️ Límite estricto**: El crate `falcon_raft` es un stub de nodo único. Las réplicas de transporte WAL **no** son followers de Raft.

---

## 6. Estructura del repositorio

```
falcon/
├── Cargo.toml                  (raíz del workspace)
├── crates/
│   ├── falcon_common/           (tipos compartidos, errores, config, RBAC)
│   ├── falcon_storage/          (tablas, LSM, índices, WAL, GC)
│   ├── falcon_txn/              (gestión txn, MVCC, OCC)
│   ├── falcon_sql_frontend/     (parser, vinculador, analizador)
│   ├── falcon_planner/          (planes lógico+físico)
│   ├── falcon_executor/         (ejecución de operadores)
│   ├── falcon_protocol_pg/      (protocolo PG wire)
│   ├── falcon_cluster/          (replicación, conmutación, scatter/gather)
│   ├── falcon_observability/    (métricas, trazado, registro)
│   ├── falcon_server/           (binario principal + tests de integración)
│   └── falcon_bench/            (arnés de benchmark YCSB)
├── clients/
│   └── falcondb-jdbc/           (driver Java JDBC)
└── scripts/
```

---

## 7. Objetivos de rendimiento

| Métrica | Objetivo |
|---------|----------|
| QPS lectura puntual | ≥ 200K |
| Lectura puntual P50 / P99 | < 0,2ms / < 1ms |
| QPS escritura puntual | ≥ 100K |
| QPS mixto (50/50) | ≥ 120K |
| QPS escritura (clúster 3 nodos) | ≥ 50K |

---

## 8. Pruebas e hitos

### 8.2 Conteo de tests (3.654 tests)

| Crate | Tests |
|-------|-------|
| `falcon_cluster` | 1.057 |
| `falcon_storage` | 776 |
| `falcon_server` (integración) | 421 |
| `falcon_common` | 254 |
| `falcon_protocol_pg` | 240 |
| `falcon_cli` | 201 |
| `falcon_executor` | 192 |
| `falcon_sql_frontend` | 149 |
| `falcon_txn` | 103 |
| `falcon_planner` | 89 |
| **Total** | **3.654** (aprobados) |

### 8.4 Hitos

| Fase | Alcance | Estado |
|------|---------|--------|
| **M1** | OLTP estable, transacciones ruta rápida/lenta, replicación WAL | **Completado** |
| **M2** | Streaming WAL gRPC, despliegue multi-nodo | Completado |
| **M3** | Endurecimiento producción: réplicas solo lectura, apagado grácil | **Completado** |
| **M4** | Optimización de consultas, EXPLAIN ANALYZE, log lento | **Completado** |
| **M5** | information_schema, vistas, caché de planes | **Completado** |
| **M6** | JSONB, subconsultas correlacionadas, FK CASCADE, CTE recursivos | **Completado** |
| **M7** | Funciones de secuencia, funciones ventana | **Completado** |
| **M8** | Tipo DATE, comando COPY | **Completado** |

---

## 9. Funciones escalares extendidas

> **Movido a [docs/extended_scalar_functions.md](docs/extended_scalar_functions.md)**. Contiene especificaciones de 260+ funciones.
