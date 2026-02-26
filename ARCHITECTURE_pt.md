# FalconDB — Banco de dados OLTP distribuído em memória, compatível com protocolo PG

> **[English](ARCHITECTURE.md)** | [简体中文](ARCHITECTURE_zh.md) | [日本語](ARCHITECTURE_ja.md) | [한국어](ARCHITECTURE_ko.md) | [Français](ARCHITECTURE_fr.md) | [Deutsch](ARCHITECTURE_de.md) | [Español](ARCHITECTURE_es.md) | Português | [Italiano](ARCHITECTURE_it.md) | [العربية](ARCHITECTURE_ar.md) | [Bahasa Melayu](ARCHITECTURE_ms.md)

## 0. Propriedade fundamental: Garantia de Commit Determinístico (DCG)

> **Se o FalconDB retorna "committed", a transação sobrevive a qualquer falha de nó único, qualquer failover e qualquer recuperação — sem exceções.**

A **Garantia de Commit Determinístico** é aplicada pelo modelo de ponto de commit (§1 em `falcon_common::consistency`): sob a política de commit ativa, nenhum ACK é enviado ao cliente antes da persistência WAL. As quatro fases de commit — **WAL registrado → WAL persistido → Visível → Confirmado** — avançam estritamente para frente; qualquer tentativa de retrocesso é rejeitada como `InvariantViolation`.

- Evidências: [docs/consistency_evidence_map.md](docs/consistency_evidence_map.md)
- Diagrama de sequência: [docs/commit_sequence.md](docs/commit_sequence.md)
- Não-objetivos: [docs/non_goals.md](docs/non_goals.md)

## 1. Visão geral da arquitetura

### 1.1 Fluxo de dados (caminho normal: SELECT)

```
psql ──TCP──▶ [Camada de protocolo (PG Wire)]
                  │
                  ▼
            [Frontend SQL: Parse → Bind → Analyze]
                  │
                  ▼
            [Planejador / Roteador]
                  │
          ┌───────┴────────┐
          │ Shard único    │ Multi-shard
          ▼                ▼
    [Executor local]   [Coordenador distribuído]
          │                │
          ▼                ▼
    [Gerenciador Txn ─── MVCC ─── Motor de armazenamento]
          │
          ▼
    [Tabela em memória + Índice]
          │
          ▼ (async)
    [WAL / Checkpoint]
```

### 1.2 Fluxo de dados (caminho de escrita)

```
Cliente INSERT ──▶ PG Wire ──▶ Frontend SQL ──▶ Planejador/Roteador
    ──▶ Início Txn ──▶ Localização de shard (hash PK)
    ──▶ LocalTxn (caminho rápido, shard único, sem 2PC)
        │  ou GlobalTxn (caminho lento, multi-shard, XA-2PC)
    ──▶ Commit ──▶ Aplicar MemTable + WAL fsync
    ──▶ WAL enviado para réplicas (async, esquema A)
    ──▶ Confirmação ao cliente
```

### 1.3 Papéis dos nós (M1: Primário / Réplica)

M1 usa topologia primário-réplica por shard:
- **Primário**: Aceita leituras/escritas; confirma commit após persistência WAL.
- **Réplica**: Recebe chunks WAL; somente leitura antes da promoção.
- **Promoção**: Failover baseado em fencing (fence → catch-up → switchover → unfence).

---

## 2. Inventário de módulos

### 2.1 `protocol_pg` — Protocolo Wire PostgreSQL

| Elemento | Detalhes |
|----------|----------|
| **Responsabilidade** | Aceitação TCP, handshake PG, parsing Simple/Extended Query, serialização de respostas |
| **Tipos chave** | `PgConnection`, `PgMessage`, `AuthMethod`, `PgSession` |
| **Reutilização OSS** | crate `pgwire` (MIT) para codec de mensagens |

### 2.2 `sql_frontend` — Parser / Analisador / Vinculador

| Elemento | Detalhes |
|----------|----------|
| **Responsabilidade** | Texto SQL → AST → Plano lógico resolvido com referências de catálogo |
| **Tipos chave** | `Statement`, `Expr`, `TableRef`, `ColumnRef`, `BoundStatement` |
| **Reutilização OSS** | `sqlparser-rs` (Apache-2.0) |

### 2.3 `planner_router` — Planejador de consultas & Roteador distribuído

| Elemento | Detalhes |
|----------|----------|
| **Responsabilidade** | Plano lógico → Plano físico; determinação do shard alvo; pushdown de filtros |
| **Tipos chave** | `LogicalPlan`, `PhysicalPlan`, `ShardTarget`, `PushDown` |

### 2.4 `executor` — Motor de execução

| Elemento | Detalhes |
|----------|----------|
| **Responsabilidade** | Execução de operadores do plano físico; governador de consultas; agregação fusionada em streaming |
| **Tipos chave** | `Operator` trait, `SeqScan`, `IndexScan`, `Filter`, `Project`, `Sort`, `Agg`, `QueryGovernor` |
| **Caminho rápido** | `exec_fused_aggregate` — WHERE+GROUP BY+agregação em passagem única MVCC (zero cópias de linhas) |

### 2.5 `txn` — Gerenciador de transações

| Elemento | Detalhes |
|----------|----------|
| **Responsabilidade** | Início/commit/aborto de transações; atribuição de timestamps MVCC; validação OCC (SI) |
| **Tipos chave** | `TxnManager`, `TxnHandle`, `TxnId`, `Timestamp`, `IsolationLevel` |

### 2.6 `storage` — Motor de armazenamento em memória

| Elemento | Detalhes |
|----------|----------|
| **Responsabilidade** | Armazenamento em memória de tuplas e versões MVCC; manutenção de índices; escrita WAL + recuperação; GC MVCC |
| **Tipos chave** | `StorageEngine`, `MemTable`, `VersionChain`, `SecondaryIndex`, `WalWriter`, `GcRunner` |

### 2.7 `raft` — Consenso (stub — fora do caminho de produção)

> **⚠️ Este crate é um stub de nó único. Não é usado no caminho de dados de produção.**

### 2.8 `cluster` — Cluster, replicação e execução distribuída

| Elemento | Detalhes |
|----------|----------|
| **Responsabilidade** | Mapa de shards; replicação primário-réplica baseada em WAL; promoção/failover com fencing; execução distribuída scatter/gather |
| **Tipos chave** | `ShardMap`, `WalChunk`, `ReplicationTransport`, `DistributedQueryEngine` |

### 2.9 `observability` — Métricas / Rastreamento / Logging

Métricas Prometheus, rastreamento OpenTelemetry, logging estruturado.

### 2.10 `common` — Infraestrutura compartilhada

Tipos compartilhados, hierarquia de erros, configuração, sistema de tipos PG, representação datum, RBAC.

---

## 3. Escopo MVP

### 3.1 Suportado

| Categoria | Escopo |
|-----------|--------|
| **Protocolo** | PG wire Simple Query + Extended Query; autenticação trust |
| **SQL DDL** | `CREATE TABLE`, `DROP TABLE`, `TRUNCATE`, `ALTER TABLE`, `CREATE INDEX`, `CREATE VIEW` |
| **SQL DML** | `INSERT`, `UPDATE`, `DELETE` (com `RETURNING`), `SELECT`, `JOIN`, subconsultas, CTE, CTE recursivos, funções janela, 260+ funções escalares |
| **Tipos** | `INT`, `BIGINT`, `TEXT`, `BOOL`, `FLOAT8`/`NUMERIC`/`DECIMAL`, `TIMESTAMP`, `JSONB`, `ARRAY`, `DATE` |
| **Transações** | `BEGIN`, `COMMIT`, `ROLLBACK`; nível de isolamento Read Committed |
| **Armazenamento** | Armazém de linhas em memória; índice hash PK; índice secundário BTree |
| **Persistência** | WAL (segmentos 64 MB, commit em grupo); recuperação após falha |
| **Distribuído** | Sharding por hash PK; replicação WAL gRPC; caminho rápido shard único + 2PC cross-shard |

### 3.2 Não suportado (M1)

- Replicação lógica, CDC
- Alteração de esquema online
- Multi-tenancy
- Rebalanceamento automático (apenas manual)

---

## 4. Matriz de reutilização OSS

| Módulo | Candidato | Licença | Razão |
|--------|-----------|---------|-------|
| **PG Wire** | `pgwire` 0.25+ | MIT | Codec completo de mensagens PG |
| **Parser SQL** | `sqlparser-rs` 0.50+ | Apache-2.0 | Dialeto PG comprovado |
| **Runtime async** | `tokio` 1.x | MIT | Padrão de fato |
| **RPC** | `tonic` (gRPC) | MIT | Maduro, geração de código, streaming |
| **Métricas** | `metrics` + `metrics-exporter-prometheus` | MIT | Leve, nativo Prometheus |

**Política de fork**: Sem forks. Todas as dependências usam crates publicados.

---

## 5. Design de transações e consistência

### 5.2 Implementado: Caminho rápido + Caminho lento + OCC

- **LocalTxn (caminho rápido)**: Transação de shard único, commit por validação OCC sob isolamento snapshot. Sem overhead de 2PC.
- **GlobalTxn (caminho lento)**: Transação cross-shard com XA-2PC.
- **Aplicação de invariantes estritas**: `TxnContext.validate_commit_invariants()` retorna erro `InvariantViolation`.

### 5.4 GC MVCC

- **Ponto seguro**: `gc_safepoint = min(min_active_ts, replica_safe_ts) - 1`
- **Consciente do WAL**: Nunca coleta versões não confirmadas ou abortadas
- **Seguro para replicação**: Respeita timestamps aplicados pelas réplicas

### 5.5 Replicação — Transporte WAL por gRPC

```
Primário ──Chunks WAL──▶ Réplica
         (stream gRPC, tonic, RPC SubscribeWal)
```

> **⚠️ Limite estrito**: O crate `falcon_raft` é um stub de nó único. Réplicas de transporte WAL **não** são followers Raft.

---

## 6. Estrutura do repositório

```
falcon/
├── Cargo.toml                  (raiz do workspace)
├── crates/
│   ├── falcon_common/           (tipos compartilhados, erros, config, RBAC)
│   ├── falcon_storage/          (tabelas, LSM, índices, WAL, GC)
│   ├── falcon_txn/              (gestão txn, MVCC, OCC)
│   ├── falcon_sql_frontend/     (parser, vinculador, analisador)
│   ├── falcon_planner/          (planos lógico+físico)
│   ├── falcon_executor/         (execução de operadores)
│   ├── falcon_protocol_pg/      (protocolo PG wire)
│   ├── falcon_cluster/          (replicação, failover, scatter/gather)
│   ├── falcon_observability/    (métricas, rastreamento, logging)
│   ├── falcon_server/           (binário principal + testes de integração)
│   └── falcon_bench/            (harness de benchmark YCSB)
├── clients/
│   └── falcondb-jdbc/           (driver Java JDBC)
└── scripts/
```

---

## 7. Objetivos de desempenho

| Métrica | Objetivo |
|---------|----------|
| QPS leitura pontual | ≥ 200K |
| Leitura pontual P50 / P99 | < 0,2ms / < 1ms |
| QPS escrita pontual | ≥ 100K |
| QPS misto (50/50) | ≥ 120K |
| QPS escrita (cluster 3 nós) | ≥ 50K |

---

## 8. Testes e marcos

### 8.2 Contagem de testes (3.654 testes)

| Crate | Testes |
|-------|--------|
| `falcon_cluster` | 1.057 |
| `falcon_storage` | 776 |
| `falcon_server` (integração) | 421 |
| `falcon_common` | 254 |
| `falcon_protocol_pg` | 240 |
| `falcon_cli` | 201 |
| `falcon_executor` | 192 |
| `falcon_sql_frontend` | 149 |
| `falcon_txn` | 103 |
| `falcon_planner` | 89 |
| **Total** | **3.654** (aprovados) |

### 8.4 Marcos

| Fase | Escopo | Estado |
|------|--------|--------|
| **M1** | OLTP estável, transações caminho rápido/lento, replicação WAL | **Concluído** |
| **M2** | Streaming WAL gRPC, implantação multi-nó | Concluído |
| **M3** | Endurecimento produção: réplicas somente leitura, desligamento gracioso | **Concluído** |
| **M4** | Otimização de consultas, EXPLAIN ANALYZE, log lento | **Concluído** |
| **M5** | information_schema, views, cache de planos | **Concluído** |
| **M6** | JSONB, subconsultas correlacionadas, FK CASCADE, CTE recursivos | **Concluído** |
| **M7** | Funções de sequência, funções janela | **Concluído** |
| **M8** | Tipo DATE, comando COPY | **Concluído** |

---

## 9. Funções escalares estendidas

> **Movido para [docs/extended_scalar_functions.md](docs/extended_scalar_functions.md)**. Contém especificações de 260+ funções.
