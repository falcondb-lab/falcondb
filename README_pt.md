# FalconDB

<p align="center">
  <img src="assets/falcondb-logo.png" alt="FalconDB Logo" width="220" />
</p>

<h1 align="center">FalconDB</h1>

<p align="center">
  <strong>Compatível com PG · Distribuído · Memória em Primeiro Lugar · Semântica de Transações Determinística</strong>
</p>

<p align="center">
  <a href="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml">
    <img src="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml/badge.svg" alt="CI" />
  </a>
  <img src="https://img.shields.io/badge/version-1.2.0-blue" alt="Versão" />
  <img src="https://img.shields.io/badge/MSRV-1.75-blue" alt="MSRV" />
  <img src="https://img.shields.io/badge/license-Apache--2.0-green" alt="Licença" />
</p>

> English | [简体中文](README_zh.md) | [Español](README_es.md) | [Français](README_fr.md) | [한국어](README_ko.md) | [日本語](README_ja.md) | [Deutsch](README_de.md) | [العربية](README_ar.md) | [Italiano](README_it.md) | [Bahasa Melayu](README_ms.md) | **Português**

> FalconDB é um **banco de dados OLTP compatível com PG, distribuído e com memória em primeiro lugar** com semântica de transações determinística. Benchmarks comparados com PostgreSQL, VoltDB e SingleStore — veja a **[Matriz de Benchmarks](benchmarks/README.md)**.
>
> - ✅ **Baixa latência** — os commits de caminho rápido de shard único contornam completamente o 2PC
> - ✅ **Desempenho de varredura** — agregados de streaming fusionados, iteração MVCC sem cópia, paridade próxima ao PG em 1M linhas
> - ✅ **Estabilidade** — p99 limitado, taxa de cancelamento < 1%, benchmarks reproduzíveis
> - ✅ **Consistência comprovável** — MVCC/OCC sob Isolamento de Snapshot, ACID verificado por CI
> - ✅ **Operabilidade** — 50+ comandos SHOW, métricas Prometheus, gate CI de failover
> - ✅ **Determinismo** — máquina de estados robusta, in-doubt limitado, retry idempotente
> - ❌ Não é HTAP — sem cargas de trabalho analíticas
> - ❌ Não é PG completo — [ver lista de não suportados](#not-supported)

FalconDB fornece OLTP estável, transações de caminho rápido/lento, replicação primário–réplica baseada em WAL com streaming gRPC, promoção/failover, coleta de lixo MVCC e benchmarks reproduzíveis.

### Plataformas Suportadas

| Plataforma | Build | Testes | Status |
|------------|:-----:|:------:|--------|
| **Linux** (x86_64, Ubuntu 22.04+) | ✅ | ✅ | Alvo CI principal |
| **Windows** (x86_64, MSVC) | ✅ | ✅ | Alvo CI |
| **macOS** (x86_64 / aarch64) | ✅ | ✅ | Testado pela comunidade |

**MSRV**: Rust **1.75** (`rust-version = "1.75"` em `Cargo.toml`)

### Compatibilidade do Protocolo PG

| Funcionalidade | Status | Notas |
|---------------|:------:|-------|
| Protocolo de consulta simples | ✅ | Instrução única + múltiplas |
| Consulta estendida (Parse/Bind/Execute) | ✅ | Instruções preparadas + portais |
| Auth: Trust | ✅ | Qualquer usuário aceito |
| Auth: MD5 | ✅ | Tipo de auth PG 5 |
| Auth: SCRAM-SHA-256 | ✅ | Compatível com PG 10+ |
| Auth: Senha (texto simples) | ✅ | Tipo de auth PG 3 |
| TLS/SSL | ✅ | SSLRequest → atualização quando configurado |
| COPY IN/OUT | ✅ | Formatos texto e CSV |
| `psql` 12+ | ✅ | Totalmente testado |
| `pgbench` (init + run) | ✅ | Scripts embutidos funcionam |
| JDBC (pgjdbc 42.x) | ✅ | Testado com 42.7+ |
| Solicitação de cancelamento | ✅ | Polling AtomicBool, latência 50ms |
| LISTEN/NOTIFY | ✅ | Hub de broadcast em memória |
| Protocolo de replicação lógica | ✅ | IDENTIFY_SYSTEM, CREATE/DROP_REPLICATION_SLOT, START_REPLICATION |

### Cobertura SQL

| Categoria | Suportado |
|-----------|----------|
| **DDL** | CREATE/DROP/ALTER TABLE, CREATE/DROP INDEX, CREATE/DROP VIEW, CREATE/DROP SEQUENCE, TRUNCATE |
| **DML** | INSERT (incl. ON CONFLICT, RETURNING, SELECT), UPDATE (incl. FROM, RETURNING), DELETE (incl. USING, RETURNING), COPY |
| **Consultas** | WHERE, ORDER BY, LIMIT/OFFSET, DISTINCT, GROUP BY/HAVING, JOINs, subconsultas, CTEs (incl. RECURSIVE), UNION/INTERSECT/EXCEPT, funções de janela |
| **Agregados** | COUNT, SUM, AVG, MIN, MAX, STRING_AGG, BOOL_AND/OR, ARRAY_AGG |
| **Tipos** | INT, BIGINT, FLOAT8, DECIMAL/NUMERIC, TEXT, BOOLEAN, TIMESTAMP, DATE, JSONB, ARRAY, SERIAL/BIGSERIAL |
| **Transações** | BEGIN/COMMIT/ROLLBACK, READ ONLY/READ WRITE, timeout por transação, Read Committed, Snapshot Isolation |
| **Funções** | 500+ funções escalares (strings, matemática, data/hora, crypto, JSON, arrays) |
| **Observabilidade** | SHOW falcon.*, EXPLAIN, EXPLAIN ANALYZE, CHECKPOINT, ANALYZE TABLE |

### <a id="not-supported"></a>Não Suportado (v1.2)

| Funcionalidade | Código de Erro | Mensagem de Erro |
|---------------|:-------------:|-----------------|
| Procedimentos armazenados / PL/pgSQL | `0A000` | `stored procedures are not supported` |
| Triggers | `0A000` | `triggers are not supported` |
| Views materializadas | `0A000` | `materialized views are not supported` |
| Wrappers de dados externos (FDW) | `0A000` | `foreign data wrappers are not supported` |
| Pesquisa de texto completo | `0A000` | `full-text search is not supported` |
| DDL online | `0A000` | `concurrent index operations are not supported` |
| HTAP / ColumnStore | — | Desativado em tempo de compilação |

---

## 1. Compilação

```bash
# Compilar todos os crates (debug)
cargo build --workspace

# Build release
cargo build --release --workspace

# Executar testes (2.643+ testes)
cargo test --workspace

# Lint
cargo clippy --workspace
```

---

## 2. Início Rápido

```bash
# Modo em memória (sem WAL, mais rápido)
cargo run -p falcon_server -- --no-wal

# Com persistência WAL
cargo run -p falcon_server -- --data-dir ./falcon_data

# Conectar via psql
psql -h 127.0.0.1 -p 5433 -U falcon
```

---

## 3. Motores de Armazenamento

| Motor | Armazenamento | Persistência | Melhor Para |
|-------|-------------|-------------|------------|
| **Rowstore** (padrão) | Em memória (cadeias de versões MVCC) | Somente WAL | OLTP de baixa latência |
| **LSM** | Disco (LSM-Tree + WAL) | Persistência completa em disco | Grandes conjuntos de dados |

---

## 4. Modelo de Transação

- **LocalTxn (caminho rápido)**: transações de shard único confirmam com OCC sob Snapshot Isolation — sem overhead de 2PC.
- **GlobalTxn (caminho lento)**: transações cross-shard usam XA-2PC com prepare/commit.

---

## 5. Arquitetura

```
┌─────────────────────────────────────────────────────────┐
│  Protocolo PG Wire (TCP)  │  Protocolo Nativo (TCP/TLS) │
├──────────────────────────┴─────────────────────────────┤
│         Frontend SQL (sqlparser-rs → Binder)            │
├────────────────────────────────────────────────────────┤
│         Planejador / Roteador                           │
├────────────────────────────────────────────────────────┤
│         Executor (linha a linha + agregados streaming)  │
├──────────────────┬─────────────────────────────────────┤
│   Gerenciador    │   Motor de Armazenamento            │
│   de Txn         │   (MemTable + LSM + WAL + GC)       │
├──────────────────┴─────────────────────────────────────┤
│  Cluster (ShardMap, Replicação, Failover, Epoch)       │
└────────────────────────────────────────────────────────┘
```

---

## 6. Métricas e Observabilidade

```sql
-- Estatísticas de transações
SHOW falcon.txn_stats;

-- Estatísticas de GC
SHOW falcon.gc_stats;

-- Estatísticas de replicação
SHOW falcon.replication_stats;
```

---

## Licença

Apache-2.0
