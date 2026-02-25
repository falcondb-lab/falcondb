# FalconDB

<p align="center">
  <img src="assets/falcondb-logo.png" alt="FalconDB Logo" width="220" />
</p>

<h1 align="center">FalconDB</h1>

<p align="center">
  <strong>Compatible PG · Distribué · Mémoire Primaire · Sémantique Transactionnelle Déterministe</strong>
</p>

<p align="center">
  <a href="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml">
    <img src="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml/badge.svg" alt="CI" />
  </a>
  <img src="https://img.shields.io/badge/version-1.2.0-blue" alt="Version" />
  <img src="https://img.shields.io/badge/MSRV-1.75-blue" alt="MSRV" />
  <img src="https://img.shields.io/badge/license-Apache--2.0-green" alt="Licence" />
</p>

> English | [简体中文](README_zh.md) | [Español](README_es.md) | **Français** | [한국어](README_ko.md) | [日本語](README_ja.md) | [Deutsch](README_de.md) | [العربية](README_ar.md) | [Italiano](README_it.md) | [Bahasa Melayu](README_ms.md) | [Português](README_pt.md)

> FalconDB est une **base de données OLTP compatible PG, distribuée et orientée mémoire** avec une sémantique transactionnelle déterministe. Évaluée contre PostgreSQL, VoltDB et SingleStore — voir la **[Matrice de Benchmarks](benchmarks/README.md)**.
>
> - ✅ **Faible latence** — les validations en chemin rapide sur un seul fragment contournent entièrement le 2PC
> - ✅ **Performance de scan** — agrégats en streaming fusionnés, itération MVCC sans copie, parité proche de PG sur 1M lignes
> - ✅ **Stabilité** — p99 borné, taux d'abandon < 1%, benchmarks reproductibles
> - ✅ **Cohérence prouvable** — MVCC/OCC sous Isolation par Instantané, ACID vérifié par CI
> - ✅ **Exploitabilité** — 50+ commandes SHOW, métriques Prometheus, porte CI de basculement
> - ✅ **Déterminisme** — machine à états robuste, en-doute borné, relance idempotente
> - ❌ Pas HTAP — pas de charges analytiques
> - ❌ Pas PG complet — [voir liste non supportée](#not-supported)

FalconDB fournit un OLTP stable, des transactions en chemin rapide/lent, une réplication primaire–réplique basée sur WAL avec streaming gRPC, promotion/basculement, collecte des ordures MVCC et des benchmarks reproductibles.

### Plateformes Supportées

| Plateforme | Compilation | Tests | Statut |
|------------|:-----------:|:-----:|--------|
| **Linux** (x86_64, Ubuntu 22.04+) | ✅ | ✅ | Cible CI principale |
| **Windows** (x86_64, MSVC) | ✅ | ✅ | Cible CI |
| **macOS** (x86_64 / aarch64) | ✅ | ✅ | Testé par la communauté |

**MSRV** : Rust **1.75** (`rust-version = "1.75"` dans `Cargo.toml`)

### Compatibilité du Protocole PG

| Fonctionnalité | Statut | Notes |
|----------------|:------:|-------|
| Protocole de requête simple | ✅ | Une ou plusieurs instructions |
| Requête étendue (Parse/Bind/Execute) | ✅ | Instructions préparées + portails |
| Auth : Trust | ✅ | Tout utilisateur accepté |
| Auth : MD5 | ✅ | Type d'auth PG 5 |
| Auth : SCRAM-SHA-256 | ✅ | Compatible PG 10+ |
| Auth : Mot de passe (texte clair) | ✅ | Type d'auth PG 3 |
| TLS/SSL | ✅ | SSLRequest → mise à niveau si configuré |
| COPY IN/OUT | ✅ | Formats texte et CSV |
| `psql` 12+ | ✅ | Entièrement testé |
| `pgbench` (init + run) | ✅ | Scripts intégrés fonctionnels |
| JDBC (pgjdbc 42.x) | ✅ | Testé avec 42.7+ |
| Requête d'annulation | ✅ | Sondage AtomicBool, latence 50ms |
| LISTEN/NOTIFY | ✅ | Hub de diffusion en mémoire |
| Protocole de réplication logique | ✅ | IDENTIFY_SYSTEM, CREATE/DROP_REPLICATION_SLOT, START_REPLICATION |

### Couverture SQL

| Catégorie | Supporté |
|-----------|----------|
| **DDL** | CREATE/DROP/ALTER TABLE, CREATE/DROP INDEX, CREATE/DROP VIEW, CREATE/DROP SEQUENCE, TRUNCATE |
| **DML** | INSERT (incl. ON CONFLICT, RETURNING, SELECT), UPDATE (incl. FROM, RETURNING), DELETE (incl. USING, RETURNING), COPY |
| **Requêtes** | WHERE, ORDER BY, LIMIT/OFFSET, DISTINCT, GROUP BY/HAVING, JOINs, sous-requêtes, CTEs (incl. RECURSIVE), UNION/INTERSECT/EXCEPT, fonctions de fenêtre |
| **Agrégats** | COUNT, SUM, AVG, MIN, MAX, STRING_AGG, BOOL_AND/OR, ARRAY_AGG |
| **Types** | INT, BIGINT, FLOAT8, DECIMAL/NUMERIC, TEXT, BOOLEAN, TIMESTAMP, DATE, JSONB, ARRAY, SERIAL/BIGSERIAL |
| **Transactions** | BEGIN/COMMIT/ROLLBACK, READ ONLY/READ WRITE, timeout par transaction, Read Committed, Snapshot Isolation |
| **Fonctions** | 500+ fonctions scalaires (chaînes, mathématiques, date/heure, crypto, JSON, tableaux) |
| **Observabilité** | SHOW falcon.*, EXPLAIN, EXPLAIN ANALYZE, CHECKPOINT, ANALYZE TABLE |

### <a id="not-supported"></a>Non Supporté (v1.2)

| Fonctionnalité | Code d'Erreur | Message d'Erreur |
|----------------|:-------------:|------------------|
| Procédures stockées / PL/pgSQL | `0A000` | `stored procedures are not supported` |
| Triggers | `0A000` | `triggers are not supported` |
| Vues matérialisées | `0A000` | `materialized views are not supported` |
| Wrappers de données externes (FDW) | `0A000` | `foreign data wrappers are not supported` |
| Recherche en texte intégral | `0A000` | `full-text search is not supported` |
| DDL en ligne | `0A000` | `concurrent index operations are not supported` |
| HTAP / ColumnStore | — | Désactivé à la compilation |

---

## 1. Compilation

```bash
# Compiler tous les crates (debug)
cargo build --workspace

# Compilation release
cargo build --release --workspace

# Exécuter les tests (2 643+ tests)
cargo test --workspace

# Lint
cargo clippy --workspace
```

---

## 2. Démarrage Rapide

```bash
# Mode en mémoire (sans WAL, plus rapide)
cargo run -p falcon_server -- --no-wal

# Avec persistance WAL
cargo run -p falcon_server -- --data-dir ./falcon_data

# Connexion via psql
psql -h 127.0.0.1 -p 5433 -U falcon
```

---

## 3. Moteurs de Stockage

| Moteur | Stockage | Persistance | Idéal Pour |
|--------|---------|-------------|------------|
| **Rowstore** (par défaut) | En mémoire (chaînes de versions MVCC) | WAL uniquement | OLTP basse latence |
| **LSM** | Disque (LSM-Tree + WAL) | Persistance disque complète | Grands ensembles de données |

---

## 4. Modèle de Transaction

- **LocalTxn (chemin rapide)** : les transactions sur un seul fragment valident avec OCC sous Snapshot Isolation — sans surcharge 2PC.
- **GlobalTxn (chemin lent)** : les transactions multi-fragments utilisent XA-2PC avec prepare/commit.

---

## 5. Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Protocole PG Wire (TCP)  │  Protocole Natif (TCP/TLS)  │
├──────────────────────────┴─────────────────────────────┤
│         Frontend SQL (sqlparser-rs → Binder)            │
├────────────────────────────────────────────────────────┤
│         Planificateur / Routeur                         │
├────────────────────────────────────────────────────────┤
│         Exécuteur (ligne par ligne + agrégats streaming)│
├──────────────────┬─────────────────────────────────────┤
│   Gestionnaire   │   Moteur de Stockage                │
│   de Txn         │   (MemTable + LSM + WAL + GC)       │
├──────────────────┴─────────────────────────────────────┤
│  Cluster (ShardMap, Réplication, Basculement, Epoch)   │
└────────────────────────────────────────────────────────┘
```

---

## 6. Métriques et Observabilité

```sql
-- Statistiques des transactions
SHOW falcon.txn_stats;

-- Statistiques GC
SHOW falcon.gc_stats;

-- Statistiques de réplication
SHOW falcon.replication_stats;
```

---

## Licence

Apache-2.0
