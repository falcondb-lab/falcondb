# FalconDB — Base de données OLTP distribuée en mémoire, compatible protocole PG

> **[English](ARCHITECTURE.md)** | [简体中文](ARCHITECTURE_zh.md) | [日本語](ARCHITECTURE_ja.md) | [한국어](ARCHITECTURE_ko.md) | Français | [Deutsch](ARCHITECTURE_de.md) | [Español](ARCHITECTURE_es.md) | [Português](ARCHITECTURE_pt.md) | [Italiano](ARCHITECTURE_it.md) | [العربية](ARCHITECTURE_ar.md) | [Bahasa Melayu](ARCHITECTURE_ms.md)

## 0. Propriété fondamentale : Garantie de Commit Déterministe (DCG)

> **Si FalconDB retourne "committed", la transaction survit à toute panne de nœud unique, tout basculement et toute récupération — sans exception.**

La **Garantie de Commit Déterministe** est appliquée par le modèle de point de commit (§1 dans `falcon_common::consistency`) : sous la politique de commit active, aucun ACK client n'est envoyé avant la persistance WAL. Les quatre phases de commit — **WAL enregistré → WAL persisté → Visible → Acquitté** — progressent strictement vers l'avant ; toute tentative de retour en arrière est rejetée comme `InvariantViolation`.

- Preuves : [docs/consistency_evidence_map.md](docs/consistency_evidence_map.md)
- Diagramme de séquence : [docs/commit_sequence.md](docs/commit_sequence.md)
- Non-objectifs : [docs/non_goals.md](docs/non_goals.md)

## 1. Vue d'ensemble de l'architecture

### 1.1 Flux de données (chemin normal : SELECT)

```
psql ──TCP──▶ [Couche protocole (PG Wire)]
                  │
                  ▼
            [Frontend SQL : Parse → Bind → Analyze]
                  │
                  ▼
            [Planificateur / Routeur]
                  │
          ┌───────┴────────┐
          │ Shard unique   │ Multi-shards
          ▼                ▼
    [Exécuteur local]  [Coordinateur distribué]
          │                │
          ▼                ▼
    [Gestionnaire Txn ─── MVCC ─── Moteur de stockage]
          │
          ▼
    [Table mémoire + Index]
          │
          ▼ (async)
    [WAL / Checkpoint]
```

### 1.2 Flux de données (chemin d'écriture)

```
Client INSERT ──▶ PG Wire ──▶ Frontend SQL ──▶ Planificateur/Routeur
    ──▶ Début Txn ──▶ Localisation shard (hash PK)
    ──▶ LocalTxn (chemin rapide, shard unique, sans 2PC)
        │  ou GlobalTxn (chemin lent, multi-shards, XA-2PC)
    ──▶ Commit ──▶ Application MemTable + WAL fsync
    ──▶ WAL envoyé aux réplicas (async, schéma A)
    ──▶ Acquittement client
```

### 1.3 Rôles des nœuds (M1 : Primaire / Réplica)

M1 utilise une topologie primaire-réplica par shard :
- **Primaire** : accepte les lectures/écritures ; acquitte le commit après persistance WAL.
- **Réplica** : reçoit les chunks WAL ; lecture seule avant promotion.
- **Promotion** : basculement basé sur le fencing (fence → rattrapage → bascule → unfence).

---

## 2. Inventaire des modules

### 2.1 `protocol_pg` — Protocole Wire PostgreSQL

| Élément | Détails |
|---------|---------|
| **Responsabilité** | Acceptation TCP, handshake PG, parsing Simple/Extended Query, sérialisation des réponses |
| **Entrée** | Flux d'octets TCP brut |
| **Sortie** | `Statement` parsé + contexte de session ; trames de réponse PG sérialisées |
| **Types clés** | `PgConnection`, `PgMessage`, `AuthMethod`, `PgSession` |
| **Réutilisation OSS** | crate `pgwire` (MIT) pour le codec de messages |

### 2.2 `sql_frontend` — Parseur / Analyseur / Lieur

| Élément | Détails |
|---------|---------|
| **Responsabilité** | Texte SQL → AST → Plan logique résolu avec références catalogue |
| **Types clés** | `Statement`, `Expr`, `TableRef`, `ColumnRef`, `BoundStatement` |
| **Réutilisation OSS** | `sqlparser-rs` (Apache-2.0) |

### 2.3 `planner_router` — Planificateur de requêtes & Routeur distribué

| Élément | Détails |
|---------|---------|
| **Responsabilité** | Plan logique → Plan physique ; détermination du shard cible ; poussée de filtres |
| **Types clés** | `LogicalPlan`, `PhysicalPlan`, `ShardTarget`, `PushDown` |

### 2.4 `executor` — Moteur d'exécution

| Élément | Détails |
|---------|---------|
| **Responsabilité** | Exécution des opérateurs du plan physique ; gouverneur de requêtes ; agrégation fusionnée en streaming |
| **Types clés** | `Operator` trait, `SeqScan`, `IndexScan`, `Filter`, `Project`, `Sort`, `Agg`, `QueryGovernor` |
| **Chemin rapide** | `exec_fused_aggregate` — WHERE+GROUP BY+agrégation en un seul passage MVCC (zéro copie de lignes) |

### 2.5 `txn` — Gestionnaire de transactions

| Élément | Détails |
|---------|---------|
| **Responsabilité** | Début/commit/annulation de transactions ; attribution de timestamps MVCC ; validation OCC (SI) |
| **Types clés** | `TxnManager`, `TxnHandle`, `TxnId`, `Timestamp`, `IsolationLevel` |

### 2.6 `storage` — Moteur de stockage en mémoire

| Élément | Détails |
|---------|---------|
| **Responsabilité** | Stockage en mémoire des tuples et versions MVCC ; maintenance des index ; écriture WAL + récupération ; GC MVCC |
| **Types clés** | `StorageEngine`, `MemTable`, `VersionChain`, `SecondaryIndex`, `WalWriter`, `GcRunner` |

### 2.7 `raft` — Consensus (stub — hors chemin de production)

> **⚠️ Ce crate est un stub à nœud unique. Il n'est pas utilisé dans le chemin de données de production.**

### 2.8 `cluster` — Cluster, réplication et exécution distribuée

| Élément | Détails |
|---------|---------|
| **Responsabilité** | Carte des shards ; réplication primaire-réplica basée sur WAL ; promotion/basculement avec fencing ; exécution distribuée scatter/gather |
| **Types clés** | `ShardMap`, `WalChunk`, `ReplicationTransport`, `DistributedQueryEngine` |

### 2.9 `observability` — Métriques / Traçage / Journalisation

Métriques Prometheus, traçage OpenTelemetry, journalisation structurée.

### 2.10 `common` — Infrastructure partagée

Types partagés, hiérarchie d'erreurs, configuration, système de types PG, représentation datum, RBAC.

---

## 3. Périmètre MVP

### 3.1 Supporté

| Catégorie | Périmètre |
|-----------|-----------|
| **Protocole** | PG wire Simple Query + Extended Query ; authentification trust |
| **SQL DDL** | `CREATE TABLE`, `DROP TABLE`, `TRUNCATE`, `ALTER TABLE`, `CREATE INDEX`, `CREATE VIEW` |
| **SQL DML** | `INSERT`, `UPDATE`, `DELETE` (avec `RETURNING`), `SELECT`, `JOIN`, sous-requêtes, CTE, CTE récursifs, fonctions fenêtres, 260+ fonctions scalaires |
| **Types** | `INT`, `BIGINT`, `TEXT`, `BOOL`, `FLOAT8`/`NUMERIC`/`DECIMAL`, `TIMESTAMP`, `JSONB`, `ARRAY`, `DATE` |
| **Transactions** | `BEGIN`, `COMMIT`, `ROLLBACK` ; niveau d'isolation Read Committed |
| **Stockage** | Stockage en lignes en mémoire ; index hash PK ; index secondaire BTree |
| **Persistance** | WAL (segments 64 Mo, commit groupé) ; récupération après crash |
| **Distribué** | Sharding par hash PK ; réplication WAL gRPC ; chemin rapide shard unique + 2PC cross-shard |

### 3.2 Non supporté (M1)

- Réplication logique, CDC
- Changement de schéma en ligne
- Multi-tenant
- Rééquilibrage automatique (manuel uniquement)

---

## 4. Matrice de réutilisation OSS

| Module | Candidat | Licence | Raison |
|--------|----------|---------|--------|
| **PG Wire** | `pgwire` 0.25+ | MIT | Codec de messages PG complet |
| **Parseur SQL** | `sqlparser-rs` 0.50+ | Apache-2.0 | Dialecte PG éprouvé |
| **Runtime async** | `tokio` 1.x | MIT | Standard de facto |
| **RPC** | `tonic` (gRPC) | MIT | Mature, génération de code, streaming |
| **Métriques** | `metrics` + `metrics-exporter-prometheus` | MIT | Léger, natif Prometheus |

**Politique de fork** : Pas de fork. Toutes les dépendances utilisent des crates publiés.

---

## 5. Conception des transactions et de la cohérence

### 5.2 Implémenté : Chemin rapide + Chemin lent + OCC

- **LocalTxn (chemin rapide)** : Transaction mono-shard, commit par validation OCC sous isolation snapshot. Sans surcoût 2PC.
- **GlobalTxn (chemin lent)** : Transaction cross-shard avec XA-2PC.
- **Application d'invariants stricts** : `TxnContext.validate_commit_invariants()` retourne une erreur `InvariantViolation`.

### 5.4 GC MVCC

- **Point de sécurité** : `gc_safepoint = min(min_active_ts, replica_safe_ts) - 1`
- **Conscient du WAL** : ne récupère jamais les versions non committées ou annulées
- **Sûr pour la réplication** : respecte les timestamps appliqués par les réplicas

### 5.5 Réplication — Transport WAL par gRPC

```
Primaire ──Chunks WAL──▶ Réplica
         (flux gRPC, tonic, RPC SubscribeWal)
```

> **⚠️ Limite stricte** : Le crate `falcon_raft` est un stub à nœud unique. Les réplicas WAL ne sont **pas** des followers Raft.

---

## 6. Structure du dépôt

```
falcon/
├── Cargo.toml                  (racine workspace)
├── crates/
│   ├── falcon_common/           (types partagés, erreurs, config, RBAC)
│   ├── falcon_storage/          (tables, LSM, index, WAL, GC)
│   ├── falcon_txn/              (gestion txn, MVCC, OCC)
│   ├── falcon_sql_frontend/     (parseur, lieur, analyseur)
│   ├── falcon_planner/          (plans logique+physique)
│   ├── falcon_executor/         (exécution d'opérateurs)
│   ├── falcon_protocol_pg/      (protocole PG wire)
│   ├── falcon_cluster/          (réplication, basculement, scatter/gather)
│   ├── falcon_observability/    (métriques, traçage, journalisation)
│   ├── falcon_server/           (binaire principal + tests d'intégration)
│   └── falcon_bench/            (harnais de benchmark YCSB)
├── clients/
│   └── falcondb-jdbc/           (pilote Java JDBC)
└── scripts/
```

---

## 7. Objectifs de performance

| Métrique | Objectif |
|----------|----------|
| QPS lecture ponctuelle | ≥ 200K |
| Lecture ponctuelle P50 / P99 | < 0,2ms / < 1ms |
| QPS écriture ponctuelle | ≥ 100K |
| QPS mixte (50/50) | ≥ 120K |
| QPS écriture (cluster 3 nœuds) | ≥ 50K |

---

## 8. Tests et jalons

### 8.2 Nombre de tests (3 654 tests)

| Crate | Tests |
|-------|-------|
| `falcon_cluster` | 1 057 |
| `falcon_storage` | 776 |
| `falcon_server` (intégration) | 421 |
| `falcon_common` | 254 |
| `falcon_protocol_pg` | 240 |
| `falcon_cli` | 201 |
| `falcon_executor` | 192 |
| `falcon_sql_frontend` | 149 |
| `falcon_txn` | 103 |
| `falcon_planner` | 89 |
| **Total** | **3 654** (réussis) |

### 8.4 Jalons

| Phase | Périmètre | Statut |
|-------|-----------|--------|
| **M1** | OLTP stable, transactions chemin rapide/lent, réplication WAL | **Terminé** |
| **M2** | Streaming WAL gRPC, déploiement multi-nœuds | Terminé |
| **M3** | Durcissement production : réplicas lecture seule, arrêt gracieux | **Terminé** |
| **M4** | Optimisation requêtes, EXPLAIN ANALYZE, journal lent | **Terminé** |
| **M5** | information_schema, vues, cache de plans | **Terminé** |
| **M6** | JSONB, sous-requêtes corrélées, FK CASCADE, CTE récursifs | **Terminé** |
| **M7** | Fonctions séquence, fonctions fenêtres | **Terminé** |
| **M8** | Type DATE, commande COPY | **Terminé** |

---

## 9. Fonctions scalaires étendues

> **Déplacé vers [docs/extended_scalar_functions.md](docs/extended_scalar_functions.md)**. Contient les spécifications de 260+ fonctions.
