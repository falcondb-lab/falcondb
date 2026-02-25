# FalconDB

<p align="center">
  <img src="assets/falcondb-logo.png" alt="FalconDB Logo" width="220" />
</p>

<h1 align="center">FalconDB</h1>

<p align="center">
  <strong>PG互換 · 分散型 · メモリファースト · 決定論的トランザクションセマンティクス</strong>
</p>

<p align="center">
  <a href="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml">
    <img src="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml/badge.svg" alt="CI" />
  </a>
  <img src="https://img.shields.io/badge/version-1.2.0-blue" alt="バージョン" />
  <img src="https://img.shields.io/badge/MSRV-1.75-blue" alt="MSRV" />
  <img src="https://img.shields.io/badge/license-Apache--2.0-green" alt="ライセンス" />
</p>

> English | [简体中文](README_zh.md) | [Español](README_es.md) | [Français](README_fr.md) | [한국어](README_ko.md) | **日本語** | [Deutsch](README_de.md) | [العربية](README_ar.md) | [Italiano](README_it.md) | [Bahasa Melayu](README_ms.md) | [Português](README_pt.md)

> FalconDB は、決定論的なトランザクションセマンティクスを備えた **PG互換・分散型・メモリファーストのOLTPデータベース**です。PostgreSQL、VoltDB、SingleStore との比較ベンチマーク — **[ベンチマークマトリクス](benchmarks/README.md)** を参照。
>
> - ✅ **低レイテンシ** — シングルシャードのファストパスコミットは 2PC を完全にバイパス
> - ✅ **スキャン性能** — 融合ストリーミング集計、ゼロコピー MVCC イテレーション、1M行でPGに近いパリティ
> - ✅ **安定性** — p99 境界値保証、中断率 < 1%、再現可能なベンチマーク
> - ✅ **証明可能な一貫性** — スナップショット分離下での MVCC/OCC、CI検証済み ACID
> - ✅ **運用性** — 50以上のSHOWコマンド、Prometheusメトリクス、フェイルオーバーCIゲート
> - ✅ **決定論** — 強化されたステートマシン、有界in-doubt、べき等リトライ
> - ❌ HTAP 非対応 — 分析ワークロードなし
> - ❌ 完全なPGではない — [非対応リスト参照](#not-supported)

### 対応プラットフォーム

| プラットフォーム | ビルド | テスト | ステータス |
|----------------|:------:|:------:|-----------|
| **Linux** (x86_64, Ubuntu 22.04+) | ✅ | ✅ | 主要CIターゲット |
| **Windows** (x86_64, MSVC) | ✅ | ✅ | CIターゲット |
| **macOS** (x86_64 / aarch64) | ✅ | ✅ | コミュニティテスト済み |

**MSRV**: Rust **1.75** (`Cargo.toml` の `rust-version = "1.75"`)

### PGプロトコル互換性

| 機能 | ステータス | 備考 |
|------|:---------:|------|
| シンプルクエリプロトコル | ✅ | 単一・複数ステートメント |
| 拡張クエリ (Parse/Bind/Execute) | ✅ | プリペアドステートメント + ポータル |
| Auth: Trust | ✅ | 任意のユーザーを受け入れ |
| Auth: MD5 | ✅ | PG auth タイプ5 |
| Auth: SCRAM-SHA-256 | ✅ | PG 10+ 互換 |
| Auth: パスワード (平文) | ✅ | PG auth タイプ3 |
| TLS/SSL | ✅ | 設定時 SSLRequest → アップグレード |
| COPY IN/OUT | ✅ | テキスト・CSV形式 |
| `psql` 12+ | ✅ | 完全テスト済み |
| `pgbench` (init + run) | ✅ | 組み込みスクリプト動作確認済み |
| JDBC (pgjdbc 42.x) | ✅ | 42.7+ でテスト済み |
| キャンセルリクエスト | ✅ | AtomicBoolポーリング、50msレイテンシ |
| LISTEN/NOTIFY | ✅ | インメモリブロードキャストハブ |
| 論理レプリケーションプロトコル | ✅ | IDENTIFY_SYSTEM, CREATE/DROP_REPLICATION_SLOT, START_REPLICATION |

### SQLカバレッジ

| カテゴリ | 対応内容 |
|---------|---------|
| **DDL** | CREATE/DROP/ALTER TABLE, CREATE/DROP INDEX, CREATE/DROP VIEW, CREATE/DROP SEQUENCE, TRUNCATE |
| **DML** | INSERT (ON CONFLICT, RETURNING, SELECT含む), UPDATE (FROM, RETURNING含む), DELETE (USING, RETURNING含む), COPY |
| **クエリ** | WHERE, ORDER BY, LIMIT/OFFSET, DISTINCT, GROUP BY/HAVING, JOIN, サブクエリ, CTE (RECURSIVE含む), UNION/INTERSECT/EXCEPT, ウィンドウ関数 |
| **集計** | COUNT, SUM, AVG, MIN, MAX, STRING_AGG, BOOL_AND/OR, ARRAY_AGG |
| **型** | INT, BIGINT, FLOAT8, DECIMAL/NUMERIC, TEXT, BOOLEAN, TIMESTAMP, DATE, JSONB, ARRAY, SERIAL/BIGSERIAL |
| **トランザクション** | BEGIN/COMMIT/ROLLBACK, READ ONLY/READ WRITE, トランザクションごとのタイムアウト, Read Committed, Snapshot Isolation |
| **関数** | 500以上のスカラー関数 (文字列, 数学, 日付/時刻, 暗号, JSON, 配列) |
| **可観測性** | SHOW falcon.*, EXPLAIN, EXPLAIN ANALYZE, CHECKPOINT, ANALYZE TABLE |

### <a id="not-supported"></a>非対応機能 (v1.2)

| 機能 | エラーコード | エラーメッセージ |
|------|:-----------:|----------------|
| ストアドプロシージャ / PL/pgSQL | `0A000` | `stored procedures are not supported` |
| トリガー | `0A000` | `triggers are not supported` |
| マテリアライズドビュー | `0A000` | `materialized views are not supported` |
| 外部データラッパー (FDW) | `0A000` | `foreign data wrappers are not supported` |
| 全文検索 | `0A000` | `full-text search is not supported` |
| オンラインDDL | `0A000` | `concurrent index operations are not supported` |
| HTAP / ColumnStore | — | コンパイル時に無効化 |

---

## 1. ビルド

```bash
# 全クレートのビルド (debug)
cargo build --workspace

# リリースビルド
cargo build --release --workspace

# テスト実行 (2,643件以上)
cargo test --workspace

# リント
cargo clippy --workspace
```

---

## 2. クイックスタート

```bash
# インメモリモード (WALなし、最速)
cargo run -p falcon_server -- --no-wal

# WAL永続化あり
cargo run -p falcon_server -- --data-dir ./falcon_data

# psqlで接続
psql -h 127.0.0.1 -p 5433 -U falcon
```

---

## 3. ストレージエンジン

| エンジン | ストレージ | 永続化 | 最適用途 |
|---------|----------|--------|---------|
| **Rowstore** (デフォルト) | インメモリ (MVCCバージョンチェーン) | WALのみ | 低レイテンシOLTP |
| **LSM** | ディスク (LSM-Tree + WAL) | 完全ディスク永続化 | 大規模データセット |

---

## 4. トランザクションモデル

- **LocalTxn (ファストパス)**: シングルシャードトランザクションはスナップショット分離下でOCCによりコミット — 2PCオーバーヘッドなし。
- **GlobalTxn (スローパス)**: クロスシャードトランザクションはprepare/commitを伴うXA-2PCを使用。

---

## 5. アーキテクチャ

```
┌─────────────────────────────────────────────────────────┐
│  PG Wireプロトコル (TCP)  │  ネイティブプロトコル (TCP/TLS) │
├──────────────────────────┴─────────────────────────────┤
│         SQLフロントエンド (sqlparser-rs → Binder)        │
├────────────────────────────────────────────────────────┤
│         プランナー / ルーター                             │
├────────────────────────────────────────────────────────┤
│         エグゼキューター (行単位 + 融合ストリーミング集計) │
├──────────────────┬─────────────────────────────────────┤
│   トランザクション│   ストレージエンジン                  │
│   マネージャー   │   (MemTable + LSM + WAL + GC)        │
├──────────────────┴─────────────────────────────────────┤
│  クラスター (ShardMap, レプリケーション, フェイルオーバー) │
└────────────────────────────────────────────────────────┘
```

---

## 6. メトリクスと可観測性

```sql
-- トランザクション統計
SHOW falcon.txn_stats;

-- GC統計
SHOW falcon.gc_stats;

-- レプリケーション統計
SHOW falcon.replication_stats;
```

---

## ライセンス

Apache-2.0
