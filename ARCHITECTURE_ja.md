# FalconDB — PGプロトコル互換の分散インメモリOLTPデータベース

> **[English](ARCHITECTURE.md)** | [简体中文](ARCHITECTURE_zh.md) | 日本語 | [한국어](ARCHITECTURE_ko.md) | [Français](ARCHITECTURE_fr.md) | [Deutsch](ARCHITECTURE_de.md) | [Español](ARCHITECTURE_es.md) | [Português](ARCHITECTURE_pt.md) | [Italiano](ARCHITECTURE_it.md) | [العربية](ARCHITECTURE_ar.md) | [Bahasa Melayu](ARCHITECTURE_ms.md)

## 0. コア特性：決定論的コミット保証 (DCG)

> **FalconDBが「committed」を返した場合、そのトランザクションはあらゆる単一ノード障害、フェイルオーバー、リカバリを通じて保持されます——例外なし。**

**決定論的コミット保証**はコミットポイントモデル（`falcon_common::consistency` の§1）により強制されます：アクティブコミットポリシーでは、WAL永続化前にクライアントACKが送信されることはありません。4つのコミットフェーズ——**WAL記録済 → WAL永続化済 → 可視 → 確認済**——は厳密に前進のみ；いかなるロールバック試行も `InvariantViolation` として拒否されます。

- エビデンス：[docs/consistency_evidence_map.md](docs/consistency_evidence_map.md)
- シーケンス図：[docs/commit_sequence.md](docs/commit_sequence.md)
- 非目標：[docs/non_goals.md](docs/non_goals.md)

## 1. アーキテクチャ概要

### 1.1 データフロー（正常パス：SELECT）

```
psql ──TCP──▶ [プロトコル層 (PG Wire)]
                  │
                  ▼
            [SQLフロントエンド: Parse → Bind → Analyze]
                  │
                  ▼
            [プランナー / ルーター]
                  │
          ┌───────┴────────┐
          │ 単一シャード   │ マルチシャード
          ▼                ▼
    [ローカル実行]     [分散コーディネーター]
          │                │
          ▼                ▼
    [トランザクションMgr ─── MVCC ─── ストレージエンジン]
          │
          ▼
    [メモリテーブル + インデックス]
          │
          ▼ (非同期)
    [WAL / チェックポイント]
```

### 1.2 データフロー（書き込みパス）

```
クライアント INSERT ──▶ PG Wire ──▶ SQLフロントエンド ──▶ プランナー/ルーター
    ──▶ トランザクション開始 ──▶ シャード特定（PKハッシュベース）
    ──▶ LocalTxn（高速パス、単一シャード、2PCなし）
        │  または GlobalTxn（低速パス、マルチシャード、XA-2PC）
    ──▶ コミット ──▶ MemTable適用 + WAL fsync
    ──▶ WALをレプリカに送信（非同期、方式A）
    ──▶ クライアント確認
```

### 1.3 ノードロール（M1：プライマリ / レプリカ）

M1はシャードごとにプライマリ-レプリカトポロジを使用：
- **プライマリ**：読み書きを受け付け、WAL永続化後にコミット確認。
- **レプリカ**：WALチャンクを受信、昇格前は読み取り専用。
- **昇格**：フェンシングベースのフェイルオーバー（fence → catch-up → switchover → unfence）。

将来（M2+）：専用のコンピュート/ストレージ/メタデータロールに分離。

---

## 2. モジュール一覧

### 2.1 `protocol_pg` — PostgreSQL Wireプロトコル

| 項目 | 詳細 |
|------|------|
| **責務** | TCP接続受付、PG起動/認証ハンドシェイク、Simple Query / Extended Queryメッセージ解析、RowDescription/DataRow/CommandComplete/Errorレスポンスのシリアライズ |
| **入力** | 生TCPバイトストリーム |
| **出力** | パース済み `Statement` + セッションコンテキスト；シリアライズ済みPGレスポンスフレーム |
| **コア型** | `PgConnection`, `PgMessage`, `AuthMethod`, `PgSession` |
| **依存** | `common`（型、エラー） |
| **交換可能** | crate全体をMySQLプロトコルまたはHTTP/gRPCゲートウェイに置換可能 |
| **OSS再利用** | `pgwire` crate（MIT）メッセージコーデック用；カスタムセッション管理 |

### 2.2 `sql_frontend` — パーサー / アナライザー / バインダー

| 項目 | 詳細 |
|------|------|
| **責務** | SQLテキスト → AST → カタログ参照付き解決済み論理プラン |
| **入力** | SQL文字列 + カタログスナップショット |
| **出力** | `BoundStatement`（型付き、解決済みAST） |
| **コア型** | `Statement`, `Expr`, `TableRef`, `ColumnRef`, `BoundStatement` |
| **依存** | `common`（型、カタログtrait） |
| **交換可能** | パーサーバックエンド（sqlparser-rs vs カスタムPEG）、方言設定 |
| **OSS再利用** | `sqlparser-rs`（Apache-2.0）パース用；カスタムバインダー/アナライザー |

### 2.3 `planner_router` — クエリプランナー＆分散ルーター

| 項目 | 詳細 |
|------|------|
| **責務** | 論理プラン → 物理プラン；シャードターゲット決定；フィルタプッシュダウン |
| **入力** | `BoundStatement` + シャードマップ |
| **出力** | `PhysicalPlan`（シャード注釈付きオペレーターツリー） |
| **コア型** | `LogicalPlan`, `PhysicalPlan`, `ShardTarget`, `PushDown` |
| **依存** | `sql_frontend`, `cluster_meta`, `common` |
| **交換可能** | 最適化ルール、コストモデル、ルーティング戦略 |

### 2.4 `executor` — 実行エンジン

| 項目 | 詳細 |
|------|------|
| **責務** | 物理プランオペレーター実行、結果行生成；トランザクション単位のREAD ONLYとタイムアウト保護；構造化アボート理由付きクエリガバナー；大テーブルスキャンの融合ストリーミング集約 |
| **入力** | `PhysicalPlan` + トランザクションハンドル |
| **出力** | `RowStream`（`Row`のイテレーター） |
| **コア型** | `Operator` trait, `SeqScan`, `IndexScan`, `Filter`, `Project`, `Sort`, `Agg`, `Insert`, `Update`, `Delete`, `QueryGovernor`, `GovernorAbortReason`, `ProjAccum`, `exec_fused_aggregate` |
| **高速パス** | `exec_fused_aggregate` — WHERE+GROUP BY+集約を単一パスMVCCチェーンで実行（行コピーゼロ）；`try_streaming_aggs` — `compute_simple_aggs`による単純カラム参照集約；`try_pk_ordered_limit` — `scan_top_k_by_pk`による有界ヒープtop-K |
| **依存** | `storage`（trait経由）, `txn`, `common` |
| **交換可能** | 行単位 ↔ ベクトル化；プッシュ ↔ プルモデル |

### 2.5 `txn` — トランザクションマネージャー

| 項目 | 詳細 |
|------|------|
| **責務** | トランザクション開始/コミット/中止；MVCCタイムスタンプ割り当て；OCC検証（SI）；高速パス（LocalTxn）と低速パス（GlobalTxn）コミット；レイテンシヒストグラム；トランザクション履歴リングバッファ；トランザクション単位のREAD ONLYモード、タイムアウト、実行サマリー |
| **入力** | エグゼキューターからのトランザクション操作 |
| **出力** | トランザクションハンドル、コミット/中止判定、`TxnStatsSnapshot`、`TxnRecord`履歴 |
| **コア型** | `TxnManager`, `TxnHandle`, `TxnId`, `Timestamp`, `IsolationLevel`, `TxnType`, `TxnPath`, `TxnContext`, `TxnRecord`, `TxnOutcome`, `LatencyStats`, `TxnExecSummary` |
| **依存** | `storage`（バージョン可視性、OCC読み取りセット）, `common` |
| **交換可能** | OCC vs 2PL vs ハイブリッド；タイムスタンプオラクル（ローカル vs 分散） |

### 2.6 `storage` — インメモリストレージエンジン

| 項目 | 詳細 |
|------|------|
| **責務** | タプルとMVCCバージョンのメモリ内格納；インデックス管理（ハッシュPK + BTreeセカンダリ + ユニーク + 複合/カバリング/プレフィックス）；WAL書き込み+リカバリ；MVCC GC；レプリケーション統計；LSMディスクエンジン；ゼロコピー行イテレーション；ストリーミング集約計算 |
| **入力** | トランザクションコンテキスト付きGet/Put/Delete/Scan |
| **出力** | タプル（指定トランザクションの可視バージョン） |
| **コア型** | `StorageEngine`, `MemTable`, `VersionChain`, `SecondaryIndex`, `WalWriter`, `WalRecord`, `GcConfig`, `GcStats`, `GcRunner`, `ReplicationStats`, `LsmEngine` |
| **MVCC高速パス** | `with_visible_data` — クロージャベースのゼロコピー行アクセス；`for_each_visible` — Vec割り当てなしのDashMapストリーミング反復；`scan_top_k_by_pk` — O(N log K)の有界`BinaryHeap` top-K；`compute_simple_aggs` — 単一パスCOUNT/SUM/MIN/MAX；全可視性チェックでheadバージョンのArc-clone排除 |
| **依存** | `common` |
| **交換可能** | エンジン全体（RocksDBアダプター、FoundationDBアダプター）；インデックス実装（BTree、SkipList、ART） |

### 2.7 `raft` — コンセンサス（スタブ——本番パスでは未使用）

> **⚠️ このcrateは単一ノードスタブです。本番データパスでは使用されません。**

| 項目 | 詳細 |
|------|------|
| **責務** | コンセンサスtrait定義 + 単一ノードno-opスタブ |
| **本番状態** | **未使用。**全RPCが `Unreachable("single-node mode")` を返す |
| **コア型** | `RaftNode`（スタブ）, `LogEntry`, `StateMachine` trait |
| **依存** | `common` |
| **将来** | ≥3ノードデプロイでの自動リーダー選出に使用可能；未スケジュール |

### 2.8 `cluster` — クラスター、レプリケーション、分散実行

| 項目 | 詳細 |
|------|------|
| **責務** | シャードマップ；WALベースのプライマリ-レプリカレプリケーション；フェンシング付き昇格/フェイルオーバー；scatter/gather分散実行；DDL協調；アドミッション制御；ノード動作モード |
| **コア型** | `ShardMap`, `ShardReplicaGroup`, `WalChunk`, `ReplicationTransport`, `DistributedQueryEngine`, `FailurePolicy`, `AdmissionControl`, `NodeModeController` |
| **依存** | `storage`, `txn`, `common` |
| **交換可能** | トランスポート層（インプロセス → gRPC）；メタデータストア（組み込み vs etcd） |

### 2.9 `observability` — メトリクス / トレーシング / ロギング

| 項目 | 詳細 |
|------|------|
| **責務** | メトリクス発行（Prometheus）、構造化トレーシング（OpenTelemetry）、構造化ロギング |
| **OSS再利用** | `metrics` + `metrics-exporter-prometheus`, `tracing` + `tracing-opentelemetry` |

### 2.10 `common` — 共有インフラ

| 項目 | 詳細 |
|------|------|
| **責務** | 共有型、エラー階層、設定、PG型システム、datum表現（`Decimal`含む）、RBAC |
| **コア型** | `Datum`, `DataType`, `Row`, `OwnedRow`, `TableId`, `ShardId`, `NodeId`, `FalconError`, `Config` |
| **依存** | なし（リーフcrate） |

---

## 3. MVPスコープ

### 3.1 サポート済み

| カテゴリ | スコープ |
|----------|----------|
| **プロトコル** | PG wire Simple Query + Extended Query；trust認証；テキスト形式 |
| **SQL DDL** | `CREATE TABLE`, `DROP TABLE`, `TRUNCATE`, `ALTER TABLE`, `CREATE INDEX`, `CREATE VIEW`, `DROP VIEW` |
| **SQL DML** | `INSERT`, `UPDATE`, `DELETE`（`RETURNING`対応）、`SELECT`, `WHERE`, `ORDER BY`, `LIMIT`, `JOIN`, サブクエリ, CTE, 再帰CTE, ウィンドウ関数, 260+スカラー関数 |
| **型** | `INT`, `BIGINT`, `TEXT`, `BOOL`, `FLOAT8`/`NUMERIC`/`DECIMAL`, `TIMESTAMP`, `JSONB`, `ARRAY`, `DATE` |
| **トランザクション** | `BEGIN`, `COMMIT`, `ROLLBACK`；Read Committed分離レベル |
| **ストレージ** | インメモリ行ストア；PKハッシュインデックス；BTreeセカンダリインデックス |
| **永続化** | WAL（セグメント64MB、グループコミット）；クラッシュリカバリ |
| **分散** | PKハッシュシャーディング；gRPC WALレプリケーション；単一シャード高速パス + クロスシャード2PC |
| **可観測性** | Prometheusメトリクスエンドポイント；構造化ログ |

### 3.2 未サポート（M1）

- 論理レプリケーション、CDC
- オンラインスキーマ変更
- マルチテナンシー
- 自動リバランス（手動のみ）

---

## 4. OSS再利用マトリクス

| モジュール | 候補 | ライセンス | 理由 |
|-----------|------|----------|------|
| **PG Wire** | `pgwire` 0.25+ | MIT | 完全なPGメッセージコーデック |
| **SQLパーサー** | `sqlparser-rs` 0.50+ | Apache-2.0 | 実績あるPG方言 |
| **Raft** | `openraft` 0.10+ | MIT | **スタブのみ——本番未使用** |
| **非同期ランタイム** | `tokio` 1.x | MIT | 事実上の標準 |
| **RPC** | `tonic` (gRPC) | MIT | 成熟、コード生成、ストリーミング |
| **メトリクス** | `metrics` + `metrics-exporter-prometheus` | MIT | 軽量、Prometheusネイティブ |
| **並行データ構造** | `crossbeam` + `dashmap` | MIT | 本番実績ある並行データ構造 |
| **WAL** | カスタム（薄い層） | — | シンプルな追記ログ |

**フォークポリシー**：フォークなし。全依存関係は公開crateを使用。

---

## 5. トランザクションと一貫性設計

### 5.2 実装済み：高速パス + 低速パス + OCC

- **LocalTxn（高速パス）**：単一シャードトランザクション、スナップショット分離でOCC検証コミット。2PCオーバーヘッドなし。
- **GlobalTxn（低速パス）**：クロスシャードトランザクション、XA-2PC使用。
- **ハード不変量強制**：`TxnContext.validate_commit_invariants()` が `InvariantViolation` エラーを返す。
- **分離レベル**：Read Committed（デフォルト）またはSnapshot Isolation（設定可能）。

### 5.4 MVCC GC

- **セーフポイント**：`gc_safepoint = min(min_active_ts, replica_safe_ts) - 1`
- **WAL対応**：未コミットまたは中止バージョンは回収しない
- **レプリケーション安全**：レプリカ適用済みタイムスタンプを尊重
- **ロックフリー**：DashMap反復による各チェーン剪定

### 5.5 レプリケーション — gRPC WALシッピング

```
プライマリ ──WALチャンク──▶ レプリカ
         (gRPCストリーム, tonic, SubscribeWal RPC)
```

- **トランスポート**：`ReplicationTransport` trait → `InProcessTransport`（テスト）→ `GrpcTransport`（本番）
- **ワイヤー形式**：`WalChunk`フレーム（`start_lsn`, `end_lsn`, CRC32チェックサム）
- **昇格/フェイルオーバー**：5ステップフェンシングプロトコル

> **⚠️ ハードバウンダリ**：`falcon_raft` crateは単一ノードスタブです。WALシッピングレプリカはRaft followerでは**ありません**。

---

## 6. リポジトリ構造とコアTrait

```
falcon/
├── Cargo.toml                  (ワークスペースルート)
├── crates/
│   ├── falcon_common/           (共有型、エラー、設定、RBAC)
│   ├── falcon_storage/          (テーブル、LSM、インデックス、WAL、GC)
│   ├── falcon_txn/              (トランザクション管理、MVCC、OCC)
│   ├── falcon_sql_frontend/     (パーサー、バインダー、アナライザー)
│   ├── falcon_planner/          (論理+物理プラン、ルーティングヒント)
│   ├── falcon_executor/         (オペレーター実行、式評価)
│   ├── falcon_protocol_pg/      (PG wireプロトコル)
│   ├── falcon_cluster/          (レプリケーション、フェイルオーバー、scatter/gather)
│   ├── falcon_observability/    (メトリクス、トレーシング、ロギング)
│   ├── falcon_server/           (メインバイナリ + 統合テスト)
│   └── falcon_bench/            (YCSBベンチマークハーネス)
├── clients/
│   └── falcondb-jdbc/           (Java JDBCドライバー)
├── docs/                       (ロードマップ、プロトコル仕様)
└── scripts/                    (デモ、フェイルオーバー、ベンチマーク)
```

---

## 7. パフォーマンス目標

### 7.1 MVP目標（単一ノード、インメモリ）

| メトリクス | 目標 |
|-----------|------|
| ポイント読み取りQPS | ≥ 200K |
| ポイント読み取りP50 / P99 | < 0.2ms / < 1ms |
| ポイント書き込みQPS | ≥ 100K |
| ポイント書き込みP50 / P99 | < 0.5ms / < 2ms |
| 混合（50/50）QPS | ≥ 120K |

### 7.2 クラスター目標（3ノード、WALレプリケーション）

| メトリクス | 目標 |
|-----------|------|
| 書き込みQPS（単一シャード） | ≥ 50K |
| 書き込みP99 | < 5ms |
| リニアスケール係数（3→9ノード） | ≥ 2.5x |

---

## 8. テスト・検証・マイルストーン

### 8.2 テスト数（3,654テスト）

| Crate | テスト数 |
|-------|---------|
| `falcon_cluster` | 1,057 |
| `falcon_storage` | 776 |
| `falcon_server`（統合） | 421 |
| `falcon_common` | 254 |
| `falcon_protocol_pg` | 240 |
| `falcon_cli` | 201 |
| `falcon_executor` | 192 |
| `falcon_sql_frontend` | 149 |
| `falcon_txn` | 103 |
| `falcon_planner` | 89 |
| **合計** | **3,654**（パス） |

### 8.4 マイルストーン

| フェーズ | スコープ | 状態 |
|---------|----------|------|
| **M1** | 安定OLTP、高速/低速パストランザクション、WALレプリケーション | **完了** |
| **M2** | gRPC WALストリーミング、マルチノードデプロイ | 完了 |
| **M3** | 本番ハードニング：読み取り専用レプリカ、グレースフルシャットダウン | **完了** |
| **M4** | クエリ最適化、EXPLAIN ANALYZE、スローログ | **完了** |
| **M5** | information_schema、ビュー、クエリプランキャッシュ | **完了** |
| **M6** | JSONB、相関サブクエリ、FK CASCADE、再帰CTE | **完了** |
| **M7** | シーケンス関数、ウィンドウ関数 | **完了** |
| **M8** | DATE型、COPYコマンド | **完了** |

---

## 9. 拡張スカラー関数

> **[docs/extended_scalar_functions.md](docs/extended_scalar_functions.md)** に移動済み。260+関数の仕様を含みます。
