# FalconDB — PG 프로토콜 호환 분산 인메모리 OLTP 데이터베이스

> **[English](ARCHITECTURE.md)** | [简体中文](ARCHITECTURE_zh.md) | [日本語](ARCHITECTURE_ja.md) | 한국어 | [Français](ARCHITECTURE_fr.md) | [Deutsch](ARCHITECTURE_de.md) | [Español](ARCHITECTURE_es.md) | [Português](ARCHITECTURE_pt.md) | [Italiano](ARCHITECTURE_it.md) | [العربية](ARCHITECTURE_ar.md) | [Bahasa Melayu](ARCHITECTURE_ms.md)

## 0. 핵심 속성: 결정적 커밋 보장 (DCG)

> **FalconDB가 "committed"를 반환하면, 해당 트랜잭션은 모든 단일 노드 장애, 페일오버, 복구 과정에서 유지됩니다——예외 없음.**

**결정적 커밋 보장**은 커밋 포인트 모델(`falcon_common::consistency`의 §1)에 의해 강제됩니다: 활성 커밋 정책에서는 WAL 영속화 전에 클라이언트 ACK가 전송되지 않습니다. 네 가지 커밋 단계——**WAL 기록됨 → WAL 영속화됨 → 가시 → 확인됨**——은 엄격히 전진만 가능하며, 모든 롤백 시도는 `InvariantViolation`으로 거부됩니다.

- 증거: [docs/consistency_evidence_map.md](docs/consistency_evidence_map.md)
- 시퀀스 다이어그램: [docs/commit_sequence.md](docs/commit_sequence.md)
- 비목표: [docs/non_goals.md](docs/non_goals.md)

## 1. 아키텍처 개요

### 1.1 데이터 흐름 (정상 경로: SELECT)

```
psql ──TCP──▶ [프로토콜 계층 (PG Wire)]
                  │
                  ▼
            [SQL 프론트엔드: Parse → Bind → Analyze]
                  │
                  ▼
            [플래너 / 라우터]
                  │
          ┌───────┴────────┐
          │ 단일 샤드      │ 다중 샤드
          ▼                ▼
    [로컬 실행기]      [분산 코디네이터]
          │                │
          ▼                ▼
    [트랜잭션 관리자 ─── MVCC ─── 스토리지 엔진]
          │
          ▼
    [메모리 테이블 + 인덱스]
          │
          ▼ (비동기)
    [WAL / 체크포인트]
```

### 1.2 데이터 흐름 (쓰기 경로)

```
클라이언트 INSERT ──▶ PG Wire ──▶ SQL 프론트엔드 ──▶ 플래너/라우터
    ──▶ 트랜잭션 시작 ──▶ 샤드 결정 (PK 해시 기반)
    ──▶ LocalTxn (빠른 경로, 단일 샤드, 2PC 없음)
        │  또는 GlobalTxn (느린 경로, 다중 샤드, XA-2PC)
    ──▶ 커밋 ──▶ MemTable 적용 + WAL fsync
    ──▶ WAL을 레플리카로 전송 (비동기, 방식 A)
    ──▶ 클라이언트 확인
```

### 1.3 노드 역할 (M1: 프라이머리 / 레플리카)

M1은 샤드당 프라이머리-레플리카 토폴로지를 사용합니다:
- **프라이머리**: 읽기/쓰기 수용; WAL 영속화 후 커밋 확인.
- **레플리카**: WAL 청크 수신; 승격 전 읽기 전용.
- **승격**: 펜싱 기반 페일오버 (fence → catch-up → switchover → unfence).

---

## 2. 모듈 목록

### 2.1 `protocol_pg` — PostgreSQL Wire 프로토콜

| 항목 | 세부사항 |
|------|----------|
| **책임** | TCP 연결 수락, PG 시작/인증 핸드셰이크, Simple/Extended Query 메시지 파싱, 응답 직렬화 |
| **입력** | 원시 TCP 바이트 스트림 |
| **출력** | 파싱된 `Statement` + 세션 컨텍스트; 직렬화된 PG 응답 프레임 |
| **핵심 타입** | `PgConnection`, `PgMessage`, `AuthMethod`, `PgSession` |
| **OSS 재사용** | `pgwire` crate (MIT) 메시지 코덱용 |

### 2.2 `sql_frontend` — 파서 / 분석기 / 바인더

| 항목 | 세부사항 |
|------|----------|
| **책임** | SQL 텍스트 → AST → 카탈로그 참조가 있는 해석된 논리 계획 |
| **핵심 타입** | `Statement`, `Expr`, `TableRef`, `ColumnRef`, `BoundStatement` |
| **OSS 재사용** | `sqlparser-rs` (Apache-2.0) |

### 2.3 `planner_router` — 쿼리 플래너 & 분산 라우터

| 항목 | 세부사항 |
|------|----------|
| **책임** | 논리 계획 → 물리 계획; 샤드 대상 결정; 필터 푸시다운 |
| **핵심 타입** | `LogicalPlan`, `PhysicalPlan`, `ShardTarget`, `PushDown` |

### 2.4 `executor` — 실행 엔진

| 항목 | 세부사항 |
|------|----------|
| **책임** | 물리 계획 연산자 실행, 결과 행 생성; 쿼리 거버너; 융합 스트리밍 집계 |
| **핵심 타입** | `Operator` trait, `SeqScan`, `IndexScan`, `Filter`, `Project`, `Sort`, `Agg`, `QueryGovernor` |
| **빠른 경로** | `exec_fused_aggregate` — WHERE+GROUP BY+집계 단일 패스 MVCC 체인 (행 복사 없음) |

### 2.5 `txn` — 트랜잭션 관리자

| 항목 | 세부사항 |
|------|----------|
| **책임** | 트랜잭션 시작/커밋/중단; MVCC 타임스탬프 할당; OCC 검증 (SI); 빠른/느린 경로 커밋 |
| **핵심 타입** | `TxnManager`, `TxnHandle`, `TxnId`, `Timestamp`, `IsolationLevel` |

### 2.6 `storage` — 인메모리 스토리지 엔진

| 항목 | 세부사항 |
|------|----------|
| **책임** | 튜플과 MVCC 버전의 메모리 내 저장; 인덱스 유지; WAL 쓰기+복구; MVCC GC |
| **핵심 타입** | `StorageEngine`, `MemTable`, `VersionChain`, `SecondaryIndex`, `WalWriter`, `GcRunner` |
| **MVCC 빠른 경로** | `with_visible_data` — 클로저 기반 제로 카피 행 접근; `scan_top_k_by_pk` — 유계 힙 top-K |

### 2.7 `raft` — 합의 (스텁——프로덕션 경로 아님)

> **⚠️ 이 crate는 단일 노드 스텁입니다. 프로덕션 데이터 경로에서 사용되지 않습니다.**

### 2.8 `cluster` — 클러스터, 복제, 분산 실행

| 항목 | 세부사항 |
|------|----------|
| **책임** | 샤드 맵; WAL 기반 프라이머리-레플리카 복제; 펜싱 기반 승격/페일오버; scatter/gather 분산 실행 |
| **핵심 타입** | `ShardMap`, `WalChunk`, `ReplicationTransport`, `DistributedQueryEngine` |

### 2.9 `observability` — 메트릭 / 트레이싱 / 로깅

Prometheus 메트릭, OpenTelemetry 트레이싱, 구조화된 로깅.

### 2.10 `common` — 공유 인프라

공유 타입, 에러 계층, 설정, PG 타입 시스템, datum 표현, RBAC.

---

## 3. MVP 범위

### 3.1 지원

| 카테고리 | 범위 |
|----------|------|
| **프로토콜** | PG wire Simple Query + Extended Query; trust 인증 |
| **SQL DDL** | `CREATE TABLE`, `DROP TABLE`, `TRUNCATE`, `ALTER TABLE`, `CREATE INDEX`, `CREATE VIEW` |
| **SQL DML** | `INSERT`, `UPDATE`, `DELETE` (`RETURNING` 지원), `SELECT`, `JOIN`, 서브쿼리, CTE, 재귀 CTE, 윈도우 함수, 260+ 스칼라 함수 |
| **타입** | `INT`, `BIGINT`, `TEXT`, `BOOL`, `FLOAT8`/`NUMERIC`/`DECIMAL`, `TIMESTAMP`, `JSONB`, `ARRAY`, `DATE` |
| **트랜잭션** | `BEGIN`, `COMMIT`, `ROLLBACK`; Read Committed 격리 수준 |
| **스토리지** | 인메모리 행 저장소; PK 해시 인덱스; BTree 세컨더리 인덱스 |
| **영속화** | WAL (세그먼트 64MB, 그룹 커밋); 크래시 복구 |
| **분산** | PK 해시 샤딩; gRPC WAL 복제; 단일 샤드 빠른 경로 + 크로스 샤드 2PC |

### 3.2 미지원 (M1)

- 논리적 복제, CDC
- 온라인 스키마 변경
- 멀티테넌시
- 자동 리밸런싱 (수동만 가능)

---

## 4. OSS 재사용 매트릭스

| 모듈 | 후보 | 라이선스 | 이유 |
|------|------|----------|------|
| **PG Wire** | `pgwire` 0.25+ | MIT | 완전한 PG 메시지 코덱 |
| **SQL 파서** | `sqlparser-rs` 0.50+ | Apache-2.0 | 검증된 PG 방언 |
| **비동기 런타임** | `tokio` 1.x | MIT | 사실상 표준 |
| **RPC** | `tonic` (gRPC) | MIT | 성숙, 코드 생성, 스트리밍 |
| **메트릭** | `metrics` + `metrics-exporter-prometheus` | MIT | 경량, Prometheus 네이티브 |

**포크 정책**: 포크 없음. 모든 의존성은 공개 crate 사용.

---

## 5. 트랜잭션 및 일관성 설계

### 5.2 구현됨: 빠른 경로 + 느린 경로 + OCC

- **LocalTxn (빠른 경로)**: 단일 샤드 트랜잭션, 스냅샷 격리에서 OCC 검증 커밋. 2PC 오버헤드 없음.
- **GlobalTxn (느린 경로)**: 크로스 샤드 트랜잭션, XA-2PC 사용.
- **하드 불변량 강제**: `TxnContext.validate_commit_invariants()`가 `InvariantViolation` 에러 반환.

### 5.4 MVCC GC

- **세이프포인트**: `gc_safepoint = min(min_active_ts, replica_safe_ts) - 1`
- **WAL 인식**: 미커밋 또는 중단된 버전 미회수
- **복제 안전**: 레플리카 적용 타임스탬프 존중

### 5.5 복제 — gRPC WAL 전송

```
프라이머리 ──WAL 청크──▶ 레플리카
         (gRPC 스트림, tonic, SubscribeWal RPC)
```

> **⚠️ 하드 바운더리**: `falcon_raft` crate는 단일 노드 스텁입니다. WAL 전송 레플리카는 Raft follower가 **아닙니다**.

---

## 6. 저장소 구조 및 핵심 Trait

```
falcon/
├── Cargo.toml                  (워크스페이스 루트)
├── crates/
│   ├── falcon_common/           (공유 타입, 에러, 설정, RBAC)
│   ├── falcon_storage/          (테이블, LSM, 인덱스, WAL, GC)
│   ├── falcon_txn/              (트랜잭션 관리, MVCC, OCC)
│   ├── falcon_sql_frontend/     (파서, 바인더, 분석기)
│   ├── falcon_planner/          (논리+물리 계획)
│   ├── falcon_executor/         (연산자 실행, 식 평가)
│   ├── falcon_protocol_pg/      (PG wire 프로토콜)
│   ├── falcon_cluster/          (복제, 페일오버, scatter/gather)
│   ├── falcon_observability/    (메트릭, 트레이싱, 로깅)
│   ├── falcon_server/           (메인 바이너리 + 통합 테스트)
│   └── falcon_bench/            (YCSB 벤치마크)
├── clients/
│   └── falcondb-jdbc/           (Java JDBC 드라이버)
└── scripts/                    (데모, 페일오버, 벤치마크)
```

---

## 7. 성능 목표

| 메트릭 | 목표 |
|--------|------|
| 포인트 읽기 QPS | ≥ 200K |
| 포인트 읽기 P50 / P99 | < 0.2ms / < 1ms |
| 포인트 쓰기 QPS | ≥ 100K |
| 혼합 (50/50) QPS | ≥ 120K |
| 쓰기 QPS (3노드 클러스터) | ≥ 50K |

---

## 8. 테스트 및 마일스톤

### 8.2 테스트 수 (3,654 테스트)

| Crate | 테스트 수 |
|-------|----------|
| `falcon_cluster` | 1,057 |
| `falcon_storage` | 776 |
| `falcon_server` (통합) | 421 |
| `falcon_common` | 254 |
| `falcon_protocol_pg` | 240 |
| `falcon_cli` | 201 |
| `falcon_executor` | 192 |
| `falcon_sql_frontend` | 149 |
| `falcon_txn` | 103 |
| `falcon_planner` | 89 |
| **합계** | **3,654** (통과) |

### 8.4 마일스톤

| 단계 | 범위 | 상태 |
|------|------|------|
| **M1** | 안정 OLTP, 빠른/느린 경로 트랜잭션, WAL 복제 | **완료** |
| **M2** | gRPC WAL 스트리밍, 멀티 노드 배포 | 완료 |
| **M3** | 프로덕션 강화: 읽기 전용 레플리카, 정상 종료 | **완료** |
| **M4** | 쿼리 최적화, EXPLAIN ANALYZE, 슬로우 로그 | **완료** |
| **M5** | information_schema, 뷰, 쿼리 플랜 캐시 | **완료** |
| **M6** | JSONB, 상관 서브쿼리, FK CASCADE, 재귀 CTE | **완료** |
| **M7** | 시퀀스 함수, 윈도우 함수 | **완료** |
| **M8** | DATE 타입, COPY 명령 | **완료** |

---

## 9. 확장 스칼라 함수

> **[docs/extended_scalar_functions.md](docs/extended_scalar_functions.md)**로 이동됨. 260+ 함수 사양 포함.
