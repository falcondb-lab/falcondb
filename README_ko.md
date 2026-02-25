# FalconDB

<p align="center">
  <img src="assets/falcondb-logo.png" alt="FalconDB Logo" width="220" />
</p>

<h1 align="center">FalconDB</h1>

<p align="center">
  <strong>PG 호환 · 분산형 · 메모리 우선 · 결정론적 트랜잭션 의미론</strong>
</p>

<p align="center">
  <a href="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml">
    <img src="https://github.com/falcondb-lab/falcondb/actions/workflows/ci.yml/badge.svg" alt="CI" />
  </a>
  <img src="https://img.shields.io/badge/version-1.2.0-blue" alt="버전" />
  <img src="https://img.shields.io/badge/MSRV-1.75-blue" alt="MSRV" />
  <img src="https://img.shields.io/badge/license-Apache--2.0-green" alt="라이선스" />
</p>

> English | [简体中文](README_zh.md) | [Español](README_es.md) | [Français](README_fr.md) | **한국어** | [日本語](README_ja.md) | [Deutsch](README_de.md) | [العربية](README_ar.md) | [Italiano](README_it.md) | [Bahasa Melayu](README_ms.md) | [Português](README_pt.md)

> FalconDB는 결정론적 트랜잭션 의미론을 갖춘 **PG 호환, 분산형, 메모리 우선 OLTP 데이터베이스**입니다. PostgreSQL, VoltDB, SingleStore와 벤치마크 비교 — **[벤치마크 매트릭스](benchmarks/README.md)** 참조.
>
> - ✅ **낮은 지연 시간** — 단일 샤드 빠른 경로 커밋은 2PC를 완전히 우회
> - ✅ **스캔 성능** — 융합 스트리밍 집계, 제로 카피 MVCC 반복, 1M 행에서 PG와 근접한 성능
> - ✅ **안정성** — p99 한계값 보장, 중단율 < 1%, 재현 가능한 벤치마크
> - ✅ **증명 가능한 일관성** — 스냅샷 격리 하의 MVCC/OCC, CI 검증 ACID
> - ✅ **운영성** — 50개 이상의 SHOW 명령, Prometheus 메트릭, 페일오버 CI 게이트
> - ✅ **결정론** — 강화된 상태 머신, 한계 in-doubt, 멱등성 재시도
> - ❌ HTAP 아님 — 분석 워크로드 없음
> - ❌ 완전한 PG 아님 — [미지원 목록 참조](#not-supported)

### 지원 플랫폼

| 플랫폼 | 빌드 | 테스트 | 상태 |
|--------|:----:|:------:|------|
| **Linux** (x86_64, Ubuntu 22.04+) | ✅ | ✅ | 기본 CI 대상 |
| **Windows** (x86_64, MSVC) | ✅ | ✅ | CI 대상 |
| **macOS** (x86_64 / aarch64) | ✅ | ✅ | 커뮤니티 테스트 |

**MSRV**: Rust **1.75** (`Cargo.toml`의 `rust-version = "1.75"`)

### PG 프로토콜 호환성

| 기능 | 상태 | 비고 |
|------|:----:|------|
| 단순 쿼리 프로토콜 | ✅ | 단일 + 다중 구문 |
| 확장 쿼리 (Parse/Bind/Execute) | ✅ | 준비된 구문 + 포털 |
| Auth: Trust | ✅ | 모든 사용자 허용 |
| Auth: MD5 | ✅ | PG auth 타입 5 |
| Auth: SCRAM-SHA-256 | ✅ | PG 10+ 호환 |
| Auth: 비밀번호 (평문) | ✅ | PG auth 타입 3 |
| TLS/SSL | ✅ | 설정 시 SSLRequest → 업그레이드 |
| COPY IN/OUT | ✅ | 텍스트 및 CSV 형식 |
| `psql` 12+ | ✅ | 완전 테스트 완료 |
| `pgbench` (init + run) | ✅ | 내장 스크립트 작동 |
| JDBC (pgjdbc 42.x) | ✅ | 42.7+ 테스트 |
| 취소 요청 | ✅ | AtomicBool 폴링, 50ms 지연 |
| LISTEN/NOTIFY | ✅ | 인메모리 브로드캐스트 허브 |
| 논리 복제 프로토콜 | ✅ | IDENTIFY_SYSTEM, CREATE/DROP_REPLICATION_SLOT, START_REPLICATION |

### SQL 지원 범위

| 카테고리 | 지원 |
|---------|------|
| **DDL** | CREATE/DROP/ALTER TABLE, CREATE/DROP INDEX, CREATE/DROP VIEW, CREATE/DROP SEQUENCE, TRUNCATE |
| **DML** | INSERT (ON CONFLICT, RETURNING, SELECT 포함), UPDATE (FROM, RETURNING 포함), DELETE (USING, RETURNING 포함), COPY |
| **쿼리** | WHERE, ORDER BY, LIMIT/OFFSET, DISTINCT, GROUP BY/HAVING, JOIN, 서브쿼리, CTE (RECURSIVE 포함), UNION/INTERSECT/EXCEPT, 윈도우 함수 |
| **집계** | COUNT, SUM, AVG, MIN, MAX, STRING_AGG, BOOL_AND/OR, ARRAY_AGG |
| **타입** | INT, BIGINT, FLOAT8, DECIMAL/NUMERIC, TEXT, BOOLEAN, TIMESTAMP, DATE, JSONB, ARRAY, SERIAL/BIGSERIAL |
| **트랜잭션** | BEGIN/COMMIT/ROLLBACK, READ ONLY/READ WRITE, 트랜잭션별 타임아웃, Read Committed, Snapshot Isolation |
| **함수** | 500개 이상 스칼라 함수 (문자열, 수학, 날짜/시간, 암호화, JSON, 배열) |
| **관찰성** | SHOW falcon.*, EXPLAIN, EXPLAIN ANALYZE, CHECKPOINT, ANALYZE TABLE |

### <a id="not-supported"></a>미지원 기능 (v1.2)

| 기능 | 오류 코드 | 오류 메시지 |
|------|:---------:|------------|
| 저장 프로시저 / PL/pgSQL | `0A000` | `stored procedures are not supported` |
| 트리거 | `0A000` | `triggers are not supported` |
| 구체화된 뷰 | `0A000` | `materialized views are not supported` |
| 외부 데이터 래퍼 (FDW) | `0A000` | `foreign data wrappers are not supported` |
| 전문 검색 | `0A000` | `full-text search is not supported` |
| 온라인 DDL | `0A000` | `concurrent index operations are not supported` |
| HTAP / ColumnStore | — | 컴파일 타임에 비활성화 |

---

## 1. 빌드

```bash
# 모든 크레이트 빌드 (debug)
cargo build --workspace

# Release 빌드
cargo build --release --workspace

# 테스트 실행 (2,643개 이상)
cargo test --workspace

# 린트
cargo clippy --workspace
```

---

## 2. 빠른 시작

```bash
# 인메모리 모드 (WAL 없음, 가장 빠름)
cargo run -p falcon_server -- --no-wal

# WAL 영속성 포함
cargo run -p falcon_server -- --data-dir ./falcon_data

# psql로 연결
psql -h 127.0.0.1 -p 5433 -U falcon
```

---

## 3. 스토리지 엔진

| 엔진 | 스토리지 | 영속성 | 최적 용도 |
|------|---------|--------|---------|
| **Rowstore** (기본값) | 인메모리 (MVCC 버전 체인) | WAL만 | 저지연 OLTP |
| **LSM** | 디스크 (LSM-Tree + WAL) | 완전한 디스크 영속성 | 대용량 데이터셋 |

---

## 4. 트랜잭션 모델

- **LocalTxn (빠른 경로)**: 단일 샤드 트랜잭션은 Snapshot Isolation 하에 OCC로 커밋 — 2PC 오버헤드 없음.
- **GlobalTxn (느린 경로)**: 크로스 샤드 트랜잭션은 prepare/commit과 함께 XA-2PC 사용.

---

## 5. 아키텍처

```
┌─────────────────────────────────────────────────────────┐
│  PG Wire 프로토콜 (TCP)  │  네이티브 프로토콜 (TCP/TLS) │
├──────────────────────────┴─────────────────────────────┤
│         SQL 프론트엔드 (sqlparser-rs → Binder)          │
├────────────────────────────────────────────────────────┤
│         플래너 / 라우터                                  │
├────────────────────────────────────────────────────────┤
│         실행기 (행 단위 + 융합 스트리밍 집계)            │
├──────────────────┬─────────────────────────────────────┤
│   트랜잭션 관리자 │   스토리지 엔진                     │
│   (MVCC, OCC)    │   (MemTable + LSM + WAL + GC)       │
├──────────────────┴─────────────────────────────────────┤
│  클러스터 (ShardMap, 복제, 페일오버, Epoch)             │
└────────────────────────────────────────────────────────┘
```

---

## 6. 메트릭 및 관찰성

```sql
-- 트랜잭션 통계
SHOW falcon.txn_stats;

-- GC 통계
SHOW falcon.gc_stats;

-- 복제 통계
SHOW falcon.replication_stats;
```

---

## 라이선스

Apache-2.0
