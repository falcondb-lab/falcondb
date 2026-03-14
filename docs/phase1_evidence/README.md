# Phase 1 Evidence Directory

存放 Phase 1 验收所需的全部性能证据文件。

## 目录结构

`
phase1_evidence/
├── README.md                        ← 本文件
├── flamegraphs/
│   ├── flamegraph_point_select_32t.svg   ← 32c point select 热路径
│   ├── flamegraph_insert_32t.svg         ← 32c INSERT 热路径
│   ├── flamegraph_mixed_32t.svg          ← 32c mixed OLTP
│   ├── flamegraph_wal_commit.svg         ← WAL commit 路径
│   └── flamegraph_mvcc_read.svg          ← VersionChain read 路径
├── bench_e2e_final.json             ← 全部 E2E bench 结果
├── bench_scalability_curve.json     ← 1→64 线程扩展曲线
├── bench_regression_report.md       ← vs bootstrap baseline delta
├── pgbench_workload_c.txt           ← pgbench 原始输出 (16c/32c/64c)
├── heaptrack_insert_1t.txt          ← 1c INSERT heaptrack 报告
├── heaptrack_select_1t.txt          ← 1c SELECT heaptrack 报告
├── dhat_insert_1t.json              ← dhat alloc 报告
├── perf_stat_insert_32t.txt         ← perf stat INSERT
├── perf_stat_select_32t.txt         ← perf stat SELECT
├── crash_recovery_report.md         ← crash recovery 5 场景测试
├── stability_report.md              ← 72h 稳定压测报告
├── regression_report.md             ← 回归测试报告
└── phase1_acceptance_signoff.md     ← 最终验收签字（Week 12 填写）
`

## 快速生成命令

`ash
# E2E bench（bench 机，release 模式）
cargo run --release -p falcon_bench -- \
    --threads 32 --read-pct 70 --ops 0 \
    --export json > docs/phase1_evidence/bench_e2e_final.json

# pgbench（FalconDB 必须已启动在 localhost:5432）
pgbench -c 16 -j 16 -T 60 -r -M simple > docs/phase1_evidence/pgbench_workload_c.txt
pgbench -c 32 -j 32 -T 60 -r -M simple >> docs/phase1_evidence/pgbench_workload_c.txt
pgbench -c 64 -j 64 -T 60 -r -M simple >> docs/phase1_evidence/pgbench_workload_c.txt

# flamegraph（需 cargo-flamegraph + Linux perf）
cargo flamegraph -p falcon_bench -- --threads 32 --read-pct 100 --ops 200000
mv flamegraph.svg docs/phase1_evidence/flamegraphs/flamegraph_point_select_32t.svg

# heaptrack（需 heaptrack 已安装）
heaptrack cargo run --release -p falcon_bench -- --threads 1 --ops 100000 --read-pct 0
heaptrack_print heaptrack.*.gz -s > docs/phase1_evidence/heaptrack_insert_1t.txt

# 回归对比
python3 benchmarks/scripts/compare.py \
    benchmarks/baselines/main.json \
    docs/phase1_evidence/bench_e2e_final.json \
    > docs/phase1_evidence/bench_regression_report.md
`

## 验收判定规则

见 docs/phase1_acceptance.md 第六节 KPI 表。

★★★★★ 必过项：K01 K02 K03 K05 K11 K12
其余项 ≥70% 达标 → 有条件通过
任意 ★★★★★ 未达标 → 不通过
