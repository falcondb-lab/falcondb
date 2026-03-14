# Phase 1 验收签字单

> 填写日期：____________  
> 验收人：____________  
> 整改负责人：____________

## 实际达成 KPI

| KPI | 目标 | 实际值 | 达标 |
|---|---|---|---|
| pgbench @16c TPS | ≥70,000 | _______ | □ |
| pgbench @64c TPS | ≥100,000 | _______ | □ |
| E2E point_select P99 @32c | ≤1,000µs | _______ | □ |
| E2E mixed_oltp TPS @32c | ≥200,000 | _______ | □ |
| commit P99 @32c | ≤800µs | _______ | □ |
| batch INSERT rows/s | ≥600,000 | _______ | □ |
| alloc/txn (INSERT 1行) | ≤8次 | _______ | □ |
| WAL group commit batch | ≥8 | _______ | □ |
| 全量测试通过率 | 100% | _______ | □ |
| 72h 压测 panic | 0 | _______ | □ |

## 判定

□ **Phase 1 完全通过**：全部 KPI 达标  
□ **Phase 1 有条件通过**：★★★★★ 项全部达标，其余 ≥70% 达标  
□ **Phase 1 不通过**：请在下方填写未达标项及原因  

## 未达标项说明

（如适用）

## 附证据清单

- [ ] bench_e2e_final.json
- [ ] pgbench_workload_c.txt  
- [ ] flamegraph_point_select_32t.svg
- [ ] flamegraph_insert_32t.svg
- [ ] heaptrack_insert_1t.txt
- [ ] crash_recovery_report.md
- [ ] stability_report.md
- [ ] regression_report.md

## 签字

技术 PM：__________________  日期：__________  

整改负责人：______________  日期：__________
