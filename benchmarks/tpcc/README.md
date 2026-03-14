# TPC-C Benchmark for FalconDB

## Overview

Industry-standard **TPC-C** OLTP benchmark adapted for FalconDB and PostgreSQL
comparison. Implements all 5 TPC-C transaction profiles using pgbench custom
scripts against the standard 9-table schema.

> **Disclaimer**: This is a **TPC-C-like** benchmark for internal evaluation.
> It is NOT an officially audited TPC-C result and must not be presented as such.
> See [tpc.org](https://www.tpc.org/) for official TPC-C rules.

## TPC-C Transaction Mix

| Transaction | Weight | Type | Description |
|-------------|:------:|------|-------------|
| New-Order | 45% | Read-Write | Most complex: allocate order ID, read warehouse/district/customer, insert order + order_lines, update stock |
| Payment | 43% | Read-Write | Update warehouse/district YTD, customer balance, insert history |
| Order-Status | 4% | Read-Only | Find customer's last order + order lines |
| Delivery | 4% | Read-Write | Batch: find oldest new_order, update carrier, sum amounts, update customer |
| Stock-Level | 4% | Read-Only | Range scan + join: count low-stock items from recent orders |

## Quick Start

```bash
# Run against FalconDB (quick mode: 10s, 4 threads)
./benchmarks/tpcc/run_tpcc.sh falcondb --quick

# Run against PostgreSQL (full: 60s, 8 threads)
./benchmarks/tpcc/run_tpcc.sh postgresql --threads 8 --duration 60

# Compare both
./benchmarks/tpcc/run_tpcc.sh falcondb --threads 8 --duration 60
./benchmarks/tpcc/run_tpcc.sh postgresql --threads 8 --duration 60
diff benchmarks/results/tpcc_falcondb_*/REPORT.md benchmarks/results/tpcc_postgresql_*/REPORT.md
```

## Schema

9 tables per TPC-C Standard v5.11 §1.2:

| Table | Rows (1 warehouse) | Description |
|-------|-------------------:|-------------|
| `item` | 100,000 | Product catalog (static) |
| `warehouse` | 1 | Warehouse master |
| `district` | 10 | Districts per warehouse |
| `customer` | 30,000 | Customers (3,000 per district) |
| `history` | 30,000 | Payment history |
| `orders` | 30,000 | Order headers |
| `new_order` | 9,000 | Pending delivery queue |
| `order_line` | 300,000 | Order detail lines |
| `stock` | 100,000 | Stock levels per item per warehouse |

**Total**: ~600K rows for scale factor 1 (1 warehouse).

## Directory Structure

```
tpcc/
├── README.md           # This file
├── schema.sql          # 9-table TPC-C schema
├── load.sql            # Data generation (scale factor 1)
├── new_order.sql       # New-Order transaction (pgbench script)
├── payment.sql         # Payment transaction
├── order_status.sql    # Order-Status transaction
├── delivery.sql        # Delivery transaction
├── stock_level.sql     # Stock-Level transaction
└── run_tpcc.sh         # One-command benchmark runner
```

## Primary Metric: tpmC

The primary TPC-C metric is **tpmC** (transactions per minute, New-Order):

```
tpmC = New-Order TPS × 60
```

Higher tpmC = better OLTP throughput under realistic mixed workload.

## Configuration Parity

Both engines configured for fair comparison:

| Parameter | FalconDB | PostgreSQL |
|-----------|----------|------------|
| WAL sync | fdatasync | fdatasync |
| Memory | 2 GB | shared_buffers=2GB |
| Connections | 100+ | 100 |
| Auth | trust | trust |

## What TPC-C Tests

| Capability | TPC-C Coverage |
|-----------|----------------|
| Point lookups (PK) | New-Order (item, stock), Payment (customer) |
| Multi-row updates | Payment (warehouse + district + customer), Delivery |
| Insert throughput | New-Order (orders, order_line, new_order), Payment (history) |
| Delete operations | Delivery (new_order) |
| Range scan + join | Stock-Level (order_line JOIN stock) |
| Transaction isolation | All 5 profiles run concurrently |
| Lock contention | Payment (warehouse YTD is a hot row) |
| Read-only queries | Order-Status, Stock-Level |

## Scaling

For multi-warehouse runs, modify `load.sql` to generate data for W > 1:
- Each additional warehouse adds ~500K rows
- Linear scaling expected for well-partitioned workloads
- Cross-warehouse transactions test distributed coordination

## License

Apache-2.0
