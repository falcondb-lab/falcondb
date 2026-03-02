# OpenTelemetry Integration

FalconDB supports exporting traces, metrics, and logs via the
[OpenTelemetry](https://opentelemetry.io/) protocol (OTLP) for end-to-end
distributed observability.

## Configuration

Add an `[observability.otlp]` section to `falcon.toml`:

```toml
[observability.otlp]
# Enable OTLP export (default: false)
enabled = true

# OTLP endpoint (gRPC). Use "http://..." for HTTP/protobuf.
endpoint = "http://localhost:4317"

# Protocol: "grpc" (default) or "http/protobuf"
protocol = "grpc"

# Export interval for metrics (milliseconds)
export_interval_ms = 15000

# Resource attributes attached to every span/metric
[observability.otlp.resource]
"service.name"       = "falcondb"
"service.version"    = "1.2.0"
"deployment.environment" = "production"

# Optional headers (e.g. for auth tokens)
[observability.otlp.headers]
# "Authorization" = "Bearer <token>"
```

## Exported Signals

### Traces

FalconDB creates spans for the following operations:

| Span Name | Attributes | Description |
|-----------|-----------|-------------|
| `falcon.query` | `db.statement`, `db.user`, `db.name`, `falcon.shard_id` | Top-level query execution |
| `falcon.parse` | `db.statement` | SQL parsing |
| `falcon.plan` | `falcon.plan_type` | Query planning |
| `falcon.execute` | `falcon.rows_affected` | Execution engine |
| `falcon.commit` | `falcon.txn_id`, `falcon.commit_lsn` | Transaction commit (WAL flush) |
| `falcon.replication.send` | `falcon.lsn`, `falcon.replica_id` | WAL segment shipped to replica |
| `falcon.replication.apply` | `falcon.lsn` | WAL applied on replica |
| `falcon.2pc.prepare` | `falcon.txn_id`, `falcon.shards` | Two-phase commit prepare |
| `falcon.2pc.commit` | `falcon.txn_id` | Two-phase commit finalize |

Trace context is propagated via the `traceparent` / `tracestate` headers in the
native FalconDB wire protocol, and via the `options` startup message parameter
for PG-compatible connections.

### Metrics

All Prometheus metrics (see [observability.md](observability.md)) are also
exported via OTLP when enabled. Key metric families:

- `falcon_txn_*` — transaction counters, latency histograms
- `falcon_wal_*` — WAL write/flush metrics
- `falcon_gc_*` — MVCC garbage collection
- `falcon_replication_*` — replication lag, bytes shipped
- `falcon_query_*` — query latency, rows returned

### Logs

Structured log records (JSON) are exported as OTLP LogRecords when
`observability.otlp.enabled = true`. Each log carries:

- `request_id`, `session_id`, `query_id`, `txn_id`, `shard_id`
- `severity` mapped to OTLP SeverityNumber
- `trace_id` and `span_id` for correlation with active traces

## Collector Setup

### Jaeger

```yaml
# docker-compose.yml snippet
services:
  jaeger:
    image: jaegertracing/all-in-one:1.53
    ports:
      - "4317:4317"   # OTLP gRPC
      - "16686:16686" # Jaeger UI
    environment:
      COLLECTOR_OTLP_ENABLED: "true"
```

### Grafana Tempo

```yaml
services:
  tempo:
    image: grafana/tempo:2.3.1
    command: ["-config.file=/etc/tempo.yaml"]
    ports:
      - "4317:4317"
```

### OpenTelemetry Collector

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317

exporters:
  otlp/jaeger:
    endpoint: jaeger:4317
    tls:
      insecure: true
  prometheus:
    endpoint: 0.0.0.0:8889

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [otlp/jaeger]
    metrics:
      receivers: [otlp]
      exporters: [prometheus]
```

## Kubernetes / Helm

When using the Helm chart, set OTLP configuration via `values.yaml`:

```yaml
falcondb:
  extraConfig: |
    [observability.otlp]
    enabled = true
    endpoint = "http://otel-collector.monitoring:4317"
    protocol = "grpc"
    export_interval_ms = 15000
    [observability.otlp.resource]
    "service.name" = "falcondb"
```

## Disabling

Set `enabled = false` (or omit the section entirely). When disabled, FalconDB
uses only local structured logging and Prometheus `/metrics` scraping — no
external OTLP connections are made.

## Compatibility

| Collector | Protocol | Tested |
|-----------|----------|--------|
| Jaeger 1.50+ | gRPC | Yes |
| Grafana Tempo 2.x | gRPC | Yes |
| OTel Collector 0.90+ | gRPC, HTTP | Yes |
| Datadog Agent 7.x | gRPC | Yes |
| Honeycomb | HTTP | Planned |
