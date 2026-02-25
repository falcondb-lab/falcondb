# ============================================================================
# FalconDB — Multi-stage Docker build
# ============================================================================
#
# Build:
#   docker build -t falcondb:latest .
#
# Run (foreground):
#   docker run -p 5443:5443 -p 8080:8080 falcondb:latest
#
# Run (persistent data):
#   docker run -p 5443:5443 -p 8080:8080 \
#     -v falcondb-data:/var/lib/falcondb \
#     falcondb:latest
#
# Connect:
#   psql -h 127.0.0.1 -p 5443 -U falcon

# ── Stage 1: Builder ────────────────────────────────────────────────────────
FROM rust:1.75-bookworm AS builder

WORKDIR /build

# Cache dependency compilation: copy only manifests first
COPY Cargo.toml Cargo.lock ./
COPY crates/falcon_common/Cargo.toml         crates/falcon_common/Cargo.toml
COPY crates/falcon_storage/Cargo.toml        crates/falcon_storage/Cargo.toml
COPY crates/falcon_txn/Cargo.toml            crates/falcon_txn/Cargo.toml
COPY crates/falcon_sql_frontend/Cargo.toml   crates/falcon_sql_frontend/Cargo.toml
COPY crates/falcon_planner/Cargo.toml        crates/falcon_planner/Cargo.toml
COPY crates/falcon_executor/Cargo.toml       crates/falcon_executor/Cargo.toml
COPY crates/falcon_protocol_pg/Cargo.toml    crates/falcon_protocol_pg/Cargo.toml
COPY crates/falcon_protocol_native/Cargo.toml crates/falcon_protocol_native/Cargo.toml
COPY crates/falcon_native_server/Cargo.toml  crates/falcon_native_server/Cargo.toml
COPY crates/falcon_raft/Cargo.toml           crates/falcon_raft/Cargo.toml
COPY crates/falcon_proto/Cargo.toml          crates/falcon_proto/Cargo.toml
COPY crates/falcon_cluster/Cargo.toml        crates/falcon_cluster/Cargo.toml
COPY crates/falcon_observability/Cargo.toml  crates/falcon_observability/Cargo.toml
COPY crates/falcon_server/Cargo.toml         crates/falcon_server/Cargo.toml
COPY crates/falcon_bench/Cargo.toml          crates/falcon_bench/Cargo.toml
COPY crates/falcon_cli/Cargo.toml            crates/falcon_cli/Cargo.toml
COPY crates/falcon_segment_codec/Cargo.toml  crates/falcon_segment_codec/Cargo.toml

# Create stub lib.rs for each crate so cargo can resolve the dep graph
RUN find crates -name Cargo.toml -exec sh -c \
    'dir=$(dirname "$1"); mkdir -p "$dir/src" && echo "" > "$dir/src/lib.rs"' _ {} \;

# Also need a stub main.rs for binary crates
RUN mkdir -p crates/falcon_server/src && echo "fn main() {}" > crates/falcon_server/src/main.rs
RUN mkdir -p crates/falcon_cli/src     && echo "fn main() {}" > crates/falcon_cli/src/main.rs
RUN mkdir -p crates/falcon_bench/src   && echo "fn main() {}" > crates/falcon_bench/src/main.rs

# Proto build script stub
COPY crates/falcon_proto/build.rs          crates/falcon_proto/build.rs
COPY crates/falcon_proto/proto             crates/falcon_proto/proto
COPY crates/falcon_cluster/build.rs        crates/falcon_cluster/build.rs

# Pre-build dependencies (cached layer)
RUN cargo build --release --workspace 2>/dev/null || true

# Copy full source and build for real
COPY . .
RUN cargo build --release -p falcon_server -p falcon_cli

# ── Stage 2: Runtime ────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create falcondb user
RUN groupadd -r falcondb && useradd -r -g falcondb -m -d /home/falcondb falcondb

# Directory layout
RUN mkdir -p /etc/falcondb /var/lib/falcondb /var/log/falcondb \
    && chown -R falcondb:falcondb /var/lib/falcondb /var/log/falcondb

# Copy binaries
COPY --from=builder /build/target/release/falcon_server /usr/local/bin/falcon
COPY --from=builder /build/target/release/falcon_cli    /usr/local/bin/falcon-cli

# Copy default config
COPY docker/falcon.toml /etc/falcondb/falcon.toml

# Expose ports: PG wire protocol + admin/health
EXPOSE 5443 8080

# Data volume
VOLUME ["/var/lib/falcondb"]

USER falcondb
WORKDIR /home/falcondb

ENTRYPOINT ["falcon"]
CMD ["--config", "/etc/falcondb/falcon.toml"]
