//! Generated protobuf types and tonic gRPC stubs for Cedar replication.
//!
//! This crate owns all `.proto` files and uses `tonic-build` to generate
//! Rust code into `$OUT_DIR` at build time. No generated `.rs` files are
//! checked into version control.

#![allow(clippy::all, clippy::pedantic)]

pub mod cedar_replication {
    tonic::include_proto!("cedar.replication");
}
