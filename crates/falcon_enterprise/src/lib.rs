//! Enterprise features extracted from `falcon_cluster`.
//!
//! Contains:
//! - [`control_plane`]: Control plane HA, metadata store, node registry, shard placement
//! - [`enterprise_security`]: AuthN/AuthZ, TLS rotation, backup/restore, audit log
//! - [`enterprise_ops`]: Auto rebalance, capacity planning, SLO engine, incident timeline, admin console
//!
//! # Integration Note
//!
//! This crate is **not yet wired into `falcon_server`**. The modules define
//! standalone managers (`AuthnManager`, `EnterpriseRbac`, `AutoRebalancer`,
//! `ControllerHAGroup`, etc.) that must be instantiated and connected to the
//! server's request pipeline to take effect.
//!
//! To activate enterprise features in production:
//!
//! 1. **AuthN/AuthZ** — Create an `AuthnManager` at server startup and call
//!    `authenticate()` in the PG wire protocol authentication handler
//!    (`falcon_protocol_pg`) instead of the current `falcon_common::security`
//!    plaintext path.
//! 2. **Control Plane** — Instantiate `ControllerHAGroup` and connect leader
//!    election to `falcon_raft::RaftConsensus`.
//! 3. **Auto Rebalance** — Spawn `AutoRebalancer` on the leader node's admin
//!    API and feed it shard-load metrics from the storage engine.
//! 4. **TLS Rotation** — Wire `CertificateManager` into the Tokio TLS
//!    acceptor for hot-reload on certificate expiry.

pub mod control_plane;
pub mod enterprise_ops;
pub mod enterprise_security;
