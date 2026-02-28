//! Enterprise features extracted from `falcon_cluster`.
//!
//! Contains:
//! - [`control_plane`]: Control plane HA, metadata store, node registry, shard placement
//! - [`enterprise_security`]: AuthN/AuthZ, TLS rotation, backup/restore, audit log
//! - [`enterprise_ops`]: Auto rebalance, capacity planning, SLO engine, incident timeline, admin console

pub mod control_plane;
pub mod enterprise_ops;
pub mod enterprise_security;
