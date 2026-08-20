//! NeoEngram's single domain boundary.
//!
//! This crate owns both the environment-independent repository model and the transport-neutral
//! control contracts. It deliberately contains no filesystem, database, network, process, or
//! terminal integration. Execution adapters belong to `neoengram-runtime`.

/// Environment-independent domain models, IDs, paths, and canonical encodings.
pub mod core;

/// Versioned transport-independent wire contracts and validators.
pub mod protocol;

// Re-export both implementation namespaces at the domain root as a compact public surface.
// Internal code may use the explicit `core`/`protocol` namespaces when a boundary is important.
pub use core::*;
pub use protocol::*;
