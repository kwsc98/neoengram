//! Transport- and storage-independent NeoEngram central control plane.
//!
//! This crate contains no daemon binary, HTTP stack, execution engine, or filesystem adapter. It
//! owns the authoritative managed-Add state machine, backend-neutral storage ports, and the
//! default single-process SQLite authority adapter.

mod agent_registry;
mod control_plane;
mod error;
mod memory;
mod model;
mod ports;
mod registry_memory;
#[cfg(feature = "authority-sqlite")]
mod registry_sqlite;
#[cfg(feature = "authority-sqlite")]
mod sqlite;
mod validation;

pub use agent_registry::*;
pub use control_plane::ControlPlane;
pub use error::{CentralError, CentralErrorCode, CentralResult};
pub use memory::{
    AllowAllAuthorizer, DenyAllAuthorizer, InMemoryAssignmentOutbox, InMemoryAuditSink,
    InMemoryClock, InMemoryComponents, InMemoryIndexPublisher, InMemoryJobRepository,
    InMemoryMetadataBatchStager, InMemoryObjectCatalog,
};
pub use model::*;
pub use ports::*;
pub use registry_memory::InMemoryAgentRegistry;
#[cfg(feature = "authority-sqlite")]
pub use registry_sqlite::{
    open_sqlite_agent_registry, SqliteAgentRegistry, SqliteAgentRegistryConfig,
};
#[cfg(feature = "authority-sqlite")]
pub use sqlite::{open_sqlite_authority, SqliteAuthority, SqliteAuthorityConfig};
