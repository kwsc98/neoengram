//! Transport- and storage-independent NeoEngram central control plane.
//!
//! This crate contains no daemon binary, HTTP stack, execution engine, or filesystem adapter. It
//! owns the authoritative managed-Add state machine, backend-neutral storage ports, and the
//! default single-process SQLite authority adapter.

mod agent_registry;
mod control_plane;
#[cfg(feature = "authority-sqlite")]
mod datasource;
mod error;
#[cfg(feature = "authority-sqlite")]
mod mapper;
mod memory;
mod model;
mod ports;
mod registry_memory;
mod validation;

pub use agent_registry::*;
pub use control_plane::ControlPlane;
pub use error::{CentralError, CentralErrorCode, CentralResult};
#[cfg(feature = "authority-sqlite")]
pub use mapper::sqlite::agent_registry::{
    open_sqlite_agent_registry, SqliteAgentRegistry, SqliteAgentRegistryConfig,
};
#[cfg(feature = "authority-sqlite")]
pub use mapper::sqlite::authority::{
    open_sqlite_authority, SqliteAuthority, SqliteAuthorityConfig,
};
pub use memory::{
    AllowAllAuthorizer, DenyAllAuthorizer, InMemoryAssignmentOutbox, InMemoryAuditSink,
    InMemoryClock, InMemoryComponents, InMemoryIndexPublisher, InMemoryJobRepository,
    InMemoryMetadataBatchStager, InMemoryObjectCatalog,
};
pub use model::*;
pub use ports::*;
pub use registry_memory::InMemoryAgentRegistry;
