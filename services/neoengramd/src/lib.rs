//! Transport- and storage-independent NeoEngram central control plane.
//!
//! This crate contains no daemon binary, HTTP stack, execution engine, or filesystem adapter. It
//! owns the authoritative managed-Add state machine, backend-neutral storage ports, and the
//! default single-process SQLite authority adapter.

mod agent_registry;
mod catalog;
mod catalog_memory;
mod control_plane;
#[cfg(feature = "authority-sqlite")]
mod datasource;
mod error;
#[cfg(feature = "authority-sqlite")]
mod mapper;
mod memory;
mod model;
mod ports;
mod precommit;
mod registry_memory;
mod validation;

pub use agent_registry::*;
pub use catalog::*;
pub use catalog_memory::InMemoryControlCatalog;
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
    InMemoryMetadataBatchStager, InMemoryObjectCatalog, InMemoryPreCommitRepository,
};
pub use model::*;
pub use ports::*;
pub use precommit::*;
pub use registry_memory::InMemoryAgentRegistry;

fn mapper_recovery_predicate(job: &JobRecord, now: neoengram_protocol::UnixMillis) -> bool {
    use neoengram_protocol::JobState;

    if matches!(
        job.operation,
        JobOperation::WorkspaceMaterialize | JobOperation::SnapshotMount
    ) {
        return matches!(
            job.state,
            JobState::Queued | JobState::Assigned | JobState::Accepted | JobState::Running
        );
    }
    matches!(
        job.state,
        JobState::Queued | JobState::Prepared | JobState::Publishing
    ) || (job.spec.deadline_unix_ms.get() <= now.get()
        && matches!(
            job.state,
            JobState::Assigned | JobState::Accepted | JobState::Running | JobState::CancelRequested
        ))
}
