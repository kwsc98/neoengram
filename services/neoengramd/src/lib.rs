//! Transport- and storage-independent NeoEngram central control plane.
//!
//! This crate deliberately contains no daemon binary, HTTP stack, database driver, execution
//! engine, or filesystem adapter. It owns the authoritative managed-Add state machine and ports
//! that production infrastructure can implement later.

mod control_plane;
mod error;
mod memory;
mod model;
mod ports;
mod validation;

pub use control_plane::ControlPlane;
pub use error::{CentralError, CentralErrorCode, CentralResult};
pub use memory::{
    AllowAllAuthorizer, DenyAllAuthorizer, InMemoryAssignmentOutbox, InMemoryAuditSink,
    InMemoryClock, InMemoryComponents, InMemoryIndexPublisher, InMemoryJobRepository,
    InMemoryMetadataBatchStager, InMemoryObjectCatalog,
};
pub use model::*;
pub use ports::*;
