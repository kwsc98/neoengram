//! Canonical Central process boundary.
//!
//! Authority, HTTP API, Gateway coordination, S3, Snapshot delivery, lifecycle, and identity
//! live in this one process crate.

pub mod agent_registry;
pub mod agent_transport;
mod authority_error;
pub mod catalog;
pub mod catalog_memory;
pub mod control_plane;
pub mod controller;
pub mod datasource;
pub mod dto;
pub mod error;
pub mod gateway_activation_transport;
pub mod gateway_connector;
pub mod gateway_registry;
pub mod gateway_registry_memory;
pub mod gateway_transport;
pub mod identity;
pub mod mapper;
pub mod memory;
pub mod model;
pub mod ports;
pub mod precommit;
pub mod registry_memory;
pub mod runtime;
pub mod service;
pub mod validation;

pub use agent_registry::*;
pub use agent_transport::{
    AgentApiHandler, AgentDataPlaneHandler, AgentHttpError, GatewayAgentRouteContext,
    RegistryAgentApiHandler, RoutedAgentControlChannel,
};
pub use authority_error::{CentralError, CentralErrorCode, CentralResult};
pub use catalog::*;
pub use catalog_memory::InMemoryControlCatalog;
pub use control_plane::ControlPlane;
pub use controller::*;
pub use error::NeoEngramProblemEncoder;
pub use gateway_activation_transport::*;
pub use gateway_connector::*;
pub use gateway_registry::*;
pub use gateway_registry_memory::InMemoryGatewayRegistry;
pub use gateway_transport::*;
pub use identity::*;
pub use mapper::sqlite::authority::{
    open_sqlite_authority, SqliteAuthority, SqliteAuthorityConfig,
};
pub use memory::*;
pub use model::*;
pub use ports::*;
pub use precommit::*;
pub use registry_memory::InMemoryAgentRegistry;
pub use runtime::{
    run, run_with_dependencies, AppState, Config, GatewayActivationDependencies,
    RuntimeDependencies, RuntimeError,
};
pub use service::*;

fn mapper_recovery_predicate(job: &JobRecord, now: neoengram_domain::protocol::UnixMillis) -> bool {
    use neoengram_domain::protocol::JobState;

    if matches!(
        job.operation,
        JobOperation::WorkspaceMaterialize | JobOperation::SnapshotDelivery
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

pub mod api {
    pub use crate::controller::*;
    pub use crate::dto;
}

pub mod authority {
    pub use crate::agent_registry::*;
    pub use crate::catalog::*;
    pub use crate::control_plane::ControlPlane;
    pub use crate::gateway_registry::*;
    pub use crate::model::*;
    pub use crate::ports::*;
    pub use crate::precommit::*;
    pub use crate::{CentralError, CentralErrorCode, CentralResult};
}

pub mod gateway {
    pub use crate::{
        AuthenticatedGatewayReplica, CentralGatewayControl, CentralGatewaySession,
        GatewayActivationDependencies, GatewayBootstrapTransport, GatewayBootstrapTransportError,
        GatewayConnectorConfig, GatewayConnectorError, GatewayReplicaActivationClient,
        GatewayReplicaActivationClientError, GatewaySessionError, HttpGatewayBootstrapTransport,
        RunningGatewayConnector,
    };
}

pub mod lifecycle {
    pub use crate::{ResourceLifecycleCoordinator, ResourceLifecycleReconcileRun};
}

pub mod s3 {
    pub use crate::{
        CatalogService, LocalS3SecretEnvelope, S3AgentPlacement, S3PlacementProvider,
        S3ReadRevocationPublisher, S3SecretEnvelope, S3SecretEnvelopeError,
        StorageAvailabilityProvider,
    };
}

pub mod snapshot {
    pub use crate::SnapshotController;
}
