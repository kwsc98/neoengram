//! Authenticated HTTP adapter for the NeoEngram central control plane.
//!
//! Transport concerns live in this crate. `neoengramd` remains the authoritative,
//! transport-independent application and persistence layer.

pub mod agent_transport;
pub mod controller;
pub mod dto;
pub mod error;
pub mod identity;
pub mod runtime;
pub mod service;

pub use agent_transport::{
    AgentApiHandler, AgentDataPlaneHandler, AgentEnrollmentHandler, AgentHttpError,
    AgentListenerConfig, AgentServerError, AgentServerHandle, RegistryAgentApiHandler,
    RegistryAgentEnrollmentHandler, RunningAgentServer,
};
pub use controller::{
    ArtifactApi, ArtifactApiClient, ArtifactApiServer, ArtifactController, JobApi, JobApiClient,
    JobApiServer, JobController, PlaygroundApi, PlaygroundApiClient, PlaygroundApiServer,
    PlaygroundController, SnapshotApi, SnapshotApiClient, SnapshotApiServer, SnapshotController,
    StorageEnrollmentApi, StorageEnrollmentApiClient, StorageEnrollmentApiServer,
    StorageEnrollmentController, StorageVolumeApi, StorageVolumeApiClient, StorageVolumeApiServer,
    StorageVolumeController, SystemApi, SystemApiClient, SystemApiServer, SystemController,
    TenantApi, TenantApiClient, TenantApiServer, TenantController,
};
pub use error::NeoEngramProblemEncoder;
pub use identity::{
    AuthenticatedIdentity, Authenticator, Permission, PrincipalContext, StaticRbacPolicy,
};
pub use runtime::{run, AppState, Config, RuntimeError};
pub use service::{
    AgentDataPlaneService, CatalogService, CoordinatorRun, EnrollmentKeyring, EnrollmentService,
    HealthService, JobCoordinator, JobService, ReadinessProbe, SystemService,
    WorkspaceCommitResult, WorkspaceCommitService,
};
