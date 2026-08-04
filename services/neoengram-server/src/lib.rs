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
    AgentEnrollmentHandler, AgentHttpError, AgentListenerConfig, AgentServerError,
    AgentServerHandle, RegistryAgentEnrollmentHandler, RunningAgentServer,
};
pub use controller::{
    JobApi, JobApiClient, JobApiServer, JobController, StorageEnrollmentApi,
    StorageEnrollmentApiClient, StorageEnrollmentApiServer, StorageEnrollmentController, SystemApi,
    SystemApiClient, SystemApiServer, SystemController,
};
pub use error::NeoEngramProblemEncoder;
pub use identity::{
    AuthenticatedIdentity, Authenticator, Permission, PrincipalContext, StaticRbacPolicy,
};
pub use runtime::{run, AppState, Config, RuntimeError};
pub use service::{
    EnrollmentKeyring, EnrollmentService, HealthService, JobService, ReadinessProbe, SystemService,
};
