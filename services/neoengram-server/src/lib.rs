//! Authenticated HTTP adapter for the NeoEngram central control plane.
//!
//! Transport concerns live in this crate. `neoengramd` remains the authoritative,
//! transport-independent application and persistence layer.

pub mod agent_transport;
pub mod controller;
pub mod dto;
pub mod error;
pub mod gateway_activation_transport;
pub mod gateway_connector;
pub mod gateway_transport;
pub mod identity;
pub mod runtime;
pub mod service;

pub use agent_transport::{
    AgentApiHandler, AgentDataPlaneHandler, AgentEnrollmentHandler, AgentHttpError,
    AgentListenerConfig, AgentServerError, AgentServerHandle, GatewayAgentRouteContext,
    RegistryAgentApiHandler, RegistryAgentEnrollmentHandler, RoutedAgentControlChannel,
    RunningAgentServer,
};
pub use controller::{
    ArtifactApi, ArtifactApiClient, ArtifactApiServer, ArtifactController, GatewayRegistryApi,
    GatewayRegistryApiClient, GatewayRegistryApiServer, GatewayRegistryController, JobApi,
    JobApiClient, JobApiServer, JobController, PlaygroundApi, PlaygroundApiClient,
    PlaygroundApiServer, PlaygroundController, SnapshotApi, SnapshotApiClient, SnapshotApiServer,
    SnapshotController, StorageEnrollmentApi, StorageEnrollmentApiClient,
    StorageEnrollmentApiServer, StorageEnrollmentController, StorageVolumeApi,
    StorageVolumeApiClient, StorageVolumeApiServer, StorageVolumeController, SystemApi,
    SystemApiClient, SystemApiServer, SystemController, TenantApi, TenantApiClient,
    TenantApiServer, TenantController,
};
pub use error::NeoEngramProblemEncoder;
pub use gateway_activation_transport::{
    GatewayBootstrapTransport, GatewayBootstrapTransportError, GatewayReplicaActivationClient,
    GatewayReplicaActivationClientError, HttpGatewayBootstrapTransport,
};
pub use gateway_connector::{
    GatewayConnectorConfig, GatewayConnectorError, RunningGatewayConnector,
};
pub use gateway_transport::{
    AuthenticatedGatewayReplica, CentralGatewayControl, CentralGatewaySession, GatewaySessionError,
};
pub use identity::{
    AuthenticatedIdentity, Authenticator, Permission, PrincipalContext, StaticRbacPolicy,
};
pub use runtime::{run, AppState, Config, GatewayActivationDependencies, RuntimeError};
pub use service::{
    parse_workload_identity_from_certificate_der, AgentDataPlaneService,
    AgentWorkloadCertificateError, AgentWorkloadCertificateService, CatalogService,
    CentralCommandKeyId, CentralCommandKeyState, CentralCommandKeyring,
    CentralCommandSecurityError, CentralCommandSignature, CentralCommandSignatureRequest,
    CentralCommandSigner, CentralCommandSignerError, CentralCommandTrustBundle,
    CentralCommandVerificationKey, CoordinatorRun, EnrollmentKeyring, EnrollmentService,
    GatewayRegistryService, GatewayReplicaActivationChallenge, GatewayReplicaActivationError,
    GatewayReplicaActivationProof, GatewayReplicaActivationResult, GatewayReplicaActivationService,
    HealthService, IssuedWorkloadCertificate, JobCoordinator, JobService, ReadinessProbe,
    SystemService, WorkloadCertificateIssuance, WorkloadCertificateIssuer,
    WorkloadCertificateIssuerError, WorkloadCertificateRequest, WorkloadCertificateUsage,
    WorkloadIdentity, WorkloadIdentityUri, WorkloadPkiError, WorkspaceCommitResult,
    WorkspaceCommitService, DEFAULT_CENTRAL_COMMAND_TTL_MS,
    DEFAULT_WORKLOAD_LEAF_CERTIFICATE_LIFETIME_MS, GATEWAY_ACTIVATION_CHALLENGE_TTL_MS,
    MAX_CENTRAL_COMMAND_TTL_MS, MAX_WORKLOAD_LEAF_CERTIFICATE_LIFETIME_MS,
};
