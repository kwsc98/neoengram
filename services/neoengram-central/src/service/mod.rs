mod agent_certificate;
mod catalog;
mod command_signing;
mod coordinator;
mod data_plane;
mod enrollment;
mod gateway;
mod gateway_activation;
mod gateway_certificate;
mod health;
mod job;
mod keyring;
mod resource_lifecycle;
mod s3_envelope;
mod snapshot_delivery;
mod system;
mod workload_pki;
mod workspace_commit;

pub use agent_certificate::{AgentWorkloadCertificateError, AgentWorkloadCertificateService};
pub(crate) use catalog::development_s3_envelope_key;
pub use catalog::{
    CatalogService, S3AgentPlacement, S3PlacementProvider, S3ReadRevocationPublisher,
    StorageAvailabilityProvider,
};
pub use command_signing::{
    CentralCommandKeyId, CentralCommandKeyState, CentralCommandKeyring,
    CentralCommandSecurityError, CentralCommandSignature, CentralCommandSignatureRequest,
    CentralCommandSigner, CentralCommandSignerError, CentralCommandTrustBundle,
    CentralCommandVerificationKey, DEFAULT_CENTRAL_COMMAND_TTL_MS, MAX_CENTRAL_COMMAND_TTL_MS,
};
pub use coordinator::{CoordinatorRun, JobCoordinator};
pub use data_plane::AgentDataPlaneService;
pub use enrollment::EnrollmentService;
pub use gateway::GatewayRegistryService;
pub use gateway_activation::{
    GatewayReplicaActivationChallenge, GatewayReplicaActivationError,
    GatewayReplicaActivationProof, GatewayReplicaActivationResult, GatewayReplicaActivationService,
    GATEWAY_ACTIVATION_CHALLENGE_TTL_MS,
};
pub use gateway_certificate::{GatewayCredentialExpiryRun, GatewayCredentialLifecycleService};
pub use health::{HealthService, ReadinessProbe};
pub use job::JobService;
pub use keyring::{EnrollmentKeyring, KeyringError};
pub use resource_lifecycle::{ResourceLifecycleCoordinator, ResourceLifecycleReconcileRun};
pub use s3_envelope::{LocalS3SecretEnvelope, S3SecretEnvelope, S3SecretEnvelopeError};
pub use system::SystemService;
#[cfg(test)]
pub(crate) use workload_pki::test_issue_workload_certificate;
pub use workload_pki::{
    parse_workload_identity_from_certificate_der, validate_issued_workload_certificate,
    IssuedWorkloadCertificate, WorkloadCertificateIssuance, WorkloadCertificateIssuer,
    WorkloadCertificateIssuerError, WorkloadCertificateRequest, WorkloadCertificateUsage,
    WorkloadIdentity, WorkloadIdentityUri, WorkloadPkiError,
    DEFAULT_WORKLOAD_LEAF_CERTIFICATE_LIFETIME_MS, MAX_WORKLOAD_LEAF_CERTIFICATE_LIFETIME_MS,
};
pub use workspace_commit::{WorkspaceCommitResult, WorkspaceCommitService};
