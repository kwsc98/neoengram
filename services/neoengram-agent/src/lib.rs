//! NeoEngram Agent service.
//!
//! The package owns both the transport-independent Agent state machine and the process adapter
//! that enrolls a volume through Gateway. Keeping those pieces in one crate makes the production
//! Agent boundary explicit while preserving a small library API for local callers and tests.

mod agent_core;
mod approved_runtime;
mod backoff;
mod client;
mod command_trust;
mod config;
mod durability;
mod error;
mod execution;
mod health;
mod identity;
mod replication;
mod resource_lifecycle;
mod runtime;
mod s3_channel;
mod s3_read;
mod session_channel;
mod session_client;
mod session_runtime;
mod snapshot_delivery;
mod snapshot_mount;
mod snapshot_reader;
mod status_clock;
mod tls;

pub use agent_core::*;
pub use backoff::EnrollmentBackoff;
pub use client::{EnrollmentClient, EnrollmentClientError, ReqwestEnrollmentClient};
pub use command_trust::CentralCommandTrustBundle;
pub use config::{
    AgentConfig, LoggingConfig, LoggingFormat, PvcReference, RegistrationConfig, SessionConfig,
    StorageAccessMode, StorageBackendType, StorageConfig,
};
pub use error::{AgentDaemonError, AgentDaemonResult};
pub use execution::{
    AuthoritativeIndexSnapshot, ExecutionBridge, FilesystemExecution, WorkspaceMaterializationFile,
    WorkspaceMaterializationSnapshot, WorkspaceMaterializationStats, WorkspaceMaterializer,
    MAX_AGENT_OBJECT_BYTES,
};
pub use health::{check_health, HealthMode, RuntimeHealthPhase};
pub use identity::{
    has_pending_outbound_reports, load_or_create_identity, load_persisted_identity,
    signing_key_from_identity, AgentSigningKey, PersistedIdentitySummary,
};
pub use replication::{ReplicationProgressSink, ReplicationWorker};
pub use runtime::{
    run, run_with, run_with_development_directory_probe, run_with_transports,
    DevelopmentDirectoryProbe, FilesystemProbe, MountProbe,
};
pub use s3_channel::{AgentS3ReadChannelConnection, AgentS3ReadChannelWriter};
pub use s3_read::S3ReadExecutor;
pub use session_channel::{AgentChannelConnection, AgentChannelWriter};
pub use session_client::{
    AgentRequestSigner, AgentSessionClient, AgentSessionClientError, AgentSessionFence,
    ReqwestAgentSessionClient,
};
pub use session_runtime::{
    AgentMessageProcessor, AgentSessionBinding, AgentSessionTransport, CoreAgentMessageProcessor,
    SessionExecutionBridge, SharedResourceVersion, SharedSessionFence,
};
pub use snapshot_delivery::{SnapshotDeliveryMaterializer, SnapshotDeliveryStats};
pub use snapshot_mount::{
    SnapshotDeliveryMountManager, SnapshotDeliveryRecoveryOutcome, SnapshotMountStats,
};
pub use snapshot_reader::{
    ImmutableByteRange, ImmutableObjectHead, ImmutableSnapshotReader, S3SnapshotSource,
    SnapshotCasReader, SnapshotCasReaderFactory,
};
