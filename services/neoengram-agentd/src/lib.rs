//! Standalone process adapter for the transport-independent NeoEngram Agent core.
//!
//! HTTP, process lifecycle, configuration, and health signaling live here. The underlying
//! `neoengram-agent` and `neoengram-protocol` crates remain independent of any HTTP stack.

mod approved_runtime;
mod backoff;
mod client;
mod config;
mod durability;
mod error;
mod execution;
mod health;
mod identity;
mod runtime;
mod session_channel;
mod session_client;
mod session_runtime;
mod snapshot_mount;
mod status_clock;

pub use backoff::EnrollmentBackoff;
pub use client::{EnrollmentClient, EnrollmentClientError, ReqwestEnrollmentClient};
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
pub use neoengram_agent::{FilesystemMountObservation, MountProbeCondition};
pub use runtime::{
    run, run_with, run_with_development_directory_probe, run_with_transports,
    DevelopmentDirectoryProbe, FilesystemProbe, MountProbe,
};
pub use session_channel::{AgentChannelConnection, AgentChannelWriter};
pub use session_client::{
    AgentRequestSigner, AgentSessionClient, AgentSessionClientError, AgentSessionFence,
    ReqwestAgentSessionClient,
};
pub use session_runtime::{
    AgentMessageProcessor, AgentSessionBinding, AgentSessionTransport, CoreAgentMessageProcessor,
    SessionExecutionBridge, SharedResourceVersion, SharedSessionFence,
};
pub use snapshot_mount::{SnapshotMountManager, SnapshotMountStats};
