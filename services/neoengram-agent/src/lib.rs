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
mod materialization_quic;
mod replication;
mod replication_quic;
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
mod transfer_quic;
mod volume_integrity;

pub use agent_core::*;
pub use backoff::EnrollmentBackoff;
pub use client::{EnrollmentClient, EnrollmentClientError, ReqwestEnrollmentClient};
pub use command_trust::CentralCommandTrustBundle;
pub use config::{
    AgentConfig, LoggingConfig, LoggingFormat, PvcReference, RegistrationConfig, ReplicationConfig,
    SessionConfig, StorageAccessMode, StorageBackendType, StorageConfig,
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
pub use materialization_quic::{
    apply_materialization_checkpoint, materialization_protocol_version,
    materialization_receipt_from_checkpoint, materialization_transfer_id,
    switch_materialization_source, validate_materialization_batch,
    validate_signed_materialization_batch, validate_signed_materialization_batch_with_trust,
    MaterializationCheckpoint, MaterializationSourceSession, MaterializationTargetSession,
};
pub use replication::{
    DurableReplicationProgressSink, MaterializationAssignmentExecutor,
    MountedVolumeMaterializationExecutor, MountedVolumeReplicationExecutor,
    ReplicationAssignmentExecutor, ReplicationProgressSink, ReplicationWorker,
};
pub use replication_quic::{
    run_quic_sink_stream, serve_quic_materialization_source_connection,
    serve_quic_materialization_source_connection_with_resolver,
    serve_quic_materialization_source_stream,
    serve_quic_materialization_source_stream_with_resolver, serve_quic_source_connection,
    serve_quic_source_stream, serve_quic_source_stream_from_ticket,
    MountedVolumeSourcePlacementResolver, PermissiveSourcePlacementResolver, QuicTransferClient,
    QuicTransferClientConfig, QuicTransferError, QuicTransferIdentity, QuicTransferNetwork,
    QuicTransferNetworkConfig, RejectingSourcePlacementResolver, SourcePlacementResolver,
};
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
pub use transfer_quic::{
    AgentTransferError, AgentTransferFrameChannel, AgentTransferSinkSession,
    AgentTransferSourceSession,
};
pub use volume_integrity::{
    VolumeIntegrityIssue, VolumeIntegrityIssueKind, VolumeIntegrityReport, VolumeIntegrityScanner,
};
