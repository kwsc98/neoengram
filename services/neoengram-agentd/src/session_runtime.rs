use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock},
    time::Duration,
};

use async_trait::async_trait;
use neoengram_agent::{
    Agent, AgentAssignmentState, AgentError, AgentErrorCode, AgentReport, AgentResult,
    AssignmentKey, Clock, LedgerRecord, OutboundReportQueue,
};
use neoengram_core::{ChunkRef, ChunkingStrategy, FileRecord, IndexVersion, Manifest, ManifestId};
use neoengram_protocol::{
    AgentHeartbeatReportPayload, AgentIndexPageQueryPayload, AgentJobReportCreatePayload,
    AgentManifestPageQueryPayload, AgentManifestPageQueryResponse, AgentMessageListQueryPayload,
    AgentMetadataBatchStagePayload, AgentMetadataPageStagePayload, AgentSessionClosePayload,
    AgentSessionOpenPayload, ArtifactId, ControlEnvelope, ControlError, ControlMessage, DecimalU64,
    ErrorCode, Extensions, IndexDeltaRecord, JobAccepted, JobAssignment, JobDecision, JobFailed,
    JobFailureStage, JobId, JobProgress, JobState, MetadataBatchDescriptor, MetadataBatchPage,
    MountGeneration, OwnerGeneration, PlaygroundId, ProtocolVersion, RequestId, ResourceVersion,
    SnapshotId, SnapshotMountAssignment, TenantId, UnixMillis, WireIndexVersion,
    WorkspaceMaterializeAssignment, AGENT_JOB_INDEX_PAGE_QUERY_PATH,
    AGENT_JOB_MANIFEST_PAGE_QUERY_PATH, AGENT_JOB_METADATA_BATCH_STAGE_PATH,
    AGENT_JOB_METADATA_PAGE_STAGE_PATH, AGENT_JOB_REPORT_CREATE_PATH, AGENT_SESSION_CLOSE_PATH,
    AGENT_SESSION_HEARTBEAT_REPORT_PATH, AGENT_SESSION_MESSAGE_LIST_QUERY_PATH,
    AGENT_SESSION_OPEN_PATH, MAX_RECORDS_PER_PAGE,
};
use tokio::runtime::Handle;

use crate::{
    AgentDaemonError, AgentDaemonResult, AgentRequestSigner, AgentSessionClient, AgentSessionFence,
    AuthoritativeIndexSnapshot, ExecutionBridge, SnapshotMountManager,
    WorkspaceMaterializationFile, WorkspaceMaterializationSnapshot, WorkspaceMaterializer,
};

#[async_trait]
pub trait AgentMessageProcessor: Send + Sync {
    async fn handle_assignment(&self, assignment: JobAssignment) -> AgentDaemonResult<()>;
    async fn recover_assignment(&self, record: LedgerRecord) -> AgentDaemonResult<()>;
    async fn handle_decision(
        &self,
        tenant_id: &TenantId,
        decision: JobDecision,
    ) -> AgentDaemonResult<()>;
}

#[derive(Debug)]
pub struct CoreAgentMessageProcessor {
    agent: Arc<Agent>,
    workspace_materializer: Option<Arc<WorkspaceMaterializer>>,
    snapshot_mounts: Option<Arc<SnapshotMountManager>>,
    reports: Option<Arc<dyn OutboundReportQueue>>,
    clock: Option<Arc<dyn Clock>>,
}

impl CoreAgentMessageProcessor {
    #[must_use]
    pub fn new(agent: Arc<Agent>) -> Self {
        Self {
            agent,
            workspace_materializer: None,
            snapshot_mounts: None,
            reports: None,
            clock: None,
        }
    }

    /// Adds the node-local Workspace materializer while retaining the existing managed-Add
    /// processor. Reports use the same durable Agent outbox as Add, so a disconnect after
    /// directory creation does not lose the terminal acknowledgement.
    #[must_use]
    pub fn with_workspace_materializer(
        agent: Arc<Agent>,
        materializer: Arc<WorkspaceMaterializer>,
        reports: Arc<dyn OutboundReportQueue>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            agent,
            workspace_materializer: Some(materializer),
            snapshot_mounts: None,
            reports: Some(reports),
            clock: Some(clock),
        }
    }

    #[must_use]
    pub fn with_materializers(
        agent: Arc<Agent>,
        workspace_materializer: Arc<WorkspaceMaterializer>,
        snapshot_mounts: Arc<SnapshotMountManager>,
        reports: Arc<dyn OutboundReportQueue>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            agent,
            workspace_materializer: Some(workspace_materializer),
            snapshot_mounts: Some(snapshot_mounts),
            reports: Some(reports),
            clock: Some(clock),
        }
    }
}

#[async_trait]
impl AgentMessageProcessor for CoreAgentMessageProcessor {
    async fn handle_assignment(&self, assignment: JobAssignment) -> AgentDaemonResult<()> {
        match assignment.assignment {
            neoengram_protocol::AssignmentOperation::Add { input, .. } => {
                let agent = Arc::clone(&self.agent);
                tokio::task::spawn_blocking(move || agent.handle_assignment(input))
                    .await
                    .map_err(join_error)?
                    .map(|_| ())
                    .map_err(Into::into)
            }
            neoengram_protocol::AssignmentOperation::WorkspaceMaterialize { input, .. } => {
                let materializer = self.workspace_materializer.clone().ok_or_else(|| {
                    AgentDaemonError::Session(
                        "Workspace materialize assignment received before the materializer was configured"
                            .to_owned(),
                    )
                })?;
                let reports = self.reports.clone().ok_or_else(|| {
                    AgentDaemonError::Session(
                        "Workspace materialize assignment received without a durable report queue"
                            .to_owned(),
                    )
                })?;
                let clock = self.clock.clone().ok_or_else(|| {
                    AgentDaemonError::Session(
                        "Workspace materialize assignment received without a clock".to_owned(),
                    )
                })?;
                tokio::task::spawn_blocking(move || {
                    materialize_workspace_assignment(
                        &materializer,
                        reports.as_ref(),
                        clock.as_ref(),
                        input,
                    )
                })
                .await
                .map_err(join_error)?
            }
            neoengram_protocol::AssignmentOperation::SnapshotMount { input, .. } => {
                let manager = self.snapshot_mounts.clone().ok_or_else(|| {
                    AgentDaemonError::Session(
                        "Snapshot mount assignment received before FUSE was configured".to_owned(),
                    )
                })?;
                let reports = self.reports.clone().ok_or_else(|| {
                    AgentDaemonError::Session(
                        "Snapshot mount assignment received without a durable report queue"
                            .to_owned(),
                    )
                })?;
                let clock = self.clock.clone().ok_or_else(|| {
                    AgentDaemonError::Session(
                        "Snapshot mount assignment received without a clock".to_owned(),
                    )
                })?;
                tokio::task::spawn_blocking(move || {
                    mount_snapshot_assignment(&manager, reports.as_ref(), clock.as_ref(), input)
                })
                .await
                .map_err(join_error)?
            }
        }
    }

    async fn recover_assignment(&self, record: LedgerRecord) -> AgentDaemonResult<()> {
        let agent = Arc::clone(&self.agent);
        tokio::task::spawn_blocking(move || {
            if record.state == AgentAssignmentState::Claimed {
                agent.handle_assignment(record.assignment)
            } else {
                agent.recover(&record.key)
            }
        })
        .await
        .map_err(join_error)?
        .map(|_| ())
        .map_err(Into::into)
    }

    async fn handle_decision(
        &self,
        tenant_id: &TenantId,
        decision: JobDecision,
    ) -> AgentDaemonResult<()> {
        let agent = Arc::clone(&self.agent);
        let tenant_id = tenant_id.clone();
        tokio::task::spawn_blocking(move || agent.handle_decision(&tenant_id, decision))
            .await
            .map_err(join_error)?
            .map(|_| ())
            .map_err(Into::into)
    }
}

/// Executes one Workspace materialize assignment and persists compatible job reports. The
/// operation is deliberately terminal without a central decision: it consumes immutable metadata
/// and Volume-local objects but has no object/index publication to finalize. Replaying the
/// assignment is safe because the filesystem materializer verifies existing components and the
/// report queue inserts by deterministic report identity where possible.
fn materialize_workspace_assignment(
    materializer: &WorkspaceMaterializer,
    reports: &dyn OutboundReportQueue,
    clock: &dyn Clock,
    assignment: WorkspaceMaterializeAssignment,
) -> AgentDaemonResult<()> {
    assignment.validate().map_err(protocol_error)?;
    materializer
        .validate_assignment(&assignment)
        .map_err(|error| AgentDaemonError::Session(format!("{}: {error}", error.stable_code())))?;
    let queued_state = queued_workspace_state(reports, &assignment)?;
    if queued_state.terminal {
        return Ok(());
    }
    let accepted_at = UnixMillis::new(clock.now_unix_ms().map_err(agent_error)?);
    if !queued_state.has_report {
        enqueue_workspace_report(
            reports,
            AgentReport::Accepted(JobAccepted {
                job_id: assignment.job_id.clone(),
                assignment_id: assignment.assignment_id.clone(),
                assignment_generation: assignment.assignment_generation,
                accepted_at_unix_ms: accepted_at,
                request_digest: assignment.request_digest,
                extensions: Extensions::new(),
            }),
            accepted_at,
        )?;
    }

    if !queued_state.running {
        enqueue_workspace_report(
            reports,
            AgentReport::Progress(JobProgress {
                job_id: assignment.job_id.clone(),
                assignment_id: assignment.assignment_id.clone(),
                assignment_generation: assignment.assignment_generation,
                state: JobState::Running,
                phase: "materializing".to_owned(),
                files_completed: DecimalU64::new(0),
                bytes_completed: DecimalU64::new(0),
                retry_after_ms: None,
                extensions: Extensions::new(),
            }),
            UnixMillis::new(clock.now_unix_ms().map_err(agent_error)?),
        )?;
    }

    let stats = match materializer.materialize(&assignment) {
        Ok(stats) => stats,
        Err(error) => {
            let code = match error.code() {
                AgentErrorCode::InvalidAssignment => "WORKSPACE_ASSIGNMENT_INVALID",
                AgentErrorCode::ScopeMismatch => "WORKSPACE_ASSIGNMENT_SCOPE_MISMATCH",
                AgentErrorCode::GenerationMismatch => "WORKSPACE_STALE_GENERATION",
                AgentErrorCode::MountUnavailable => "WORKSPACE_MOUNT_UNAVAILABLE",
                AgentErrorCode::ProtocolInvalid => "WORKSPACE_METADATA_PROTOCOL_INVALID",
                AgentErrorCode::ObjectTransferFailed => "WORKSPACE_OBJECT_OR_METADATA_UNAVAILABLE",
                AgentErrorCode::InvalidState => "WORKSPACE_MATERIALIZER_INVALID_STATE",
                _ => "WORKSPACE_MATERIALIZE_FAILED",
            };
            let control_error = ControlError {
                code: ErrorCode::new(code).map_err(protocol_error)?,
                message: error.message().to_owned(),
                retryable: matches!(
                    error.code(),
                    AgentErrorCode::MountUnavailable | AgentErrorCode::ObjectTransferFailed
                ),
                retry_after_ms: None,
                extensions: Extensions::new(),
            };
            enqueue_workspace_report(
                reports,
                AgentReport::Failed(JobFailed {
                    tenant_id: assignment.tenant_id,
                    job_id: assignment.job_id,
                    assignment_id: assignment.assignment_id,
                    assignment_generation: assignment.assignment_generation,
                    final_state: JobState::Failed,
                    failed_at_unix_ms: UnixMillis::new(clock.now_unix_ms().map_err(agent_error)?),
                    stage: JobFailureStage::Execution,
                    error: control_error,
                    extensions: Extensions::new(),
                }),
                accepted_at,
            )?;
            return Ok(());
        }
    };

    enqueue_workspace_report(
        reports,
        AgentReport::Progress(JobProgress {
            job_id: assignment.job_id,
            assignment_id: assignment.assignment_id,
            assignment_generation: assignment.assignment_generation,
            state: JobState::Succeeded,
            phase: "materialized".to_owned(),
            files_completed: DecimalU64::new(stats.files),
            bytes_completed: DecimalU64::new(stats.bytes),
            retry_after_ms: None,
            extensions: Extensions::new(),
        }),
        UnixMillis::new(clock.now_unix_ms().map_err(agent_error)?),
    )?;
    Ok(())
}

fn mount_snapshot_assignment(
    manager: &SnapshotMountManager,
    reports: &dyn OutboundReportQueue,
    clock: &dyn Clock,
    assignment: SnapshotMountAssignment,
) -> AgentDaemonResult<()> {
    assignment.validate().map_err(protocol_error)?;
    manager
        .validate_assignment(&assignment)
        .map_err(agent_error)?;
    let queued_state = queued_snapshot_state(reports, &assignment)?;
    if queued_state.terminal {
        return Ok(());
    }
    if manager.is_mounted(&assignment).map_err(agent_error)? {
        let snapshot = manager.mount(assignment.clone()).map_err(agent_error)?;
        return enqueue_snapshot_mounted_observation(
            reports,
            clock,
            &assignment,
            snapshot,
            manager.session_generation(),
        );
    }

    let accepted_at = UnixMillis::new(clock.now_unix_ms().map_err(agent_error)?);
    if !queued_state.has_report {
        enqueue_workspace_report(
            reports,
            AgentReport::Accepted(JobAccepted {
                job_id: assignment.job_id.clone(),
                assignment_id: assignment.assignment_id.clone(),
                assignment_generation: assignment.assignment_generation,
                accepted_at_unix_ms: accepted_at,
                request_digest: assignment.request_digest,
                extensions: Extensions::new(),
            }),
            accepted_at,
        )?;
    }
    if !queued_state.running {
        enqueue_workspace_report(
            reports,
            AgentReport::Progress(JobProgress {
                job_id: assignment.job_id.clone(),
                assignment_id: assignment.assignment_id.clone(),
                assignment_generation: assignment.assignment_generation,
                state: JobState::Running,
                phase: "mounting".to_owned(),
                files_completed: DecimalU64::new(0),
                bytes_completed: DecimalU64::new(0),
                retry_after_ms: None,
                extensions: Extensions::new(),
            }),
            UnixMillis::new(clock.now_unix_ms().map_err(agent_error)?),
        )?;
    }

    let stats = match manager.mount(assignment.clone()) {
        Ok(stats) => stats,
        Err(error) => {
            let code = match error.code() {
                AgentErrorCode::InvalidAssignment => "SNAPSHOT_ASSIGNMENT_INVALID",
                AgentErrorCode::ScopeMismatch => "SNAPSHOT_ASSIGNMENT_SCOPE_MISMATCH",
                AgentErrorCode::GenerationMismatch => "SNAPSHOT_STALE_GENERATION",
                AgentErrorCode::MountUnavailable => "SNAPSHOT_FUSE_MOUNT_UNAVAILABLE",
                AgentErrorCode::ProtocolInvalid => "SNAPSHOT_METADATA_PROTOCOL_INVALID",
                AgentErrorCode::ObjectTransferFailed => "SNAPSHOT_OBJECT_UNAVAILABLE",
                AgentErrorCode::AssignmentMismatch => "SNAPSHOT_ASSIGNMENT_MISMATCH",
                _ => "SNAPSHOT_MOUNT_FAILED",
            };
            let control_error = ControlError {
                code: ErrorCode::new(code).map_err(protocol_error)?,
                message: error.message().to_owned(),
                retryable: matches!(
                    error.code(),
                    AgentErrorCode::MountUnavailable | AgentErrorCode::ObjectTransferFailed
                ),
                retry_after_ms: None,
                extensions: Extensions::new(),
            };
            enqueue_workspace_report(
                reports,
                AgentReport::Failed(JobFailed {
                    tenant_id: assignment.tenant_id,
                    job_id: assignment.job_id,
                    assignment_id: assignment.assignment_id,
                    assignment_generation: assignment.assignment_generation,
                    final_state: JobState::Failed,
                    failed_at_unix_ms: UnixMillis::new(clock.now_unix_ms().map_err(agent_error)?),
                    stage: JobFailureStage::Execution,
                    error: control_error,
                    extensions: Extensions::new(),
                }),
                accepted_at,
            )?;
            return Ok(());
        }
    };
    enqueue_snapshot_mounted_observation(
        reports,
        clock,
        &assignment,
        stats,
        manager.session_generation(),
    )
}

pub(crate) fn enqueue_snapshot_mounted_observation(
    reports: &dyn OutboundReportQueue,
    clock: &dyn Clock,
    assignment: &SnapshotMountAssignment,
    stats: crate::SnapshotMountStats,
    session_generation: neoengram_protocol::SessionGeneration,
) -> AgentDaemonResult<()> {
    let (report, observed_at) =
        build_snapshot_mounted_observation(clock, assignment, stats, session_generation)?;
    enqueue_workspace_report(reports, report, observed_at)
}

pub(crate) fn build_snapshot_mounted_observation(
    clock: &dyn Clock,
    assignment: &SnapshotMountAssignment,
    stats: crate::SnapshotMountStats,
    session_generation: neoengram_protocol::SessionGeneration,
) -> AgentDaemonResult<(AgentReport, UnixMillis)> {
    let observed_at = UnixMillis::new(clock.now_unix_ms().map_err(agent_error)?);
    let mut extensions = Extensions::new();
    extensions.insert(
        "snapshot_mount_session_generation".to_owned(),
        serde_json::Value::String(session_generation.get().to_string()),
    );
    extensions.insert(
        "snapshot_mount_observed_at_unix_ms".to_owned(),
        serde_json::Value::String(observed_at.get().to_string()),
    );
    Ok((
        AgentReport::Progress(JobProgress {
            job_id: assignment.job_id.clone(),
            assignment_id: assignment.assignment_id.clone(),
            assignment_generation: assignment.assignment_generation,
            state: JobState::Succeeded,
            phase: "mounted".to_owned(),
            files_completed: DecimalU64::new(stats.files),
            bytes_completed: DecimalU64::new(stats.bytes),
            retry_after_ms: None,
            extensions,
        }),
        observed_at,
    ))
}

pub(crate) fn build_snapshot_recovery_failure(
    clock: &dyn Clock,
    assignment: &SnapshotMountAssignment,
    code: AgentErrorCode,
    message: String,
    session_generation: neoengram_protocol::SessionGeneration,
) -> AgentDaemonResult<(AgentReport, UnixMillis)> {
    let stable_code = match code {
        AgentErrorCode::ScopeMismatch => "SNAPSHOT_ASSIGNMENT_SCOPE_MISMATCH",
        AgentErrorCode::GenerationMismatch => "SNAPSHOT_STALE_GENERATION",
        AgentErrorCode::MountUnavailable => "SNAPSHOT_FUSE_MOUNT_UNAVAILABLE",
        AgentErrorCode::ProtocolInvalid => "SNAPSHOT_METADATA_PROTOCOL_INVALID",
        AgentErrorCode::ObjectTransferFailed => "SNAPSHOT_OBJECT_UNAVAILABLE",
        _ => "SNAPSHOT_RECOVERY_FAILED",
    };
    let mut extensions = Extensions::new();
    extensions.insert(
        "snapshot_mount_session_generation".to_owned(),
        serde_json::Value::String(session_generation.get().to_string()),
    );
    extensions.insert(
        "snapshot_mount_recovery".to_owned(),
        serde_json::Value::Bool(true),
    );
    let now = UnixMillis::new(clock.now_unix_ms().map_err(agent_error)?);
    Ok((
        AgentReport::Failed(JobFailed {
            tenant_id: assignment.tenant_id.clone(),
            job_id: assignment.job_id.clone(),
            assignment_id: assignment.assignment_id.clone(),
            assignment_generation: assignment.assignment_generation,
            final_state: JobState::Failed,
            failed_at_unix_ms: now,
            stage: JobFailureStage::Execution,
            error: ControlError {
                code: ErrorCode::new(stable_code).map_err(protocol_error)?,
                message,
                retryable: matches!(
                    code,
                    AgentErrorCode::MountUnavailable | AgentErrorCode::ObjectTransferFailed
                ),
                retry_after_ms: None,
                extensions: Extensions::new(),
            },
            extensions,
        }),
        now,
    ))
}

fn queued_snapshot_state(
    reports: &dyn OutboundReportQueue,
    assignment: &SnapshotMountAssignment,
) -> AgentDaemonResult<QueuedWorkspaceState> {
    let mut state = QueuedWorkspaceState::default();
    for queued in reports.list(256).map_err(agent_error)? {
        let (job_id, assignment_id, generation) = match &queued.report {
            AgentReport::Accepted(report) => (
                &report.job_id,
                &report.assignment_id,
                report.assignment_generation,
            ),
            AgentReport::Progress(report) => (
                &report.job_id,
                &report.assignment_id,
                report.assignment_generation,
            ),
            AgentReport::Prepared(report) => (
                &report.job_id,
                &report.assignment_id,
                report.assignment_generation,
            ),
            AgentReport::Finalized(report) => (
                &report.job_id,
                &report.assignment_id,
                report.assignment_generation,
            ),
            AgentReport::Failed(report) => (
                &report.job_id,
                &report.assignment_id,
                report.assignment_generation,
            ),
        };
        if job_id != &assignment.job_id {
            continue;
        }
        if assignment_id != &assignment.assignment_id
            || generation != assignment.assignment_generation
        {
            return Err(AgentDaemonError::Session(format!(
                "durable outbox contains another assignment for Snapshot mount Job {}",
                assignment.job_id
            )));
        }
        state.has_report = true;
        match &queued.report {
            AgentReport::Accepted(report) if report.request_digest != assignment.request_digest => {
                return Err(AgentDaemonError::Session(format!(
                    "durable Snapshot acceptance has another request digest for Job {}",
                    assignment.job_id
                )));
            }
            AgentReport::Progress(report) if report.state == JobState::Running => {
                state.running = true;
            }
            AgentReport::Progress(report) if report.state == JobState::Succeeded => {
                state.terminal = true;
            }
            AgentReport::Failed(_) => state.terminal = true,
            AgentReport::Prepared(_) | AgentReport::Finalized(_) => {
                return Err(AgentDaemonError::Session(format!(
                    "durable outbox contains an incompatible report for Snapshot mount Job {}",
                    assignment.job_id
                )));
            }
            _ => {}
        }
    }
    Ok(state)
}

#[derive(Debug, Default)]
struct QueuedWorkspaceState {
    has_report: bool,
    running: bool,
    terminal: bool,
}

fn queued_workspace_state(
    reports: &dyn OutboundReportQueue,
    assignment: &WorkspaceMaterializeAssignment,
) -> AgentDaemonResult<QueuedWorkspaceState> {
    let mut state = QueuedWorkspaceState::default();
    for queued in reports.list(256).map_err(agent_error)? {
        let (job_id, assignment_id, generation) = match &queued.report {
            AgentReport::Accepted(report) => (
                &report.job_id,
                &report.assignment_id,
                report.assignment_generation,
            ),
            AgentReport::Progress(report) => (
                &report.job_id,
                &report.assignment_id,
                report.assignment_generation,
            ),
            AgentReport::Prepared(report) => (
                &report.job_id,
                &report.assignment_id,
                report.assignment_generation,
            ),
            AgentReport::Finalized(report) => (
                &report.job_id,
                &report.assignment_id,
                report.assignment_generation,
            ),
            AgentReport::Failed(report) => (
                &report.job_id,
                &report.assignment_id,
                report.assignment_generation,
            ),
        };
        if job_id != &assignment.job_id {
            continue;
        }
        if assignment_id != &assignment.assignment_id
            || generation != assignment.assignment_generation
        {
            return Err(AgentDaemonError::Session(format!(
                "durable outbox already contains another assignment for Workspace materialize Job {}",
                assignment.job_id
            )));
        }
        state.has_report = true;
        match &queued.report {
            AgentReport::Accepted(report) if report.request_digest != assignment.request_digest => {
                return Err(AgentDaemonError::Session(format!(
                    "durable Workspace materialize acceptance has another request digest for Job {}",
                    assignment.job_id
                )));
            }
            AgentReport::Progress(report) if report.state == JobState::Running => {
                state.running = true;
            }
            AgentReport::Progress(report) if report.state == JobState::Succeeded => {
                state.terminal = true;
            }
            AgentReport::Failed(_) => state.terminal = true,
            AgentReport::Prepared(_) | AgentReport::Finalized(_) => {
                return Err(AgentDaemonError::Session(format!(
                    "durable outbox contains an incompatible report for Workspace materialize Job {}",
                    assignment.job_id
                )));
            }
            _ => {}
        }
    }
    Ok(state)
}

fn enqueue_workspace_report(
    reports: &dyn OutboundReportQueue,
    report: AgentReport,
    enqueued_at: UnixMillis,
) -> AgentDaemonResult<()> {
    reports
        .enqueue(report, enqueued_at)
        .map(|_| ())
        .map_err(agent_error)
}

fn agent_error(error: AgentError) -> AgentDaemonError {
    AgentDaemonError::Session(error.to_string())
}

fn protocol_error(error: neoengram_protocol::ProtocolError) -> AgentDaemonError {
    AgentDaemonError::Session(error.to_string())
}

/// Session generation shared by control transport and synchronous data-plane calls.
#[derive(Debug, Clone, Default)]
pub struct SharedSessionFence(Arc<RwLock<Option<AgentSessionFence>>>);

impl SharedSessionFence {
    pub(crate) fn get(&self) -> AgentDaemonResult<AgentSessionFence> {
        self.0
            .read()
            .map_err(|_| AgentDaemonError::Session("session fence lock is poisoned".to_owned()))?
            .clone()
            .ok_or_else(|| AgentDaemonError::Session("Agent session is not open".to_owned()))
    }

    pub(crate) fn replace(&self, fence: Option<AgentSessionFence>) -> AgentDaemonResult<()> {
        *self.0.write().map_err(|_| {
            AgentDaemonError::Session("session fence lock is poisoned".to_owned())
        })? = fence;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SharedResourceVersion(Arc<RwLock<ResourceVersion>>);

impl SharedResourceVersion {
    #[must_use]
    pub fn new(version: ResourceVersion) -> Self {
        Self(Arc::new(RwLock::new(version)))
    }

    pub(crate) fn get(&self) -> AgentDaemonResult<ResourceVersion> {
        self.0
            .read()
            .map(|version| *version)
            .map_err(|_| AgentDaemonError::Session("resource version lock is poisoned".to_owned()))
    }

    pub(crate) fn replace(&self, version: ResourceVersion) -> AgentDaemonResult<()> {
        *self.0.write().map_err(|_| {
            AgentDaemonError::Session("resource version lock is poisoned".to_owned())
        })? = version;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentSessionBinding {
    pub fence: AgentSessionFence,
    pub agent_mount_id: neoengram_protocol::AgentMountId,
    pub mount_generation: MountGeneration,
    pub owner_generation: OwnerGeneration,
}

/// Stateful driver for one Agent boot. Executor and data-plane adapters remain injected behind the
/// message processor; this type owns only signed HTTP transport and durable report acknowledgement.
pub struct AgentSessionTransport<C> {
    tenant_id: TenantId,
    signer: Arc<AgentRequestSigner>,
    client: Arc<C>,
    reports: Arc<dyn OutboundReportQueue>,
    processor: Arc<dyn AgentMessageProcessor>,
    fence: SharedSessionFence,
    resource_version: SharedResourceVersion,
}

impl<C> std::fmt::Debug for AgentSessionTransport<C> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentSessionTransport")
            .field("tenant_id", &self.tenant_id)
            .field("signer", &self.signer)
            .field("fence", &self.fence)
            .field("resource_version", &self.resource_version.get().ok())
            .finish_non_exhaustive()
    }
}

impl<C: AgentSessionClient> AgentSessionTransport<C> {
    #[must_use]
    pub fn new(
        tenant_id: TenantId,
        signer: AgentRequestSigner,
        client: C,
        reports: Arc<dyn OutboundReportQueue>,
        processor: Arc<dyn AgentMessageProcessor>,
        resource_version: ResourceVersion,
    ) -> Self {
        Self::new_shared(
            tenant_id,
            Arc::new(signer),
            Arc::new(client),
            reports,
            processor,
            SharedSessionFence::default(),
            SharedResourceVersion::new(resource_version),
        )
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new_shared(
        tenant_id: TenantId,
        signer: Arc<AgentRequestSigner>,
        client: Arc<C>,
        reports: Arc<dyn OutboundReportQueue>,
        processor: Arc<dyn AgentMessageProcessor>,
        fence: SharedSessionFence,
        resource_version: SharedResourceVersion,
    ) -> Self {
        Self {
            tenant_id,
            signer,
            client,
            reports,
            processor,
            fence,
            resource_version,
        }
    }

    #[must_use]
    pub fn fence(&self) -> Option<AgentSessionFence> {
        self.fence.get().ok()
    }

    pub fn resource_version(&self) -> AgentDaemonResult<ResourceVersion> {
        self.resource_version.get()
    }

    pub async fn open(
        &mut self,
        signed_at_unix_ms: UnixMillis,
        mut payload: AgentSessionOpenPayload,
    ) -> AgentDaemonResult<AgentSessionBinding> {
        payload.expected_resource_version = self.resource_version.get()?;
        let request = self
            .signer
            .sign(AGENT_SESSION_OPEN_PATH, None, signed_at_unix_ms, payload)
            .map_err(session_error)?;
        let response = retry_session_action(|| self.client.open(&request)).await?;
        if response.agent_id != request.agent_id || response.request_id != request.request_id {
            return Err(AgentDaemonError::Session(
                "session open response identity differs from the request".to_owned(),
            ));
        }
        if response.session_generation.get() == 0
            || response.mount_generation.get() == 0
            || response.owner_generation.get() == 0
            || response.resource_version.get() == 0
        {
            return Err(AgentDaemonError::Session(
                "session open response contains a zero generation or resource version".to_owned(),
            ));
        }
        self.resource_version.replace(response.resource_version)?;
        let fence = AgentSessionFence {
            session_id: response.session_id,
            session_generation: response.session_generation,
        };
        self.fence.replace(Some(fence.clone()))?;
        Ok(AgentSessionBinding {
            fence,
            agent_mount_id: response.agent_mount_id,
            mount_generation: response.mount_generation,
            owner_generation: response.owner_generation,
        })
    }

    pub async fn heartbeat(
        &mut self,
        signed_at_unix_ms: UnixMillis,
        payload: AgentHeartbeatReportPayload,
    ) -> AgentDaemonResult<()> {
        let fence = self.require_fence()?.clone();
        let request = self
            .signer
            .sign(
                AGENT_SESSION_HEARTBEAT_REPORT_PATH,
                Some(&fence),
                signed_at_unix_ms,
                payload,
            )
            .map_err(session_error)?;
        let response = retry_session_action(|| self.client.heartbeat(&request)).await?;
        self.resource_version.replace(response.resource_version)?;
        Ok(())
    }

    /// Sends durable reports oldest-first. A report is deleted only after the center returns a
    /// successful idempotent acknowledgement.
    pub async fn flush_reports(
        &mut self,
        signed_at_unix_ms: UnixMillis,
    ) -> AgentDaemonResult<usize> {
        let fence = self.require_fence()?.clone();
        let queued = self.reports.list(32)?;
        let mut acknowledged = 0;
        for queued_report in queued {
            let envelope = ControlEnvelope {
                protocol_version: ProtocolVersion::V1,
                message_id: queued_report.message_id.clone(),
                session_generation: fence.session_generation,
                resource_version: None,
                request_id: None,
                trace_id: None,
                sent_at_unix_ms: queued_report.enqueued_at_unix_ms,
                message: queued_report.report.into_control_message(),
                extensions: Extensions::new(),
            };
            envelope
                .validate()
                .map_err(|error| AgentDaemonError::Session(error.to_string()))?;
            let request = self
                .signer
                .sign(
                    AGENT_JOB_REPORT_CREATE_PATH,
                    Some(&fence),
                    signed_at_unix_ms,
                    AgentJobReportCreatePayload {
                        tenant_id: self.tenant_id.clone(),
                        report: envelope,
                        extensions: Extensions::new(),
                    },
                )
                .map_err(session_error)?;
            let response = retry_session_action(|| self.client.create_report(&request)).await?;
            // This is the Job resource version, not the Agent registry/session version.
            let _ = response.resource_version;
            if !self.reports.acknowledge(&queued_report.message_id)? {
                return Err(AgentDaemonError::Session(
                    "acknowledged outbound report disappeared from the local queue".to_owned(),
                ));
            }
            acknowledged += 1;
        }
        Ok(acknowledged)
    }

    pub async fn poll_once(&self, signed_at_unix_ms: UnixMillis) -> AgentDaemonResult<usize> {
        let fence = self.require_fence()?.clone();
        let request = self
            .signer
            .sign(
                AGENT_SESSION_MESSAGE_LIST_QUERY_PATH,
                Some(&fence),
                signed_at_unix_ms,
                AgentMessageListQueryPayload {
                    max_messages: 32,
                    extensions: Extensions::new(),
                },
            )
            .map_err(session_error)?;
        let response = retry_session_action(|| self.client.poll(&request)).await?;
        for envelope in &response.messages {
            envelope
                .validate()
                .map_err(|error| AgentDaemonError::Session(error.to_string()))?;
            if envelope.session_generation != fence.session_generation {
                return Err(AgentDaemonError::Session(
                    "center returned a message for another session generation".to_owned(),
                ));
            }
            match &envelope.message {
                ControlMessage::Assignment(assignment) => {
                    self.processor
                        .handle_assignment((**assignment).clone())
                        .await?;
                }
                ControlMessage::Decision(decision) => {
                    self.processor
                        .handle_decision(&self.tenant_id, decision.clone())
                        .await?;
                }
                _ => {
                    return Err(AgentDaemonError::Session(
                        "center poll returned a non-deliverable control message".to_owned(),
                    ));
                }
            }
        }
        Ok(response.messages.len())
    }

    pub async fn close(&mut self, signed_at_unix_ms: UnixMillis) -> AgentDaemonResult<()> {
        let fence = self.require_fence()?.clone();
        let request = self
            .signer
            .sign(
                AGENT_SESSION_CLOSE_PATH,
                Some(&fence),
                signed_at_unix_ms,
                AgentSessionClosePayload {
                    expected_resource_version: self.resource_version.get()?,
                    extensions: Extensions::new(),
                },
            )
            .map_err(session_error)?;
        let response = retry_session_action(|| self.client.close(&request)).await?;
        self.resource_version.replace(response.resource_version)?;
        self.fence.replace(None)?;
        Ok(())
    }

    fn require_fence(&self) -> AgentDaemonResult<AgentSessionFence> {
        self.fence.get()
    }
}

/// Typed action/RPC adapter used only from Agent `spawn_blocking` execution.
pub struct SessionExecutionBridge<C> {
    client: Arc<C>,
    signer: Arc<AgentRequestSigner>,
    fence: SharedSessionFence,
    reports: Arc<dyn OutboundReportQueue>,
    runtime: Handle,
    index_cache: RwLock<BTreeMap<AssignmentKey, AuthoritativeIndexSnapshot>>,
}

impl<C> std::fmt::Debug for SessionExecutionBridge<C> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionExecutionBridge")
            .field("fence", &self.fence)
            .finish_non_exhaustive()
    }
}

impl<C: AgentSessionClient + 'static> SessionExecutionBridge<C> {
    #[must_use]
    pub fn new(
        _tenant_id: TenantId,
        client: Arc<C>,
        signer: Arc<AgentRequestSigner>,
        fence: SharedSessionFence,
        reports: Arc<dyn OutboundReportQueue>,
        runtime: Handle,
    ) -> Self {
        Self {
            client,
            signer,
            fence,
            reports,
            runtime,
            index_cache: RwLock::new(BTreeMap::new()),
        }
    }

    fn signed<T: serde::Serialize>(
        &self,
        path: &'static str,
        payload: T,
    ) -> AgentResult<neoengram_protocol::AgentAuthenticatedRequest<T>> {
        let fence = self.fence.get().map_err(data_plane_error)?;
        self.signer
            .sign(path, Some(&fence), UnixMillis::new(system_now()?), payload)
            .map_err(data_plane_error)
    }

    fn call<T, F>(&self, mut action: F) -> AgentResult<T>
    where
        F: FnMut(
            Arc<C>,
        )
            -> Pin<Box<dyn Future<Output = Result<T, crate::AgentSessionClientError>> + Send>>,
    {
        self.runtime.block_on(async {
            let mut delay = Duration::from_millis(100);
            for attempt in 0..3 {
                match action(Arc::clone(&self.client)).await {
                    Ok(value) => return Ok(value),
                    Err(error) if error.retryable() && attempt < 2 => {
                        tokio::time::sleep(delay).await;
                        delay = delay.saturating_mul(2);
                    }
                    Err(error) => return Err(data_plane_error(error)),
                }
            }
            Err(data_plane_error("Agent action retry loop exhausted"))
        })
    }

    fn load_index(
        &self,
        assignment: &neoengram_protocol::AddAssignment,
    ) -> AgentResult<AuthoritativeIndexSnapshot> {
        self.load_index_snapshot(
            &assignment.tenant_id,
            &assignment.job_id,
            &assignment.artifact_id,
            Some(&assignment.playground_id),
            None,
            &assignment.expected_index_version,
        )
    }

    fn load_index_snapshot(
        &self,
        tenant_id: &TenantId,
        job_id: &JobId,
        artifact_id: &ArtifactId,
        playground_id: Option<&PlaygroundId>,
        snapshot_id: Option<&SnapshotId>,
        index_version: &WireIndexVersion,
    ) -> AgentResult<AuthoritativeIndexSnapshot> {
        let mut page_number = 0_u32;
        let mut page_count = None;
        let mut records = Vec::new();
        loop {
            let request = self.signed(
                AGENT_JOB_INDEX_PAGE_QUERY_PATH,
                AgentIndexPageQueryPayload {
                    tenant_id: tenant_id.clone(),
                    job_id: job_id.clone(),
                    artifact_id: artifact_id.clone(),
                    playground_id: playground_id.cloned(),
                    snapshot_id: snapshot_id.cloned(),
                    index_version: index_version.clone(),
                    page_number,
                    max_records: u16::try_from(MAX_RECORDS_PER_PAGE)
                        .map_err(|_| protocol_data_plane_error("Index page limit exceeds u16"))?,
                    extensions: Extensions::new(),
                },
            )?;
            let response = self.call(|client| {
                let request = request.clone();
                Box::pin(async move { client.query_index_page(&request).await })
            })?;
            if response.protocol_version != ProtocolVersion::V1
                || response.request_id != request.request_id
                || &response.index_version != index_version
                || response.page_number != page_number
                || response.page_count == 0
                || response.records.len() > MAX_RECORDS_PER_PAGE
                || page_count.is_some_and(|count| count != response.page_count)
            {
                return Err(protocol_data_plane_error(
                    "Index page response changed identity or pagination",
                ));
            }
            page_count = Some(response.page_count);
            for record in response.records {
                match record {
                    IndexDeltaRecord::Upsert {
                        path,
                        manifest_id,
                        total_size,
                        chunk_count,
                        ..
                    } => records.push(
                        FileRecord::new(path, manifest_id, total_size.get(), chunk_count.get())
                            .map_err(|error| protocol_data_plane_error(error.to_string()))?,
                    ),
                    IndexDeltaRecord::Delete { .. } => {
                        return Err(protocol_data_plane_error(
                            "authoritative Index snapshot contains a delete record",
                        ));
                    }
                }
            }
            page_number = page_number
                .checked_add(1)
                .ok_or_else(|| protocol_data_plane_error("Index page number overflow"))?;
            if page_number == page_count.unwrap_or_default() {
                break;
            }
            if page_number > page_count.unwrap_or_default() {
                return Err(protocol_data_plane_error(
                    "Index page response exceeded page_count",
                ));
            }
        }
        AuthoritativeIndexSnapshot::new(IndexVersion::from(index_version.clone()), records)
            .map_err(as_protocol_data_plane_error)
    }

    fn load_manifest(
        &self,
        tenant_id: &TenantId,
        job_id: &JobId,
        artifact_id: &ArtifactId,
        manifest_id: ManifestId,
    ) -> AgentResult<Manifest> {
        let mut accumulator = ManifestPageAccumulator::new(manifest_id);
        loop {
            let page_number = accumulator.next_page_number();
            let request = self.signed(
                AGENT_JOB_MANIFEST_PAGE_QUERY_PATH,
                AgentManifestPageQueryPayload {
                    tenant_id: tenant_id.clone(),
                    job_id: job_id.clone(),
                    artifact_id: artifact_id.clone(),
                    manifest_id,
                    page_number,
                    max_chunks: u16::try_from(MAX_RECORDS_PER_PAGE).map_err(|_| {
                        protocol_data_plane_error("Manifest page limit exceeds u16")
                    })?,
                    extensions: Extensions::new(),
                },
            )?;
            let response = self.call(|client| {
                let request = request.clone();
                Box::pin(async move { client.query_manifest_page(&request).await })
            })?;
            if accumulator.push(&request.request_id, response)? {
                return accumulator.finish();
            }
        }
    }

    fn await_report_acknowledgement(&self) -> AgentResult<()> {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            if self.reports.list(1)?.is_empty() {
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                return Err(data_plane_error(
                    "timed out waiting for the control channel to acknowledge durable reports",
                ));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ManifestPageIdentity {
    total_size: u64,
    chunking: ChunkingStrategy,
    page_count: u32,
}

#[derive(Debug)]
struct ManifestPageAccumulator {
    manifest_id: ManifestId,
    identity: Option<ManifestPageIdentity>,
    next_page_number: u32,
    next_offset: u64,
    chunks: Vec<ChunkRef>,
}

impl ManifestPageAccumulator {
    fn new(manifest_id: ManifestId) -> Self {
        Self {
            manifest_id,
            identity: None,
            next_page_number: 0,
            next_offset: 0,
            chunks: Vec::new(),
        }
    }

    const fn next_page_number(&self) -> u32 {
        self.next_page_number
    }

    fn push(
        &mut self,
        expected_request_id: &RequestId,
        response: AgentManifestPageQueryResponse,
    ) -> AgentResult<bool> {
        response
            .validate()
            .map_err(|error| protocol_data_plane_error(error.to_string()))?;
        if &response.request_id != expected_request_id
            || response.manifest_id != self.manifest_id
            || response.page_number != self.next_page_number
        {
            return Err(protocol_data_plane_error(
                "Manifest page response changed request, Manifest, or page identity",
            ));
        }

        let observed = ManifestPageIdentity {
            total_size: response.total_size.get(),
            chunking: response.chunking.into(),
            page_count: response.page_count,
        };
        if self.identity.is_some_and(|identity| {
            identity.total_size != observed.total_size
                || identity.chunking != observed.chunking
                || identity.page_count != observed.page_count
        }) {
            return Err(protocol_data_plane_error(
                "Manifest metadata changed between pages",
            ));
        }
        self.identity.get_or_insert(observed);

        for chunk in response.chunks {
            if chunk.offset.get() != self.next_offset {
                return Err(protocol_data_plane_error(
                    "Manifest chunks are not contiguous across pages",
                ));
            }
            let chunk = ChunkRef::new(chunk.object_id, chunk.offset.get(), chunk.size.get())
                .map_err(|error| protocol_data_plane_error(error.to_string()))?;
            self.next_offset = self
                .next_offset
                .checked_add(chunk.size)
                .ok_or_else(|| protocol_data_plane_error("Manifest chunk coverage exceeds u64"))?;
            self.chunks.push(chunk);
        }
        self.next_page_number = self
            .next_page_number
            .checked_add(1)
            .ok_or_else(|| protocol_data_plane_error("Manifest page number overflow"))?;
        Ok(self.next_page_number == observed.page_count)
    }

    fn finish(self) -> AgentResult<Manifest> {
        let identity = self
            .identity
            .ok_or_else(|| protocol_data_plane_error("Manifest response contained no pages"))?;
        if self.next_page_number != identity.page_count || self.next_offset != identity.total_size {
            return Err(protocol_data_plane_error(
                "Manifest pages do not provide complete byte coverage",
            ));
        }
        let manifest = Manifest::new(identity.total_size, identity.chunking, self.chunks)
            .map_err(|error| protocol_data_plane_error(error.to_string()))?;
        let observed_id = manifest
            .canonical_id()
            .map_err(|error| protocol_data_plane_error(error.to_string()))?;
        if observed_id != self.manifest_id {
            return Err(protocol_data_plane_error(format!(
                "Manifest canonical ID mismatch: requested {}, observed {observed_id}",
                self.manifest_id
            )));
        }
        Ok(manifest)
    }
}

impl<C: AgentSessionClient + 'static> ExecutionBridge for SessionExecutionBridge<C> {
    fn authoritative_index(
        &self,
        assignment: &neoengram_protocol::AddAssignment,
    ) -> AgentResult<AuthoritativeIndexSnapshot> {
        let key = AssignmentKey::from_assignment(assignment);
        if let Some(snapshot) = self
            .index_cache
            .read()
            .map_err(|_| data_plane_error("Index cache lock is poisoned"))?
            .get(&key)
            .cloned()
        {
            return Ok(snapshot);
        }
        let snapshot = self.load_index(assignment)?;
        self.index_cache
            .write()
            .map_err(|_| data_plane_error("Index cache lock is poisoned"))?
            .insert(key, snapshot.clone());
        Ok(snapshot)
    }

    fn workspace_materialization_snapshot(
        &self,
        assignment: &WorkspaceMaterializeAssignment,
    ) -> AgentResult<WorkspaceMaterializationSnapshot> {
        assignment
            .validate()
            .map_err(|error| AgentError::new(AgentErrorCode::ProtocolInvalid, error.to_string()))?;
        let index_version = assignment.base_index_version.as_ref().ok_or_else(|| {
            AgentError::new(
                AgentErrorCode::InvalidAssignment,
                "Workspace materialization snapshot requires a base Index version",
            )
        })?;
        let index = self.load_index_snapshot(
            &assignment.tenant_id,
            &assignment.job_id,
            &assignment.artifact_id,
            Some(&assignment.playground_id),
            None,
            index_version,
        )?;
        let mut manifests = BTreeMap::<ManifestId, Manifest>::new();
        let mut files = Vec::with_capacity(index.records().len());
        for record in index.records() {
            let manifest = if let Some(manifest) = manifests.get(&record.manifest_id) {
                manifest.clone()
            } else {
                let manifest = self.load_manifest(
                    &assignment.tenant_id,
                    &assignment.job_id,
                    &assignment.artifact_id,
                    record.manifest_id,
                )?;
                manifests.insert(record.manifest_id, manifest.clone());
                manifest
            };
            files.push(WorkspaceMaterializationFile {
                record: record.clone(),
                manifest,
            });
        }
        WorkspaceMaterializationSnapshot::new(*index.version(), files)
            .map_err(as_protocol_data_plane_error)
    }

    fn snapshot_mount_snapshot(
        &self,
        assignment: &SnapshotMountAssignment,
    ) -> AgentResult<WorkspaceMaterializationSnapshot> {
        assignment
            .validate()
            .map_err(|error| AgentError::new(AgentErrorCode::ProtocolInvalid, error.to_string()))?;
        let index = self.load_index_snapshot(
            &assignment.tenant_id,
            &assignment.job_id,
            &assignment.artifact_id,
            None,
            Some(&assignment.snapshot_id),
            &assignment.index_version,
        )?;
        let mut manifests = BTreeMap::<ManifestId, Manifest>::new();
        let mut files = Vec::with_capacity(index.records().len());
        for record in index.records() {
            let manifest = if let Some(manifest) = manifests.get(&record.manifest_id) {
                manifest.clone()
            } else {
                let manifest = self.load_manifest(
                    &assignment.tenant_id,
                    &assignment.job_id,
                    &assignment.artifact_id,
                    record.manifest_id,
                )?;
                manifests.insert(record.manifest_id, manifest.clone());
                manifest
            };
            files.push(WorkspaceMaterializationFile {
                record: record.clone(),
                manifest,
            });
        }
        WorkspaceMaterializationSnapshot::new(*index.version(), files)
            .map_err(as_protocol_data_plane_error)
    }

    fn stage_metadata_descriptor(
        &self,
        assignment: &neoengram_protocol::AddAssignment,
        descriptor: &MetadataBatchDescriptor,
    ) -> AgentResult<()> {
        // Prepared is enqueued before ObjectTransfer::stage_metadata. The center rejects metadata
        // until Accepted/Running/Prepared have been observed in durable order.
        self.await_report_acknowledgement()?;
        let request = self.signed(
            AGENT_JOB_METADATA_BATCH_STAGE_PATH,
            AgentMetadataBatchStagePayload {
                tenant_id: assignment.tenant_id.clone(),
                job_id: assignment.job_id.clone(),
                descriptor: descriptor.clone(),
                extensions: Extensions::new(),
            },
        )?;
        let response = self.call(|client| {
            let request = request.clone();
            Box::pin(async move { client.stage_metadata_batch(&request).await })
        })?;
        if response.batch_id != descriptor.batch_id {
            return Err(data_plane_error(
                "metadata descriptor acknowledgement changed batch ID",
            ));
        }
        Ok(())
    }

    fn stage_metadata_page(
        &self,
        assignment: &neoengram_protocol::AddAssignment,
        page: &MetadataBatchPage,
    ) -> AgentResult<()> {
        let request = self.signed(
            AGENT_JOB_METADATA_PAGE_STAGE_PATH,
            AgentMetadataPageStagePayload {
                tenant_id: assignment.tenant_id.clone(),
                job_id: assignment.job_id.clone(),
                page: page.clone(),
                extensions: Extensions::new(),
            },
        )?;
        let response = self.call(|client| {
            let request = request.clone();
            Box::pin(async move { client.stage_metadata_page(&request).await })
        })?;
        if response.batch_id != page.batch_id {
            return Err(data_plane_error(
                "metadata page acknowledgement changed batch ID",
            ));
        }
        Ok(())
    }

    fn now_unix_ms(&self) -> AgentResult<u64> {
        system_now()
    }
}

fn system_now() -> AgentResult<u64> {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| data_plane_error(error.to_string()))?
        .as_millis();
    u64::try_from(millis).map_err(|_| data_plane_error("system timestamp exceeds u64"))
}

fn data_plane_error(error: impl std::fmt::Display) -> AgentError {
    AgentError::new(AgentErrorCode::ObjectTransferFailed, error.to_string())
}

fn protocol_data_plane_error(error: impl std::fmt::Display) -> AgentError {
    AgentError::new(AgentErrorCode::ProtocolInvalid, error.to_string())
}

fn as_protocol_data_plane_error(error: AgentError) -> AgentError {
    protocol_data_plane_error(error.message())
}

fn join_error(error: tokio::task::JoinError) -> AgentDaemonError {
    AgentDaemonError::Session(format!("Agent execution task failed: {error}"))
}

async fn retry_session_action<T, F, Fut>(mut action: F) -> AgentDaemonResult<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, crate::AgentSessionClientError>>,
{
    let mut delay = Duration::from_millis(100);
    for attempt in 0..3 {
        match action().await {
            Ok(value) => return Ok(value),
            Err(error) if error.retryable() && attempt < 2 => {
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2);
            }
            Err(error) => return Err(session_error(error)),
        }
    }
    Err(session_error("Agent action retry loop exhausted"))
}

fn session_error(error: impl std::fmt::Display) -> AgentDaemonError {
    AgentDaemonError::Session(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::Mutex};

    use neoengram_agent::QueuedAgentReport;
    use neoengram_core::{ContentDigest, LogicalPath, ObjectId};
    use neoengram_protocol::{
        AgentId, AgentMountId, ArtifactId, AssignmentGeneration, AssignmentId, MessageId,
        MountGeneration, OwnerGeneration, PlaygroundId, PrincipalId, PrincipalKind, PrincipalRef,
        ProjectId, StorageVolumeId, WireChunkRef, WireChunkingStrategy,
    };
    use tempfile::TempDir;

    use super::*;

    #[derive(Debug, Default)]
    struct RecordingQueue(Mutex<Vec<QueuedAgentReport>>);

    impl RecordingQueue {
        fn reports(&self) -> Vec<AgentReport> {
            self.0
                .lock()
                .unwrap()
                .iter()
                .map(|queued| queued.report.clone())
                .collect()
        }
    }

    impl OutboundReportQueue for RecordingQueue {
        fn enqueue(
            &self,
            report: AgentReport,
            enqueued_at_unix_ms: UnixMillis,
        ) -> AgentResult<QueuedAgentReport> {
            let mut reports = self.0.lock().unwrap();
            let sequence = reports.len() as u64 + 1;
            let queued = QueuedAgentReport {
                sequence,
                message_id: MessageId::new(format!("workspace-report-{sequence}")).unwrap(),
                enqueued_at_unix_ms,
                report,
            };
            reports.push(queued.clone());
            Ok(queued)
        }

        fn list(&self, limit: usize) -> AgentResult<Vec<QueuedAgentReport>> {
            Ok(self.0.lock().unwrap().iter().take(limit).cloned().collect())
        }

        fn acknowledge(&self, message_id: &MessageId) -> AgentResult<bool> {
            let mut reports = self.0.lock().unwrap();
            let Some(index) = reports
                .iter()
                .position(|queued| &queued.message_id == message_id)
            else {
                return Ok(false);
            };
            reports.remove(index);
            Ok(true)
        }
    }

    #[derive(Debug)]
    struct FixedClock;

    impl Clock for FixedClock {
        fn now_unix_ms(&self) -> AgentResult<u64> {
            Ok(1_000)
        }
    }

    #[test]
    fn snapshot_mounted_observation_binds_fresh_timestamp_to_report_and_queue() {
        let queue = RecordingQueue::default();
        let assignment = snapshot_assignment();
        enqueue_snapshot_mounted_observation(
            &queue,
            &FixedClock,
            &assignment,
            crate::SnapshotMountStats {
                files: 3,
                bytes: 42,
                objects: 2,
            },
            neoengram_protocol::SessionGeneration::new(7),
        )
        .unwrap();

        let queued = queue.list(1).unwrap().pop().unwrap();
        assert_eq!(queued.enqueued_at_unix_ms, UnixMillis::new(1_000));
        let AgentReport::Progress(progress) = queued.report else {
            panic!("mounted observation must be JobProgress");
        };
        assert_eq!(progress.phase, "mounted");
        assert_eq!(progress.state, JobState::Succeeded);
        assert_eq!(progress.files_completed.get(), 3);
        assert_eq!(progress.bytes_completed.get(), 42);
        assert_eq!(
            progress.extensions["snapshot_mount_session_generation"],
            serde_json::Value::String("7".to_owned())
        );
        assert_eq!(
            progress.extensions["snapshot_mount_observed_at_unix_ms"],
            serde_json::Value::String("1000".to_owned())
        );
    }

    #[test]
    fn workspace_materialize_reports_terminal_success_in_durable_order() {
        let temporary = TempDir::new().unwrap();
        let mount = temporary.path().join("mount");
        fs::create_dir(&mount).unwrap();
        let queue = RecordingQueue::default();

        let assignment = workspace_assignment(None);
        materialize_workspace_assignment(
            &WorkspaceMaterializer::new(&mount),
            &queue,
            &FixedClock,
            assignment.clone(),
        )
        .unwrap();
        materialize_workspace_assignment(
            &WorkspaceMaterializer::new(&mount),
            &queue,
            &FixedClock,
            assignment,
        )
        .unwrap();

        let reports = queue.reports();
        assert_eq!(reports.len(), 3);
        assert!(matches!(reports[0], AgentReport::Accepted(_)));
        assert!(matches!(
            reports[1],
            AgentReport::Progress(ref report)
                if report.state == JobState::Running && report.phase == "materializing"
        ));
        assert!(matches!(
            reports[2],
            AgentReport::Progress(ref report)
                if report.state == JobState::Succeeded
                    && report.phase == "materialized"
                    && report.files_completed.get() == 0
                    && report.bytes_completed.get() == 0
        ));
        assert!(mount
            .join("playgrounds/project-a/artifact-a/playground-a")
            .is_dir());
    }

    #[test]
    fn workspace_materialize_reports_missing_bridge_as_structured_failure() {
        let temporary = TempDir::new().unwrap();
        let mount = temporary.path().join("mount");
        fs::create_dir(&mount).unwrap();
        let queue = RecordingQueue::default();

        materialize_workspace_assignment(
            &WorkspaceMaterializer::new(&mount),
            &queue,
            &FixedClock,
            workspace_assignment(Some(ContentDigest::from_bytes([0x42; 32]))),
        )
        .unwrap();

        let reports = queue.reports();
        assert!(matches!(reports[0], AgentReport::Accepted(_)));
        assert!(matches!(
            reports[2],
            AgentReport::Failed(ref report)
                if report.error.code.as_str() == "WORKSPACE_MATERIALIZER_INVALID_STATE"
                    && report.final_state == JobState::Failed
        ));
        assert!(!mount.join("playgrounds").exists());
    }

    #[test]
    fn manifest_pages_reassemble_only_with_cross_page_byte_coverage() {
        let first = b"first";
        let second = b"second";
        let manifest = Manifest::new(
            (first.len() + second.len()) as u64,
            ChunkingStrategy::FastCdc,
            vec![
                ChunkRef::new(ObjectId::for_bytes(first), 0, first.len() as u64).unwrap(),
                ChunkRef::new(
                    ObjectId::for_bytes(second),
                    first.len() as u64,
                    second.len() as u64,
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let manifest_id = manifest.canonical_id().unwrap();
        let mut accumulator = ManifestPageAccumulator::new(manifest_id);
        let first_request = RequestId::new("manifest-page-first").unwrap();
        let second_request = RequestId::new("manifest-page-second").unwrap();

        assert!(!accumulator
            .push(
                &first_request,
                manifest_response(
                    first_request.clone(),
                    manifest_id,
                    manifest.total_size,
                    0,
                    2,
                    vec![WireChunkRef {
                        object_id: ObjectId::for_bytes(first),
                        offset: DecimalU64::new(0),
                        size: DecimalU64::new(first.len() as u64),
                        extensions: Extensions::new(),
                    }],
                ),
            )
            .unwrap());
        assert!(accumulator
            .push(
                &second_request,
                manifest_response(
                    second_request.clone(),
                    manifest_id,
                    manifest.total_size,
                    1,
                    2,
                    vec![WireChunkRef {
                        object_id: ObjectId::for_bytes(second),
                        offset: DecimalU64::new(first.len() as u64),
                        size: DecimalU64::new(second.len() as u64),
                        extensions: Extensions::new(),
                    }],
                ),
            )
            .unwrap());
        assert_eq!(accumulator.finish().unwrap(), manifest);
    }

    #[test]
    fn manifest_pages_reject_a_cross_page_gap_even_when_each_page_is_valid() {
        let manifest_id = ManifestId::from_bytes([0x44; 32]);
        let mut accumulator = ManifestPageAccumulator::new(manifest_id);
        let first_request = RequestId::new("manifest-gap-first").unwrap();
        let second_request = RequestId::new("manifest-gap-second").unwrap();
        accumulator
            .push(
                &first_request,
                manifest_response(
                    first_request.clone(),
                    manifest_id,
                    10,
                    0,
                    2,
                    vec![WireChunkRef {
                        object_id: ObjectId::from_bytes([1; 32]),
                        offset: DecimalU64::new(0),
                        size: DecimalU64::new(4),
                        extensions: Extensions::new(),
                    }],
                ),
            )
            .unwrap();
        let error = accumulator
            .push(
                &second_request,
                manifest_response(
                    second_request.clone(),
                    manifest_id,
                    10,
                    1,
                    2,
                    vec![WireChunkRef {
                        object_id: ObjectId::from_bytes([2; 32]),
                        offset: DecimalU64::new(5),
                        size: DecimalU64::new(5),
                        extensions: Extensions::new(),
                    }],
                ),
            )
            .unwrap_err();
        assert_eq!(error.code(), AgentErrorCode::ProtocolInvalid);
        assert!(error.message().contains("across pages"));
    }

    #[test]
    fn manifest_pages_reject_a_noncanonical_manifest_identity() {
        let requested_id = ManifestId::from_bytes([0x55; 32]);
        let request_id = RequestId::new("manifest-id-mismatch").unwrap();
        let payload = b"payload";
        let mut accumulator = ManifestPageAccumulator::new(requested_id);
        assert!(accumulator
            .push(
                &request_id,
                manifest_response(
                    request_id.clone(),
                    requested_id,
                    payload.len() as u64,
                    0,
                    1,
                    vec![WireChunkRef {
                        object_id: ObjectId::for_bytes(payload),
                        offset: DecimalU64::new(0),
                        size: DecimalU64::new(payload.len() as u64),
                        extensions: Extensions::new(),
                    }],
                ),
            )
            .unwrap());
        let error = accumulator.finish().unwrap_err();
        assert_eq!(error.code(), AgentErrorCode::ProtocolInvalid);
        assert!(error.message().contains("canonical ID mismatch"));
    }

    fn manifest_response(
        request_id: RequestId,
        manifest_id: ManifestId,
        total_size: u64,
        page_number: u32,
        page_count: u32,
        chunks: Vec<WireChunkRef>,
    ) -> AgentManifestPageQueryResponse {
        AgentManifestPageQueryResponse {
            protocol_version: ProtocolVersion::V1,
            request_id,
            manifest_id,
            total_size: DecimalU64::new(total_size),
            chunking: WireChunkingStrategy::FastCdc,
            page_number,
            page_count,
            chunks,
            extensions: Extensions::new(),
        }
    }

    fn workspace_assignment(
        base_commit_id: Option<ContentDigest>,
    ) -> WorkspaceMaterializeAssignment {
        let project_id = ProjectId::new("project-a").unwrap();
        let artifact_id = ArtifactId::new("artifact-a").unwrap();
        let playground_id = PlaygroundId::new("playground-a").unwrap();
        let mut assignment = WorkspaceMaterializeAssignment {
            job_id: neoengram_protocol::JobId::new("job-materialize-a").unwrap(),
            assignment_id: AssignmentId::new("assignment-materialize-a").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            agent_id: AgentId::new("agent-a").unwrap(),
            principal: PrincipalRef {
                kind: PrincipalKind::System,
                id: PrincipalId::new("system-a").unwrap(),
                extensions: Extensions::new(),
            },
            tenant_id: TenantId::new("tenant-a").unwrap(),
            project_id,
            artifact_id,
            playground_id,
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            agent_mount_id: AgentMountId::new("mount-a").unwrap(),
            mount_generation: MountGeneration::new(1),
            owner_generation: OwnerGeneration::new(1),
            relative_root: LogicalPath::parse("playgrounds/project-a/artifact-a/playground-a")
                .unwrap(),
            base_commit_id,
            base_index_version: base_commit_id
                .map(|_| WireIndexVersion::from(IndexVersion::from_snapshot(1, &[]).unwrap())),
            request_digest: ContentDigest::from_bytes([0; 32]),
            deadline_unix_ms: UnixMillis::new(u64::MAX),
            extensions: Extensions::new(),
        };
        assignment.request_digest = assignment.computed_request_digest().unwrap();
        assignment
    }

    fn snapshot_assignment() -> SnapshotMountAssignment {
        let project_id = ProjectId::new("project-a").unwrap();
        let artifact_id = ArtifactId::new("artifact-a").unwrap();
        let snapshot_id = SnapshotId::new("snapshot-a").unwrap();
        let relative_root = SnapshotMountAssignment::canonical_relative_root(
            &project_id,
            &artifact_id,
            &snapshot_id,
        )
        .unwrap();
        let mut assignment = SnapshotMountAssignment {
            job_id: JobId::new("job-snapshot-a").unwrap(),
            assignment_id: AssignmentId::new("assignment-snapshot-a").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            agent_id: AgentId::new("agent-a").unwrap(),
            principal: PrincipalRef {
                kind: PrincipalKind::System,
                id: PrincipalId::new("system-a").unwrap(),
                extensions: Extensions::new(),
            },
            tenant_id: TenantId::new("tenant-a").unwrap(),
            project_id,
            artifact_id,
            snapshot_id,
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            agent_mount_id: AgentMountId::new("mount-a").unwrap(),
            mount_generation: MountGeneration::new(1),
            owner_generation: OwnerGeneration::new(1),
            relative_root,
            commit_id: ContentDigest::from_bytes([0x11; 32]),
            index_version: IndexVersion::from_snapshot(1, &[]).unwrap().into(),
            request_digest: ContentDigest::from_bytes([0; 32]),
            deadline_unix_ms: UnixMillis::new(u64::MAX),
            extensions: Extensions::new(),
        };
        assignment.request_digest = assignment.computed_request_digest().unwrap();
        assignment
    }
}
