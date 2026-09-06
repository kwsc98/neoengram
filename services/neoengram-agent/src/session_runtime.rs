use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    time::Duration,
};

use crate::{
    Agent, AgentAssignmentState, AgentError, AgentErrorCode, AgentReport, AgentResult,
    AssignmentKey, Clock, LedgerRecord, LifecycleJournal, OutboundReportQueue,
    SingleVolumeAgentConfig,
};
use async_trait::async_trait;
use neoengram_domain::core::{
    ChunkRef, ChunkingStrategy, FileRecord, IndexVersion, Manifest, ManifestId,
};
use neoengram_domain::protocol::{
    new_control_envelope, AgentHeartbeatReportPayload, AgentIndexPageQueryPayload,
    AgentJobReportCreatePayload, AgentManifestPageQueryPayload, AgentManifestPageQueryResponse,
    AgentMetadataBatchStagePayload, AgentMetadataPageStagePayload,
    AgentResourceLifecycleAssignment, AgentSessionClosePayload, AgentSessionOpenPayload,
    ArtifactId, ControlError, DecimalU64, ErrorCode, Extensions, IndexDeltaRecord, IndexRevision,
    JobAccepted, JobAssignment, JobDecision, JobFailed, JobFailureStage, JobId, JobProgress,
    JobState, MaterializationAssignment, MetadataBatchDescriptor, MetadataBatchPage,
    MountGeneration, OwnerGeneration, ReplicationAssignment, ReplicationId, ReplicationState,
    RequestId, ResourceLifecycleReport, ResourceLifecycleReportState, ResourceVersion,
    SessionGeneration, SnapshotId, TenantId, TraceId, UnixMillis, WireIndexVersion, WorkspaceId,
    WorkspaceMaterializeAssignment, AGENT_JOB_INDEX_PAGE_QUERY_PATH,
    AGENT_JOB_MANIFEST_PAGE_QUERY_PATH, AGENT_JOB_METADATA_BATCH_STAGE_PATH,
    AGENT_JOB_METADATA_PAGE_STAGE_PATH, AGENT_JOB_REPORT_CREATE_PATH, AGENT_SESSION_CLOSE_PATH,
    AGENT_SESSION_HEARTBEAT_REPORT_PATH, AGENT_SESSION_OPEN_PATH, CURRENT_WIRE_VERSION,
    MAX_RECORDS_PER_PAGE,
};
use tokio::runtime::Handle;

const SESSION_DATA_PLANE_MAX_RETRY_DELAY: Duration = Duration::from_secs(5);

use crate::{
    resource_lifecycle::{LifecycleJobGate, ResourceLifecycleExecutor},
    AgentDaemonError, AgentDaemonResult, AgentRequestSigner, AgentSessionClient, AgentSessionFence,
    AuthoritativeIndexSnapshot, CentralCommandTrustBundle, DurableReplicationProgressSink,
    ExecutionBridge, MaterializationAssignmentExecutor, ReplicationAssignmentExecutor,
    ReplicationProgressSink, SnapshotDeliveryMaterializer, SnapshotDeliveryMountManager,
    WorkspaceMaterializationFile, WorkspaceMaterializationSnapshot, WorkspaceMaterializer,
};

#[async_trait]
pub trait AgentMessageProcessor: Send + Sync {
    async fn handle_assignment(&self, assignment: JobAssignment) -> AgentDaemonResult<()>;
    async fn handle_replication(
        &self,
        _assignment: ReplicationAssignment,
    ) -> AgentDaemonResult<()> {
        Err(AgentDaemonError::Session(
            "replication assignments are not supported by this Agent processor".to_owned(),
        ))
    }
    async fn handle_materialization(
        &self,
        _assignment: MaterializationAssignment,
    ) -> AgentDaemonResult<()> {
        Err(AgentDaemonError::Session(
            "materialization assignments are not supported by this Agent processor".to_owned(),
        ))
    }
    async fn handle_lifecycle_assignment(
        &self,
        assignment: AgentResourceLifecycleAssignment,
    ) -> AgentDaemonResult<()>;
    async fn recover_assignment(&self, record: LedgerRecord) -> AgentDaemonResult<()>;
    async fn handle_decision(
        &self,
        tenant_id: &TenantId,
        decision: JobDecision,
    ) -> AgentDaemonResult<()>;
}

pub struct CoreAgentMessageProcessor {
    agent: Arc<Agent>,
    workspace_materializer: Option<Arc<WorkspaceMaterializer>>,
    snapshot_delivery_mounts: Option<Arc<SnapshotDeliveryMountManager>>,
    snapshot_delivery_materializer: Option<Arc<SnapshotDeliveryMaterializer>>,
    reports: Option<Arc<dyn OutboundReportQueue>>,
    clock: Option<Arc<dyn Clock>>,
    lifecycle_journal: Option<Arc<dyn LifecycleJournal>>,
    lifecycle_binding: Option<(SingleVolumeAgentConfig, SessionGeneration)>,
    lifecycle_executor: Option<Arc<dyn ResourceLifecycleExecutor>>,
    replication_executor: Option<Arc<dyn ReplicationAssignmentExecutor>>,
    materialization_executor: Option<Arc<dyn MaterializationAssignmentExecutor>>,
    lifecycle_job_gate: Arc<LifecycleJobGate>,
}

impl CoreAgentMessageProcessor {
    #[must_use]
    pub fn new(agent: Arc<Agent>) -> Self {
        Self {
            agent,
            workspace_materializer: None,
            snapshot_delivery_mounts: None,
            snapshot_delivery_materializer: None,
            reports: None,
            clock: None,
            lifecycle_journal: None,
            lifecycle_binding: None,
            lifecycle_executor: None,
            replication_executor: None,
            materialization_executor: None,
            lifecycle_job_gate: Arc::new(LifecycleJobGate::default()),
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
            snapshot_delivery_mounts: None,
            snapshot_delivery_materializer: None,
            reports: Some(reports),
            clock: Some(clock),
            lifecycle_journal: None,
            lifecycle_binding: None,
            lifecycle_executor: None,
            replication_executor: None,
            materialization_executor: None,
            lifecycle_job_gate: Arc::new(LifecycleJobGate::default()),
        }
    }

    #[must_use]
    pub fn with_materializers(
        agent: Arc<Agent>,
        workspace_materializer: Arc<WorkspaceMaterializer>,
        reports: Arc<dyn OutboundReportQueue>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            agent,
            workspace_materializer: Some(workspace_materializer),
            snapshot_delivery_mounts: None,
            snapshot_delivery_materializer: None,
            reports: Some(reports),
            clock: Some(clock),
            lifecycle_journal: None,
            lifecycle_binding: None,
            lifecycle_executor: None,
            replication_executor: None,
            materialization_executor: None,
            lifecycle_job_gate: Arc::new(LifecycleJobGate::default()),
        }
    }

    #[must_use]
    pub fn with_lifecycle_journal(mut self, journal: Arc<dyn LifecycleJournal>) -> Self {
        self.lifecycle_journal = Some(journal);
        self
    }

    #[must_use]
    pub fn with_snapshot_delivery_materializer(
        mut self,
        materializer: Arc<SnapshotDeliveryMaterializer>,
    ) -> Self {
        self.snapshot_delivery_materializer = Some(materializer);
        self
    }

    #[must_use]
    pub fn with_snapshot_delivery_mounts(
        mut self,
        mounts: Arc<SnapshotDeliveryMountManager>,
    ) -> Self {
        self.snapshot_delivery_mounts = Some(mounts);
        self
    }

    #[must_use]
    pub fn with_lifecycle_binding(
        mut self,
        volume: SingleVolumeAgentConfig,
        session_generation: SessionGeneration,
    ) -> Self {
        self.lifecycle_binding = Some((volume, session_generation));
        self
    }

    #[must_use]
    pub(crate) fn with_lifecycle_executor(
        mut self,
        executor: Arc<dyn ResourceLifecycleExecutor>,
    ) -> Self {
        self.lifecycle_executor = Some(executor);
        self
    }

    #[must_use]
    pub fn with_replication_executor(
        mut self,
        executor: Arc<dyn ReplicationAssignmentExecutor>,
    ) -> Self {
        self.replication_executor = Some(executor);
        self
    }

    #[must_use]
    pub fn with_materialization_executor(
        mut self,
        executor: Arc<dyn MaterializationAssignmentExecutor>,
    ) -> Self {
        self.materialization_executor = Some(executor);
        self
    }
}

#[async_trait]
impl AgentMessageProcessor for CoreAgentMessageProcessor {
    async fn handle_assignment(&self, assignment: JobAssignment) -> AgentDaemonResult<()> {
        let _job_guard = self
            .lifecycle_job_gate
            .begin(&assignment)
            .map_err(AgentDaemonError::from)?;
        if let Some(executor) = &self.lifecycle_executor {
            executor
                .validate_job_admission(&assignment)
                .map_err(AgentDaemonError::from)?;
        }
        match assignment.assignment {
            neoengram_domain::protocol::AssignmentOperation::Add { input, .. } => {
                let agent = Arc::clone(&self.agent);
                tokio::task::spawn_blocking(move || agent.handle_assignment(input))
                    .await
                    .map_err(join_error)?
                    .map(|_| ())
                    .map_err(Into::into)
            }
            neoengram_domain::protocol::AssignmentOperation::WorkspaceMaterialize {
                input, ..
            } => {
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
            neoengram_domain::protocol::AssignmentOperation::SnapshotDelivery { input, .. } => {
                let reports = self.reports.clone().ok_or_else(|| {
                    AgentDaemonError::Session(
                        "Snapshot delivery assignment received without a durable report queue"
                            .to_owned(),
                    )
                })?;
                let clock = self.clock.clone().ok_or_else(|| {
                    AgentDaemonError::Session(
                        "Snapshot delivery assignment received without a clock".to_owned(),
                    )
                })?;
                if input.action == neoengram_domain::protocol::SnapshotDeliveryAction::Delete {
                    let manager = self.snapshot_delivery_mounts.clone();
                    let materializer = self.snapshot_delivery_materializer.clone();
                    return tokio::task::spawn_blocking(move || {
                        delete_snapshot_delivery_assignment(
                            manager.as_deref(),
                            materializer.as_deref(),
                            reports.as_ref(),
                            clock.as_ref(),
                            input,
                        )
                    })
                    .await
                    .map_err(join_error)?;
                }
                if input.mode == neoengram_domain::protocol::SnapshotDeliveryMode::Fuse {
                    let manager = self.snapshot_delivery_mounts.clone().ok_or_else(|| {
                        AgentDaemonError::Session(
                            "FUSE SnapshotDelivery assignment received before its mount manager was configured"
                                .to_owned(),
                        )
                    })?;
                    tokio::task::spawn_blocking(move || {
                        mount_snapshot_delivery_assignment(
                            &manager,
                            reports.as_ref(),
                            clock.as_ref(),
                            input,
                        )
                    })
                    .await
                    .map_err(join_error)?
                } else {
                    let materializer = self.snapshot_delivery_materializer.clone().ok_or_else(|| {
                        AgentDaemonError::Session(
                            "Snapshot delivery assignment received before the materializer was configured"
                                .to_owned(),
                        )
                    })?;
                    tokio::task::spawn_blocking(move || {
                        materialize_snapshot_delivery_assignment(
                            &materializer,
                            reports.as_ref(),
                            clock.as_ref(),
                            input,
                        )
                    })
                    .await
                    .map_err(join_error)?
                }
            }
        }
    }

    async fn handle_replication(&self, assignment: ReplicationAssignment) -> AgentDaemonResult<()> {
        let executor = self.replication_executor.clone().ok_or_else(|| {
            AgentDaemonError::Session(
                "replication assignment received before the data-plane executor was configured"
                    .to_owned(),
            )
        })?;
        let reports = self.reports.clone().ok_or_else(|| {
            AgentDaemonError::Session(
                "replication assignment received without a durable report queue".to_owned(),
            )
        })?;
        let clock = self.clock.clone().ok_or_else(|| {
            AgentDaemonError::Session("replication assignment received without a clock".to_owned())
        })?;
        tokio::task::spawn_blocking(move || {
            assignment
                .validate()
                .map_err(|error| AgentDaemonError::Session(error.to_string()))?;
            let progress = DurableReplicationProgressSink::new(assignment.clone(), reports, clock);
            let result = executor.execute(&assignment, &progress);
            settle_replication_execution(&assignment.replication_id, &progress, result)
        })
        .await
        .map_err(join_error)?
    }

    async fn handle_materialization(
        &self,
        assignment: MaterializationAssignment,
    ) -> AgentDaemonResult<()> {
        let executor = self.materialization_executor.clone().ok_or_else(|| {
            AgentDaemonError::Session(
                "materialization assignment received before the data-plane executor was configured"
                    .to_owned(),
            )
        })?;
        tokio::task::spawn_blocking(move || {
            assignment
                .validate()
                .map_err(|error| AgentDaemonError::Session(error.to_string()))?;
            executor.execute(&assignment)
        })
        .await
        .map_err(join_error)?
    }

    async fn handle_lifecycle_assignment(
        &self,
        assignment: AgentResourceLifecycleAssignment,
    ) -> AgentDaemonResult<()> {
        let journal = self.lifecycle_journal.clone().ok_or_else(|| {
            AgentDaemonError::Session(
                "resource lifecycle assignment received before its durable journal was configured"
                    .to_owned(),
            )
        })?;
        let reports = self.reports.clone().ok_or_else(|| {
            AgentDaemonError::Session(
                "resource lifecycle assignment received without a durable report queue".to_owned(),
            )
        })?;
        let clock = self.clock.clone().ok_or_else(|| {
            AgentDaemonError::Session(
                "resource lifecycle assignment received without a clock".to_owned(),
            )
        })?;
        let executor = self.lifecycle_executor.clone().ok_or_else(|| {
            AgentDaemonError::Session(
                "resource lifecycle assignment received without a physical executor".to_owned(),
            )
        })?;
        let job_gate = Arc::clone(&self.lifecycle_job_gate);
        let lifecycle_binding = self.lifecycle_binding.clone();
        tokio::task::spawn_blocking(move || {
            let now = UnixMillis::new(clock.now_unix_ms().map_err(agent_error)?);
            if let Some((volume, session_generation)) = &lifecycle_binding {
                volume
                    .validate_lifecycle_assignment(&assignment, *session_generation, now)
                    .map_err(AgentDaemonError::from)?;
            } else {
                assignment.validate_at(now).map_err(protocol_error)?;
            }
            let outcome = journal
                .claim(&assignment, now)
                .map_err(AgentDaemonError::from)?;
            let record = match outcome {
                crate::LifecycleClaimOutcome::Claimed(record)
                | crate::LifecycleClaimOutcome::Existing(record) => record,
            };
            if let Some(report) = record.report {
                if matches!(report.state, ResourceLifecycleReportState::Restored) {
                    job_gate
                        .restore(&assignment)
                        .map_err(AgentDaemonError::from)?;
                }
                reports
                    .enqueue(
                        AgentReport::Lifecycle(Box::new(report.clone())),
                        report.reported_at_unix_ms,
                    )
                    .map_err(AgentDaemonError::from)?;
                return Ok(());
            }

            let accepted = ResourceLifecycleReport::accepted(&assignment, record.claimed_at_unix_ms);
            accepted.validate().map_err(protocol_error)?;
            reports
                .enqueue(
                    AgentReport::Lifecycle(Box::new(accepted)),
                    record.claimed_at_unix_ms,
                )
                .map_err(AgentDaemonError::from)?;

            let active_jobs = job_gate
                .fence(&assignment)
                .map_err(AgentDaemonError::from)?;
            let execution = executor.prepare_fence(&assignment).and_then(|()| {
                if active_jobs == 0 {
                    executor.execute(&assignment, now)
                } else {
                    Err(AgentError::new(
                        AgentErrorCode::InvalidState,
                        format!(
                            "{active_jobs} matching Agent Job(s) remain active after the lifecycle fence"
                        ),
                    ))
                }
            });
            let terminal = match execution {
                Ok(report) => report,
                Err(error) => lifecycle_error_report(&assignment, now, error)?,
            };
            journal
                .complete(terminal.clone())
                .map_err(AgentDaemonError::from)?;
            reports
                .enqueue(AgentReport::Lifecycle(Box::new(terminal.clone())), now)
                .map_err(AgentDaemonError::from)?;
            if matches!(terminal.state, ResourceLifecycleReportState::Restored) {
                job_gate
                    .restore(&assignment)
                    .map_err(AgentDaemonError::from)?;
            }
            Ok(())
        })
        .await
        .map_err(join_error)?
    }

    async fn recover_assignment(&self, record: LedgerRecord) -> AgentDaemonResult<()> {
        let delivery = JobAssignment {
            assignment: neoengram_domain::protocol::AssignmentOperation::Add {
                input: record.assignment.clone(),
                extensions: Extensions::new(),
            },
            extensions: Extensions::new(),
        };
        let _job_guard = self
            .lifecycle_job_gate
            .begin(&delivery)
            .map_err(AgentDaemonError::from)?;
        if let Some(executor) = &self.lifecycle_executor {
            executor
                .validate_job_admission(&delivery)
                .map_err(AgentDaemonError::from)?;
        }
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

fn lifecycle_error_report(
    assignment: &AgentResourceLifecycleAssignment,
    now_unix_ms: UnixMillis,
    error: AgentError,
) -> AgentDaemonResult<ResourceLifecycleReport> {
    let retryable = matches!(
        error.code(),
        AgentErrorCode::MountUnavailable
            | AgentErrorCode::InvalidState
            | AgentErrorCode::ExecutionFailed
            | AgentErrorCode::ObjectTransferFailed
            | AgentErrorCode::ReportFailed
    );
    let mut report = ResourceLifecycleReport::accepted(assignment, now_unix_ms);
    report.state = if retryable {
        ResourceLifecycleReportState::Blocked
    } else {
        ResourceLifecycleReportState::Failed
    };
    report.error = Some(ControlError {
        code: ErrorCode::new(error.stable_code()).map_err(protocol_error)?,
        message: error.message().chars().take(4_096).collect(),
        retryable,
        retry_after_ms: retryable.then_some(DecimalU64::new(1_000)),
        extensions: Extensions::new(),
    });
    report
        .validate_for_assignment(assignment)
        .map_err(protocol_error)?;
    Ok(report)
}

/// Executes one Workspace materialize assignment and persists compatible job reports. The
/// operation is deliberately terminal without a central decision: it consumes immutable metadata
/// and Volume-local objects but has no object/index publication to finalize. Replaying the
/// assignment is safe because the filesystem materializer verifies existing components and the
/// report queue inserts by deterministic report identity where possible.
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
        let (job_id, assignment_id, generation, task_fence) = match &queued.report {
            AgentReport::Accepted(report) => (
                &report.job_id,
                &report.assignment_id,
                report.assignment_generation,
                &report.task_fence,
            ),
            AgentReport::Progress(report) => (
                &report.job_id,
                &report.assignment_id,
                report.assignment_generation,
                &report.task_fence,
            ),
            AgentReport::Prepared(report) => (
                &report.job_id,
                &report.assignment_id,
                report.assignment_generation,
                &report.task_fence,
            ),
            AgentReport::Finalized(report) => (
                &report.job_id,
                &report.assignment_id,
                report.assignment_generation,
                &report.task_fence,
            ),
            AgentReport::Failed(report) => (
                &report.job_id,
                &report.assignment_id,
                report.assignment_generation,
                &report.task_fence,
            ),
            AgentReport::Lifecycle(_)
            | AgentReport::Replication(_)
            | AgentReport::Materialization(_)
            | AgentReport::Integrity(_) => continue,
        };
        if job_id != &assignment.job_id {
            continue;
        }
        if assignment_id != &assignment.assignment_id
            || generation != assignment.assignment_generation
            || task_fence != &assignment.task_fence
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
                state.running = true
            }
            AgentReport::Progress(report) if report.state == JobState::Succeeded => {
                state.terminal = true
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
                task_fence: assignment.task_fence.clone(),
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
                task_fence: assignment.task_fence.clone(),
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
                    task_fence: assignment.task_fence.clone(),
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
            task_fence: assignment.task_fence.clone(),
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

fn delete_snapshot_delivery_assignment(
    manager: Option<&SnapshotDeliveryMountManager>,
    materializer: Option<&SnapshotDeliveryMaterializer>,
    reports: &dyn OutboundReportQueue,
    clock: &dyn Clock,
    assignment: neoengram_domain::protocol::SnapshotDeliveryAssignment,
) -> AgentDaemonResult<()> {
    assignment.validate().map_err(protocol_error)?;
    let queued_state = queued_delivery_state(reports, &assignment)?;
    if queued_state.terminal {
        return Ok(());
    }
    let accepted_at = UnixMillis::new(clock.now_unix_ms().map_err(agent_error)?);
    if !queued_state.has_report {
        enqueue_workspace_report(
            reports,
            AgentReport::Accepted(JobAccepted {
                task_fence: assignment.task_fence.clone(),
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
                task_fence: assignment.task_fence.clone(),
                job_id: assignment.job_id.clone(),
                assignment_id: assignment.assignment_id.clone(),
                assignment_generation: assignment.assignment_generation,
                state: JobState::Running,
                phase: "deleting".to_owned(),
                files_completed: DecimalU64::new(0),
                bytes_completed: DecimalU64::new(0),
                retry_after_ms: None,
                extensions: Extensions::new(),
            }),
            UnixMillis::new(clock.now_unix_ms().map_err(agent_error)?),
        )?;
    }
    let result = if assignment.mode == neoengram_domain::protocol::SnapshotDeliveryMode::Fuse {
        manager
            .ok_or_else(|| {
                AgentError::new(
                    AgentErrorCode::InvalidState,
                    "FUSE Delivery manager is unavailable",
                )
            })?
            .delete(&assignment)
    } else {
        materializer
            .ok_or_else(|| {
                AgentError::new(
                    AgentErrorCode::InvalidState,
                    "Delivery materializer is unavailable",
                )
            })?
            .delete(&assignment)
    };
    if let Err(error) = result {
        enqueue_workspace_report(
            reports,
            AgentReport::Failed(JobFailed {
                task_fence: assignment.task_fence.clone(),
                tenant_id: assignment.tenant_id,
                job_id: assignment.job_id,
                assignment_id: assignment.assignment_id,
                assignment_generation: assignment.assignment_generation,
                final_state: JobState::Failed,
                failed_at_unix_ms: UnixMillis::new(clock.now_unix_ms().map_err(agent_error)?),
                stage: JobFailureStage::Execution,
                error: ControlError {
                    code: ErrorCode::new("DELIVERY_DELETE_FAILED").map_err(protocol_error)?,
                    message: error.message().to_owned(),
                    retryable: true,
                    retry_after_ms: Some(DecimalU64::new(1_000)),
                    extensions: Extensions::new(),
                },
                extensions: Extensions::new(),
            }),
            accepted_at,
        )?;
        return Ok(());
    }
    enqueue_workspace_report(
        reports,
        AgentReport::Progress(JobProgress {
            task_fence: assignment.task_fence.clone(),
            job_id: assignment.job_id,
            assignment_id: assignment.assignment_id,
            assignment_generation: assignment.assignment_generation,
            state: JobState::Succeeded,
            phase: "deleted".to_owned(),
            files_completed: DecimalU64::new(0),
            bytes_completed: DecimalU64::new(0),
            retry_after_ms: None,
            extensions: Extensions::new(),
        }),
        UnixMillis::new(clock.now_unix_ms().map_err(agent_error)?),
    )?;
    Ok(())
}

fn mount_snapshot_delivery_assignment(
    manager: &SnapshotDeliveryMountManager,
    reports: &dyn OutboundReportQueue,
    clock: &dyn Clock,
    assignment: neoengram_domain::protocol::SnapshotDeliveryAssignment,
) -> AgentDaemonResult<()> {
    assignment.validate().map_err(protocol_error)?;
    manager
        .validate_assignment(&assignment)
        .map_err(agent_error)?;
    let queued_state = queued_delivery_state(reports, &assignment)?;
    if queued_state.terminal {
        return Ok(());
    }
    let accepted_at = UnixMillis::new(clock.now_unix_ms().map_err(agent_error)?);
    if !queued_state.has_report {
        enqueue_workspace_report(
            reports,
            AgentReport::Accepted(JobAccepted {
                task_fence: assignment.task_fence.clone(),
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
                task_fence: assignment.task_fence.clone(),
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
            let code = snapshot_delivery_error_code(
                &error,
                "DELIVERY_FUSE_MOUNT_UNAVAILABLE",
                "DELIVERY_FUSE_MOUNT_FAILED",
            );
            enqueue_workspace_report(
                reports,
                AgentReport::Failed(JobFailed {
                    task_fence: assignment.task_fence.clone(),
                    tenant_id: assignment.tenant_id,
                    job_id: assignment.job_id,
                    assignment_id: assignment.assignment_id,
                    assignment_generation: assignment.assignment_generation,
                    final_state: JobState::Failed,
                    failed_at_unix_ms: UnixMillis::new(clock.now_unix_ms().map_err(agent_error)?),
                    stage: JobFailureStage::Execution,
                    error: ControlError {
                        code: ErrorCode::new(code).map_err(protocol_error)?,
                        message: error.message().to_owned(),
                        retryable: !snapshot_delivery_error_is_permanent(code)
                            && matches!(
                                error.code(),
                                AgentErrorCode::MountUnavailable
                                    | AgentErrorCode::ObjectTransferFailed
                            ),
                        retry_after_ms: None,
                        extensions: Extensions::new(),
                    },
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
            task_fence: assignment.task_fence.clone(),
            job_id: assignment.job_id,
            assignment_id: assignment.assignment_id,
            assignment_generation: assignment.assignment_generation,
            state: JobState::Succeeded,
            phase: "mounted".to_owned(),
            files_completed: DecimalU64::new(stats.files),
            bytes_completed: DecimalU64::new(stats.bytes),
            retry_after_ms: None,
            extensions: Extensions::new(),
        }),
        UnixMillis::new(clock.now_unix_ms().map_err(agent_error)?),
    )?;
    Ok(())
}

/// Executes one Copy/Hardlink SnapshotDelivery and persists the same idempotent Job reports used
/// by the central Assignment outbox.
fn materialize_snapshot_delivery_assignment(
    materializer: &SnapshotDeliveryMaterializer,
    reports: &dyn OutboundReportQueue,
    clock: &dyn Clock,
    assignment: neoengram_domain::protocol::SnapshotDeliveryAssignment,
) -> AgentDaemonResult<()> {
    assignment.validate().map_err(protocol_error)?;
    let queued_state = queued_delivery_state(reports, &assignment)?;
    if queued_state.terminal {
        return Ok(());
    }
    let accepted_at = UnixMillis::new(clock.now_unix_ms().map_err(agent_error)?);
    if !queued_state.has_report {
        enqueue_workspace_report(
            reports,
            AgentReport::Accepted(JobAccepted {
                task_fence: assignment.task_fence.clone(),
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
                task_fence: assignment.task_fence.clone(),
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
    if assignment.mode == neoengram_domain::protocol::SnapshotDeliveryMode::Fuse {
        let error = ControlError {
            code: ErrorCode::new("DELIVERY_FUSE_UNSUPPORTED").map_err(protocol_error)?,
            message: "FUSE SnapshotDelivery requires a delivery-specific FUSE descriptor"
                .to_owned(),
            retryable: false,
            retry_after_ms: None,
            extensions: Extensions::new(),
        };
        enqueue_workspace_report(
            reports,
            AgentReport::Failed(JobFailed {
                task_fence: assignment.task_fence.clone(),
                tenant_id: assignment.tenant_id,
                job_id: assignment.job_id,
                assignment_id: assignment.assignment_id,
                assignment_generation: assignment.assignment_generation,
                final_state: JobState::Failed,
                failed_at_unix_ms: UnixMillis::new(clock.now_unix_ms().map_err(agent_error)?),
                stage: JobFailureStage::Execution,
                error,
                extensions: Extensions::new(),
            }),
            accepted_at,
        )?;
        return Ok(());
    }
    let stats = match materializer.materialize(&assignment) {
        Ok(stats) => stats,
        Err(error) => {
            let stable_code = snapshot_delivery_error_code(
                &error,
                "DELIVERY_STORAGE_UNAVAILABLE",
                "DELIVERY_MATERIALIZATION_FAILED",
            );
            let control_error = ControlError {
                code: ErrorCode::new(stable_code).map_err(protocol_error)?,
                message: error.message().to_owned(),
                retryable: !snapshot_delivery_error_is_permanent(stable_code)
                    && matches!(
                        error.code(),
                        AgentErrorCode::MountUnavailable
                            | AgentErrorCode::ObjectTransferFailed
                            | AgentErrorCode::ExecutionFailed
                    ),
                retry_after_ms: None,
                extensions: Extensions::new(),
            };
            enqueue_workspace_report(
                reports,
                AgentReport::Failed(JobFailed {
                    task_fence: assignment.task_fence.clone(),
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
            task_fence: assignment.task_fence.clone(),
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

fn snapshot_delivery_error_code(
    error: &AgentError,
    mount_unavailable: &'static str,
    fallback: &'static str,
) -> &'static str {
    const EXPLICIT_CODES: [&str; 6] = [
        "HARDLINK_REQUIRES_WHOLE_FILE",
        "HARDLINK_CROSS_FILESYSTEM",
        "HARDLINK_UNSAFE_VOLUME",
        "HARDLINK_OBJECT_NOT_SEALED",
        "DELIVERY_TARGET_CONFLICT",
        "COPY_INSUFFICIENT_SPACE",
    ];
    if let Some(code) = EXPLICIT_CODES
        .iter()
        .find(|code| error.message().starts_with(**code))
    {
        return code;
    }
    match error.code() {
        AgentErrorCode::InvalidAssignment => "DELIVERY_ASSIGNMENT_INVALID",
        AgentErrorCode::ScopeMismatch => "DELIVERY_ASSIGNMENT_SCOPE_MISMATCH",
        AgentErrorCode::GenerationMismatch => "DELIVERY_STALE_GENERATION",
        AgentErrorCode::MountUnavailable => mount_unavailable,
        AgentErrorCode::ProtocolInvalid => "DELIVERY_METADATA_PROTOCOL_INVALID",
        AgentErrorCode::ObjectTransferFailed => "DELIVERY_OBJECT_UNAVAILABLE",
        AgentErrorCode::AssignmentMismatch => "DELIVERY_ASSIGNMENT_MISMATCH",
        AgentErrorCode::InvalidState => "DELIVERY_STATE_INVALID",
        _ => fallback,
    }
}

fn snapshot_delivery_error_is_permanent(code: &str) -> bool {
    matches!(
        code,
        "HARDLINK_REQUIRES_WHOLE_FILE"
            | "HARDLINK_CROSS_FILESYSTEM"
            | "HARDLINK_UNSAFE_VOLUME"
            | "HARDLINK_OBJECT_NOT_SEALED"
            | "DELIVERY_TARGET_CONFLICT"
    )
}

fn queued_delivery_state(
    reports: &dyn OutboundReportQueue,
    assignment: &neoengram_domain::protocol::SnapshotDeliveryAssignment,
) -> AgentDaemonResult<QueuedWorkspaceState> {
    let mut state = QueuedWorkspaceState::default();
    for queued in reports.list(256).map_err(agent_error)? {
        let (job_id, assignment_id, generation, task_fence) = match &queued.report {
            AgentReport::Accepted(report) => (
                &report.job_id,
                &report.assignment_id,
                report.assignment_generation,
                &report.task_fence,
            ),
            AgentReport::Progress(report) => (
                &report.job_id,
                &report.assignment_id,
                report.assignment_generation,
                &report.task_fence,
            ),
            AgentReport::Prepared(report) => (
                &report.job_id,
                &report.assignment_id,
                report.assignment_generation,
                &report.task_fence,
            ),
            AgentReport::Finalized(report) => (
                &report.job_id,
                &report.assignment_id,
                report.assignment_generation,
                &report.task_fence,
            ),
            AgentReport::Failed(report) => (
                &report.job_id,
                &report.assignment_id,
                report.assignment_generation,
                &report.task_fence,
            ),
            AgentReport::Lifecycle(_)
            | AgentReport::Replication(_)
            | AgentReport::Materialization(_)
            | AgentReport::Integrity(_) => continue,
        };
        if job_id != &assignment.job_id {
            continue;
        }
        if assignment_id != &assignment.assignment_id
            || generation != assignment.assignment_generation
            || task_fence != &assignment.task_fence
        {
            return Err(AgentDaemonError::Session(format!(
                "durable outbox already contains another assignment for Snapshot delivery Job {}",
                assignment.job_id
            )));
        }
        state.has_report = true;
        match &queued.report {
            AgentReport::Accepted(report) if report.request_digest != assignment.request_digest => {
                return Err(AgentDaemonError::Session(format!(
                    "durable Snapshot delivery acceptance has another request digest for Job {}",
                    assignment.job_id
                )));
            }
            AgentReport::Progress(report) if report.state == JobState::Running => {
                state.running = true
            }
            AgentReport::Progress(report) if report.state == JobState::Succeeded => {
                state.terminal = true
            }
            AgentReport::Failed(_) => state.terminal = true,
            AgentReport::Prepared(_) | AgentReport::Finalized(_) => {
                return Err(AgentDaemonError::Session(format!(
                    "durable outbox contains an incompatible report for Snapshot delivery Job {}",
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

fn protocol_error(error: neoengram_domain::protocol::ProtocolError) -> AgentDaemonError {
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
    pub agent_mount_id: neoengram_domain::protocol::AgentMountId,
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
    fence: SharedSessionFence,
    resource_version: SharedResourceVersion,
    command_trust_bundle: Option<Arc<CentralCommandTrustBundle>>,
}

impl<C> std::fmt::Debug for AgentSessionTransport<C> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentSessionTransport")
            .field("tenant_id", &self.tenant_id)
            .field("signer", &self.signer)
            .field("fence", &self.fence)
            .field("resource_version", &self.resource_version.get().ok())
            .field(
                "command_verification_enabled",
                &self.command_trust_bundle.is_some(),
            )
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
        _processor: Arc<dyn AgentMessageProcessor>,
        resource_version: ResourceVersion,
    ) -> Self {
        Self::new_shared(
            tenant_id,
            Arc::new(signer),
            Arc::new(client),
            reports,
            _processor,
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
        _processor: Arc<dyn AgentMessageProcessor>,
        fence: SharedSessionFence,
        resource_version: SharedResourceVersion,
    ) -> Self {
        Self {
            tenant_id,
            signer,
            client,
            reports,
            fence,
            resource_version,
            command_trust_bundle: None,
        }
    }

    /// Enables fail-closed Central signature verification for downstream channel commands.
    #[must_use]
    pub fn with_command_trust_bundle(mut self, bundle: Arc<CentralCommandTrustBundle>) -> Self {
        self.command_trust_bundle = Some(bundle);
        self
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
            let request_id = RequestId::new(queued_report.message_id.to_string())
                .map_err(|error| AgentDaemonError::Session(error.to_string()))?;
            let trace_id = TraceId::new(format!("report-{}", queued_report.message_id))
                .map_err(|error| AgentDaemonError::Session(error.to_string()))?;
            let envelope = new_control_envelope(
                request_id,
                trace_id,
                Some(self.tenant_id.clone()),
                Some(fence.session_generation),
                queued_report.enqueued_at_unix_ms,
                queued_report.report.into_control_message(),
            );
            neoengram_domain::protocol::validate_control_envelope(&envelope)
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
    shutdown_signal: Arc<AtomicBool>,
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
            shutdown_signal: Arc::new(AtomicBool::new(false)),
            runtime,
            index_cache: RwLock::new(BTreeMap::new()),
        }
    }

    /// Shares the daemon shutdown signal with blocking execution workers. The default keeps the
    /// bridge usable for embedded callers that do not have a lifecycle signal.
    #[must_use]
    pub fn with_shutdown_signal(mut self, shutdown_signal: Arc<AtomicBool>) -> Self {
        self.shutdown_signal = shutdown_signal;
        self
    }

    fn signed<T: serde::Serialize>(
        &self,
        path: &'static str,
        payload: T,
    ) -> AgentResult<neoengram_domain::protocol::AgentAuthenticatedRequest<T>> {
        let fence = self.fence.get().map_err(data_plane_error)?;
        self.signer
            .sign(path, Some(&fence), UnixMillis::new(system_now()?), payload)
            .map_err(data_plane_error)
    }

    fn signed_with_request_id<T: serde::Serialize>(
        &self,
        path: &'static str,
        request_id: RequestId,
        payload: T,
    ) -> AgentResult<neoengram_domain::protocol::AgentAuthenticatedRequest<T>> {
        let fence = self.fence.get().map_err(data_plane_error)?;
        self.signer
            .sign_with_request_id(
                path,
                request_id,
                Some(&fence),
                UnixMillis::new(system_now()?),
                payload,
            )
            .map_err(data_plane_error)
    }

    fn call_signed<P, T, F>(
        &self,
        path: &'static str,
        payload: P,
        mut action: F,
    ) -> AgentResult<(neoengram_domain::protocol::AgentAuthenticatedRequest<P>, T)>
    where
        P: serde::Serialize + Clone + Send + 'static,
        T: Send + 'static,
        F: FnMut(
            Arc<C>,
            neoengram_domain::protocol::AgentAuthenticatedRequest<P>,
        )
            -> Pin<Box<dyn Future<Output = Result<T, crate::AgentSessionClientError>> + Send>>,
    {
        self.runtime.block_on(async {
            let mut delay = Duration::from_millis(100);
            let mut attempt = 0_u64;
            let mut request_id: Option<RequestId> = None;
            loop {
                if self.shutdown_signal.load(Ordering::Acquire) {
                    return Err(data_plane_error(
                        "Agent shutdown requested while waiting for the data plane",
                    ));
                }
                attempt = attempt.saturating_add(1);
                // The control channel can replace the session fence while this blocking worker is
                // asleep. Re-signing here prevents a recovered Gateway from rejecting a request
                // that was created with the old generation. Keep one request identity across all
                // attempts so a mutating action remains idempotent if its first response was
                // lost after the server applied it.
                let request = match &request_id {
                    Some(request_id) => {
                        self.signed_with_request_id(path, request_id.clone(), payload.clone())?
                    }
                    None => {
                        let request = self.signed(path, payload.clone())?;
                        request_id = Some(request.request_id.clone());
                        request
                    }
                };
                match action(Arc::clone(&self.client), request.clone()).await {
                    Ok(value) => return Ok((request, value)),
                    Err(error) if error.transient() => {
                        tracing::debug!(
                            path,
                            attempt,
                            error = %error,
                            "Agent data-plane request is temporarily unavailable; retrying"
                        );
                        if wait_for_data_plane_retry(delay, &self.shutdown_signal).await {
                            return Err(data_plane_error(
                                "Agent shutdown requested while waiting for the data plane",
                            ));
                        }
                        delay = delay
                            .saturating_mul(2)
                            .min(SESSION_DATA_PLANE_MAX_RETRY_DELAY);
                    }
                    Err(error) => return Err(data_plane_error(error)),
                }
            }
        })
    }

    fn load_index(
        &self,
        assignment: &neoengram_domain::protocol::AddAssignment,
    ) -> AgentResult<AuthoritativeIndexSnapshot> {
        self.load_index_snapshot(
            &assignment.tenant_id,
            &assignment.job_id,
            &assignment.artifact_id,
            Some(&assignment.workspace_id),
            None,
            &assignment.expected_index_version,
        )
    }

    fn load_index_snapshot(
        &self,
        tenant_id: &TenantId,
        job_id: &JobId,
        artifact_id: &ArtifactId,
        workspace_id: Option<&WorkspaceId>,
        snapshot_id: Option<&SnapshotId>,
        index_version: &WireIndexVersion,
    ) -> AgentResult<AuthoritativeIndexSnapshot> {
        let mut page_number = 0_u32;
        let mut page_count = None;
        let mut records = Vec::new();
        let mut observed_index_version;
        loop {
            let (request, response) = self.call_signed(
                AGENT_JOB_INDEX_PAGE_QUERY_PATH,
                AgentIndexPageQueryPayload {
                    tenant_id: tenant_id.clone(),
                    job_id: job_id.clone(),
                    artifact_id: artifact_id.clone(),
                    workspace_id: workspace_id.cloned(),
                    snapshot_id: snapshot_id.cloned(),
                    index_version: index_version.clone(),
                    page_number,
                    max_records: u16::try_from(MAX_RECORDS_PER_PAGE)
                        .map_err(|_| protocol_data_plane_error("Index page limit exceeds u16"))?,
                    s3_ticket: None,
                    extensions: Extensions::new(),
                },
                |client, request| Box::pin(async move { client.query_index_page(&request).await }),
            )?;
            if response.wire_version != CURRENT_WIRE_VERSION
                || response.request_id != request.request_id
                || response.index_version.digest != index_version.digest
                || (index_version.revision.get() != 0
                    && response.index_version.revision != index_version.revision)
                || response.page_number != page_number
                || response.page_count == 0
                || response.records.len() > MAX_RECORDS_PER_PAGE
                || page_count.is_some_and(|count| count != response.page_count)
            {
                return Err(protocol_data_plane_error(
                    "Index page response changed identity or pagination",
                ));
            }
            observed_index_version = Some(response.index_version.clone());
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
        let observed_index_version = observed_index_version.ok_or_else(|| {
            protocol_data_plane_error("authoritative Index returned no response pages")
        })?;
        AuthoritativeIndexSnapshot::new(IndexVersion::from(observed_index_version), records)
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
            let (request, response) = self.call_signed(
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
                    s3_ticket: None,
                    extensions: Extensions::new(),
                },
                |client, request| {
                    Box::pin(async move { client.query_manifest_page(&request).await })
                },
            )?;
            if accumulator.push(&request.request_id, response)? {
                return accumulator.finish();
            }
        }
    }

    fn await_report_acknowledgement(&self) -> AgentResult<()> {
        loop {
            if self.shutdown_signal.load(Ordering::Acquire) {
                return Err(data_plane_error(
                    "Agent shutdown requested while waiting for a report acknowledgement",
                ));
            }
            if self.reports.list(1)?.is_empty() {
                return Ok(());
            }
            // Reports are durable and the control channel reconnects independently. Keep the
            // prepared execution at its ordering barrier until the reconnecting channel has
            // acknowledged the report instead of converting a temporary outage into a terminal
            // ObjectTransferFailed result.
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

async fn wait_for_data_plane_retry(delay: Duration, shutdown_signal: &AtomicBool) -> bool {
    let deadline = tokio::time::Instant::now() + delay;
    while !shutdown_signal.load(Ordering::Acquire) {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return false;
        }
        tokio::time::sleep(remaining.min(Duration::from_millis(100))).await;
    }
    true
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
        assignment: &neoengram_domain::protocol::AddAssignment,
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
            Some(&assignment.workspace_id),
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

    fn snapshot_delivery_snapshot(
        &self,
        assignment: &neoengram_domain::protocol::SnapshotDeliveryAssignment,
    ) -> AgentResult<WorkspaceMaterializationSnapshot> {
        assignment
            .validate()
            .map_err(|error| AgentError::new(AgentErrorCode::ProtocolInvalid, error.to_string()))?;
        // Delivery assignments intentionally bind the frozen Index digest rather than a
        // mutable revision. The Central data plane returns the authoritative revision after
        // validating that digest; revision zero is the explicit wire sentinel for this query.
        let index_version = WireIndexVersion {
            revision: IndexRevision::new(0),
            digest: assignment.source_index_digest,
            extensions: Extensions::new(),
        };
        let index = self.load_index_snapshot(
            &assignment.tenant_id,
            &assignment.job_id,
            &assignment.artifact_id,
            None,
            Some(&assignment.snapshot_id),
            &index_version,
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

    fn snapshot_read_snapshot(
        &self,
        ticket: &neoengram_domain::protocol::S3ReadTicket,
    ) -> AgentResult<WorkspaceMaterializationSnapshot> {
        let tenant_id = TenantId::new(ticket.tenant_id.clone())
            .map_err(|_| protocol_data_plane_error("S3 ticket Tenant ID is invalid"))?;
        let artifact_id = ArtifactId::new(ticket.artifact_id.clone())
            .map_err(|_| protocol_data_plane_error("S3 ticket Artifact ID is invalid"))?;
        let snapshot_id = SnapshotId::new(ticket.snapshot_id.clone())
            .map_err(|_| protocol_data_plane_error("S3 ticket Snapshot ID is invalid"))?;
        let job_id = JobId::new(format!("s3-{}", ticket.ticket_id))
            .map_err(|_| protocol_data_plane_error("S3 ticket ID cannot form a query identity"))?;
        let index_version = WireIndexVersion {
            revision: IndexRevision::new(0),
            digest: ticket.index_digest,
            extensions: Extensions::new(),
        };
        let mut page_number = 0_u32;
        let mut page_count = None;
        let mut records = Vec::new();
        let mut observed;
        loop {
            let (request, response) = self.call_signed(
                AGENT_JOB_INDEX_PAGE_QUERY_PATH,
                AgentIndexPageQueryPayload {
                    tenant_id: tenant_id.clone(),
                    job_id: job_id.clone(),
                    artifact_id: artifact_id.clone(),
                    workspace_id: None,
                    snapshot_id: Some(snapshot_id.clone()),
                    index_version: index_version.clone(),
                    page_number,
                    max_records: u16::try_from(MAX_RECORDS_PER_PAGE)
                        .map_err(|_| protocol_data_plane_error("Index page limit exceeds u16"))?,
                    s3_ticket: Some(ticket.clone()),
                    extensions: Extensions::new(),
                },
                |client, request| Box::pin(async move { client.query_index_page(&request).await }),
            )?;
            if response.request_id != request.request_id
                || response.index_version.digest != ticket.index_digest
                || response.page_number != page_number
                || response.page_count == 0
                || response.records.len() > MAX_RECORDS_PER_PAGE
                || page_count.is_some_and(|count| count != response.page_count)
            {
                return Err(protocol_data_plane_error(
                    "S3 Index page response changed identity or pagination",
                ));
            }
            observed = Some(response.index_version.clone());
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
                            "S3 Snapshot Index contains a delete record",
                        ));
                    }
                }
            }
            page_number = page_number
                .checked_add(1)
                .ok_or_else(|| protocol_data_plane_error("S3 Index page number overflow"))?;
            if page_number == page_count.unwrap_or_default() {
                break;
            }
        }
        let observed = observed
            .ok_or_else(|| protocol_data_plane_error("S3 Snapshot Index returned no pages"))?;
        let index = AuthoritativeIndexSnapshot::new(IndexVersion::from(observed), records)
            .map_err(as_protocol_data_plane_error)?;
        let mut manifests = BTreeMap::<ManifestId, Manifest>::new();
        let mut files = Vec::with_capacity(index.records().len());
        for record in index.records() {
            let manifest = if let Some(manifest) = manifests.get(&record.manifest_id) {
                manifest.clone()
            } else {
                let manifest = self.load_s3_manifest(
                    &tenant_id,
                    &job_id,
                    &artifact_id,
                    record.manifest_id,
                    ticket,
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
        assignment: &neoengram_domain::protocol::AddAssignment,
        descriptor: &MetadataBatchDescriptor,
    ) -> AgentResult<()> {
        // Prepared is enqueued before ObjectTransfer::stage_metadata. The center rejects metadata
        // until Accepted/Running/Prepared have been observed in durable order.
        self.await_report_acknowledgement()?;
        let (_, response) = self.call_signed(
            AGENT_JOB_METADATA_BATCH_STAGE_PATH,
            AgentMetadataBatchStagePayload {
                tenant_id: assignment.tenant_id.clone(),
                job_id: assignment.job_id.clone(),
                descriptor: descriptor.clone(),
                extensions: Extensions::new(),
            },
            |client, request| Box::pin(async move { client.stage_metadata_batch(&request).await }),
        )?;
        if response.batch_id != descriptor.batch_id {
            return Err(data_plane_error(
                "metadata descriptor acknowledgement changed batch ID",
            ));
        }
        Ok(())
    }

    fn stage_metadata_page(
        &self,
        assignment: &neoengram_domain::protocol::AddAssignment,
        page: &MetadataBatchPage,
    ) -> AgentResult<()> {
        let (_, response) = self.call_signed(
            AGENT_JOB_METADATA_PAGE_STAGE_PATH,
            AgentMetadataPageStagePayload {
                tenant_id: assignment.tenant_id.clone(),
                job_id: assignment.job_id.clone(),
                page: page.clone(),
                extensions: Extensions::new(),
            },
            |client, request| Box::pin(async move { client.stage_metadata_page(&request).await }),
        )?;
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

impl<C: AgentSessionClient + 'static> SessionExecutionBridge<C> {
    fn load_s3_manifest(
        &self,
        tenant_id: &TenantId,
        job_id: &JobId,
        artifact_id: &ArtifactId,
        manifest_id: ManifestId,
        ticket: &neoengram_domain::protocol::S3ReadTicket,
    ) -> AgentResult<Manifest> {
        let mut accumulator = ManifestPageAccumulator::new(manifest_id);
        loop {
            let page_number = accumulator.next_page_number();
            let (request, response) = self.call_signed(
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
                    s3_ticket: Some(ticket.clone()),
                    extensions: Extensions::new(),
                },
                |client, request| {
                    Box::pin(async move { client.query_manifest_page(&request).await })
                },
            )?;
            if accumulator.push(&request.request_id, response)? {
                return accumulator.finish();
            }
        }
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

fn settle_replication_execution(
    replication_id: &ReplicationId,
    progress: &dyn ReplicationProgressSink,
    result: AgentDaemonResult<()>,
) -> AgentDaemonResult<()> {
    let Err(error) = result else {
        return Ok(());
    };
    if let AgentDaemonError::SessionTransport(message) = &error {
        // A QUIC disconnect is not a replication failure.  Keep the attempt active so Central's
        // redelivery loop can issue the same immutable assignment and let the target CAS resume
        // from its durable staging offset. The dispatcher recognizes this variant for
        // redeliverable work and keeps the control channel alive without marking the attempt
        // complete.
        progress.state(replication_id, ReplicationState::Transferring)?;
        tracing::warn!(
            %replication_id,
            error = %message,
            "Replication data-plane transport is unavailable; retaining active attempt for retry"
        );
        return Err(error);
    }
    progress.state(replication_id, ReplicationState::Failed)?;
    tracing::warn!(%replication_id, %error, "Replication task failed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{Arc, Mutex},
    };

    use crate::QueuedAgentReport;
    use neoengram_domain::core::{ContentDigest, LogicalPath, ObjectId};
    use neoengram_domain::protocol::{
        AgentId, AgentMountId, ArtifactId, AssignmentGeneration, AssignmentId, Generation,
        MessageId, MountGeneration, OwnerGeneration, PrincipalId, PrincipalKind, PrincipalRef,
        ProjectId, RequestId, SessionGeneration, SessionId, StorageVolumeId, TaskExecutionFence,
        TaskId, WireChunkRef, WireChunkingStrategy, WorkspaceId,
        AGENT_JOB_METADATA_PAGE_STAGE_PATH,
    };
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn hardlink_policy_and_safety_errors_are_not_retryable() {
        for code in [
            "HARDLINK_REQUIRES_WHOLE_FILE",
            "HARDLINK_CROSS_FILESYSTEM",
            "HARDLINK_UNSAFE_VOLUME",
            "HARDLINK_OBJECT_NOT_SEALED",
            "DELIVERY_TARGET_CONFLICT",
        ] {
            assert!(snapshot_delivery_error_is_permanent(code));
        }
        assert!(!snapshot_delivery_error_is_permanent(
            "DELIVERY_OBJECT_UNAVAILABLE"
        ));
        assert!(!snapshot_delivery_error_is_permanent(
            "DELIVERY_STORAGE_UNAVAILABLE"
        ));
    }

    #[tokio::test]
    async fn data_plane_retry_resigns_after_session_fence_replacement() {
        let key_document =
            ring::signature::Ed25519KeyPair::generate_pkcs8(&ring::rand::SystemRandom::new())
                .unwrap();
        let signer = Arc::new(
            AgentRequestSigner::new(
                AgentId::new("agent-a").unwrap(),
                neoengram_domain::protocol::AgentInstallationId::new("installation-a").unwrap(),
                neoengram_domain::protocol::AgentBootId::new("boot-a").unwrap(),
                Arc::new(
                    ring::signature::Ed25519KeyPair::from_pkcs8(key_document.as_ref()).unwrap(),
                ),
            )
            .unwrap(),
        );
        let fence = SharedSessionFence::default();
        fence
            .replace(Some(AgentSessionFence {
                session_id: SessionId::new("session-before").unwrap(),
                session_generation: SessionGeneration::new(1),
            }))
            .unwrap();
        let client = Arc::new(
            crate::ReqwestAgentSessionClient::new(url::Url::parse("http://127.0.0.1:1/").unwrap())
                .unwrap(),
        );
        let bridge = Arc::new(SessionExecutionBridge::new(
            TenantId::new("tenant-a").unwrap(),
            client,
            signer,
            fence.clone(),
            Arc::new(RecordingQueue::default()),
            tokio::runtime::Handle::current(),
        ));
        let observed = Arc::new(Mutex::new(Vec::<(SessionGeneration, RequestId)>::new()));
        let observed_for_call = Arc::clone(&observed);
        let fence_for_call = fence.clone();
        let (request, ()) = tokio::task::spawn_blocking(move || {
            bridge.call_signed(
                AGENT_JOB_METADATA_PAGE_STAGE_PATH,
                serde_json::json!({"test": true}),
                move |_client, request| {
                    let observed = Arc::clone(&observed_for_call);
                    let fence = fence_for_call.clone();
                    Box::pin(async move {
                        let attempt = {
                            let mut observed = observed.lock().unwrap();
                            let attempt = observed.len();
                            observed.push((
                                request.session_generation.expect("signed request fence"),
                                request.request_id.clone(),
                            ));
                            attempt
                        };
                        if attempt == 0 {
                            fence
                                .replace(Some(AgentSessionFence {
                                    session_id: SessionId::new("session-after").unwrap(),
                                    session_generation: SessionGeneration::new(2),
                                }))
                                .unwrap();
                            Err(crate::AgentSessionClientError::transport(
                                "Gateway disconnected",
                            ))
                        } else {
                            Ok(())
                        }
                    })
                },
            )
        })
        .await
        .unwrap()
        .unwrap();

        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 2);
        assert_eq!(observed[0].0, SessionGeneration::new(1));
        assert_eq!(observed[1].0, SessionGeneration::new(2));
        assert_eq!(
            observed[0].1, observed[1].1,
            "a retry must preserve the logical request identity"
        );
        assert_eq!(
            request.session_id,
            Some(SessionId::new("session-after").unwrap())
        );
        assert_eq!(request.session_generation, Some(SessionGeneration::new(2)));
        request
            .verify(AGENT_JOB_METADATA_PAGE_STAGE_PATH)
            .expect("successful retry must be signed with the current fence");
    }

    #[tokio::test]
    async fn data_plane_retry_stops_when_agent_shutdown_is_requested() {
        let key_document =
            ring::signature::Ed25519KeyPair::generate_pkcs8(&ring::rand::SystemRandom::new())
                .unwrap();
        let signer = Arc::new(
            AgentRequestSigner::new(
                AgentId::new("agent-a").unwrap(),
                neoengram_domain::protocol::AgentInstallationId::new("installation-a").unwrap(),
                neoengram_domain::protocol::AgentBootId::new("boot-a").unwrap(),
                Arc::new(
                    ring::signature::Ed25519KeyPair::from_pkcs8(key_document.as_ref()).unwrap(),
                ),
            )
            .unwrap(),
        );
        let fence = SharedSessionFence::default();
        fence
            .replace(Some(AgentSessionFence {
                session_id: SessionId::new("session-a").unwrap(),
                session_generation: SessionGeneration::new(1),
            }))
            .unwrap();
        let bridge = SessionExecutionBridge::new(
            TenantId::new("tenant-a").unwrap(),
            Arc::new(
                crate::ReqwestAgentSessionClient::new(
                    url::Url::parse("http://127.0.0.1:1/").unwrap(),
                )
                .unwrap(),
            ),
            signer,
            fence,
            Arc::new(RecordingQueue::default()),
            tokio::runtime::Handle::current(),
        )
        .with_shutdown_signal(Arc::new(std::sync::atomic::AtomicBool::new(true)));

        let error = tokio::task::spawn_blocking(move || {
            bridge
                .call_signed(
                    AGENT_JOB_METADATA_PAGE_STAGE_PATH,
                    serde_json::json!({"test": true}),
                    |_client, _request| {
                        Box::pin(async { Ok::<_, crate::AgentSessionClientError>(()) })
                    },
                )
                .expect_err("shutdown must stop a retrying data-plane call")
        })
        .await
        .unwrap();
        assert_eq!(error.code(), AgentErrorCode::ObjectTransferFailed);
        assert!(error.message().contains("shutdown"));
    }

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

    #[derive(Debug, Default)]
    struct RecordingReplicationProgress(Mutex<Vec<ReplicationState>>);

    impl ReplicationProgressSink for RecordingReplicationProgress {
        fn state(
            &self,
            _replication_id: &ReplicationId,
            state: ReplicationState,
        ) -> AgentDaemonResult<()> {
            self.0.lock().unwrap().push(state);
            Ok(())
        }

        fn object(
            &self,
            _replication_id: &ReplicationId,
            _object_id: &ObjectId,
            _offset: u64,
            _state: neoengram_domain::protocol::ReplicationObjectState,
        ) -> AgentDaemonResult<()> {
            Ok(())
        }

        fn publish(
            &self,
            _replication_id: &ReplicationId,
            _tenant_id: &TenantId,
            _commit_id: &neoengram_domain::CommitId,
            _object_set_digest: &ContentDigest,
        ) -> AgentDaemonResult<()> {
            Ok(())
        }
    }

    #[test]
    fn replication_task_failure_does_not_close_the_agent_session() {
        let replication_id = ReplicationId::new("replication-task-failure").unwrap();
        let progress = RecordingReplicationProgress::default();

        settle_replication_execution(
            &replication_id,
            &progress,
            Err(AgentDaemonError::Session("transfer failed".to_owned())),
        )
        .unwrap();

        assert_eq!(*progress.0.lock().unwrap(), vec![ReplicationState::Failed]);
    }

    #[test]
    fn transient_replication_transport_failure_keeps_attempt_active() {
        let replication_id = ReplicationId::new("replication-transport-retry").unwrap();
        let progress = RecordingReplicationProgress::default();

        let error = settle_replication_execution(
            &replication_id,
            &progress,
            Err(AgentDaemonError::SessionTransport(
                "Gateway connection closed".to_owned(),
            )),
        )
        .unwrap_err();

        assert_eq!(
            *progress.0.lock().unwrap(),
            vec![ReplicationState::Transferring]
        );
        assert!(matches!(error, AgentDaemonError::SessionTransport(_)));
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
            .join("workspaces/project-a/artifact-a/workspace-a")
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
        assert!(!mount.join("workspaces").exists());
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
            wire_version: CURRENT_WIRE_VERSION,
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
        let workspace_id = WorkspaceId::new("workspace-a").unwrap();
        let mut assignment = WorkspaceMaterializeAssignment {
            job_id: neoengram_domain::protocol::JobId::new("job-materialize-a").unwrap(),
            task_fence: TaskExecutionFence::new(
                TaskId::new("task-job-materialize-a").unwrap(),
                Generation::new(1),
                "materialize",
                Generation::new(1),
                Generation::new(1),
            ),
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
            workspace_id,
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            agent_mount_id: AgentMountId::new("mount-a").unwrap(),
            mount_generation: MountGeneration::new(1),
            owner_generation: OwnerGeneration::new(1),
            relative_root: LogicalPath::parse("workspaces/project-a/artifact-a/workspace-a")
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
}
