use std::sync::Arc;

use neoengram_protocol::{
    AgentId, AssignmentOperation, ControlEnvelope, ControlError, ControlMessage,
    DecisionGeneration, ErrorCode, Extensions, IndexRevision, JobAssignment, JobDecision,
    JobFinalized, JobState, MessageId, PrincipalKind, ProtocolVersion, PublishDecision,
    ResourceVersion, SessionGeneration, SnapshotMountAssignment, WireIndexVersion,
    WorkspaceMaterializeAssignment,
};

use crate::{
    validation::{
        invalid, validate_assignment_target, validate_descriptor_scope, validate_job_spec,
        validate_prepared, validate_report_identity, validate_staged_metadata,
        validate_terminal_state,
    },
    Action, Actor, AddJobSpec, AgentReport, AssignJobRequest, AssignJobResult,
    AssignSnapshotMountRequest, AssignSnapshotMountResult, AssignWorkspaceMaterializationRequest,
    AssignWorkspaceMaterializationResult, AssignmentOutbox, AuditEvent, AuditKind, AuditSink,
    AuthorityStore, AuthorizationRequest, Authorizer, CentralErrorCode, CentralResult, Clock,
    ControlCatalogRepository, CreateAddJobRequest, CreateAddJobResult, CreateSnapshotMountRequest,
    CreateSnapshotMountResult, CreateWorkspaceMaterializationRequest,
    CreateWorkspaceMaterializationResult, ExpireAddJobRequest, ExpireAddJobResult,
    FinalizeAddRequest, FinalizeAddResult, IndexPublishOutcome, IndexPublishRejection,
    IndexPublishRequest, IndexPublisher, JobInsertOutcome, JobOperation, JobRecord, JobRepository,
    MetadataBatchStager, MetadataBatchSubmission, ObjectCatalog, PublicationCandidate,
    QueryJobRequest, QueryJobResult, ReceiveReportRequest, ReceiveReportResult,
    ResumePublicationRequest, StageMetadataBatchRequest, StageMetadataBatchResult,
};

const CONTROL_ERROR_MESSAGE_LIMIT: usize = 4096;

/// Central managed-Add application service composed entirely from explicit ports.
pub struct ControlPlane {
    authorizer: Arc<dyn Authorizer>,
    jobs: Arc<dyn JobRepository>,
    outbox: Arc<dyn AssignmentOutbox>,
    metadata: Arc<dyn MetadataBatchStager>,
    objects: Arc<dyn ObjectCatalog>,
    publisher: Arc<dyn IndexPublisher>,
    audit: Arc<dyn AuditSink>,
    catalog: Option<Arc<dyn ControlCatalogRepository>>,
    clock: Arc<dyn Clock>,
}

impl ControlPlane {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        authorizer: Arc<dyn Authorizer>,
        authority: AuthorityStore,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            authorizer,
            jobs: authority.jobs(),
            outbox: authority.outbox(),
            metadata: authority.metadata(),
            objects: authority.objects(),
            publisher: authority.publisher(),
            audit: authority.audit(),
            catalog: authority.control_catalog(),
            clock,
        }
    }

    /// Derives the current Agent delivery set from the durable assignment outbox and Job CAS.
    /// Add assignments disappear after Accepted because their execution ledger is Agent-durable.
    /// Workspace materialization assignments remain deliverable through Accepted/Running and
    /// disappear only at a terminal outcome, allowing an Agent restart to resume physical checkout.
    /// Decisions disappear only after the matching Finalized acknowledgement is persisted.
    pub async fn poll_agent_messages(
        &self,
        agent_id: &AgentId,
        session_generation: SessionGeneration,
        limit: usize,
    ) -> CentralResult<Vec<ControlEnvelope>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut messages = Vec::with_capacity(limit);
        for assignment in self.outbox.pending_for_agent(agent_id, limit).await? {
            let (tenant_id, job_id, assignment_id, valid) = match &assignment.assignment {
                AssignmentOperation::Add { input, .. } => {
                    let job = self
                        .jobs
                        .get(&crate::JobKey::new(
                            input.tenant_id.clone(),
                            input.job_id.clone(),
                        ))
                        .await?;
                    (
                        input.tenant_id.clone(),
                        input.job_id.clone(),
                        input.assignment_id.clone(),
                        job.is_some_and(|job| {
                            job.operation == JobOperation::Add
                                && job.state == JobState::Assigned
                                && job.assignment.as_ref() == Some(input)
                        }),
                    )
                }
                AssignmentOperation::WorkspaceMaterialize { input, .. } => {
                    let job = self
                        .jobs
                        .get(&crate::JobKey::new(
                            input.tenant_id.clone(),
                            input.job_id.clone(),
                        ))
                        .await?;
                    (
                        input.tenant_id.clone(),
                        input.job_id.clone(),
                        input.assignment_id.clone(),
                        job.is_some_and(|job| {
                            job.operation == JobOperation::WorkspaceMaterialize
                                && matches!(
                                    job.state,
                                    JobState::Assigned
                                        | JobState::Accepted
                                        | JobState::Running
                                        | JobState::Succeeded
                                        | JobState::RecoveryRequired
                                )
                                && job.workspace_assignment.as_ref() == Some(input)
                        }),
                    )
                }
                AssignmentOperation::SnapshotMount { input, .. } => {
                    let job = self
                        .jobs
                        .get(&crate::JobKey::new(
                            input.tenant_id.clone(),
                            input.job_id.clone(),
                        ))
                        .await?;
                    (
                        input.tenant_id.clone(),
                        input.job_id.clone(),
                        input.assignment_id.clone(),
                        job.is_some_and(|job| {
                            job.operation == JobOperation::SnapshotMount
                                && matches!(
                                    job.state,
                                    JobState::Assigned
                                        | JobState::Accepted
                                        | JobState::Running
                                        | JobState::Succeeded
                                        | JobState::RecoveryRequired
                                )
                                && job.snapshot_assignment.as_ref() == Some(input)
                        }),
                    )
                }
            };
            if !valid {
                continue;
            }
            let job = self
                .jobs
                .get(&crate::JobKey::new(tenant_id.clone(), job_id.clone()))
                .await?
                .ok_or_else(|| {
                    invalid(CentralErrorCode::JobNotFound, "assignment Job disappeared")
                })?;
            let envelope = ControlEnvelope {
                protocol_version: ProtocolVersion::V1,
                message_id: MessageId::new(format!("assignment-{assignment_id}"))?,
                session_generation,
                resource_version: Some(job.resource_version),
                request_id: None,
                trace_id: None,
                sent_at_unix_ms: self.clock.now(),
                message: ControlMessage::Assignment(Box::new(assignment)),
                extensions: Extensions::new(),
            };
            envelope.validate()?;
            messages.push(envelope);
            if messages.len() == limit {
                return Ok(messages);
            }
        }

        let remaining = limit.saturating_sub(messages.len());
        for job in self
            .jobs
            .list_pending_decisions_for_agent(agent_id, remaining)
            .await?
        {
            let Some(assignment) = &job.assignment else {
                continue;
            };
            let Some(decision) = &job.decision else {
                continue;
            };
            if assignment.agent_id != *agent_id || job.finalized_ack.is_some() {
                continue;
            }
            let envelope = ControlEnvelope {
                protocol_version: ProtocolVersion::V1,
                message_id: MessageId::new(format!(
                    "decision-{}-{}",
                    decision.job_id, decision.decision_generation
                ))?,
                session_generation,
                resource_version: Some(job.resource_version),
                request_id: None,
                trace_id: None,
                sent_at_unix_ms: self.clock.now(),
                message: ControlMessage::Decision(decision.clone()),
                extensions: Extensions::new(),
            };
            envelope.validate()?;
            messages.push(envelope);
            if messages.len() == limit {
                break;
            }
        }
        Ok(messages)
    }

    /// Checks Create Add Job authorization without reading or mutating Job authority state.
    pub async fn preauthorize_create_add_job(
        &self,
        actor: &neoengram_protocol::PrincipalRef,
        spec: &AddJobSpec,
    ) -> CentralResult<()> {
        self.authorize(Actor::Principal(actor.clone()), Action::CreateAddJob, spec)
            .await
    }

    /// Creates the authoritative queued job. Reusing a JobId with another digest/spec is rejected.
    pub async fn create_add_job(
        &self,
        request: CreateAddJobRequest,
    ) -> CentralResult<CreateAddJobResult> {
        self.authorize(
            Actor::Principal(request.actor),
            Action::CreateAddJob,
            &request.spec,
        )
        .await?;
        self.persist_add_job(request.spec, "create").await
    }

    /// Creates the internal Add Job associated with a durable Pre-commit attempt.
    ///
    /// Public authorization occurs before the Pre-commit aggregate is persisted. Recovery must
    /// not depend on that principal retaining mutable RBAC grants, so only this fixed system
    /// principal may enter the trusted continuation path.
    pub async fn create_precommit_add_job(
        &self,
        spec: AddJobSpec,
    ) -> CentralResult<CreateAddJobResult> {
        if spec.principal.kind != PrincipalKind::System
            || spec.principal.id.as_str() != "precommit-scanner"
        {
            return Err(invalid(
                CentralErrorCode::ProtocolInvalid,
                "Pre-commit Add Job requires the fixed system principal",
            ));
        }
        self.persist_add_job(spec, "precommit-create").await
    }

    async fn persist_add_job(
        &self,
        spec: AddJobSpec,
        audit_action: &'static str,
    ) -> CentralResult<CreateAddJobResult> {
        let key = crate::JobKey::new(spec.tenant_id.clone(), spec.job_id.clone());
        if let Some(existing) = self.jobs.get(&key).await? {
            if existing.spec != spec {
                return Err(invalid(
                    CentralErrorCode::JobIdReused,
                    "JobId already belongs to a different managed Add request",
                ));
            }
            self.audit(&existing, AuditKind::JobCreated, audit_action)
                .await?;
            return Ok(CreateAddJobResult {
                job: existing,
                replayed: true,
            });
        }
        validate_job_spec(&spec, self.clock.now().get())?;

        let job = JobRecord {
            spec: spec.clone(),
            operation: JobOperation::Add,
            workspace_spec: None,
            snapshot_spec: None,
            state: JobState::Queued,
            resource_version: ResourceVersion::new(1),
            assignment: None,
            workspace_assignment: None,
            snapshot_assignment: None,
            accepted: None,
            progress: None,
            prepared: None,
            publication_candidate: None,
            decision: None,
            finalized: None,
            finalized_ack: None,
            failure: None,
        };
        let (job, replayed) = match self.jobs.insert_or_load(job).await? {
            JobInsertOutcome::Inserted(job) => (job, false),
            JobInsertOutcome::Existing(existing) => {
                if existing.spec != spec {
                    return Err(invalid(
                        CentralErrorCode::JobIdReused,
                        "JobId already belongs to a different managed Add request",
                    ));
                }
                (existing, true)
            }
        };
        self.audit(&job, AuditKind::JobCreated, audit_action)
            .await?;
        Ok(CreateAddJobResult { job, replayed })
    }

    /// Creates the durable infrastructure Job used to materialize a Playground directory.
    ///
    /// The record is intentionally stored in the same Job table as managed Add so the existing
    /// assignment outbox foreign key and recovery scanner cover both operations. Its operation
    /// discriminant and immutable WorkspaceMaterializeSpec make replay identity explicit.
    pub async fn create_workspace_materialization(
        &self,
        request: CreateWorkspaceMaterializationRequest,
    ) -> CentralResult<CreateWorkspaceMaterializationResult> {
        let spec = request.spec;
        let canonical = WorkspaceMaterializeAssignment::canonical_relative_root(
            &spec.project_id,
            &spec.artifact_id,
            &spec.playground_id,
        )?;
        if spec.relative_root != canonical {
            return Err(invalid(
                CentralErrorCode::ProtocolInvalid,
                "WorkspaceMaterialize relative_root is not server-derived",
            ));
        }
        if spec.request_digest != spec.computed_request_digest()? {
            return Err(invalid(
                CentralErrorCode::MetadataInvalid,
                "WorkspaceMaterialize request digest does not bind its immutable spec",
            ));
        }
        if spec.deadline_unix_ms.get() <= self.clock.now().get() {
            return Err(invalid(
                CentralErrorCode::DeadlineExceeded,
                "WorkspaceMaterialize deadline has elapsed",
            ));
        }

        // The legacy JobRecord spec is retained for the shared relational identity and audit
        // schema. It is never exposed or used for materialization validation.
        let mut legacy_spec = AddJobSpec {
            job_id: spec.job_id.clone(),
            principal: spec.principal.clone(),
            tenant_id: spec.tenant_id.clone(),
            project_id: spec.project_id.clone(),
            artifact_id: spec.artifact_id.clone(),
            playground_id: spec.playground_id.clone(),
            expected_index_version: WireIndexVersion {
                revision: IndexRevision::new(0),
                digest: neoengram_core::ContentDigest::from_bytes([0; 32]),
                extensions: Extensions::new(),
            },
            request_digest: neoengram_core::ContentDigest::from_bytes([0; 32]),
            deadline_unix_ms: spec.deadline_unix_ms,
            paths: Vec::new(),
            all: true,
            extensions: Extensions::new(),
        };
        legacy_spec.request_digest = legacy_spec.computed_request_digest()?;
        let job = JobRecord {
            spec: legacy_spec,
            operation: JobOperation::WorkspaceMaterialize,
            workspace_spec: Some(spec.clone()),
            snapshot_spec: None,
            state: JobState::Queued,
            resource_version: ResourceVersion::new(1),
            assignment: None,
            workspace_assignment: None,
            snapshot_assignment: None,
            accepted: None,
            progress: None,
            prepared: None,
            publication_candidate: None,
            decision: None,
            finalized: None,
            finalized_ack: None,
            failure: None,
        };
        let (job, replayed) = match self.jobs.insert_or_load(job).await? {
            JobInsertOutcome::Inserted(job) => (job, false),
            JobInsertOutcome::Existing(existing) => {
                if existing.operation != JobOperation::WorkspaceMaterialize
                    || existing.workspace_spec.as_ref() != Some(&spec)
                {
                    return Err(invalid(
                        CentralErrorCode::JobIdReused,
                        "materialization JobId is already bound to another operation",
                    ));
                }
                (existing, true)
            }
        };
        self.audit(&job, AuditKind::JobCreated, "materialize-create")
            .await?;
        Ok(CreateWorkspaceMaterializationResult { job, replayed })
    }

    /// Persists and publishes a server-selected WorkspaceMaterialize assignment.
    pub async fn assign_workspace_materialization(
        &self,
        request: AssignWorkspaceMaterializationRequest,
    ) -> CentralResult<AssignWorkspaceMaterializationResult> {
        let mut job = self.load(&request.tenant_id, &request.job_id).await?;
        if job.operation != JobOperation::WorkspaceMaterialize {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "Job is not a WorkspaceMaterialize operation",
            ));
        }
        let spec = job.workspace_spec.clone().ok_or_else(|| {
            invalid(
                CentralErrorCode::Internal,
                "WorkspaceMaterialize Job has no immutable operation spec",
            )
        })?;
        if spec.storage_volume_id != request.target.storage_volume_id {
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "materialization assignment selected a different StorageVolume",
            ));
        }
        let relative_root = spec.relative_root.clone();
        let assignment = WorkspaceMaterializeAssignment {
            job_id: spec.job_id.clone(),
            assignment_id: request.target.assignment_id.clone(),
            assignment_generation: request.target.assignment_generation,
            agent_id: request.target.agent_id.clone(),
            principal: spec.principal.clone(),
            tenant_id: spec.tenant_id.clone(),
            project_id: spec.project_id.clone(),
            artifact_id: spec.artifact_id.clone(),
            playground_id: spec.playground_id.clone(),
            storage_volume_id: spec.storage_volume_id.clone(),
            agent_mount_id: request.target.agent_mount_id.clone(),
            mount_generation: request.target.mount_generation,
            owner_generation: request.target.owner_generation,
            relative_root,
            base_commit_id: spec.base_commit_id,
            base_index_version: spec.base_index_version.clone(),
            request_digest: spec.request_digest,
            deadline_unix_ms: spec.deadline_unix_ms,
            extensions: Extensions::new(),
        };
        assignment.validate()?;
        let envelope = JobAssignment {
            assignment: AssignmentOperation::WorkspaceMaterialize {
                input: assignment.clone(),
                extensions: Extensions::new(),
            },
            extensions: Extensions::new(),
        };
        envelope.validate()?;

        if let Some(existing) = &job.workspace_assignment {
            if existing != &assignment {
                return Err(invalid(
                    CentralErrorCode::JobIdReused,
                    "materialization Job already has a different assignment",
                ));
            }
            let _ = self.outbox.reserve(envelope.clone()).await?;
            let _ = self.outbox.publish(envelope.clone()).await?;
            return Ok(AssignWorkspaceMaterializationResult {
                job,
                assignment: envelope,
                replayed: true,
            });
        }
        if job.state != JobState::Queued {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                format!("cannot assign materialization Job in state {:?}", job.state),
            ));
        }
        let _ = self.outbox.reserve(envelope.clone()).await?;
        let previous = job.resource_version.get();
        job.workspace_assignment = Some(assignment.clone());
        job.state = JobState::Assigned;
        job.resource_version = ResourceVersion::new(previous.saturating_add(1));
        job = match self.replace(previous, job).await {
            Ok(job) => job,
            Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => {
                let persisted = self.load(&request.tenant_id, &request.job_id).await?;
                if persisted.workspace_assignment.as_ref() != Some(&assignment) {
                    return Err(error);
                }
                let _ = self.outbox.reserve(envelope.clone()).await?;
                let _ = self.outbox.publish(envelope.clone()).await?;
                return Ok(AssignWorkspaceMaterializationResult {
                    job: persisted,
                    assignment: envelope,
                    replayed: true,
                });
            }
            Err(error) => return Err(error),
        };
        let _ = self.outbox.publish(envelope.clone()).await?;
        self.audit(&job, AuditKind::AssignmentQueued, "materialize-assignment")
            .await?;
        Ok(AssignWorkspaceMaterializationResult {
            job,
            assignment: envelope,
            replayed: false,
        })
    }

    /// Creates the durable infrastructure Job used to mount a read-only Snapshot.
    pub async fn create_snapshot_mount(
        &self,
        request: CreateSnapshotMountRequest,
    ) -> CentralResult<CreateSnapshotMountResult> {
        let spec = request.spec;
        let canonical = SnapshotMountAssignment::canonical_relative_root(
            &spec.project_id,
            &spec.artifact_id,
            &spec.snapshot_id,
        )?;
        if spec.relative_root != canonical {
            return Err(invalid(
                CentralErrorCode::ProtocolInvalid,
                "SnapshotMount relative_root is not server-derived",
            ));
        }
        if spec.request_digest != spec.computed_request_digest()? {
            return Err(invalid(
                CentralErrorCode::MetadataInvalid,
                "SnapshotMount request digest does not bind its immutable spec",
            ));
        }
        if spec.deadline_unix_ms.get() <= self.clock.now().get() {
            return Err(invalid(
                CentralErrorCode::DeadlineExceeded,
                "SnapshotMount deadline has elapsed",
            ));
        }
        let playground_id =
            neoengram_protocol::PlaygroundId::new(spec.snapshot_id.as_str().to_owned())?;
        let mut legacy_spec = AddJobSpec {
            job_id: spec.job_id.clone(),
            principal: spec.principal.clone(),
            tenant_id: spec.tenant_id.clone(),
            project_id: spec.project_id.clone(),
            artifact_id: spec.artifact_id.clone(),
            playground_id,
            expected_index_version: spec.index_version.clone(),
            request_digest: neoengram_core::ContentDigest::from_bytes([0; 32]),
            deadline_unix_ms: spec.deadline_unix_ms,
            paths: Vec::new(),
            all: true,
            extensions: Extensions::new(),
        };
        legacy_spec.request_digest = legacy_spec.computed_request_digest()?;
        let job = JobRecord {
            spec: legacy_spec,
            operation: JobOperation::SnapshotMount,
            workspace_spec: None,
            snapshot_spec: Some(spec.clone()),
            state: JobState::Queued,
            resource_version: ResourceVersion::new(1),
            assignment: None,
            workspace_assignment: None,
            snapshot_assignment: None,
            accepted: None,
            progress: None,
            prepared: None,
            publication_candidate: None,
            decision: None,
            finalized: None,
            finalized_ack: None,
            failure: None,
        };
        let (job, replayed) = match self.jobs.insert_or_load(job).await? {
            JobInsertOutcome::Inserted(job) => (job, false),
            JobInsertOutcome::Existing(existing) => {
                if existing.operation != JobOperation::SnapshotMount
                    || existing.snapshot_spec.as_ref() != Some(&spec)
                {
                    return Err(invalid(
                        CentralErrorCode::JobIdReused,
                        "Snapshot mount JobId is already bound to another operation",
                    ));
                }
                (existing, true)
            }
        };
        self.audit(&job, AuditKind::JobCreated, "snapshot-mount-create")
            .await?;
        Ok(CreateSnapshotMountResult { job, replayed })
    }

    /// Persists and publishes a server-selected SnapshotMount assignment.
    pub async fn assign_snapshot_mount(
        &self,
        request: AssignSnapshotMountRequest,
    ) -> CentralResult<AssignSnapshotMountResult> {
        let mut job = self.load(&request.tenant_id, &request.job_id).await?;
        if job.operation != JobOperation::SnapshotMount {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "Job is not a SnapshotMount operation",
            ));
        }
        let spec = job.snapshot_spec.clone().ok_or_else(|| {
            invalid(
                CentralErrorCode::Internal,
                "SnapshotMount Job has no immutable operation spec",
            )
        })?;
        if spec.storage_volume_id != request.target.storage_volume_id {
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "Snapshot mount assignment selected a different StorageVolume",
            ));
        }
        let assignment = SnapshotMountAssignment {
            job_id: spec.job_id.clone(),
            assignment_id: request.target.assignment_id.clone(),
            assignment_generation: request.target.assignment_generation,
            agent_id: request.target.agent_id.clone(),
            principal: spec.principal.clone(),
            tenant_id: spec.tenant_id.clone(),
            project_id: spec.project_id.clone(),
            artifact_id: spec.artifact_id.clone(),
            snapshot_id: spec.snapshot_id.clone(),
            storage_volume_id: spec.storage_volume_id.clone(),
            agent_mount_id: request.target.agent_mount_id.clone(),
            mount_generation: request.target.mount_generation,
            owner_generation: request.target.owner_generation,
            relative_root: spec.relative_root.clone(),
            commit_id: spec.commit_id,
            index_version: spec.index_version.clone(),
            request_digest: spec.request_digest,
            deadline_unix_ms: spec.deadline_unix_ms,
            extensions: Extensions::new(),
        };
        assignment.validate()?;
        let envelope = JobAssignment {
            assignment: AssignmentOperation::SnapshotMount {
                input: assignment.clone(),
                extensions: Extensions::new(),
            },
            extensions: Extensions::new(),
        };
        envelope.validate()?;
        if let Some(existing) = &job.snapshot_assignment {
            if existing == &assignment {
                // Snapshot serving is a durable responsibility. A heartbeat gap can mark the
                // catalog Abnormal after this Job succeeded, so make the exact fenced assignment
                // deliverable again and let the Agent report its durable mount observation.
                let _ = self.outbox.reserve(envelope.clone()).await?;
                let _ = self.outbox.reactivate(envelope.clone()).await?;
                return Ok(AssignSnapshotMountResult {
                    job,
                    assignment: envelope,
                    replayed: true,
                });
            }
            let next_generation = existing
                .assignment_generation
                .get()
                .checked_add(1)
                .ok_or_else(|| {
                    invalid(
                        CentralErrorCode::GenerationMismatch,
                        "assignment generation exhausted",
                    )
                })?;
            if !job.state.is_terminal()
                || request.target.assignment_generation.get() != next_generation
                || request.target.assignment_id == existing.assignment_id
            {
                return Err(invalid(
                    CentralErrorCode::AssignmentMismatch,
                    "Snapshot remount must fence a terminal assignment with its next generation",
                ));
            }
            let snapshot = self
                .catalog
                .as_ref()
                .ok_or_else(|| {
                    invalid(
                        CentralErrorCode::InvalidState,
                        "ControlPlane has no control catalog for Snapshot state",
                    )
                })?
                .get_snapshot(&spec.tenant_id, &spec.snapshot_id)
                .await?
                .ok_or_else(|| {
                    invalid(CentralErrorCode::JobNotFound, "Snapshot no longer exists")
                })?;
            if snapshot.state != crate::SnapshotState::Abnormal {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "Snapshot remount requires an Abnormal catalog state",
                ));
            }
            let previous_assignment_id = existing.assignment_id.clone();
            let _ = self.outbox.reserve(envelope.clone()).await?;
            let previous = job.resource_version.get();
            job.snapshot_assignment = Some(assignment.clone());
            job.accepted = None;
            job.progress = None;
            job.failure = None;
            job.state = JobState::Assigned;
            job.resource_version = ResourceVersion::new(previous.saturating_add(1));
            job = self.replace(previous, job).await?;
            let _ = self
                .outbox
                .retire(&request.tenant_id, &previous_assignment_id)
                .await?;
            let _ = self.outbox.publish(envelope.clone()).await?;
            self.audit(
                &job,
                AuditKind::AssignmentQueued,
                "snapshot-remount-assignment",
            )
            .await?;
            return Ok(AssignSnapshotMountResult {
                job,
                assignment: envelope,
                replayed: false,
            });
        }
        if job.state != JobState::Queued {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                format!("cannot assign SnapshotMount Job in state {:?}", job.state),
            ));
        }
        let _ = self.outbox.reserve(envelope.clone()).await?;
        let previous = job.resource_version.get();
        job.snapshot_assignment = Some(assignment.clone());
        job.state = JobState::Assigned;
        job.resource_version = ResourceVersion::new(previous.saturating_add(1));
        job = match self.replace(previous, job).await {
            Ok(job) => job,
            Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => {
                let persisted = self.load(&request.tenant_id, &request.job_id).await?;
                if persisted.snapshot_assignment.as_ref() != Some(&assignment) {
                    return Err(error);
                }
                let _ = self.outbox.reserve(envelope.clone()).await?;
                let _ = self.outbox.publish(envelope.clone()).await?;
                return Ok(AssignSnapshotMountResult {
                    job: persisted,
                    assignment: envelope,
                    replayed: true,
                });
            }
            Err(error) => return Err(error),
        };
        let _ = self.outbox.publish(envelope.clone()).await?;
        self.audit(
            &job,
            AuditKind::AssignmentQueued,
            "snapshot-mount-assignment",
        )
        .await?;
        Ok(AssignSnapshotMountResult {
            job,
            assignment: envelope,
            replayed: false,
        })
    }

    pub async fn expire_snapshot_mount(
        &self,
        tenant_id: &neoengram_protocol::TenantId,
        job_id: &neoengram_protocol::JobId,
    ) -> CentralResult<JobRecord> {
        let mut job = self.load(tenant_id, job_id).await?;
        if job.operation != JobOperation::SnapshotMount {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "Job is not a SnapshotMount operation",
            ));
        }
        if job.state == JobState::TimedOut {
            return Ok(job);
        }
        if job.spec.deadline_unix_ms.get() > self.clock.now().get() {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "cannot expire SnapshotMount before its deadline",
            ));
        }
        let spec = job.snapshot_spec.as_ref().ok_or_else(|| {
            invalid(
                CentralErrorCode::Internal,
                "SnapshotMount Job lost its immutable spec",
            )
        })?;
        self.catalog
            .as_ref()
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::InvalidState,
                    "ControlPlane has no control catalog for Snapshot state",
                )
            })?
            .transition_snapshot_state(
                &spec.tenant_id,
                &spec.snapshot_id,
                crate::SnapshotState::Creating,
                crate::SnapshotState::Abnormal,
                crate::SnapshotPhase::Idle,
                self.clock.now(),
            )
            .await?;
        let previous = job.resource_version.get();
        job.state = JobState::TimedOut;
        job = self.replace(previous, job).await?;
        if let Some(assignment) = &job.snapshot_assignment {
            let _ = self
                .outbox
                .retire(tenant_id, &assignment.assignment_id)
                .await?;
        }
        Ok(job)
    }

    pub async fn recover_snapshot_mount(
        &self,
        tenant_id: &neoengram_protocol::TenantId,
        job_id: &neoengram_protocol::JobId,
    ) -> CentralResult<JobRecord> {
        let mut job = self.load(tenant_id, job_id).await?;
        if job.operation != JobOperation::SnapshotMount || job.state.is_terminal() {
            return Ok(job);
        }
        let spec = job.snapshot_spec.as_ref().ok_or_else(|| {
            invalid(
                CentralErrorCode::Internal,
                "SnapshotMount Job lost its immutable spec",
            )
        })?;
        let snapshot = self
            .catalog
            .as_ref()
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::InvalidState,
                    "ControlPlane has no control catalog for Snapshot state",
                )
            })?
            .get_snapshot(&spec.tenant_id, &spec.snapshot_id)
            .await?
            .ok_or_else(|| invalid(CentralErrorCode::JobNotFound, "Snapshot no longer exists"))?;
        let recovered_state = match snapshot.state {
            crate::SnapshotState::Creating => return Ok(job),
            crate::SnapshotState::Ready => JobState::Succeeded,
            // Abnormal is also the expected catalog state while a remount assignment is in
            // flight. Do not repeatedly fence a newly Assigned/Accepted/Running attempt; an
            // authenticated recovery failure performs the explicit RecoveryRequired CAS.
            crate::SnapshotState::Abnormal => return Ok(job),
        };
        let previous = job.resource_version.get();
        job.state = recovered_state;
        job = self.replace(previous, job).await?;
        if let Some(assignment) = &job.snapshot_assignment {
            let _ = self
                .outbox
                .retire(tenant_id, &assignment.assignment_id)
                .await?;
        }
        Ok(job)
    }

    /// Marks an elapsed materialization terminal and exposes the failed lifecycle on Playground.
    pub async fn expire_workspace_materialization(
        &self,
        tenant_id: &neoengram_protocol::TenantId,
        job_id: &neoengram_protocol::JobId,
    ) -> CentralResult<JobRecord> {
        let mut job = self.load(tenant_id, job_id).await?;
        if job.operation != JobOperation::WorkspaceMaterialize {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "Job is not a WorkspaceMaterialize operation",
            ));
        }
        if job.state == JobState::TimedOut {
            return Ok(job);
        }
        if job.spec.deadline_unix_ms.get() > self.clock.now().get() {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "cannot expire WorkspaceMaterialize before its deadline",
            ));
        }
        let spec = job.workspace_spec.as_ref().ok_or_else(|| {
            invalid(
                CentralErrorCode::Internal,
                "WorkspaceMaterialize Job lost its immutable spec",
            )
        })?;
        self.catalog
            .as_ref()
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::InvalidState,
                    "ControlPlane has no control catalog for materialization state",
                )
            })?
            .transition_playground_state(
                &spec.tenant_id,
                &spec.project_id,
                &spec.artifact_id,
                &spec.playground_id,
                crate::PlaygroundState::Creating,
                crate::PlaygroundState::Abnormal,
                self.clock.now(),
            )
            .await?;
        let previous = job.resource_version.get();
        job.state = JobState::TimedOut;
        job = self.replace(previous, job).await?;
        if let Some(assignment) = &job.workspace_assignment {
            let _ = self
                .outbox
                .retire(tenant_id, &assignment.assignment_id)
                .await?;
        }
        Ok(job)
    }

    /// Converges the Job side of the cross-database publication after a crash between the
    /// catalog lifecycle CAS and the Job CAS. No success or failure is fabricated while the
    /// Playground remains Creating.
    pub async fn recover_workspace_materialization(
        &self,
        tenant_id: &neoengram_protocol::TenantId,
        job_id: &neoengram_protocol::JobId,
    ) -> CentralResult<JobRecord> {
        let mut job = self.load(tenant_id, job_id).await?;
        if job.operation != JobOperation::WorkspaceMaterialize || job.state.is_terminal() {
            return Ok(job);
        }
        let spec = job.workspace_spec.as_ref().ok_or_else(|| {
            invalid(
                CentralErrorCode::Internal,
                "WorkspaceMaterialize Job lost its immutable spec",
            )
        })?;
        let playground = self
            .catalog
            .as_ref()
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::InvalidState,
                    "ControlPlane has no control catalog for materialization state",
                )
            })?
            .get_playground(
                &spec.tenant_id,
                &spec.project_id,
                &spec.artifact_id,
                &spec.playground_id,
            )
            .await?
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::JobNotFound,
                    "materialization Playground no longer exists",
                )
            })?;
        let recovered_state = match playground.state {
            crate::PlaygroundState::Creating => return Ok(job),
            crate::PlaygroundState::Ready => JobState::Succeeded,
            crate::PlaygroundState::Abnormal => JobState::RecoveryRequired,
        };
        let previous = job.resource_version.get();
        job.state = recovered_state;
        job = self.replace(previous, job).await?;
        if let Some(assignment) = &job.workspace_assignment {
            let _ = self
                .outbox
                .retire(tenant_id, &assignment.assignment_id)
                .await?;
        }
        Ok(job)
    }

    /// Returns the authoritative Job only when its persisted scope is visible to the actor.
    pub async fn query_job(&self, request: QueryJobRequest) -> CentralResult<QueryJobResult> {
        let job = self.load(&request.tenant_id, &request.job_id).await?;
        if let Err(error) = self
            .authorize(Actor::Principal(request.actor), Action::QueryJob, &job.spec)
            .await
        {
            if error.code() == CentralErrorCode::Unauthorized {
                return Err(job_not_found(&request.job_id));
            }
            return Err(error);
        }
        Ok(QueryJobResult { job })
    }

    /// Reserves its delivery identity, persists the assignment, then exposes it in the outbox.
    pub async fn assign_job(&self, request: AssignJobRequest) -> CentralResult<AssignJobResult> {
        let mut job = self.load(&request.tenant_id, &request.job_id).await?;
        self.authorize(
            Actor::Principal(request.actor),
            Action::AssignJob,
            &job.spec,
        )
        .await?;
        let assignment = neoengram_protocol::AddAssignment {
            job_id: job.spec.job_id.clone(),
            assignment_id: request.target.assignment_id.clone(),
            assignment_generation: request.target.assignment_generation,
            agent_id: request.target.agent_id.clone(),
            principal: job.spec.principal.clone(),
            tenant_id: job.spec.tenant_id.clone(),
            project_id: job.spec.project_id.clone(),
            artifact_id: job.spec.artifact_id.clone(),
            playground_id: job.spec.playground_id.clone(),
            edge_cluster_id: request.target.edge_cluster_id.clone(),
            storage_volume_id: request.target.storage_volume_id.clone(),
            artifact_placement_id: request.target.artifact_placement_id.clone(),
            placement_generation: request.target.placement_generation,
            agent_mount_id: request.target.agent_mount_id.clone(),
            mount_generation: request.target.mount_generation,
            owner_generation: request.target.owner_generation,
            expected_index_version: job.spec.expected_index_version.clone(),
            lease: request.target.lease.clone(),
            request_digest: job.spec.request_digest,
            deadline_unix_ms: job.spec.deadline_unix_ms,
            paths: job.spec.paths.clone(),
            all: job.spec.all,
            extensions: job.spec.extensions.clone(),
        };
        let envelope = JobAssignment {
            assignment: AssignmentOperation::Add {
                input: assignment.clone(),
                extensions: Extensions::new(),
            },
            extensions: Extensions::new(),
        };

        if let Some(existing) = &job.assignment {
            if existing != &assignment {
                return Err(invalid(
                    CentralErrorCode::JobIdReused,
                    "job already has a different persisted assignment",
                ));
            }
            let _ = self.outbox.reserve(envelope.clone()).await?;
            let _ = self.outbox.publish(envelope.clone()).await?;
            self.audit(&job, AuditKind::AssignmentQueued, "assignment")
                .await?;
            return Ok(AssignJobResult {
                job,
                assignment: envelope,
                replayed: true,
            });
        }
        validate_assignment_target(&request.target, self.clock.now().get())?;
        if job.spec.deadline_unix_ms.get() <= self.clock.now().get() {
            return Err(invalid(
                CentralErrorCode::DeadlineExceeded,
                "managed Add deadline elapsed before assignment",
            ));
        }
        if job.state != JobState::Queued {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                format!("cannot assign a job in state {:?}", job.state),
            ));
        }

        // Reserve first so a tenant-scoped AssignmentId conflict cannot mutate the job. The
        // reservation remains delivery-invisible until the authoritative assignment is durable.
        let _ = self.outbox.reserve(envelope.clone()).await?;
        let previous = job.resource_version.get();
        job.assignment = Some(assignment.clone());
        job.state = JobState::Assigned;
        job = match self.replace(previous, job).await {
            Ok(job) => job,
            Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => {
                let persisted = self.load(&request.tenant_id, &request.job_id).await?;
                if persisted.assignment.as_ref() != Some(&assignment) {
                    return Err(error);
                }
                let _ = self.outbox.reserve(envelope.clone()).await?;
                let _ = self.outbox.publish(envelope.clone()).await?;
                self.audit(&persisted, AuditKind::AssignmentQueued, "assignment")
                    .await?;
                return Ok(AssignJobResult {
                    job: persisted,
                    assignment: envelope,
                    replayed: true,
                });
            }
            Err(error) => return Err(error),
        };

        let _ = self.outbox.publish(envelope.clone()).await?;
        self.audit(&job, AuditKind::AssignmentQueued, "assignment")
            .await?;
        Ok(AssignJobResult {
            job,
            assignment: envelope,
            replayed: false,
        })
    }

    /// Applies one idempotent agent report to the persisted state machine.
    pub async fn receive_report(
        &self,
        request: ReceiveReportRequest,
    ) -> CentralResult<ReceiveReportResult> {
        self.receive_report_with_session(request, None).await
    }

    /// Applies a report received over an authenticated Agent session.
    ///
    /// Snapshot serving observations additionally bind their original durable report to the
    /// current session generation, preventing an old success from being re-signed after restart.
    pub async fn receive_session_report(
        &self,
        request: ReceiveReportRequest,
        session_generation: SessionGeneration,
    ) -> CentralResult<ReceiveReportResult> {
        self.receive_report_with_session(request, Some(session_generation))
            .await
    }

    async fn receive_report_with_session(
        &self,
        request: ReceiveReportRequest,
        session_generation: Option<SessionGeneration>,
    ) -> CentralResult<ReceiveReportResult> {
        let mut job = self
            .load(&request.tenant_id, request.report.job_id())
            .await?;
        if job.is_workspace_materialization() {
            return self
                .receive_workspace_materialization_report(job, request)
                .await;
        }
        if job.is_snapshot_mount() {
            return self
                .receive_snapshot_mount_report(job, request, session_generation)
                .await;
        }
        self.authorize(
            Actor::Agent(request.agent_id.clone()),
            Action::ReceiveReport,
            &job.spec,
        )
        .await?;
        let assignment = job.assignment.as_ref().ok_or_else(|| {
            invalid(
                CentralErrorCode::InvalidState,
                "cannot receive an agent report before assignment",
            )
        })?;
        if assignment.agent_id != request.agent_id {
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "reporting agent does not own the persisted assignment",
            ));
        }

        let assignment_id = assignment.assignment_id.clone();
        let previous = job.resource_version.get();
        let replayed = match request.report {
            AgentReport::Accepted(report) => {
                report.validate()?;
                validate_report_identity(
                    assignment,
                    &report.job_id,
                    &report.assignment_id,
                    report.assignment_generation,
                )?;
                if report.request_digest != assignment.request_digest {
                    return Err(invalid(
                        CentralErrorCode::AssignmentMismatch,
                        "accepted report carries a different request digest",
                    ));
                }
                if let Some(existing) = &job.accepted {
                    if existing == &report {
                        true
                    } else {
                        return Err(invalid(
                            CentralErrorCode::AssignmentMismatch,
                            "assignment was accepted with a different report payload",
                        ));
                    }
                } else {
                    if job.state != JobState::Assigned {
                        return Err(invalid(
                            CentralErrorCode::InvalidState,
                            format!("cannot accept a job in state {:?}", job.state),
                        ));
                    }
                    job.accepted = Some(report);
                    job.state = JobState::Accepted;
                    false
                }
            }
            AgentReport::Progress(report) => {
                report.validate()?;
                validate_report_identity(
                    assignment,
                    &report.job_id,
                    &report.assignment_id,
                    report.assignment_generation,
                )?;
                if report.state != JobState::Running {
                    return Err(invalid(
                        CentralErrorCode::InvalidState,
                        "progress reports must carry running state",
                    ));
                }
                if job.progress.as_ref() == Some(&report) {
                    true
                } else {
                    if !matches!(job.state, JobState::Accepted | JobState::Running) {
                        return Err(invalid(
                            CentralErrorCode::InvalidState,
                            format!("cannot apply progress in state {:?}", job.state),
                        ));
                    }
                    job.progress = Some(report);
                    job.state = JobState::Running;
                    false
                }
            }
            AgentReport::Prepared(report) => {
                validate_prepared(&job, &report)?;
                if let Some(existing) = &job.prepared {
                    if existing == &report {
                        true
                    } else {
                        return Err(invalid(
                            CentralErrorCode::BatchTampered,
                            "job was prepared with different metadata descriptors",
                        ));
                    }
                } else {
                    if job.state != JobState::Running {
                        return Err(invalid(
                            CentralErrorCode::InvalidState,
                            format!("cannot prepare a job in state {:?}", job.state),
                        ));
                    }
                    job.prepared = Some(report);
                    job.state = JobState::Prepared;
                    false
                }
            }
            AgentReport::Finalized(report) => {
                report.validate()?;
                validate_report_identity(
                    assignment,
                    &report.job_id,
                    &report.assignment_id,
                    report.assignment_generation,
                )?;
                let finalized = job.finalized.as_ref().ok_or_else(|| {
                    invalid(
                        CentralErrorCode::InvalidState,
                        "agent finalized before the central publish decision",
                    )
                })?;
                if report.decision_generation != finalized.decision_generation
                    || report.final_state != finalized.final_state
                {
                    return Err(invalid(
                        CentralErrorCode::GenerationMismatch,
                        "agent finalized acknowledgement differs from central decision",
                    ));
                }
                if let Some(existing) = &job.finalized_ack {
                    if existing == &report {
                        true
                    } else {
                        return Err(invalid(
                            CentralErrorCode::AssignmentMismatch,
                            "agent replayed a different finalized acknowledgement",
                        ));
                    }
                } else {
                    job.finalized_ack = Some(report);
                    false
                }
            }
            AgentReport::Failed(report) => {
                report.validate()?;
                if report.tenant_id != request.tenant_id {
                    return Err(invalid(
                        CentralErrorCode::AssignmentMismatch,
                        "job failure tenant does not match the report authority scope",
                    ));
                }
                validate_report_identity(
                    assignment,
                    &report.job_id,
                    &report.assignment_id,
                    report.assignment_generation,
                )?;
                validate_terminal_state(report.final_state)?;
                if job.state == JobState::Publishing {
                    return Err(invalid(
                        CentralErrorCode::InvalidState,
                        "cannot replace a publishing job with an agent terminal report",
                    ));
                }
                if let Some(existing) = &job.failure {
                    if existing == &report {
                        true
                    } else {
                        return Err(invalid(
                            CentralErrorCode::AssignmentMismatch,
                            "agent replayed a different terminal report",
                        ));
                    }
                } else {
                    if job.state.is_terminal() {
                        return Err(invalid(
                            CentralErrorCode::InvalidState,
                            "job already has a different terminal outcome",
                        ));
                    }
                    let decision_generation = DecisionGeneration::new(1);
                    let decision = JobDecision {
                        job_id: assignment.job_id.clone(),
                        assignment_id: assignment.assignment_id.clone(),
                        assignment_generation: assignment.assignment_generation,
                        decision_generation,
                        decision: PublishDecision::Reject {
                            error: report.error.clone(),
                            extensions: Extensions::new(),
                        },
                        final_state: report.final_state,
                        extensions: Extensions::new(),
                    };
                    let finalized = JobFinalized {
                        job_id: assignment.job_id.clone(),
                        assignment_id: assignment.assignment_id.clone(),
                        assignment_generation: assignment.assignment_generation,
                        decision_generation,
                        final_state: report.final_state,
                        finalized_at_unix_ms: report.failed_at_unix_ms,
                        extensions: Extensions::new(),
                    };
                    decision.validate()?;
                    finalized.validate()?;
                    job.decision = Some(decision);
                    job.finalized = Some(finalized);
                    job.state = report.final_state;
                    job.failure = Some(report);
                    false
                }
            }
        };

        if !replayed {
            job = self.replace(previous, job).await?;
        }
        // Every valid report proves delivery. Accepted is the normal acknowledgement; retiring on
        // later reports also repairs a lost acknowledgement response without leaving a stale row.
        let _ = self
            .outbox
            .retire(&request.tenant_id, &assignment_id)
            .await?;
        self.audit(&job, AuditKind::ReportReceived, "report")
            .await?;
        Ok(ReceiveReportResult { job, replayed })
    }

    async fn receive_workspace_materialization_report(
        &self,
        mut job: JobRecord,
        request: ReceiveReportRequest,
    ) -> CentralResult<ReceiveReportResult> {
        let assignment = job.workspace_assignment.clone().ok_or_else(|| {
            invalid(
                CentralErrorCode::InvalidState,
                "WorkspaceMaterialize Job has no persisted assignment",
            )
        })?;
        if assignment.agent_id != request.agent_id {
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "reporting Agent does not own the WorkspaceMaterialize assignment",
            ));
        }
        let catalog = self.catalog.as_ref().ok_or_else(|| {
            invalid(
                CentralErrorCode::InvalidState,
                "ControlPlane has no control catalog for materialization state",
            )
        })?;
        let assignment_id = assignment.assignment_id.clone();
        let mut replayed = false;
        match request.report {
            AgentReport::Accepted(report) => {
                report.validate()?;
                validate_workspace_report_identity(
                    &assignment,
                    &report.job_id,
                    &report.assignment_id,
                    report.assignment_generation,
                )?;
                if report.request_digest != assignment.request_digest {
                    return Err(invalid(
                        CentralErrorCode::AssignmentMismatch,
                        "accepted materialization report carries a different request digest",
                    ));
                }
                // A restarted materializer no longer has the acknowledged report in its local
                // outbox. Identity and request_digest are the immutable acceptance facts; a new
                // observation timestamp is therefore a semantic replay, not a conflicting claim.
                if job.accepted.is_some() {
                    replayed = true;
                } else {
                    if job.state != JobState::Assigned {
                        return Err(invalid(
                            CentralErrorCode::InvalidState,
                            format!("cannot accept materialization Job in state {:?}", job.state),
                        ));
                    }
                    let previous = job.resource_version.get();
                    job.accepted = Some(report);
                    job.state = JobState::Accepted;
                    job = self.replace(previous, job).await?;
                }
            }
            AgentReport::Progress(report) => {
                report.validate()?;
                validate_workspace_report_identity(
                    &assignment,
                    &report.job_id,
                    &report.assignment_id,
                    report.assignment_generation,
                )?;
                if job.progress.as_ref() == Some(&report) {
                    replayed = true;
                } else {
                    match report.state {
                        JobState::Running => {
                            if !matches!(job.state, JobState::Accepted | JobState::Running) {
                                return Err(invalid(
                                    CentralErrorCode::InvalidState,
                                    format!(
                                        "cannot apply materialization progress in state {:?}",
                                        job.state
                                    ),
                                ));
                            }
                        }
                        JobState::Succeeded => {
                            if !matches!(
                                job.state,
                                JobState::Accepted | JobState::Running | JobState::Succeeded
                            ) {
                                return Err(invalid(
                                    CentralErrorCode::InvalidState,
                                    format!(
                                        "cannot complete materialization Job in state {:?}",
                                        job.state
                                    ),
                                ));
                            }
                            catalog
                                .transition_playground_state(
                                    &assignment.tenant_id,
                                    &assignment.project_id,
                                    &assignment.artifact_id,
                                    &assignment.playground_id,
                                    crate::PlaygroundState::Creating,
                                    crate::PlaygroundState::Ready,
                                    self.clock.now(),
                                )
                                .await?;
                        }
                        state => {
                            return Err(invalid(
                                CentralErrorCode::InvalidState,
                                format!(
                                    "WorkspaceMaterialize progress cannot carry state {state:?}"
                                ),
                            ));
                        }
                    }
                    let previous = job.resource_version.get();
                    job.state = report.state;
                    job.progress = Some(report);
                    job = self.replace(previous, job).await?;
                }
            }
            AgentReport::Failed(report) => {
                report.validate()?;
                validate_workspace_report_identity(
                    &assignment,
                    &report.job_id,
                    &report.assignment_id,
                    report.assignment_generation,
                )?;
                validate_terminal_state(report.final_state)?;
                if job.failure.as_ref() == Some(&report) {
                    replayed = true;
                } else {
                    if job.state.is_terminal() && job.state != JobState::RecoveryRequired {
                        return Err(invalid(
                            CentralErrorCode::InvalidState,
                            "materialization Job already has another terminal outcome",
                        ));
                    }
                    catalog
                        .transition_playground_state(
                            &assignment.tenant_id,
                            &assignment.project_id,
                            &assignment.artifact_id,
                            &assignment.playground_id,
                            crate::PlaygroundState::Creating,
                            crate::PlaygroundState::Abnormal,
                            self.clock.now(),
                        )
                        .await?;
                    let previous = job.resource_version.get();
                    job.state = report.final_state;
                    job.failure = Some(report);
                    job = self.replace(previous, job).await?;
                }
            }
            AgentReport::Prepared(_) | AgentReport::Finalized(_) => {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "WorkspaceMaterialize does not publish metadata or await a decision",
                ));
            }
        }
        if job.state.is_terminal() {
            let _ = self
                .outbox
                .retire(&request.tenant_id, &assignment_id)
                .await?;
        }
        self.audit(&job, AuditKind::ReportReceived, "materialize-report")
            .await?;
        Ok(ReceiveReportResult { job, replayed })
    }

    async fn receive_snapshot_mount_report(
        &self,
        mut job: JobRecord,
        request: ReceiveReportRequest,
        session_generation: Option<SessionGeneration>,
    ) -> CentralResult<ReceiveReportResult> {
        let assignment = job.snapshot_assignment.clone().ok_or_else(|| {
            invalid(
                CentralErrorCode::InvalidState,
                "SnapshotMount Job has no persisted assignment",
            )
        })?;
        if assignment.agent_id != request.agent_id {
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "reporting Agent does not own the SnapshotMount assignment",
            ));
        }
        let catalog = self.catalog.as_ref().ok_or_else(|| {
            invalid(
                CentralErrorCode::InvalidState,
                "ControlPlane has no control catalog for Snapshot state",
            )
        })?;
        let assignment_id = assignment.assignment_id.clone();
        let mut replayed = false;
        if let Some(session_generation) = session_generation {
            validate_snapshot_report_session_generation(&request.report, session_generation)?;
        }
        match request.report {
            AgentReport::Accepted(report) => {
                report.validate()?;
                validate_snapshot_report_identity(
                    &assignment,
                    &report.job_id,
                    &report.assignment_id,
                    report.assignment_generation,
                )?;
                if report.request_digest != assignment.request_digest {
                    return Err(invalid(
                        CentralErrorCode::AssignmentMismatch,
                        "accepted SnapshotMount report carries a different request digest",
                    ));
                }
                if job.accepted.is_some() {
                    replayed = true;
                } else {
                    if job.state != JobState::Assigned {
                        return Err(invalid(
                            CentralErrorCode::InvalidState,
                            format!("cannot accept SnapshotMount Job in state {:?}", job.state),
                        ));
                    }
                    let previous = job.resource_version.get();
                    job.accepted = Some(report);
                    job.state = JobState::Accepted;
                    job = self.replace(previous, job).await?;
                }
            }
            AgentReport::Progress(report) => {
                report.validate()?;
                validate_snapshot_report_identity(
                    &assignment,
                    &report.job_id,
                    &report.assignment_id,
                    report.assignment_generation,
                )?;
                match report.state {
                    JobState::Running => {
                        if !matches!(job.state, JobState::Accepted | JobState::Running) {
                            return Err(invalid(
                                CentralErrorCode::InvalidState,
                                format!(
                                    "cannot apply SnapshotMount progress in state {:?}",
                                    job.state
                                ),
                            ));
                        }
                        let snapshot = catalog
                            .get_snapshot(&assignment.tenant_id, &assignment.snapshot_id)
                            .await?
                            .ok_or_else(|| {
                                invalid(CentralErrorCode::JobNotFound, "Snapshot no longer exists")
                            })?;
                        if snapshot.state == crate::SnapshotState::Creating
                            && snapshot.phase == crate::SnapshotPhase::Planning
                        {
                            let _ = catalog
                                .transition_snapshot_state(
                                    &assignment.tenant_id,
                                    &assignment.snapshot_id,
                                    crate::SnapshotState::Creating,
                                    crate::SnapshotState::Creating,
                                    crate::SnapshotPhase::Materializing,
                                    self.clock.now(),
                                )
                                .await?;
                        }
                    }
                    JobState::Succeeded => {
                        if report.phase != "mounted" {
                            return Err(invalid(
                                CentralErrorCode::InvalidState,
                                "SnapshotMount success must report phase mounted",
                            ));
                        }
                        if !matches!(
                            job.state,
                            JobState::Accepted
                                | JobState::Running
                                | JobState::Succeeded
                                | JobState::RecoveryRequired
                        ) {
                            return Err(invalid(
                                CentralErrorCode::InvalidState,
                                format!(
                                    "cannot complete SnapshotMount Job in state {:?}",
                                    job.state
                                ),
                            ));
                        }
                        let snapshot = catalog
                            .get_snapshot(&assignment.tenant_id, &assignment.snapshot_id)
                            .await?
                            .ok_or_else(|| {
                                invalid(CentralErrorCode::JobNotFound, "Snapshot no longer exists")
                            })?;
                        match snapshot.state {
                            crate::SnapshotState::Creating => {
                                let _ = catalog
                                    .transition_snapshot_state(
                                        &assignment.tenant_id,
                                        &assignment.snapshot_id,
                                        crate::SnapshotState::Creating,
                                        crate::SnapshotState::Ready,
                                        crate::SnapshotPhase::Idle,
                                        self.clock.now(),
                                    )
                                    .await?;
                            }
                            crate::SnapshotState::Abnormal => {
                                // A durable mount descriptor can be remounted by the same fenced
                                // Agent after restart. Its fresh mounted observation restores the
                                // catalog without creating a second Snapshot identity.
                                let _ = catalog
                                    .transition_snapshot_state(
                                        &assignment.tenant_id,
                                        &assignment.snapshot_id,
                                        crate::SnapshotState::Abnormal,
                                        crate::SnapshotState::Ready,
                                        crate::SnapshotPhase::Idle,
                                        self.clock.now(),
                                    )
                                    .await?;
                            }
                            crate::SnapshotState::Ready => {}
                        }
                    }
                    state => {
                        return Err(invalid(
                            CentralErrorCode::InvalidState,
                            format!("SnapshotMount progress cannot carry state {state:?}"),
                        ));
                    }
                }
                if job.progress.as_ref() == Some(&report) {
                    replayed = true;
                } else {
                    let previous = job.resource_version.get();
                    job.state = report.state;
                    job.progress = Some(report);
                    job.failure = None;
                    job = self.replace(previous, job).await?;
                }
            }
            AgentReport::Failed(report) => {
                report.validate()?;
                validate_snapshot_report_identity(
                    &assignment,
                    &report.job_id,
                    &report.assignment_id,
                    report.assignment_generation,
                )?;
                validate_terminal_state(report.final_state)?;
                let recovery_failure = report
                    .extensions
                    .get("snapshot_mount_recovery")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true);
                if job.failure.as_ref() == Some(&report) {
                    replayed = true;
                } else {
                    let recovery_after_success = recovery_failure
                        && matches!(job.state, JobState::Succeeded | JobState::RecoveryRequired);
                    if job.state.is_terminal()
                        && job.state != JobState::RecoveryRequired
                        && !recovery_after_success
                    {
                        return Err(invalid(
                            CentralErrorCode::InvalidState,
                            "SnapshotMount Job already has another terminal outcome",
                        ));
                    }
                    let snapshot = catalog
                        .get_snapshot(&assignment.tenant_id, &assignment.snapshot_id)
                        .await?
                        .ok_or_else(|| {
                            invalid(CentralErrorCode::JobNotFound, "Snapshot no longer exists")
                        })?;
                    match snapshot.state {
                        crate::SnapshotState::Creating => {
                            let _ = catalog
                                .transition_snapshot_state(
                                    &assignment.tenant_id,
                                    &assignment.snapshot_id,
                                    crate::SnapshotState::Creating,
                                    crate::SnapshotState::Abnormal,
                                    crate::SnapshotPhase::Idle,
                                    self.clock.now(),
                                )
                                .await?;
                        }
                        crate::SnapshotState::Ready if recovery_after_success => {
                            // The Agent authenticated the same assignment fence but could no
                            // longer reconstruct its durable read-only mount. Demote the serving
                            // view and let the coordinator issue a fresh assignment generation.
                            let _ = catalog
                                .transition_snapshot_state(
                                    &assignment.tenant_id,
                                    &assignment.snapshot_id,
                                    crate::SnapshotState::Ready,
                                    crate::SnapshotState::Abnormal,
                                    crate::SnapshotPhase::Idle,
                                    self.clock.now(),
                                )
                                .await?;
                        }
                        crate::SnapshotState::Abnormal => {}
                        crate::SnapshotState::Ready => {
                            return Err(invalid(
                                CentralErrorCode::InvalidState,
                                "a Ready Snapshot only accepts an explicit mount recovery failure",
                            ));
                        }
                    }
                    let previous = job.resource_version.get();
                    job.state = if recovery_after_success {
                        JobState::RecoveryRequired
                    } else {
                        report.final_state
                    };
                    job.failure = Some(report);
                    job = self.replace(previous, job).await?;
                }
            }
            AgentReport::Prepared(_) | AgentReport::Finalized(_) => {
                return Err(invalid(
                    CentralErrorCode::InvalidState,
                    "SnapshotMount does not publish metadata or await a decision",
                ));
            }
        }
        if job.state.is_terminal() {
            let _ = self
                .outbox
                .retire(&request.tenant_id, &assignment_id)
                .await?;
        }
        self.audit(&job, AuditKind::ReportReceived, "snapshot-mount-report")
            .await?;
        Ok(ReceiveReportResult { job, replayed })
    }

    /// Stages an exact prepared descriptor or page after validating assignment and batch scope.
    pub async fn stage_metadata_batch(
        &self,
        request: StageMetadataBatchRequest,
    ) -> CentralResult<StageMetadataBatchResult> {
        let job = self.load(&request.tenant_id, &request.job_id).await?;
        self.authorize(
            Actor::Agent(request.agent_id.clone()),
            Action::StageMetadataBatch,
            &job.spec,
        )
        .await?;
        if !matches!(
            job.state,
            JobState::Prepared | JobState::Publishing | JobState::Succeeded | JobState::Conflicted
        ) {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                format!("cannot stage metadata in state {:?}", job.state),
            ));
        }
        let assignment = job
            .assignment
            .as_ref()
            .ok_or_else(|| invalid(CentralErrorCode::InvalidState, "job has no assignment"))?;
        if assignment.agent_id != request.agent_id {
            return Err(invalid(
                CentralErrorCode::AssignmentMismatch,
                "metadata uploader does not own the assignment",
            ));
        }
        let prepared = job
            .prepared
            .as_ref()
            .ok_or_else(|| invalid(CentralErrorCode::InvalidState, "job has no prepared report"))?;
        let batch_id = request.submission.batch_id().clone();
        let declared = prepared
            .metadata_batches
            .iter()
            .find(|descriptor| descriptor.batch_id == batch_id)
            .ok_or_else(|| {
                invalid(
                    CentralErrorCode::BatchUndeclared,
                    format!("metadata batch {batch_id} was not declared by JobPrepared"),
                )
            })?;
        validate_descriptor_scope(assignment, declared)?;
        let audit_suffix = match &request.submission {
            MetadataBatchSubmission::Descriptor(_) => format!("batch-{batch_id}-descriptor"),
            MetadataBatchSubmission::Page(page) => {
                format!("batch-{batch_id}-page-{}", page.page_number)
            }
        };
        let replayed = match request.submission {
            MetadataBatchSubmission::Descriptor(descriptor) => {
                if descriptor != *declared {
                    return Err(invalid(
                        CentralErrorCode::BatchTampered,
                        "staged descriptor differs from JobPrepared",
                    ));
                }
                self.metadata.stage_descriptor(descriptor).await?
            }
            MetadataBatchSubmission::Page(page) => {
                declared.validate_page(&page)?;
                self.metadata.stage_page(declared, page).await?
            }
        };
        let complete = self
            .metadata
            .get(&assignment.tenant_id, &batch_id)
            .await?
            .is_some_and(|batch| batch.is_complete());
        self.audit(&job, AuditKind::MetadataStaged, &audit_suffix)
            .await?;
        Ok(StageMetadataBatchResult {
            batch_id,
            complete,
            replayed,
        })
    }

    /// Atomically marks an elapsed managed Add as TimedOut and creates a terminal decision when
    /// the job already has an assignment.
    pub async fn expire_add_job(
        &self,
        request: ExpireAddJobRequest,
    ) -> CentralResult<ExpireAddJobResult> {
        let mut job = self.load(&request.tenant_id, &request.job_id).await?;
        self.authorize(
            Actor::Principal(request.actor),
            Action::ExpireAddJob,
            &job.spec,
        )
        .await?;

        if job.state == JobState::TimedOut {
            let decision = job.decision.clone();
            let finalized = job.finalized.clone();
            match (&job.assignment, &decision, &finalized) {
                (None, None, None) => {}
                (Some(assignment), Some(decision), Some(finalized)) => {
                    decision.validate()?;
                    finalized.validate()?;
                    validate_report_identity(
                        assignment,
                        &decision.job_id,
                        &decision.assignment_id,
                        decision.assignment_generation,
                    )?;
                    if !matches!(decision.decision, PublishDecision::Reject { .. })
                        || decision.final_state != JobState::TimedOut
                        || finalized.job_id != decision.job_id
                        || finalized.assignment_id != decision.assignment_id
                        || finalized.assignment_generation != decision.assignment_generation
                        || finalized.decision_generation != decision.decision_generation
                        || finalized.final_state != JobState::TimedOut
                    {
                        return Err(invalid(
                            CentralErrorCode::Internal,
                            "timed-out job has an inconsistent durable decision",
                        ));
                    }
                }
                _ => {
                    return Err(invalid(
                        CentralErrorCode::Internal,
                        "timed-out job has inconsistent assignment or decision state",
                    ));
                }
            }
            if let Some(assignment) = &job.assignment {
                let _ = self
                    .outbox
                    .retire(&request.tenant_id, &assignment.assignment_id)
                    .await?;
            }
            self.audit(&job, AuditKind::AddExpired, "expire").await?;
            return Ok(ExpireAddJobResult {
                job,
                decision,
                finalized,
                replayed: true,
            });
        }

        if !matches!(
            job.state,
            JobState::Queued
                | JobState::Assigned
                | JobState::Accepted
                | JobState::Running
                | JobState::Prepared
                | JobState::CancelRequested
        ) {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                format!("cannot expire managed Add in state {:?}", job.state),
            ));
        }
        let now = self.clock.now();
        if job.spec.deadline_unix_ms.get() > now.get() {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                "cannot expire managed Add before its deadline",
            ));
        }
        if job.decision.is_some() || job.finalized.is_some() {
            return Err(invalid(
                CentralErrorCode::Internal,
                "non-terminal job already has a publish decision",
            ));
        }

        let (decision, finalized) = match &job.assignment {
            Some(assignment) if job.state != JobState::Queued => {
                let decision_generation = DecisionGeneration::new(1);
                let decision = JobDecision {
                    job_id: assignment.job_id.clone(),
                    assignment_id: assignment.assignment_id.clone(),
                    assignment_generation: assignment.assignment_generation,
                    decision_generation,
                    decision: PublishDecision::Reject {
                        error: ControlError {
                            code: ErrorCode::new(CentralErrorCode::DeadlineExceeded.as_str())?,
                            message: "managed Add deadline elapsed before publication".to_owned(),
                            retryable: false,
                            retry_after_ms: None,
                            extensions: Extensions::new(),
                        },
                        extensions: Extensions::new(),
                    },
                    final_state: JobState::TimedOut,
                    extensions: Extensions::new(),
                };
                let finalized = JobFinalized {
                    job_id: assignment.job_id.clone(),
                    assignment_id: assignment.assignment_id.clone(),
                    assignment_generation: assignment.assignment_generation,
                    decision_generation,
                    final_state: JobState::TimedOut,
                    finalized_at_unix_ms: now,
                    extensions: Extensions::new(),
                };
                (Some(decision), Some(finalized))
            }
            None if job.state == JobState::Queued => (None, None),
            _ => {
                return Err(invalid(
                    CentralErrorCode::Internal,
                    "job assignment is inconsistent with its pre-timeout state",
                ));
            }
        };

        let previous = job.resource_version.get();
        if let Some(decision) = &decision {
            decision.validate()?;
        }
        if let Some(finalized) = &finalized {
            finalized.validate()?;
        }
        job.state = JobState::TimedOut;
        job.decision.clone_from(&decision);
        job.finalized.clone_from(&finalized);
        job = self.replace(previous, job).await?;
        if let Some(assignment) = &job.assignment {
            let _ = self
                .outbox
                .retire(&request.tenant_id, &assignment.assignment_id)
                .await?;
        }
        self.audit(&job, AuditKind::AddExpired, "expire").await?;
        Ok(ExpireAddJobResult {
            job,
            decision,
            finalized,
            replayed: false,
        })
    }

    /// Validates complete staged metadata and assigned-Volume placement evidence, then performs
    /// one CAS.
    pub async fn finalize_add(
        &self,
        request: FinalizeAddRequest,
    ) -> CentralResult<FinalizeAddResult> {
        let job = self.load(&request.tenant_id, &request.job_id).await?;
        self.authorize(
            Actor::Principal(request.actor),
            Action::FinalizeAdd,
            &job.spec,
        )
        .await?;
        self.finalize_loaded(job).await
    }

    /// Finalizes a Prepared job under the server's recovery authority.
    ///
    /// Scheduler recovery must not depend on mutable user RBAC after job creation. Public callers
    /// continue to use [`Self::finalize_add`], which performs principal authorization.
    pub async fn finalize_prepared(
        &self,
        request: ResumePublicationRequest,
    ) -> CentralResult<FinalizeAddResult> {
        let job = self.load(&request.tenant_id, &request.job_id).await?;
        if job.state != JobState::Prepared {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                format!("cannot internally finalize job in state {:?}", job.state),
            ));
        }
        self.finalize_loaded(job).await
    }

    /// Resumes only a previously frozen Publishing job without re-entering mutable user policy.
    /// Transport adapters must keep this internal and expose [`Self::finalize_add`] to users.
    pub async fn resume_publication(
        &self,
        request: ResumePublicationRequest,
    ) -> CentralResult<FinalizeAddResult> {
        let job = self.load(&request.tenant_id, &request.job_id).await?;
        if job.state != JobState::Publishing {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                format!("cannot resume publication in state {:?}", job.state),
            ));
        }
        self.finalize_loaded(job).await
    }

    async fn finalize_loaded(&self, mut job: JobRecord) -> CentralResult<FinalizeAddResult> {
        let resumed_publication = job.state == JobState::Publishing;
        if let (Some(decision), Some(finalized)) = (&job.decision, &job.finalized) {
            if matches!(
                job.state,
                JobState::Succeeded | JobState::Conflicted | JobState::Failed
            ) {
                self.audit(&job, AuditKind::AddFinalized, "finalize")
                    .await?;
                return Ok(FinalizeAddResult {
                    job: job.clone(),
                    decision: decision.clone(),
                    finalized: finalized.clone(),
                    replayed: true,
                });
            }
        }
        if !matches!(job.state, JobState::Prepared | JobState::Publishing) {
            return Err(invalid(
                CentralErrorCode::InvalidState,
                format!("cannot finalize managed Add in state {:?}", job.state),
            ));
        }
        let assignment = job.assignment.clone().ok_or_else(|| {
            invalid(
                CentralErrorCode::Internal,
                "publishing job lost its assignment",
            )
        })?;
        let publication_candidate = if resumed_publication {
            let candidate = job.publication_candidate.clone().ok_or_else(|| {
                invalid(
                    CentralErrorCode::Internal,
                    "publishing job lost its frozen publication candidate",
                )
            })?;
            validate_frozen_publication(&job, &candidate)?;
            candidate
        } else {
            if job.publication_candidate.is_some() {
                return Err(invalid(
                    CentralErrorCode::Internal,
                    "prepared job already contains a frozen publication candidate",
                ));
            }
            let metadata = validate_staged_metadata(&job, self.metadata.as_ref()).await?;
            for receipt in &metadata.placements {
                let evidence = crate::ObjectPlacementEvidence {
                    receipt: receipt.clone(),
                    placement_generation: assignment.placement_generation,
                };
                self.objects.record_placement(&evidence).await?;
                let placed = self
                    .objects
                    .object_placement(
                        &assignment.tenant_id,
                        &assignment.artifact_id,
                        &assignment.storage_volume_id,
                        &assignment.artifact_placement_id,
                        assignment.placement_generation,
                        receipt.object_id,
                    )
                    .await?
                    .ok_or_else(|| {
                        invalid(
                            CentralErrorCode::ObjectNotDurable,
                            format!(
                                "object {} has no evidence on the assigned Volume placement generation",
                                receipt.object_id
                            ),
                        )
                    })?;
                if placed.receipt.size != receipt.size {
                    return Err(invalid(
                        CentralErrorCode::ObjectNotDurable,
                        format!(
                            "object {} placement evidence differs from its declaration",
                            receipt.object_id
                        ),
                    ));
                }
            }
            // This is the last authority gate before Publishing becomes durable. Once that state
            // and its canonical candidate are persisted, recovery must converge a possibly
            // completed CAS without consulting mutable authority or transient staging again.
            let now = self.clock.now().get();
            if job.spec.deadline_unix_ms.get() <= now {
                return Err(invalid(
                    CentralErrorCode::DeadlineExceeded,
                    "managed Add deadline elapsed before publication",
                ));
            }
            if assignment
                .lease
                .as_ref()
                .is_some_and(|lease| lease.expires_at_unix_ms.get() <= now)
            {
                return Err(invalid(
                    CentralErrorCode::DeadlineExceeded,
                    "managed Add assignment lease elapsed before publication",
                ));
            }

            let prepared = job.prepared.as_ref().ok_or_else(|| {
                invalid(
                    CentralErrorCode::Internal,
                    "prepared job lost its publication identity",
                )
            })?;
            let candidate = PublicationCandidate {
                expected_index_version: job.spec.expected_index_version.clone(),
                result_index_digest: prepared.result_index_digest,
                publication_digest: prepared.publication_digest,
                manifests: metadata.manifests,
                mutations: metadata.mutations,
            };
            let previous = job.resource_version.get();
            job.state = JobState::Publishing;
            job.publication_candidate = Some(candidate.clone());
            job = self.replace(previous, job).await?;
            candidate
        };

        let prepared_result_digest = publication_candidate.result_index_digest;
        let publish = self
            .publisher
            .compare_and_swap(IndexPublishRequest {
                job_key: job.key(),
                index_key: job.index_key(),
                expected_index_version: publication_candidate.expected_index_version,
                expected_result_digest: prepared_result_digest,
                manifests: publication_candidate.manifests,
                mutations: publication_candidate.mutations,
            })
            .await?;
        let decision_generation = DecisionGeneration::new(1);
        let (state, outcome) = match publish {
            IndexPublishOutcome::Published(version) => (
                JobState::Succeeded,
                PublishDecision::Publish {
                    published_index_version: version,
                    extensions: Extensions::new(),
                },
            ),
            IndexPublishOutcome::Conflict(version) => (
                JobState::Conflicted,
                PublishDecision::Conflict {
                    current_index_version: version,
                    extensions: Extensions::new(),
                },
            ),
            IndexPublishOutcome::Rejected(IndexPublishRejection::ResultDigestMismatch {
                expected_digest,
                observed_digest,
            }) => (
                JobState::Failed,
                PublishDecision::Reject {
                    error: ControlError {
                        code: ErrorCode::new("INDEX_RESULT_DIGEST_MISMATCH")?,
                        message: format!(
                            "published Index digest {observed_digest} differs from prepared result {expected_digest}"
                        ),
                        retryable: false,
                        retry_after_ms: None,
                        extensions: Extensions::new(),
                    },
                    extensions: Extensions::new(),
                },
            ),
            IndexPublishOutcome::Rejected(IndexPublishRejection::InvalidMetadata {
                message,
            }) => (
                JobState::Failed,
                PublishDecision::Reject {
                    error: ControlError {
                        code: ErrorCode::new(CentralErrorCode::MetadataInvalid.as_str())?,
                        message: bounded_control_error_message(message),
                        retryable: false,
                        retry_after_ms: None,
                        extensions: Extensions::new(),
                    },
                    extensions: Extensions::new(),
                },
            ),
            IndexPublishOutcome::Rejected(IndexPublishRejection::RevisionExhausted) => (
                JobState::Failed,
                PublishDecision::Reject {
                    error: ControlError {
                        code: ErrorCode::new("INDEX_REVISION_EXHAUSTED")?,
                        message: "Index revision reached its maximum value".to_owned(),
                        retryable: false,
                        retry_after_ms: None,
                        extensions: Extensions::new(),
                    },
                    extensions: Extensions::new(),
                },
            ),
        };
        let decision = JobDecision {
            job_id: assignment.job_id.clone(),
            assignment_id: assignment.assignment_id.clone(),
            assignment_generation: assignment.assignment_generation,
            decision_generation,
            decision: outcome,
            final_state: state,
            extensions: Extensions::new(),
        };
        let finalized = JobFinalized {
            job_id: assignment.job_id.clone(),
            assignment_id: assignment.assignment_id.clone(),
            assignment_generation: assignment.assignment_generation,
            decision_generation,
            final_state: state,
            finalized_at_unix_ms: self.clock.now(),
            extensions: Extensions::new(),
        };
        decision.validate()?;
        finalized.validate()?;
        let previous = job.resource_version.get();
        job.state = state;
        job.decision = Some(decision.clone());
        job.finalized = Some(finalized.clone());
        job = self.replace(previous, job).await?;
        self.audit(&job, AuditKind::AddFinalized, "finalize")
            .await?;
        Ok(FinalizeAddResult {
            job,
            decision,
            finalized,
            replayed: resumed_publication,
        })
    }

    async fn load(
        &self,
        tenant_id: &neoengram_protocol::TenantId,
        job_id: &neoengram_protocol::JobId,
    ) -> CentralResult<JobRecord> {
        let key = crate::JobKey::new(tenant_id.clone(), job_id.clone());
        self.jobs
            .get(&key)
            .await?
            .ok_or_else(|| job_not_found(job_id))
    }

    async fn replace(&self, expected: u64, mut job: JobRecord) -> CentralResult<JobRecord> {
        let next = expected
            .checked_add(1)
            .ok_or_else(|| invalid(CentralErrorCode::Internal, "job ResourceVersion overflow"))?;
        job.resource_version = ResourceVersion::new(next);
        self.jobs.replace(expected, job).await
    }

    async fn authorize(
        &self,
        actor: Actor,
        action: Action,
        spec: &AddJobSpec,
    ) -> CentralResult<()> {
        self.authorizer
            .authorize(&AuthorizationRequest {
                actor,
                action,
                tenant_id: spec.tenant_id.clone(),
                artifact_id: spec.artifact_id.clone(),
                playground_id: spec.playground_id.clone(),
                job_id: spec.job_id.clone(),
            })
            .await
    }

    async fn audit(&self, job: &JobRecord, kind: AuditKind, suffix: &str) -> CentralResult<()> {
        let event = AuditEvent {
            event_id: format!(
                "{}:{}:{}:{suffix}",
                job.spec.tenant_id, job.spec.job_id, job.resource_version
            ),
            kind,
            job_key: job.key(),
            state: job.state,
            occurred_at_unix_ms: self.clock.now(),
        };
        let _ = self.audit.record(event).await?;
        Ok(())
    }
}

fn job_not_found(job_id: &neoengram_protocol::JobId) -> crate::CentralError {
    invalid(
        CentralErrorCode::JobNotFound,
        format!("managed Add job {job_id} was not found"),
    )
}

fn validate_workspace_report_identity(
    assignment: &WorkspaceMaterializeAssignment,
    job_id: &neoengram_protocol::JobId,
    assignment_id: &neoengram_protocol::AssignmentId,
    generation: neoengram_protocol::AssignmentGeneration,
) -> CentralResult<()> {
    if job_id != &assignment.job_id || assignment_id != &assignment.assignment_id {
        return Err(invalid(
            CentralErrorCode::AssignmentMismatch,
            "materialization report does not identify the persisted assignment",
        ));
    }
    if generation != assignment.assignment_generation {
        return Err(invalid(
            CentralErrorCode::GenerationMismatch,
            "materialization report carries a stale assignment generation",
        ));
    }
    Ok(())
}

fn validate_snapshot_report_identity(
    assignment: &SnapshotMountAssignment,
    job_id: &neoengram_protocol::JobId,
    assignment_id: &neoengram_protocol::AssignmentId,
    generation: neoengram_protocol::AssignmentGeneration,
) -> CentralResult<()> {
    if job_id != &assignment.job_id || assignment_id != &assignment.assignment_id {
        return Err(invalid(
            CentralErrorCode::AssignmentMismatch,
            "Snapshot mount report does not identify the persisted assignment",
        ));
    }
    if generation != assignment.assignment_generation {
        return Err(invalid(
            CentralErrorCode::GenerationMismatch,
            "Snapshot mount report carries a stale assignment generation",
        ));
    }
    Ok(())
}

fn validate_snapshot_report_session_generation(
    report: &AgentReport,
    session_generation: SessionGeneration,
) -> CentralResult<()> {
    let extensions = match report {
        AgentReport::Progress(progress)
            if progress.state == JobState::Succeeded && progress.phase == "mounted" =>
        {
            Some(&progress.extensions)
        }
        AgentReport::Failed(failed)
            if failed
                .extensions
                .get("snapshot_mount_recovery")
                .and_then(serde_json::Value::as_bool)
                == Some(true) =>
        {
            Some(&failed.extensions)
        }
        _ => None,
    };
    let Some(extensions) = extensions else {
        return Ok(());
    };
    let observed = extensions
        .get("snapshot_mount_session_generation")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| value.parse::<u64>().ok());
    if observed != Some(session_generation.get()) {
        return Err(invalid(
            CentralErrorCode::GenerationMismatch,
            "Snapshot mount report was produced under another Agent session generation",
        ));
    }
    Ok(())
}

fn bounded_control_error_message(mut message: String) -> String {
    if message.trim().is_empty() {
        return "Index publication was rejected by the publisher".to_owned();
    }
    if message.len() <= CONTROL_ERROR_MESSAGE_LIMIT {
        return message;
    }

    const ELLIPSIS: &str = "...";
    let mut end = CONTROL_ERROR_MESSAGE_LIMIT - ELLIPSIS.len();
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message.truncate(end);
    message.push_str(ELLIPSIS);
    message
}

fn validate_frozen_publication(
    job: &JobRecord,
    candidate: &PublicationCandidate,
) -> CentralResult<()> {
    let prepared = job.prepared.as_ref().ok_or_else(|| {
        invalid(
            CentralErrorCode::Internal,
            "publishing job lost its prepared publication identity",
        )
    })?;
    if candidate.expected_index_version != job.spec.expected_index_version
        || candidate.result_index_digest != prepared.result_index_digest
        || candidate.publication_digest != prepared.publication_digest
    {
        return Err(invalid(
            CentralErrorCode::Internal,
            "frozen publication identity differs from the durable prepared report",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use neoengram_protocol::{
        AssignmentGeneration, AssignmentId, ControlError, DecimalU64, ErrorCode, Extensions,
        JobFailed, JobFailureStage, JobId, JobProgress, JobState, SessionGeneration, TenantId,
        UnixMillis,
    };

    use crate::{AgentReport, CentralErrorCode};

    use super::{
        bounded_control_error_message, validate_snapshot_report_session_generation,
        CONTROL_ERROR_MESSAGE_LIMIT,
    };

    #[test]
    fn control_error_messages_are_bounded_on_utf8_boundaries() {
        let original = "界".repeat(CONTROL_ERROR_MESSAGE_LIMIT);
        let bounded = bounded_control_error_message(original);

        assert!(bounded.len() <= CONTROL_ERROR_MESSAGE_LIMIT);
        assert!(bounded.ends_with("..."));
        assert!(std::str::from_utf8(bounded.as_bytes()).is_ok());
        assert!(!bounded_control_error_message("   ".to_owned())
            .trim()
            .is_empty());
    }

    #[test]
    fn snapshot_mounted_report_is_fenced_by_original_session_generation() {
        let mut extensions = Extensions::new();
        extensions.insert(
            "snapshot_mount_session_generation".to_owned(),
            serde_json::Value::String("7".to_owned()),
        );
        let report = AgentReport::Progress(JobProgress {
            job_id: JobId::new("job-a").unwrap(),
            assignment_id: AssignmentId::new("assignment-a").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            state: JobState::Succeeded,
            phase: "mounted".to_owned(),
            files_completed: DecimalU64::new(0),
            bytes_completed: DecimalU64::new(0),
            retry_after_ms: None,
            extensions,
        });

        validate_snapshot_report_session_generation(&report, SessionGeneration::new(7)).unwrap();
        let stale = validate_snapshot_report_session_generation(&report, SessionGeneration::new(8))
            .unwrap_err();
        assert_eq!(stale.code(), CentralErrorCode::GenerationMismatch);
    }

    #[test]
    fn snapshot_recovery_report_requires_current_session_generation() {
        let mut extensions = Extensions::new();
        extensions.insert(
            "snapshot_mount_recovery".to_owned(),
            serde_json::Value::Bool(true),
        );
        let report = AgentReport::Failed(JobFailed {
            tenant_id: TenantId::new("tenant-a").unwrap(),
            job_id: JobId::new("job-a").unwrap(),
            assignment_id: AssignmentId::new("assignment-a").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            final_state: JobState::Failed,
            failed_at_unix_ms: UnixMillis::new(1),
            stage: JobFailureStage::Execution,
            error: ControlError {
                code: ErrorCode::new("SNAPSHOT_OBJECT_UNAVAILABLE").unwrap(),
                message: "Snapshot object is unavailable".to_owned(),
                retryable: true,
                retry_after_ms: None,
                extensions: Extensions::new(),
            },
            extensions,
        });

        let missing =
            validate_snapshot_report_session_generation(&report, SessionGeneration::new(8))
                .unwrap_err();
        assert_eq!(missing.code(), CentralErrorCode::GenerationMismatch);
    }
}
