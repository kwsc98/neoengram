use std::sync::Arc;

use neoengram_protocol::{
    AssignmentOperation, ControlError, DecisionGeneration, ErrorCode, Extensions, JobAssignment,
    JobDecision, JobFinalized, JobState, PublishDecision, ResourceVersion,
};

use crate::{
    validation::{
        invalid, validate_assignment_target, validate_descriptor_scope, validate_job_spec,
        validate_prepared, validate_report_identity, validate_staged_metadata,
        validate_terminal_state,
    },
    Action, Actor, AddJobSpec, AgentReport, AssignJobRequest, AssignJobResult, AssignmentOutbox,
    AuditEvent, AuditKind, AuditSink, AuthorityStore, AuthorizationRequest, Authorizer,
    CentralErrorCode, CentralResult, Clock, CreateAddJobRequest, CreateAddJobResult,
    ExpireAddJobRequest, ExpireAddJobResult, FinalizeAddRequest, FinalizeAddResult,
    IndexPublishOutcome, IndexPublishRejection, IndexPublishRequest, IndexPublisher,
    JobInsertOutcome, JobRecord, JobRepository, MetadataBatchStager, MetadataBatchSubmission,
    ObjectCatalog, PublicationCandidate, QueryJobRequest, QueryJobResult, ReceiveReportRequest,
    ReceiveReportResult, ResumePublicationRequest, StageMetadataBatchRequest,
    StageMetadataBatchResult,
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
            clock,
        }
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
        let key = crate::JobKey::new(request.spec.tenant_id.clone(), request.spec.job_id.clone());
        if let Some(existing) = self.jobs.get(&key).await? {
            if existing.spec != request.spec {
                return Err(invalid(
                    CentralErrorCode::JobIdReused,
                    "JobId already belongs to a different managed Add request",
                ));
            }
            self.audit(&existing, AuditKind::JobCreated, "create")
                .await?;
            return Ok(CreateAddJobResult {
                job: existing,
                replayed: true,
            });
        }
        validate_job_spec(&request.spec, self.clock.now().get())?;

        let job = JobRecord {
            spec: request.spec.clone(),
            state: JobState::Queued,
            resource_version: ResourceVersion::new(1),
            assignment: None,
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
                if existing.spec != request.spec {
                    return Err(invalid(
                        CentralErrorCode::JobIdReused,
                        "JobId already belongs to a different managed Add request",
                    ));
                }
                (existing, true)
            }
        };
        self.audit(&job, AuditKind::JobCreated, "create").await?;
        Ok(CreateAddJobResult { job, replayed })
    }

    /// Loads a managed Add job by tenant and job identity, after authorization.
    pub async fn query_job(&self, request: QueryJobRequest) -> CentralResult<QueryJobResult> {
        let actor = request.actor.clone();
        let sentinel = move || invalid(CentralErrorCode::ProtocolInvalid, "invalid query argument");
        self.authorize(
            Actor::Principal(actor.clone()),
            Action::QueryJob,
            &AddJobSpec {
                job_id: request.job_id.clone(),
                principal: actor,
                tenant_id: request.tenant_id.clone(),
                project_id: neoengram_protocol::ProjectId::new("query").map_err(|_| sentinel())?,
                artifact_id: neoengram_protocol::ArtifactId::new("query")
                    .map_err(|_| sentinel())?,
                playground_id: neoengram_protocol::PlaygroundId::new("query")
                    .map_err(|_| sentinel())?,
                expected_index_version: neoengram_protocol::WireIndexVersion {
                    revision: neoengram_protocol::IndexRevision::new(0),
                    digest: neoengram_core::ContentDigest::hash(b"query-sentinel"),
                    extensions: Default::default(),
                },
                request_digest: neoengram_core::ContentDigest::hash(b"query-sentinel"),
                deadline_unix_ms: neoengram_protocol::UnixMillis::new(0),
                paths: Vec::new(),
                all: false,
                extensions: Default::default(),
            },
        )
        .await?;
        let job = self.load(&request.tenant_id, &request.job_id).await?;
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
        let mut job = self
            .load(&request.tenant_id, request.report.job_id())
            .await?;
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
        self.audit(&job, AuditKind::ReportReceived, "report")
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
        self.audit(&job, AuditKind::AddExpired, "expire").await?;
        Ok(ExpireAddJobResult {
            job,
            decision,
            finalized,
            replayed: false,
        })
    }

    /// Validates complete staged metadata and central object durability, then performs one CAS.
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
            let metadata =
                validate_staged_metadata(&job, self.metadata.as_ref(), self.objects.as_ref())
                    .await?;
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
        self.jobs.get(&key).await?.ok_or_else(|| {
            invalid(
                CentralErrorCode::JobNotFound,
                format!("managed Add job {job_id} was not found"),
            )
        })
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
    use super::{bounded_control_error_message, CONTROL_ERROR_MESSAGE_LIMIT};

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
}
