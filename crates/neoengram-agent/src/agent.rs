use std::sync::Arc;

use neoengram_engine::PreparedAdd;
use neoengram_protocol::{
    AddAssignment, ControlError, DecimalU64, ErrorCode as WireErrorCode, Extensions, JobAccepted,
    JobDecision, JobFailed, JobFailureStage, JobFinalized, JobPrepared, JobProgress, JobState,
    PublishDecision, TenantId, UnixMillis,
};

use crate::{
    AddExecutor, AgentAssignmentState, AgentError, AgentErrorCode, AgentReport, AgentResult,
    AssignmentAcceptance, AssignmentKey, AssignmentValidator, ClaimDisposition, ClaimOutcome,
    Clock, Ledger, LedgerClaim, LedgerRecord, ObjectTransfer, PreparedExecution, ReportSink,
};

/// Synchronous, durable Agent state machine composed entirely from explicit ports.
#[derive(Debug)]
pub struct Agent {
    ledger: Arc<dyn Ledger>,
    validator: Arc<dyn AssignmentValidator>,
    executor: Arc<dyn AddExecutor>,
    object_transfer: Arc<dyn ObjectTransfer>,
    reports: Arc<dyn ReportSink>,
    clock: Arc<dyn Clock>,
}

impl Agent {
    #[must_use]
    pub fn new(
        ledger: Arc<dyn Ledger>,
        validator: Arc<dyn AssignmentValidator>,
        executor: Arc<dyn AddExecutor>,
        object_transfer: Arc<dyn ObjectTransfer>,
        reports: Arc<dyn ReportSink>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            ledger,
            validator,
            executor,
            object_transfer,
            reports,
            clock,
        }
    }

    /// Atomically claims and accepts an assignment. The durable claim always precedes Accepted.
    pub fn accept_assignment(
        &self,
        assignment: AddAssignment,
    ) -> AgentResult<AssignmentAcceptance> {
        let now = self.clock.now_unix_ms()?;
        let key = AssignmentKey::from_assignment(&assignment);

        // Durable replays keep their original validation result. In particular, a completed or
        // accepted assignment remains replayable after its original deadline has elapsed.
        if let Some(mut record) = self.ledger.load(&key)? {
            self.verify_claim_identity(&assignment, &record)?;
            if record.state == AgentAssignmentState::Claimed {
                record = self.persist_transition(
                    &record,
                    AgentAssignmentState::Claimed,
                    AgentAssignmentState::Accepted,
                    now,
                    |next| next.accepted_at_unix_ms = Some(now),
                )?;
                self.send_accepted(&record)?;
            } else if record.state == AgentAssignmentState::Accepted {
                self.send_accepted(&record)?;
            }
            return Ok(AssignmentAcceptance {
                disposition: ClaimDisposition::Replayed,
                record,
            });
        }

        self.validator.validate(&assignment, now)?;
        let outcome = self.ledger.claim(LedgerClaim {
            assignment: assignment.clone(),
            claimed_at_unix_ms: now,
        })?;
        let (disposition, mut record) = match outcome {
            ClaimOutcome::Claimed(record) => (ClaimDisposition::New, record),
            ClaimOutcome::Existing(record) => (ClaimDisposition::Replayed, record),
        };
        self.verify_claim_identity(&assignment, &record)?;

        if record.state == AgentAssignmentState::Claimed {
            record = self.persist_transition(
                &record,
                AgentAssignmentState::Claimed,
                AgentAssignmentState::Accepted,
                now,
                |next| next.accepted_at_unix_ms = Some(now),
            )?;
            self.send_accepted(&record)?;
        } else if disposition == ClaimDisposition::Replayed
            && record.state == AgentAssignmentState::Accepted
        {
            self.send_accepted(&record)?;
        }

        Ok(AssignmentAcceptance {
            disposition,
            record,
        })
    }

    /// Accepts a new assignment and advances any resumable accepted/prepared work.
    pub fn handle_assignment(&self, assignment: AddAssignment) -> AgentResult<LedgerRecord> {
        let acceptance = self.accept_assignment(assignment)?;
        match acceptance.record.state {
            AgentAssignmentState::Accepted | AgentAssignmentState::Prepared => {
                self.execute(&acceptance.record.key)
            }
            _ => Ok(acceptance.record),
        }
    }

    /// Runs Add preparation and object transfer for an accepted or explicitly recovering job.
    pub fn execute(&self, key: &AssignmentKey) -> AgentResult<LedgerRecord> {
        let record = self.load_required(key)?;
        if record.state == AgentAssignmentState::Prepared {
            return self.report_prepared(record);
        }
        if !matches!(
            record.state,
            AgentAssignmentState::Accepted | AgentAssignmentState::Recovering
        ) {
            return match record.state {
                AgentAssignmentState::Running
                | AgentAssignmentState::AwaitingDecision
                | AgentAssignmentState::Finalizing
                | AgentAssignmentState::Completed => Ok(record),
                _ => Err(invalid_state(
                    &record,
                    "execute requires accepted or recovering state",
                )),
            };
        }

        let now = match self.validate_active_assignment(&record) {
            Ok(now) => now,
            Err(error) => {
                return Err(self.persist_failure(&record, JobFailureStage::Execution, error));
            }
        };
        let running = self.persist_transition(
            &record,
            record.state,
            AgentAssignmentState::Running,
            now,
            |next| next.failure = None,
        )?;
        if let Err(error) = self.send_running(&running) {
            return Err(self.persist_failure(&running, JobFailureStage::Reporting, error));
        }
        let prepared = match self.executor.prepare_add(&running.assignment) {
            Ok(prepared) => prepared,
            Err(error) => {
                return Err(self.persist_failure(&running, JobFailureStage::Execution, error));
            }
        };
        if let Err(error) = validate_candidate(&running, &prepared) {
            return Err(self.persist_failure(&running, JobFailureStage::Execution, error));
        }
        let transfer = match self
            .object_transfer
            .transfer(&running.assignment, &prepared)
        {
            Ok(transfer) => transfer,
            Err(error) => {
                return Err(self.persist_failure(&running, JobFailureStage::ObjectTransfer, error));
            }
        };
        if let Err(error) = transfer.validate_for(&running.assignment, &prepared) {
            return Err(self.persist_failure(&running, JobFailureStage::ObjectTransfer, error));
        }

        let now = self.clock.now_unix_ms()?;
        let prepared_record = self.persist_transition(
            &running,
            AgentAssignmentState::Running,
            AgentAssignmentState::Prepared,
            now,
            move |next| {
                next.prepared = Some(PreparedExecution {
                    candidate: prepared,
                    transfer,
                });
            },
        )?;
        self.report_prepared(prepared_record)
    }

    /// Applies a central publish decision and idempotently acknowledges finalization.
    pub fn handle_decision(
        &self,
        tenant_id: &TenantId,
        decision: JobDecision,
    ) -> AgentResult<LedgerRecord> {
        if decision.decision_generation.get() == 0 {
            return Err(AgentError::new(
                AgentErrorCode::AssignmentMismatch,
                "decision generation must be greater than zero",
            ));
        }
        let key = AssignmentKey {
            tenant_id: tenant_id.clone(),
            job_id: decision.job_id.clone(),
        };
        let record = self.load_required(&key)?;
        validate_decision_identity(&record, &decision)?;
        validate_publish_result(&record, &decision)?;

        if let Some(existing) = &record.decision {
            if existing != &decision {
                return Err(AgentError::new(
                    AgentErrorCode::AssignmentMismatch,
                    format!("job {key} already has a different durable decision"),
                ));
            }
            return match record.state {
                AgentAssignmentState::Completed => Ok(record),
                AgentAssignmentState::Finalizing => self.finalize(record),
                AgentAssignmentState::FailedReported | AgentAssignmentState::Recovering => {
                    self.recover(&key)
                }
                _ => Err(invalid_state(
                    &record,
                    "durable decision is inconsistent with Agent state",
                )),
            };
        }
        let reject = matches!(decision.decision, PublishDecision::Reject { .. });
        let authoritative_publication = matches!(
            decision.decision,
            PublishDecision::Publish { .. } | PublishDecision::Conflict { .. }
        );
        let has_durable_candidate = record.prepared.is_some();
        let may_apply = record.state == AgentAssignmentState::AwaitingDecision
            || (reject
                && matches!(
                    record.state,
                    AgentAssignmentState::Claimed
                        | AgentAssignmentState::Accepted
                        | AgentAssignmentState::Running
                        | AgentAssignmentState::Prepared
                        | AgentAssignmentState::FailedReported
                        | AgentAssignmentState::Recovering
                ))
            || (authoritative_publication
                && has_durable_candidate
                && matches!(
                    record.state,
                    AgentAssignmentState::Prepared
                        | AgentAssignmentState::FailedReported
                        | AgentAssignmentState::Recovering
                ));
        if !may_apply {
            return Err(invalid_state(
                &record,
                "publish/conflict requires decision wait or a durable prepared candidate; reject may stop active work",
            ));
        }

        let now = self.clock.now_unix_ms()?;
        let finalized = JobFinalized {
            job_id: record.assignment.job_id.clone(),
            assignment_id: record.assignment.assignment_id.clone(),
            assignment_generation: record.assignment.assignment_generation,
            decision_generation: decision.decision_generation,
            final_state: decision.final_state,
            finalized_at_unix_ms: UnixMillis::new(now),
            extensions: Extensions::new(),
        };
        let previous_state = record.state;
        let finalizing = self.persist_transition(
            &record,
            previous_state,
            AgentAssignmentState::Finalizing,
            now,
            move |next| {
                next.decision = Some(decision);
                next.finalized = Some(finalized);
            },
        )?;
        self.finalize(finalizing)
    }

    /// Explicitly resumes a crashed or failed boundary. It never claims a new assignment.
    pub fn recover(&self, key: &AssignmentKey) -> AgentResult<LedgerRecord> {
        let record = self.load_required(key)?;
        match record.state {
            AgentAssignmentState::Accepted => {
                self.send_accepted(&record)?;
                self.execute(key)
            }
            AgentAssignmentState::Prepared => self.report_prepared(record),
            AgentAssignmentState::AwaitingDecision | AgentAssignmentState::Completed => Ok(record),
            AgentAssignmentState::Finalizing => self.finalize(record),
            AgentAssignmentState::Running => {
                let now = self.clock.now_unix_ms()?;
                self.persist_transition(
                    &record,
                    AgentAssignmentState::Running,
                    AgentAssignmentState::Recovering,
                    now,
                    |_| {},
                )?;
                self.execute(key)
            }
            AgentAssignmentState::FailedReported | AgentAssignmentState::Recovering => {
                if let Some(decision) = record.decision.as_ref() {
                    validate_decision_identity(&record, decision)?;
                    validate_publish_result(&record, decision)?;
                    let now = self.clock.now_unix_ms()?;
                    let finalizing = self.persist_transition(
                        &record,
                        record.state,
                        AgentAssignmentState::Finalizing,
                        now,
                        |next| next.failure = None,
                    )?;
                    self.finalize(finalizing)
                } else if record.failure.is_some() {
                    let failed = if record.state == AgentAssignmentState::Recovering {
                        let now = self.clock.now_unix_ms()?;
                        self.persist_transition(
                            &record,
                            AgentAssignmentState::Recovering,
                            AgentAssignmentState::FailedReported,
                            now,
                            |_| {},
                        )?
                    } else {
                        record
                    };
                    self.report_failure(failed)
                } else if record.state == AgentAssignmentState::Recovering {
                    self.execute(key)
                } else {
                    Err(invalid_state(
                        &record,
                        "failed_reported assignment has no durable terminal failure",
                    ))
                }
            }
            AgentAssignmentState::Claimed => Err(invalid_state(
                &record,
                "claimed assignment must be accepted before recovery",
            )),
        }
    }

    /// Loads one durable assignment snapshot.
    pub fn assignment(&self, key: &AssignmentKey) -> AgentResult<Option<LedgerRecord>> {
        self.ledger.load(key)
    }

    /// Lists every assignment that still needs execution, replay, or central acknowledgement.
    pub fn active_assignments(&self) -> AgentResult<Vec<LedgerRecord>> {
        self.ledger.list_active()
    }

    /// Replays all durable active assignments in stable Ledger order after process restart.
    pub fn recover_active(&self) -> AgentResult<Vec<LedgerRecord>> {
        self.ledger
            .list_active()?
            .into_iter()
            .map(|record| {
                if record.state == AgentAssignmentState::Claimed {
                    self.handle_assignment(record.assignment)
                } else {
                    self.recover(&record.key)
                }
            })
            .collect()
    }

    fn report_prepared(&self, record: LedgerRecord) -> AgentResult<LedgerRecord> {
        if record.state != AgentAssignmentState::Prepared {
            return Err(invalid_state(
                &record,
                "prepared report requires prepared state",
            ));
        }
        if let Err(error) = self.validate_active_assignment(&record) {
            return Err(self.persist_failure(&record, JobFailureStage::Execution, error));
        }
        let prepared = record.prepared.as_ref().ok_or_else(|| {
            AgentError::new(
                AgentErrorCode::InvalidState,
                format!("job {} has no durable prepared candidate", record.key),
            )
        })?;
        validate_candidate(&record, &prepared.candidate)?;
        prepared
            .transfer
            .validate_for(&record.assignment, &prepared.candidate)?;
        let publication_digest = prepared.candidate.publication_digest()?;
        let report = JobPrepared::new(
            record.assignment.job_id.clone(),
            record.assignment.assignment_id.clone(),
            record.assignment.assignment_generation,
            record.assignment.expected_index_version.clone(),
            prepared.candidate.index_delta.result_digest,
            publication_digest,
            prepared.transfer.metadata_batches.clone(),
            Extensions::new(),
        )?;
        self.reports.send(AgentReport::Prepared(report))?;
        self.object_transfer
            .stage_metadata(&record.assignment, &prepared.transfer)?;
        let now = self.clock.now_unix_ms()?;
        self.persist_transition(
            &record,
            AgentAssignmentState::Prepared,
            AgentAssignmentState::AwaitingDecision,
            now,
            |_| {},
        )
    }

    fn finalize(&self, record: LedgerRecord) -> AgentResult<LedgerRecord> {
        if record.state != AgentAssignmentState::Finalizing {
            return Err(invalid_state(
                &record,
                "finalization requires finalizing state",
            ));
        }
        let decision = record.decision.as_ref().ok_or_else(|| {
            AgentError::new(
                AgentErrorCode::InvalidState,
                format!("job {} has no durable central decision", record.key),
            )
        })?;
        validate_decision_identity(&record, decision)?;
        validate_publish_result(&record, decision)?;

        if let Some(prepared) = record.prepared.as_ref() {
            validate_candidate(&record, &prepared.candidate)?;
            if let Err(error) =
                self.object_transfer
                    .finalize(&record.assignment, &prepared.candidate, decision)
            {
                return Err(self.persist_failure(&record, JobFailureStage::Finalization, error));
            }
        } else if !matches!(decision.decision, PublishDecision::Reject { .. }) {
            return Err(AgentError::new(
                AgentErrorCode::InvalidState,
                format!(
                    "job {} cannot apply a publish decision without a prepared candidate",
                    record.key
                ),
            ));
        }

        let finalized = record.finalized.as_ref().ok_or_else(|| {
            AgentError::new(
                AgentErrorCode::InvalidState,
                format!(
                    "job {} has no durable finalized acknowledgement",
                    record.key
                ),
            )
        })?;
        validate_finalized_identity(&record, decision, finalized)?;
        self.reports
            .send(AgentReport::Finalized(finalized.clone()))?;
        let now = self.clock.now_unix_ms()?;
        self.persist_transition(
            &record,
            AgentAssignmentState::Finalizing,
            AgentAssignmentState::Completed,
            now,
            |next| next.failure = None,
        )
    }

    fn persist_failure(
        &self,
        record: &LedgerRecord,
        stage: JobFailureStage,
        error: AgentError,
    ) -> AgentError {
        let result = (|| -> AgentResult<()> {
            let now = self.clock.now_unix_ms()?;
            let wire_code = WireErrorCode::new(error.stable_code())?;
            let failure = JobFailed {
                tenant_id: record.assignment.tenant_id.clone(),
                job_id: record.assignment.job_id.clone(),
                assignment_id: record.assignment.assignment_id.clone(),
                assignment_generation: record.assignment.assignment_generation,
                final_state: JobState::Failed,
                failed_at_unix_ms: UnixMillis::new(now),
                stage,
                error: ControlError {
                    code: wire_code,
                    message: truncate_message(error.message(), 4_096),
                    retryable: is_retryable(error.code()),
                    retry_after_ms: None,
                    extensions: Extensions::new(),
                },
                extensions: Extensions::new(),
            };
            failure.validate()?;
            let failed = self.persist_transition(
                record,
                record.state,
                AgentAssignmentState::FailedReported,
                now,
                |next| next.failure = Some(failure.clone()),
            )?;
            let persisted = failed.failure.ok_or_else(|| {
                AgentError::new(
                    AgentErrorCode::Internal,
                    "failure disappeared after durable ledger update",
                )
            })?;
            self.reports.send(AgentReport::Failed(persisted))?;
            Ok(())
        })();
        match result {
            Ok(()) => error,
            Err(report_error) => AgentError::new(
                error.code(),
                format!(
                    "{}; additionally failed to persist or report failure: {}",
                    error.message(),
                    report_error
                ),
            ),
        }
    }

    fn report_failure(&self, record: LedgerRecord) -> AgentResult<LedgerRecord> {
        let failure = record.failure.as_ref().ok_or_else(|| {
            invalid_state(
                &record,
                "terminal failure must be durable before it is reported",
            )
        })?;
        validate_failure_identity(&record, failure)?;
        self.reports.send(AgentReport::Failed(failure.clone()))?;
        Ok(record)
    }

    fn persist_transition(
        &self,
        current: &LedgerRecord,
        expected: AgentAssignmentState,
        target: AgentAssignmentState,
        now_unix_ms: u64,
        update: impl FnOnce(&mut LedgerRecord),
    ) -> AgentResult<LedgerRecord> {
        if current.state != expected {
            return Err(invalid_state(
                current,
                &format!("expected {expected:?} before transition to {target:?}"),
            ));
        }
        let mut next = current.clone();
        next.state = target;
        next.updated_at_unix_ms = now_unix_ms;
        update(&mut next);
        self.ledger.compare_exchange(current.revision, next)
    }

    fn load_required(&self, key: &AssignmentKey) -> AgentResult<LedgerRecord> {
        self.ledger.load(key)?.ok_or_else(|| {
            AgentError::new(
                AgentErrorCode::AssignmentNotFound,
                format!("assignment {key} is not present in the ledger"),
            )
        })
    }

    fn validate_active_assignment(&self, record: &LedgerRecord) -> AgentResult<u64> {
        let now = self.clock.now_unix_ms()?;
        self.validator.validate(&record.assignment, now)?;
        Ok(now)
    }

    fn send_accepted(&self, record: &LedgerRecord) -> AgentResult<()> {
        let accepted_at = record.accepted_at_unix_ms.ok_or_else(|| {
            AgentError::new(
                AgentErrorCode::InvalidState,
                format!("accepted job {} has no accepted timestamp", record.key),
            )
        })?;
        self.reports.send(AgentReport::Accepted(JobAccepted {
            job_id: record.assignment.job_id.clone(),
            assignment_id: record.assignment.assignment_id.clone(),
            assignment_generation: record.assignment.assignment_generation,
            accepted_at_unix_ms: UnixMillis::new(accepted_at),
            request_digest: record.request_digest,
            extensions: Extensions::new(),
        }))
    }

    fn send_running(&self, record: &LedgerRecord) -> AgentResult<()> {
        self.reports.send(AgentReport::Progress(JobProgress {
            job_id: record.assignment.job_id.clone(),
            assignment_id: record.assignment.assignment_id.clone(),
            assignment_generation: record.assignment.assignment_generation,
            state: JobState::Running,
            phase: "prepare_add".to_owned(),
            files_completed: DecimalU64::new(0),
            bytes_completed: DecimalU64::new(0),
            retry_after_ms: None,
            extensions: Extensions::new(),
        }))
    }

    fn verify_claim_identity(
        &self,
        assignment: &AddAssignment,
        record: &LedgerRecord,
    ) -> AgentResult<()> {
        let key = AssignmentKey::from_assignment(assignment);
        if record.key != key {
            return Err(AgentError::new(
                AgentErrorCode::AssignmentMismatch,
                "ledger claim returned a record for a different tenant or job",
            ));
        }
        if record.request_digest != assignment.request_digest {
            return Err(AgentError::new(
                AgentErrorCode::JobIdReused,
                format!("job {key} is already bound to a different request digest"),
            ));
        }
        if record.assignment != *assignment {
            return Err(AgentError::new(
                AgentErrorCode::AssignmentMismatch,
                format!("job {key} replay carries a different assignment payload"),
            ));
        }
        Ok(())
    }
}

fn validate_candidate(record: &LedgerRecord, prepared: &PreparedAdd) -> AgentResult<()> {
    prepared.validate()?;
    let assignment = &record.assignment;
    let resource = &prepared.resource;
    let expected = &assignment.expected_index_version;
    let matches_scope = resource.tenant_id == assignment.tenant_id.as_str()
        && resource.project_id == assignment.project_id.as_str()
        && resource.artifact_id == assignment.artifact_id.as_str()
        && resource.playground_id == assignment.playground_id.as_str()
        && resource.job_id == assignment.job_id.as_str();
    let matches_version = prepared.base_index_version.revision == expected.revision.get()
        && prepared.base_index_version.digest == expected.digest;
    if !matches_scope || !matches_version {
        return Err(AgentError::new(
            AgentErrorCode::ExecutionFailed,
            format!(
                "prepared candidate does not match assignment scope or base version for {}",
                record.key
            ),
        ));
    }
    if !assignment.all {
        for mutation in prepared.index_delta.iter() {
            let path = match mutation {
                neoengram_core::IndexMutation::Upsert { record } => &record.path,
                neoengram_core::IndexMutation::Delete { path } => path,
            };
            if !assignment
                .paths
                .iter()
                .any(|scope| scope == path || scope.is_ancestor_of(path))
            {
                return Err(AgentError::new(
                    AgentErrorCode::ExecutionFailed,
                    format!(
                        "prepared IndexDelta path {path} is outside the Add assignment scope for {}",
                        record.key
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn validate_decision_identity(record: &LedgerRecord, decision: &JobDecision) -> AgentResult<()> {
    if decision.job_id != record.assignment.job_id
        || decision.assignment_id != record.assignment.assignment_id
        || decision.assignment_generation != record.assignment.assignment_generation
    {
        return Err(AgentError::new(
            AgentErrorCode::AssignmentMismatch,
            format!("decision does not match assignment {}", record.key),
        ));
    }
    decision
        .validate()
        .map_err(|error| AgentError::new(AgentErrorCode::AssignmentMismatch, error.to_string()))?;
    Ok(())
}

fn validate_publish_result(record: &LedgerRecord, decision: &JobDecision) -> AgentResult<()> {
    let PublishDecision::Publish {
        published_index_version,
        ..
    } = &decision.decision
    else {
        return Ok(());
    };
    let prepared = record.prepared.as_ref().ok_or_else(|| {
        AgentError::new(
            AgentErrorCode::InvalidState,
            format!(
                "job {} cannot verify a publish decision without a durable prepared candidate",
                record.key
            ),
        )
    })?;
    let expected = prepared.candidate.index_delta.result_digest;
    let expected_revision = prepared
        .candidate
        .base_index_version
        .revision
        .checked_add(1)
        .ok_or_else(|| {
            AgentError::new(
                AgentErrorCode::InvalidState,
                format!("prepared base revision overflows for {}", record.key),
            )
        })?;
    if published_index_version.digest != expected
        || published_index_version.revision.get() != expected_revision
    {
        return Err(AgentError::new(
            AgentErrorCode::AssignmentMismatch,
            format!(
                "publish decision result {}@{} differs from durable prepared result {expected}@{expected_revision} for {}",
                published_index_version.digest,
                published_index_version.revision.get(),
                record.key
            ),
        ));
    }
    Ok(())
}

fn validate_finalized_identity(
    record: &LedgerRecord,
    decision: &JobDecision,
    finalized: &JobFinalized,
) -> AgentResult<()> {
    if finalized.job_id != record.assignment.job_id
        || finalized.assignment_id != record.assignment.assignment_id
        || finalized.assignment_generation != record.assignment.assignment_generation
        || finalized.decision_generation != decision.decision_generation
        || finalized.final_state != decision.final_state
    {
        return Err(AgentError::new(
            AgentErrorCode::AssignmentMismatch,
            format!(
                "durable finalized acknowledgement does not match decision for {}",
                record.key
            ),
        ));
    }
    Ok(())
}

fn validate_failure_identity(record: &LedgerRecord, failure: &JobFailed) -> AgentResult<()> {
    if failure.tenant_id != record.assignment.tenant_id
        || failure.job_id != record.assignment.job_id
        || failure.assignment_id != record.assignment.assignment_id
        || failure.assignment_generation != record.assignment.assignment_generation
    {
        return Err(AgentError::new(
            AgentErrorCode::AssignmentMismatch,
            format!(
                "durable terminal failure does not match assignment {}",
                record.key
            ),
        ));
    }
    failure
        .validate()
        .map_err(|error| AgentError::new(AgentErrorCode::AssignmentMismatch, error.to_string()))
}

const fn is_retryable(code: AgentErrorCode) -> bool {
    matches!(
        code,
        AgentErrorCode::ExecutionFailed
            | AgentErrorCode::ObjectTransferFailed
            | AgentErrorCode::ReportFailed
            | AgentErrorCode::Internal
    )
}

fn truncate_message(message: &str, limit: usize) -> String {
    if message.len() <= limit {
        return message.to_owned();
    }
    let mut end = limit;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message[..end].to_owned()
}

fn invalid_state(record: &LedgerRecord, message: &str) -> AgentError {
    AgentError::new(
        AgentErrorCode::InvalidState,
        format!("job {} is in {:?}: {message}", record.key, record.state),
    )
}
