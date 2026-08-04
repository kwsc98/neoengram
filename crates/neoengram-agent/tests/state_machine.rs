use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};

use neoengram_agent::{
    AddExecutor, Agent, AgentAssignmentState, AgentError, AgentErrorCode, AgentReport,
    AssignmentKey, AssignmentValidator, BasicAssignmentValidator, ClaimDisposition, ClaimOutcome,
    FakeAddExecutor, FakeClock, FakeObjectTransfer, FakeReportSink, InMemoryLedger, Ledger,
    LedgerClaim, LedgerRecord, PreparedExecution, ReportSink, TransferReceipt,
};
use neoengram_core::{
    ContentDigest, IndexDelta, IndexMutation, IndexVersion, LogicalPath,
    MAX_INDEX_MUTATIONS_PER_PAGE,
};
use neoengram_engine::{AddStatistics, ManagedResource, PreparedAdd};
use neoengram_protocol::{
    AddAssignment, AgentId, AgentMountId, ArtifactId, ArtifactPlacementId, AssignmentGeneration,
    AssignmentId, ControlError, DecimalU64, DecisionGeneration, EdgeClusterId, ErrorCode,
    Extensions, FencingToken, IndexDeltaRecord, IndexRevision, JobDecision, JobId, JobState,
    LeaseGrant, LeaseId, LeaseMode, MetadataBatchDescriptor, MetadataBatchId, MetadataBatchPage,
    MetadataBatchRecords, MetadataBatchScope, MountGeneration, OwnerGeneration,
    PlacementGeneration, PlaygroundId, PrincipalId, PrincipalKind, PrincipalRef, ProjectId,
    PublishDecision, StorageVolumeId, TenantId, UnixMillis, WireIndexVersion,
};

const NOW: u64 = 1_000;

#[test]
fn claim_is_durable_before_accepted_is_reported() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let ledger = Arc::new(RecordingLedger::new(Arc::clone(&events)));
    let reports = Arc::new(RecordingReportSink::new(Arc::clone(&events)));
    let assignment = assignment(1);
    let agent = Agent::new(
        ledger,
        Arc::new(BasicAssignmentValidator),
        Arc::new(FakeAddExecutor::new(prepared(&assignment))),
        Arc::new(FakeObjectTransfer::new(TransferReceipt::default())),
        reports,
        Arc::new(FakeClock::new(NOW)),
    );

    let accepted = agent.accept_assignment(assignment).unwrap();

    assert_eq!(accepted.disposition, ClaimDisposition::New);
    assert_eq!(accepted.record.state, AgentAssignmentState::Accepted);
    assert_eq!(
        *events.lock().unwrap(),
        ["claimed", "accepted_persisted", "accepted_reported"]
    );
}

#[test]
fn duplicate_digest_returns_existing_state_without_reexecution() {
    let ledger = Arc::new(InMemoryLedger::new());
    let reports = Arc::new(FakeReportSink::new());
    let assignment = assignment(2);
    let agent = Agent::new(
        ledger.clone(),
        Arc::new(BasicAssignmentValidator),
        Arc::new(FakeAddExecutor::new(prepared(&assignment))),
        Arc::new(FakeObjectTransfer::new(TransferReceipt::default())),
        reports.clone(),
        Arc::new(FakeClock::new(NOW)),
    );

    let first = agent.accept_assignment(assignment.clone()).unwrap();
    let replay = agent.accept_assignment(assignment).unwrap();

    assert_eq!(first.disposition, ClaimDisposition::New);
    assert_eq!(replay.disposition, ClaimDisposition::Replayed);
    assert_eq!(replay.record, first.record);
    assert_eq!(ledger.records().unwrap().len(), 1);
    assert_eq!(reports.reports().unwrap().len(), 2);
}

#[test]
fn durable_replay_is_returned_after_the_original_deadline() {
    let ledger = Arc::new(InMemoryLedger::new());
    let clock = Arc::new(FakeClock::new(NOW));
    let assignment = assignment(22);
    let agent = Agent::new(
        ledger,
        Arc::new(BasicAssignmentValidator),
        Arc::new(FakeAddExecutor::new(prepared(&assignment))),
        Arc::new(FakeObjectTransfer::new(TransferReceipt::default())),
        Arc::new(FakeReportSink::new()),
        clock.clone(),
    );
    agent.accept_assignment(assignment.clone()).unwrap();
    clock.set(assignment.deadline_unix_ms.get() + 1);

    let replay = agent.accept_assignment(assignment).unwrap();

    assert_eq!(replay.disposition, ClaimDisposition::Replayed);
    assert_eq!(replay.record.state, AgentAssignmentState::Accepted);
}

#[test]
fn expired_lease_after_claim_is_reported_without_execution() {
    let ledger = Arc::new(InMemoryLedger::new());
    let reports = Arc::new(FakeReportSink::new());
    let clock = Arc::new(FakeClock::new(NOW));
    let mut assignment = assignment(23);
    assignment.lease = Some(LeaseGrant {
        lease_id: LeaseId::new("lease-23").unwrap(),
        mode: LeaseMode::ExclusiveWrite,
        fencing_token: Some(FencingToken::new(23)),
        expires_at_unix_ms: UnixMillis::new(NOW + 10),
        extensions: Extensions::new(),
    });
    let executor = Arc::new(FakeAddExecutor::new(prepared(&assignment)));
    let agent = Agent::new(
        ledger.clone(),
        Arc::new(BasicAssignmentValidator),
        executor.clone(),
        Arc::new(FakeObjectTransfer::new(TransferReceipt::default())),
        reports.clone(),
        clock.clone(),
    );
    let key = AssignmentKey::from_assignment(&assignment);
    agent.accept_assignment(assignment).unwrap();
    clock.set(NOW + 10);

    let error = agent.execute(&key).unwrap_err();

    assert_eq!(error.code(), AgentErrorCode::DeadlineExceeded);
    assert!(executor.calls().unwrap().is_empty());
    let failed = ledger.load(&key).unwrap().unwrap();
    assert_eq!(failed.state, AgentAssignmentState::FailedReported);
    assert!(matches!(
        reports.reports().unwrap().last(),
        Some(AgentReport::Failed(report))
            if report.error.code.as_str() == "ASSIGNMENT_DEADLINE_EXCEEDED"
    ));
}

#[test]
fn active_work_rechecks_custom_generation_validation() {
    let ledger = Arc::new(InMemoryLedger::new());
    let assignment = assignment(24);
    let validator = Arc::new(RejectGenerationAfterClaim::default());
    let executor = Arc::new(FakeAddExecutor::new(prepared(&assignment)));
    let agent = Agent::new(
        ledger.clone(),
        validator.clone(),
        executor.clone(),
        Arc::new(FakeObjectTransfer::new(TransferReceipt::default())),
        Arc::new(FakeReportSink::new()),
        Arc::new(FakeClock::new(NOW)),
    );
    let key = AssignmentKey::from_assignment(&assignment);
    agent.accept_assignment(assignment).unwrap();

    let error = agent.execute(&key).unwrap_err();

    assert_eq!(error.code(), AgentErrorCode::InvalidAssignment);
    assert_eq!(validator.calls.load(Ordering::SeqCst), 2);
    assert!(executor.calls().unwrap().is_empty());
    assert_eq!(
        ledger.load(&key).unwrap().unwrap().state,
        AgentAssignmentState::FailedReported
    );
}

#[test]
fn reused_job_id_with_a_different_digest_is_rejected_stably() {
    let assignment = assignment(3);
    let agent = test_agent(&assignment, FakeAddExecutor::new(prepared(&assignment)));
    agent.accept_assignment(assignment.clone()).unwrap();

    let mut reused = assignment;
    reused.request_digest = ContentDigest::from_bytes([99; 32]);
    let error = agent.accept_assignment(reused).unwrap_err();

    assert_eq!(error.code(), AgentErrorCode::JobIdReused);
    assert_eq!(error.stable_code(), "JOB_ID_REUSED");
}

#[test]
fn replay_with_the_same_digest_but_a_different_assignment_is_rejected() {
    let assignment = assignment(33);
    let agent = test_agent(&assignment, FakeAddExecutor::new(prepared(&assignment)));
    agent.accept_assignment(assignment.clone()).unwrap();

    let mut tampered = assignment;
    tampered.agent_mount_id = AgentMountId::new("mount-other").unwrap();
    let error = agent.accept_assignment(tampered).unwrap_err();

    assert_eq!(error.code(), AgentErrorCode::AssignmentMismatch);
}

#[test]
fn ledger_cas_cannot_rewrite_the_durable_assignment_payload() {
    let ledger = InMemoryLedger::new();
    let assignment = assignment(34);
    let claimed = ledger
        .claim(LedgerClaim {
            assignment,
            claimed_at_unix_ms: NOW,
        })
        .unwrap();
    let ClaimOutcome::Claimed(mut record) = claimed else {
        panic!("new assignment must be claimed");
    };
    let revision = record.revision;
    record.assignment.agent_mount_id = AgentMountId::new("mount-rewritten").unwrap();

    let error = ledger.compare_exchange(revision, record).unwrap_err();

    assert_eq!(error.code(), AgentErrorCode::AssignmentMismatch);
}

#[test]
fn complete_add_decision_loop_is_idempotent() {
    let ledger = Arc::new(InMemoryLedger::new());
    let reports = Arc::new(FakeReportSink::new());
    let assignment = assignment(4);
    let candidate = prepared(&assignment);
    let executor = Arc::new(FakeAddExecutor::new(candidate.clone()));
    let transfer = Arc::new(FakeObjectTransfer::new(mutation_receipt(
        &assignment,
        &candidate,
    )));
    let agent = Agent::new(
        ledger,
        Arc::new(BasicAssignmentValidator),
        executor.clone(),
        transfer.clone(),
        reports.clone(),
        Arc::new(FakeClock::new(NOW)),
    );

    let awaiting = agent.handle_assignment(assignment.clone()).unwrap();
    assert_eq!(awaiting.state, AgentAssignmentState::AwaitingDecision);
    let decision = publish_decision(&assignment);
    let completed = agent
        .handle_decision(&assignment.tenant_id, decision.clone())
        .unwrap();
    assert_eq!(completed.state, AgentAssignmentState::Completed);
    assert_eq!(
        completed
            .finalized
            .as_ref()
            .map(|report| report.final_state),
        Some(JobState::Succeeded)
    );

    let replay = agent
        .handle_decision(&assignment.tenant_id, decision)
        .unwrap();
    assert_eq!(replay.revision, completed.revision);
    assert_eq!(executor.calls().unwrap().len(), 1);
    assert_eq!(transfer.transfers().unwrap().len(), 1);
    assert_eq!(transfer.stagings().unwrap().len(), 1);
    assert_eq!(transfer.finalizations().unwrap().len(), 1);
    assert!(matches!(
        reports.reports().unwrap().as_slice(),
        [
            AgentReport::Accepted(_),
            AgentReport::Progress(_),
            AgentReport::Prepared(_),
            AgentReport::Finalized(_)
        ]
    ));
}

#[test]
fn multi_page_index_delta_remains_one_prepared_agent_candidate() {
    let assignment = assignment(77);
    let mutations = (0..=MAX_INDEX_MUTATIONS_PER_PAGE)
        .map(|index| IndexMutation::Delete {
            path: LogicalPath::parse(format!("files/file-{index:05}.bin")).unwrap(),
        })
        .collect();
    let candidate = prepared_with_mutations(&assignment, mutations);
    let receipt = mutation_receipt(&assignment, &candidate);
    let agent = Agent::new(
        Arc::new(InMemoryLedger::new()),
        Arc::new(BasicAssignmentValidator),
        Arc::new(FakeAddExecutor::new(candidate)),
        Arc::new(FakeObjectTransfer::new(receipt)),
        Arc::new(FakeReportSink::new()),
        Arc::new(FakeClock::new(NOW)),
    );

    let awaiting = agent.handle_assignment(assignment).unwrap();
    let prepared = awaiting.prepared.as_ref().unwrap();

    assert_eq!(prepared.candidate.index_delta.page_count(), 2);
    assert_eq!(
        prepared.candidate.index_delta.mutation_count(),
        MAX_INDEX_MUTATIONS_PER_PAGE + 1
    );
    assert_eq!(awaiting.state, AgentAssignmentState::AwaitingDecision);
}

#[test]
fn transfer_receipt_cannot_substitute_another_valid_publication() {
    let assignment = assignment(78);
    let candidate_a = prepared_with_mutations(
        &assignment,
        vec![IndexMutation::Delete {
            path: LogicalPath::parse("files/a.bin").unwrap(),
        }],
    );
    let candidate_b = prepared_with_mutations(
        &assignment,
        vec![IndexMutation::Delete {
            path: LogicalPath::parse("files/b.bin").unwrap(),
        }],
    );
    let ledger = Arc::new(InMemoryLedger::new());
    let agent = Agent::new(
        ledger.clone(),
        Arc::new(BasicAssignmentValidator),
        Arc::new(FakeAddExecutor::new(candidate_a)),
        Arc::new(FakeObjectTransfer::new(mutation_receipt(
            &assignment,
            &candidate_b,
        ))),
        Arc::new(FakeReportSink::new()),
        Arc::new(FakeClock::new(NOW)),
    );
    let key = AssignmentKey::from_assignment(&assignment);

    let error = agent.handle_assignment(assignment).unwrap_err();

    assert_eq!(error.code(), AgentErrorCode::ObjectTransferFailed);
    let failed = ledger.load(&key).unwrap().unwrap();
    assert_eq!(failed.state, AgentAssignmentState::FailedReported);
    assert!(failed.prepared.is_none());
}

#[test]
fn no_op_publication_requires_all_three_explicit_metadata_batches() {
    let assignment = assignment(79);
    let candidate = prepared(&assignment);
    let complete = mutation_receipt(&assignment, &candidate);
    complete.validate_for(&assignment, &candidate).unwrap();
    assert_eq!(complete.metadata_batches.len(), 3);
    assert_eq!(complete.metadata_pages.len(), 3);

    let mut incomplete = complete;
    incomplete.metadata_batches.retain(|descriptor| {
        descriptor.kind != neoengram_protocol::MetadataBatchKind::ObjectReceipt
    });
    incomplete
        .metadata_pages
        .retain(|page| !matches!(page.records, MetadataBatchRecords::ObjectReceipt(_)));
    let ledger = Arc::new(InMemoryLedger::new());
    let agent = Agent::new(
        ledger.clone(),
        Arc::new(BasicAssignmentValidator),
        Arc::new(FakeAddExecutor::new(candidate)),
        Arc::new(FakeObjectTransfer::new(incomplete)),
        Arc::new(FakeReportSink::new()),
        Arc::new(FakeClock::new(NOW)),
    );
    let key = AssignmentKey::from_assignment(&assignment);

    let error = agent.handle_assignment(assignment).unwrap_err();

    assert_eq!(error.code(), AgentErrorCode::ObjectTransferFailed);
    let failed = ledger.load(&key).unwrap().unwrap();
    assert_eq!(failed.state, AgentAssignmentState::FailedReported);
    assert!(failed.prepared.is_none());
    assert!(failed.decision.is_none());
}

#[test]
fn prepared_candidate_cannot_escape_assignment_path_scope() {
    let mut assignment = assignment(80);
    assignment.all = false;
    assignment.paths = vec![LogicalPath::parse("allowed").unwrap()];
    assignment.request_digest = assignment.computed_request_digest().unwrap();
    let candidate = prepared_with_mutations(
        &assignment,
        vec![IndexMutation::Delete {
            path: LogicalPath::parse("outside/file.bin").unwrap(),
        }],
    );
    let transfer = Arc::new(FakeObjectTransfer::new(mutation_receipt(
        &assignment,
        &candidate,
    )));
    let ledger = Arc::new(InMemoryLedger::new());
    let agent = Agent::new(
        ledger.clone(),
        Arc::new(BasicAssignmentValidator),
        Arc::new(FakeAddExecutor::new(candidate)),
        transfer.clone(),
        Arc::new(FakeReportSink::new()),
        Arc::new(FakeClock::new(NOW)),
    );
    let key = AssignmentKey::from_assignment(&assignment);

    let error = agent.handle_assignment(assignment).unwrap_err();

    assert_eq!(error.code(), AgentErrorCode::ExecutionFailed);
    assert!(transfer.transfers().unwrap().is_empty());
    let failed = ledger.load(&key).unwrap().unwrap();
    assert_eq!(failed.state, AgentAssignmentState::FailedReported);
    assert!(failed.prepared.is_none());
}

#[test]
fn publish_result_mismatch_is_rejected_stably_without_finalization() {
    let ledger = Arc::new(InMemoryLedger::new());
    let assignment = assignment(81);
    let candidate = prepared(&assignment);
    let transfer = Arc::new(FakeObjectTransfer::new(mutation_receipt(
        &assignment,
        &candidate,
    )));
    let agent = Agent::new(
        ledger.clone(),
        Arc::new(BasicAssignmentValidator),
        Arc::new(FakeAddExecutor::new(candidate)),
        transfer.clone(),
        Arc::new(FakeReportSink::new()),
        Arc::new(FakeClock::new(NOW)),
    );
    let key = AssignmentKey::from_assignment(&assignment);
    let awaiting = agent.handle_assignment(assignment.clone()).unwrap();

    let mut wrong_digest = publish_decision(&assignment);
    let PublishDecision::Publish {
        published_index_version,
        ..
    } = &mut wrong_digest.decision
    else {
        unreachable!();
    };
    published_index_version.digest = ContentDigest::from_bytes([99; 32]);
    let first = agent
        .handle_decision(&assignment.tenant_id, wrong_digest.clone())
        .unwrap_err();
    let replay = agent
        .handle_decision(&assignment.tenant_id, wrong_digest)
        .unwrap_err();
    assert_eq!(first, replay);
    assert_eq!(first.code(), AgentErrorCode::AssignmentMismatch);

    let mut wrong_revision = publish_decision(&assignment);
    let PublishDecision::Publish {
        published_index_version,
        ..
    } = &mut wrong_revision.decision
    else {
        unreachable!();
    };
    published_index_version.revision = IndexRevision::new(9);
    let revision_error = agent
        .handle_decision(&assignment.tenant_id, wrong_revision)
        .unwrap_err();
    assert_eq!(revision_error.code(), AgentErrorCode::AssignmentMismatch);

    let durable = ledger.load(&key).unwrap().unwrap();
    assert_eq!(durable.state, AgentAssignmentState::AwaitingDecision);
    assert_eq!(durable.revision, awaiting.revision);
    assert!(durable.decision.is_none());
    assert!(durable.finalized.is_none());
    assert!(transfer.finalizations().unwrap().is_empty());

    let mut durable_mismatch = durable;
    durable_mismatch.state = AgentAssignmentState::FailedReported;
    durable_mismatch.decision = Some({
        let mut decision = publish_decision(&assignment);
        let PublishDecision::Publish {
            published_index_version,
            ..
        } = &mut decision.decision
        else {
            unreachable!();
        };
        published_index_version.digest = ContentDigest::from_bytes([99; 32]);
        decision
    });
    let durable_mismatch = ledger
        .compare_exchange(durable_mismatch.revision, durable_mismatch)
        .unwrap();
    let recovery_error = agent.recover(&key).unwrap_err();
    assert_eq!(recovery_error.code(), AgentErrorCode::AssignmentMismatch);
    let after_recovery = ledger.load(&key).unwrap().unwrap();
    assert_eq!(after_recovery.state, AgentAssignmentState::FailedReported);
    assert_eq!(after_recovery.revision, durable_mismatch.revision);
    assert!(transfer.finalizations().unwrap().is_empty());
}

#[test]
fn successful_lifecycle_is_persisted_in_protocol_order() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let ledger = Arc::new(RecordingLedger::new(Arc::clone(&events)));
    let reports = Arc::new(RecordingReportSink::new(Arc::clone(&events)));
    let assignment = assignment(44);
    let candidate = prepared(&assignment);
    let agent = Agent::new(
        ledger,
        Arc::new(BasicAssignmentValidator),
        Arc::new(FakeAddExecutor::new(candidate.clone())),
        Arc::new(FakeObjectTransfer::new(mutation_receipt(
            &assignment,
            &candidate,
        ))),
        reports,
        Arc::new(FakeClock::new(NOW)),
    );

    agent.handle_assignment(assignment.clone()).unwrap();
    agent
        .handle_decision(&assignment.tenant_id, publish_decision(&assignment))
        .unwrap();

    assert_eq!(
        *events.lock().unwrap(),
        [
            "claimed",
            "accepted_persisted",
            "accepted_reported",
            "running_persisted",
            "prepared_persisted",
            "prepared_reported",
            "awaiting_decision_persisted",
            "finalizing_persisted",
            "finalized_reported",
            "completed_persisted",
        ]
    );
}

#[test]
fn metadata_staging_failure_keeps_prepared_material_and_recovers_idempotently() {
    let ledger = Arc::new(InMemoryLedger::new());
    let reports = Arc::new(FakeReportSink::new());
    let assignment = assignment(45);
    let candidate = prepared(&assignment);
    let executor = Arc::new(FakeAddExecutor::new(candidate.clone()));
    let transfer = Arc::new(FakeObjectTransfer::with_staging_results(
        [Ok(mutation_receipt(&assignment, &candidate))],
        [
            Err(AgentError::new(
                AgentErrorCode::ObjectTransferFailed,
                "injected metadata staging failure",
            )),
            Ok(()),
        ],
        [Ok(())],
    ));
    let agent = Agent::new(
        ledger.clone(),
        Arc::new(BasicAssignmentValidator),
        executor.clone(),
        transfer.clone(),
        reports.clone(),
        Arc::new(FakeClock::new(NOW)),
    );
    let key = AssignmentKey::from_assignment(&assignment);

    let error = agent.handle_assignment(assignment).unwrap_err();
    assert_eq!(error.code(), AgentErrorCode::ObjectTransferFailed);
    let durable = ledger.load(&key).unwrap().unwrap();
    assert_eq!(durable.state, AgentAssignmentState::Prepared);
    assert!(durable.prepared.is_some());
    assert!(durable.failure.is_none());

    let recovered = agent.recover(&key).unwrap();
    assert_eq!(recovered.state, AgentAssignmentState::AwaitingDecision);
    assert_eq!(executor.calls().unwrap().len(), 1);
    assert_eq!(transfer.transfers().unwrap().len(), 1);
    assert_eq!(transfer.stagings().unwrap().len(), 2);
    assert_eq!(
        reports
            .reports()
            .unwrap()
            .iter()
            .filter(|report| matches!(report, AgentReport::Prepared(_)))
            .count(),
        2
    );
    assert!(!reports
        .reports()
        .unwrap()
        .iter()
        .any(|report| matches!(report, AgentReport::Failed(_))));
}

#[test]
fn authoritative_publish_converges_after_staging_outlives_local_state_persistence() {
    let ledger = Arc::new(FailAwaitingDecisionOnceLedger::new());
    let reports = Arc::new(FakeReportSink::new());
    let clock = Arc::new(FakeClock::new(NOW));
    let mut assignment = assignment(46);
    assignment.lease = Some(LeaseGrant {
        lease_id: LeaseId::new("lease-46").unwrap(),
        mode: LeaseMode::ExclusiveWrite,
        fencing_token: Some(FencingToken::new(46)),
        expires_at_unix_ms: UnixMillis::new(NOW + 10),
        extensions: Extensions::new(),
    });
    let candidate = prepared(&assignment);
    let transfer = Arc::new(FakeObjectTransfer::new(mutation_receipt(
        &assignment,
        &candidate,
    )));
    let agent = Agent::new(
        ledger.clone(),
        Arc::new(BasicAssignmentValidator),
        Arc::new(FakeAddExecutor::new(candidate)),
        transfer.clone(),
        reports,
        clock.clone(),
    );
    let key = AssignmentKey::from_assignment(&assignment);

    let persistence_error = agent.handle_assignment(assignment.clone()).unwrap_err();
    assert_eq!(persistence_error.code(), AgentErrorCode::Internal);
    let durable_prepared = ledger.load(&key).unwrap().unwrap();
    assert_eq!(durable_prepared.state, AgentAssignmentState::Prepared);
    assert!(durable_prepared.prepared.is_some());
    assert_eq!(transfer.stagings().unwrap().len(), 1);

    clock.set(NOW + 10);
    let expiry_error = agent.recover(&key).unwrap_err();
    assert_eq!(expiry_error.code(), AgentErrorCode::DeadlineExceeded);
    let locally_failed = ledger.load(&key).unwrap().unwrap();
    assert_eq!(locally_failed.state, AgentAssignmentState::FailedReported);
    assert!(locally_failed.prepared.is_some());

    let mut mismatched = publish_decision(&assignment);
    let PublishDecision::Publish {
        published_index_version,
        ..
    } = &mut mismatched.decision
    else {
        unreachable!();
    };
    published_index_version.digest = ContentDigest::from_bytes([99; 32]);
    let mismatch = agent
        .handle_decision(&assignment.tenant_id, mismatched)
        .unwrap_err();
    assert_eq!(mismatch.code(), AgentErrorCode::AssignmentMismatch);
    let still_failed = ledger.load(&key).unwrap().unwrap();
    assert_eq!(still_failed.state, AgentAssignmentState::FailedReported);
    assert!(still_failed.decision.is_none());
    assert!(transfer.finalizations().unwrap().is_empty());

    let completed = agent
        .handle_decision(&assignment.tenant_id, publish_decision(&assignment))
        .unwrap();

    assert_eq!(completed.state, AgentAssignmentState::Completed);
    assert_eq!(
        completed.finalized.unwrap().final_state,
        JobState::Succeeded
    );
    assert!(completed.failure.is_none());
    assert_eq!(transfer.finalizations().unwrap().len(), 1);
}

#[test]
fn failed_execution_recovery_replays_failure_and_waits_for_reject() {
    let ledger = Arc::new(InMemoryLedger::new());
    let reports = Arc::new(FakeReportSink::new());
    let assignment = assignment(5);
    let executor = Arc::new(FakeAddExecutor::with_results([Err(AgentError::new(
        AgentErrorCode::ExecutionFailed,
        "injected executor failure",
    ))]));
    let agent = Agent::new(
        ledger.clone(),
        Arc::new(BasicAssignmentValidator),
        executor.clone(),
        Arc::new(FakeObjectTransfer::new(TransferReceipt::default())),
        reports.clone(),
        Arc::new(FakeClock::new(NOW)),
    );
    let key = AssignmentKey::from_assignment(&assignment);
    agent.accept_assignment(assignment.clone()).unwrap();

    let error = agent.execute(&key).unwrap_err();
    assert_eq!(error.code(), AgentErrorCode::ExecutionFailed);
    let failed = ledger.load(&key).unwrap().unwrap();
    assert_eq!(failed.state, AgentAssignmentState::FailedReported);
    assert_eq!(
        failed.failure.as_ref().unwrap().error.code.as_str(),
        "EXECUTION_FAILED"
    );

    let waiting = agent.recover(&key).unwrap();
    assert_eq!(waiting.state, AgentAssignmentState::FailedReported);
    assert_eq!(executor.calls().unwrap().len(), 1);
    let failures = reports
        .reports()
        .unwrap()
        .into_iter()
        .filter_map(|report| match report {
            AgentReport::Failed(failure) => Some(failure),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(failures.len(), 2);
    assert_eq!(failures[0], failures[1]);

    let completed = agent
        .handle_decision(&assignment.tenant_id, reject_decision(&assignment))
        .unwrap();
    assert_eq!(completed.state, AgentAssignmentState::Completed);
    assert_eq!(completed.finalized.unwrap().final_state, JobState::Failed);
}

#[test]
fn recovering_with_a_terminal_failure_never_reexecutes() {
    let ledger = Arc::new(InMemoryLedger::new());
    let reports = Arc::new(FakeReportSink::new());
    let assignment = assignment(15);
    let executor = Arc::new(FakeAddExecutor::with_results([Err(AgentError::new(
        AgentErrorCode::ExecutionFailed,
        "injected executor failure",
    ))]));
    let agent = Agent::new(
        ledger.clone(),
        Arc::new(BasicAssignmentValidator),
        executor.clone(),
        Arc::new(FakeObjectTransfer::new(TransferReceipt::default())),
        reports.clone(),
        Arc::new(FakeClock::new(NOW)),
    );
    let key = AssignmentKey::from_assignment(&assignment);
    agent.accept_assignment(assignment).unwrap();
    agent.execute(&key).unwrap_err();

    let failed = ledger.load(&key).unwrap().unwrap();
    let mut interrupted_recovery = failed.clone();
    interrupted_recovery.state = AgentAssignmentState::Recovering;
    ledger
        .compare_exchange(failed.revision, interrupted_recovery)
        .unwrap();

    let waiting = agent.recover(&key).unwrap();
    assert_eq!(waiting.state, AgentAssignmentState::FailedReported);
    assert_eq!(waiting.failure, failed.failure);
    assert_eq!(executor.calls().unwrap().len(), 1);
    assert_eq!(
        reports
            .reports()
            .unwrap()
            .iter()
            .filter(|report| matches!(report, AgentReport::Failed(_)))
            .count(),
        2
    );
}

#[test]
fn failed_execution_accepts_reject_without_a_prepared_candidate() {
    let ledger = Arc::new(InMemoryLedger::new());
    let reports = Arc::new(FakeReportSink::new());
    let assignment = assignment(55);
    let transfer = Arc::new(FakeObjectTransfer::new(TransferReceipt::default()));
    let agent = Agent::new(
        ledger.clone(),
        Arc::new(BasicAssignmentValidator),
        Arc::new(FakeAddExecutor::with_results([Err(AgentError::new(
            AgentErrorCode::ExecutionFailed,
            "injected executor failure",
        ))])),
        transfer.clone(),
        reports.clone(),
        Arc::new(FakeClock::new(NOW)),
    );
    let key = AssignmentKey::from_assignment(&assignment);
    agent.accept_assignment(assignment.clone()).unwrap();
    agent.execute(&key).unwrap_err();

    let publish_error = agent
        .handle_decision(&assignment.tenant_id, publish_decision(&assignment))
        .unwrap_err();
    assert_eq!(publish_error.code(), AgentErrorCode::InvalidState);

    let completed = agent
        .handle_decision(&assignment.tenant_id, reject_decision(&assignment))
        .unwrap();

    assert_eq!(completed.state, AgentAssignmentState::Completed);
    assert_eq!(
        completed.finalized.as_ref().unwrap().final_state,
        JobState::Failed
    );
    assert!(transfer.finalizations().unwrap().is_empty());
    assert!(matches!(
        reports.reports().unwrap().last(),
        Some(AgentReport::Finalized(report)) if report.final_state == JobState::Failed
    ));
}

#[test]
fn timeout_decision_stops_an_accepted_assignment_without_execution() {
    let ledger = Arc::new(InMemoryLedger::new());
    let reports = Arc::new(FakeReportSink::new());
    let assignment = assignment(56);
    let executor = Arc::new(FakeAddExecutor::new(prepared(&assignment)));
    let transfer = Arc::new(FakeObjectTransfer::new(TransferReceipt::default()));
    let agent = Agent::new(
        ledger,
        Arc::new(BasicAssignmentValidator),
        executor.clone(),
        transfer.clone(),
        reports.clone(),
        Arc::new(FakeClock::new(NOW)),
    );
    agent.accept_assignment(assignment.clone()).unwrap();

    let completed = agent
        .handle_decision(
            &assignment.tenant_id,
            reject_decision_with_state(&assignment, JobState::TimedOut),
        )
        .unwrap();

    assert_eq!(completed.state, AgentAssignmentState::Completed);
    assert_eq!(
        completed.finalized.as_ref().unwrap().final_state,
        JobState::TimedOut
    );
    assert!(executor.calls().unwrap().is_empty());
    assert!(transfer.transfers().unwrap().is_empty());
    assert!(transfer.finalizations().unwrap().is_empty());
    assert!(matches!(
        reports.reports().unwrap().last(),
        Some(AgentReport::Finalized(report)) if report.final_state == JobState::TimedOut
    ));
}

#[test]
fn finalized_acknowledgement_is_stable_when_completed_persistence_is_retried() {
    let ledger = Arc::new(FailCompletedOnceLedger::new());
    let reports = Arc::new(FakeReportSink::new());
    let clock = Arc::new(FakeClock::new(NOW));
    let assignment = assignment(66);
    let candidate = prepared(&assignment);
    let agent = Agent::new(
        ledger.clone(),
        Arc::new(BasicAssignmentValidator),
        Arc::new(FakeAddExecutor::new(candidate.clone())),
        Arc::new(FakeObjectTransfer::with_results(
            [Ok(mutation_receipt(&assignment, &candidate))],
            [Ok(()), Ok(())],
        )),
        reports.clone(),
        clock.clone(),
    );
    let key = AssignmentKey::from_assignment(&assignment);
    agent.handle_assignment(assignment.clone()).unwrap();

    let error = agent
        .handle_decision(&assignment.tenant_id, publish_decision(&assignment))
        .unwrap_err();
    assert_eq!(error.code(), AgentErrorCode::Internal);
    assert_eq!(
        ledger.load(&key).unwrap().unwrap().state,
        AgentAssignmentState::Finalizing
    );

    clock.advance(500).unwrap();
    let completed = agent.recover(&key).unwrap();
    assert_eq!(completed.state, AgentAssignmentState::Completed);
    let finalized = reports
        .reports()
        .unwrap()
        .into_iter()
        .filter_map(|report| match report {
            AgentReport::Finalized(report) => Some(report),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(finalized.len(), 2);
    assert_eq!(finalized[0], finalized[1]);
    assert_eq!(finalized[0].finalized_at_unix_ms.get(), NOW);
}

#[test]
fn durable_prepared_replay_rejects_a_tampered_candidate() {
    let ledger = Arc::new(InMemoryLedger::new());
    let reports = Arc::new(FakeReportSink::new());
    let assignment = assignment(6);
    let agent = Agent::new(
        ledger.clone(),
        Arc::new(BasicAssignmentValidator),
        Arc::new(FakeAddExecutor::new(prepared(&assignment))),
        Arc::new(FakeObjectTransfer::new(TransferReceipt::default())),
        reports.clone(),
        Arc::new(FakeClock::new(NOW)),
    );
    let accepted = agent.accept_assignment(assignment.clone()).unwrap().record;
    let mut tampered = prepared(&assignment);
    tampered.prepared_at_unix_ms += 1;
    let mut durable = accepted.clone();
    durable.state = AgentAssignmentState::Prepared;
    durable.prepared = Some(PreparedExecution {
        candidate: tampered,
        transfer: TransferReceipt::default(),
    });
    ledger.compare_exchange(accepted.revision, durable).unwrap();

    let error = agent
        .execute(&AssignmentKey::from_assignment(&assignment))
        .unwrap_err();

    assert_eq!(error.code(), AgentErrorCode::ExecutionFailed);
    assert!(matches!(
        reports.reports().unwrap().as_slice(),
        [AgentReport::Accepted(_)]
    ));
}

fn test_agent(assignment: &AddAssignment, executor: impl AddExecutor + 'static) -> Agent {
    Agent::new(
        Arc::new(InMemoryLedger::new()),
        Arc::new(BasicAssignmentValidator),
        Arc::new(executor),
        Arc::new(FakeObjectTransfer::new(TransferReceipt::default())),
        Arc::new(FakeReportSink::new()),
        Arc::new(FakeClock::new(
            assignment.deadline_unix_ms.get().saturating_sub(1_000),
        )),
    )
}

fn assignment(seed: u8) -> AddAssignment {
    let mut assignment = AddAssignment {
        job_id: JobId::new(format!("job-{seed}")).unwrap(),
        assignment_id: AssignmentId::new(format!("assignment-{seed}")).unwrap(),
        assignment_generation: AssignmentGeneration::new(1),
        agent_id: AgentId::new("agent-a").unwrap(),
        principal: PrincipalRef {
            kind: PrincipalKind::Service,
            id: PrincipalId::new("service-a").unwrap(),
            extensions: Extensions::new(),
        },
        tenant_id: TenantId::new("tenant-a").unwrap(),
        project_id: ProjectId::new("project-a").unwrap(),
        artifact_id: ArtifactId::new("artifact-a").unwrap(),
        playground_id: PlaygroundId::new("playground-a").unwrap(),
        edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
        storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
        artifact_placement_id: ArtifactPlacementId::new("placement-a").unwrap(),
        placement_generation: PlacementGeneration::new(1),
        agent_mount_id: AgentMountId::new("mount-a").unwrap(),
        mount_generation: MountGeneration::new(1),
        owner_generation: OwnerGeneration::new(1),
        expected_index_version: WireIndexVersion {
            revision: IndexRevision::new(7),
            digest: ContentDigest::from_bytes([7; 32]),
            extensions: Extensions::new(),
        },
        lease: None,
        request_digest: ContentDigest::from_bytes([0; 32]),
        deadline_unix_ms: UnixMillis::new(NOW + 100_000),
        paths: Vec::new(),
        all: true,
        extensions: Extensions::new(),
    };
    assignment.request_digest = assignment.computed_request_digest().unwrap();
    assignment
}

#[derive(Debug, Default)]
struct RejectGenerationAfterClaim {
    calls: AtomicUsize,
}

impl AssignmentValidator for RejectGenerationAfterClaim {
    fn validate(
        &self,
        assignment: &AddAssignment,
        now_unix_ms: u64,
    ) -> neoengram_agent::AgentResult<()> {
        BasicAssignmentValidator.validate(assignment, now_unix_ms)?;
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(())
        } else {
            Err(AgentError::new(
                AgentErrorCode::InvalidAssignment,
                "assignment generations are no longer current",
            ))
        }
    }
}

fn prepared(assignment: &AddAssignment) -> PreparedAdd {
    prepared_with_mutations(assignment, Vec::new())
}

fn prepared_with_mutations(
    assignment: &AddAssignment,
    mutations: Vec<IndexMutation>,
) -> PreparedAdd {
    PreparedAdd::new(
        ManagedResource {
            tenant_id: assignment.tenant_id.to_string(),
            project_id: assignment.project_id.to_string(),
            artifact_id: assignment.artifact_id.to_string(),
            playground_id: assignment.playground_id.to_string(),
            job_id: assignment.job_id.to_string(),
        },
        IndexVersion::new(
            assignment.expected_index_version.revision.get(),
            assignment.expected_index_version.digest,
        ),
        format!("batch-{}", assignment.job_id),
        IndexDelta::new(
            IndexVersion::new(
                assignment.expected_index_version.revision.get(),
                assignment.expected_index_version.digest,
            ),
            mutations,
            ContentDigest::from_bytes([41; 32]),
        )
        .unwrap(),
        Vec::new(),
        Vec::new(),
        AddStatistics::default(),
        NOW + 1,
    )
    .unwrap()
}

fn mutation_receipt(assignment: &AddAssignment, prepared: &PreparedAdd) -> TransferReceipt {
    let scope = MetadataBatchScope {
        tenant_id: assignment.tenant_id.clone(),
        project_id: assignment.project_id.clone(),
        artifact_id: assignment.artifact_id.clone(),
        playground_id: assignment.playground_id.clone(),
        job_id: assignment.job_id.clone(),
        base_index_version: assignment.expected_index_version.clone(),
        extensions: Extensions::new(),
    };
    let mutations = prepared
        .index_delta
        .iter()
        .map(|mutation| match mutation {
            IndexMutation::Upsert { record } => IndexDeltaRecord::Upsert {
                path: record.path.clone(),
                manifest_id: record.manifest_id,
                total_size: DecimalU64::new(record.total_size),
                chunk_count: DecimalU64::new(record.chunk_count),
                extensions: Extensions::new(),
            },
            IndexMutation::Delete { path } => IndexDeltaRecord::Delete {
                path: path.clone(),
                extensions: Extensions::new(),
            },
        })
        .collect::<Vec<_>>();
    let index_page_count = u32::try_from(mutations.len().div_ceil(MAX_INDEX_MUTATIONS_PER_PAGE))
        .unwrap()
        .max(1);
    let mut index_pages = mutations
        .chunks(MAX_INDEX_MUTATIONS_PER_PAGE)
        .enumerate()
        .map(|(page_number, records)| {
            MetadataBatchPage::new(
                MetadataBatchId::new("batch-index").unwrap(),
                scope.clone(),
                u32::try_from(page_number).unwrap(),
                index_page_count,
                MetadataBatchRecords::IndexDelta(records.to_vec()),
                Extensions::new(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    if index_pages.is_empty() {
        index_pages.push(
            MetadataBatchPage::new(
                MetadataBatchId::new("batch-index").unwrap(),
                scope.clone(),
                0,
                1,
                MetadataBatchRecords::IndexDelta(Vec::new()),
                Extensions::new(),
            )
            .unwrap(),
        );
    }
    let manifest_page = MetadataBatchPage::new(
        MetadataBatchId::new("batch-manifest").unwrap(),
        scope.clone(),
        0,
        1,
        MetadataBatchRecords::Manifest(Vec::new()),
        Extensions::new(),
    )
    .unwrap();
    let receipt_page = MetadataBatchPage::new(
        MetadataBatchId::new("batch-receipt").unwrap(),
        scope.clone(),
        0,
        1,
        MetadataBatchRecords::ObjectReceipt(Vec::new()),
        Extensions::new(),
    )
    .unwrap();
    let descriptors = vec![
        MetadataBatchDescriptor::from_pages(
            manifest_page.batch_id.clone(),
            scope.clone(),
            std::slice::from_ref(&manifest_page),
            Extensions::new(),
        )
        .unwrap(),
        MetadataBatchDescriptor::from_pages(
            index_pages[0].batch_id.clone(),
            scope.clone(),
            &index_pages,
            Extensions::new(),
        )
        .unwrap(),
        MetadataBatchDescriptor::from_pages(
            receipt_page.batch_id.clone(),
            scope,
            std::slice::from_ref(&receipt_page),
            Extensions::new(),
        )
        .unwrap(),
    ];
    let mut pages = vec![manifest_page];
    pages.extend(index_pages);
    pages.push(receipt_page);
    TransferReceipt::new(descriptors, pages)
}

fn publish_decision(assignment: &AddAssignment) -> JobDecision {
    JobDecision {
        job_id: assignment.job_id.clone(),
        assignment_id: assignment.assignment_id.clone(),
        assignment_generation: assignment.assignment_generation,
        decision_generation: DecisionGeneration::new(1),
        decision: PublishDecision::Publish {
            published_index_version: WireIndexVersion {
                revision: IndexRevision::new(8),
                digest: ContentDigest::from_bytes([41; 32]),
                extensions: Extensions::new(),
            },
            extensions: Extensions::new(),
        },
        final_state: JobState::Succeeded,
        extensions: Extensions::new(),
    }
}

fn reject_decision(assignment: &AddAssignment) -> JobDecision {
    reject_decision_with_state(assignment, JobState::Failed)
}

fn reject_decision_with_state(assignment: &AddAssignment, final_state: JobState) -> JobDecision {
    JobDecision {
        job_id: assignment.job_id.clone(),
        assignment_id: assignment.assignment_id.clone(),
        assignment_generation: assignment.assignment_generation,
        decision_generation: DecisionGeneration::new(1),
        decision: PublishDecision::Reject {
            error: ControlError {
                code: ErrorCode::new("EXECUTION_FAILED").unwrap(),
                message: "execution failed".to_owned(),
                retryable: false,
                retry_after_ms: None,
                extensions: Extensions::new(),
            },
            extensions: Extensions::new(),
        },
        final_state,
        extensions: Extensions::new(),
    }
}

#[derive(Debug)]
struct FailCompletedOnceLedger {
    inner: InMemoryLedger,
    fail_completed: AtomicBool,
}

#[derive(Debug)]
struct FailAwaitingDecisionOnceLedger {
    inner: InMemoryLedger,
    fail_awaiting_decision: AtomicBool,
}

impl FailAwaitingDecisionOnceLedger {
    fn new() -> Self {
        Self {
            inner: InMemoryLedger::new(),
            fail_awaiting_decision: AtomicBool::new(true),
        }
    }
}

impl Ledger for FailAwaitingDecisionOnceLedger {
    fn claim(&self, claim: LedgerClaim) -> neoengram_agent::AgentResult<ClaimOutcome> {
        self.inner.claim(claim)
    }

    fn load(&self, key: &AssignmentKey) -> neoengram_agent::AgentResult<Option<LedgerRecord>> {
        self.inner.load(key)
    }

    fn list_active(&self) -> neoengram_agent::AgentResult<Vec<LedgerRecord>> {
        self.inner.list_active()
    }

    fn compare_exchange(
        &self,
        expected_revision: u64,
        next: LedgerRecord,
    ) -> neoengram_agent::AgentResult<LedgerRecord> {
        if next.state == AgentAssignmentState::AwaitingDecision
            && self.fail_awaiting_decision.swap(false, Ordering::SeqCst)
        {
            return Err(AgentError::new(
                AgentErrorCode::Internal,
                "injected AwaitingDecision persistence failure",
            ));
        }
        self.inner.compare_exchange(expected_revision, next)
    }
}

impl FailCompletedOnceLedger {
    fn new() -> Self {
        Self {
            inner: InMemoryLedger::new(),
            fail_completed: AtomicBool::new(true),
        }
    }
}

impl Ledger for FailCompletedOnceLedger {
    fn claim(&self, claim: LedgerClaim) -> neoengram_agent::AgentResult<ClaimOutcome> {
        self.inner.claim(claim)
    }

    fn load(&self, key: &AssignmentKey) -> neoengram_agent::AgentResult<Option<LedgerRecord>> {
        self.inner.load(key)
    }

    fn list_active(&self) -> neoengram_agent::AgentResult<Vec<LedgerRecord>> {
        self.inner.list_active()
    }

    fn compare_exchange(
        &self,
        expected_revision: u64,
        next: LedgerRecord,
    ) -> neoengram_agent::AgentResult<LedgerRecord> {
        if next.state == AgentAssignmentState::Completed
            && self.fail_completed.swap(false, Ordering::SeqCst)
        {
            return Err(AgentError::new(
                AgentErrorCode::Internal,
                "injected Completed persistence failure",
            ));
        }
        self.inner.compare_exchange(expected_revision, next)
    }
}

#[derive(Debug)]
struct RecordingLedger {
    inner: InMemoryLedger,
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl RecordingLedger {
    fn new(events: Arc<Mutex<Vec<&'static str>>>) -> Self {
        Self {
            inner: InMemoryLedger::new(),
            events,
        }
    }
}

impl Ledger for RecordingLedger {
    fn claim(&self, claim: LedgerClaim) -> neoengram_agent::AgentResult<ClaimOutcome> {
        let outcome = self.inner.claim(claim)?;
        self.events.lock().unwrap().push("claimed");
        Ok(outcome)
    }

    fn load(&self, key: &AssignmentKey) -> neoengram_agent::AgentResult<Option<LedgerRecord>> {
        self.inner.load(key)
    }

    fn list_active(&self) -> neoengram_agent::AgentResult<Vec<LedgerRecord>> {
        self.inner.list_active()
    }

    fn compare_exchange(
        &self,
        expected_revision: u64,
        next: LedgerRecord,
    ) -> neoengram_agent::AgentResult<LedgerRecord> {
        let event = match next.state {
            AgentAssignmentState::Accepted => Some("accepted_persisted"),
            AgentAssignmentState::Running => Some("running_persisted"),
            AgentAssignmentState::Prepared => Some("prepared_persisted"),
            AgentAssignmentState::AwaitingDecision => Some("awaiting_decision_persisted"),
            AgentAssignmentState::Finalizing => Some("finalizing_persisted"),
            AgentAssignmentState::Completed => Some("completed_persisted"),
            AgentAssignmentState::FailedReported => Some("failed_reported_persisted"),
            AgentAssignmentState::Recovering => Some("recovering_persisted"),
            AgentAssignmentState::Claimed => None,
        };
        let updated = self.inner.compare_exchange(expected_revision, next)?;
        if let Some(event) = event {
            self.events.lock().unwrap().push(event);
        }
        Ok(updated)
    }
}

#[derive(Debug)]
struct RecordingReportSink {
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl RecordingReportSink {
    fn new(events: Arc<Mutex<Vec<&'static str>>>) -> Self {
        Self { events }
    }
}

impl ReportSink for RecordingReportSink {
    fn send(&self, report: AgentReport) -> neoengram_agent::AgentResult<()> {
        let event = match report {
            AgentReport::Accepted(_) => Some("accepted_reported"),
            AgentReport::Prepared(_) => Some("prepared_reported"),
            AgentReport::Finalized(_) => Some("finalized_reported"),
            AgentReport::Failed(_) => Some("failure_reported"),
            AgentReport::Progress(_) => None,
        };
        if let Some(event) = event {
            self.events.lock().unwrap().push(event);
        }
        Ok(())
    }
}
