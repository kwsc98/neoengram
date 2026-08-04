use std::{
    collections::VecDeque,
    fmt,
    sync::{Arc, Mutex},
};

use futures::executor::block_on;
use neoengram_agent::{
    Agent, AgentAssignmentState, AgentError, AgentErrorCode, AgentReport as LocalReport,
    AgentResult, AssignmentKey, BasicAssignmentValidator, ClaimDisposition, FakeAddExecutor,
    FakeClock, FakeObjectTransfer, InMemoryLedger, Ledger, ObjectTransfer, ReportSink,
    TransferReceipt,
};
use neoengram_core::{
    ChunkRef, ChunkingStrategy, ContentDigest, FileRecord, IndexDelta, IndexMutation, IndexVersion,
    LogicalPath, Manifest, ObjectId, ObjectSpec,
};
use neoengram_engine::{AddStatistics, ManagedResource, PreparedAdd};
use neoengram_protocol::{
    AgentId, AgentMountId, ArtifactId, ArtifactPlacementId, AssignmentGeneration, AssignmentId,
    AssignmentOperation, ControlEnvelope, ControlMessage, DecimalU64, DurabilityState,
    EdgeClusterId, Extensions, IndexDeltaRecord, JobState, ManifestRecord, MessageId,
    MetadataBatchDescriptor, MetadataBatchId, MetadataBatchPage, MetadataBatchRecords,
    MetadataBatchScope, MountGeneration, ObjectDurabilityReceipt, ObjectReceiptId,
    ObjectReceiptRecord, OwnerGeneration, PlacementGeneration, PlaygroundId, PrincipalId,
    PrincipalKind, PrincipalRef, ProjectId, SessionGeneration, SessionId, StorageVolumeId,
    TenantId, UnixMillis, WireChunkRef, WireChunkingStrategy, WireObjectSpec, PROTOCOL_VERSION_V1,
};
use neoengramd::{
    AddJobSpec, AgentReport as CentralReport, AssignJobRequest, AssignmentTarget, ControlPlane,
    CreateAddJobRequest, FinalizeAddRequest, InMemoryComponents, InMemoryObjectCatalog, IndexKey,
    IndexPublisher, MetadataBatchSubmission, ObjectCatalog, ReceiveReportRequest,
    ReceiveReportResult, StageMetadataBatchRequest, StageMetadataBatchResult,
};

const NOW: u64 = 1_000;

#[tokio::test]
async fn agent_and_control_plane_complete_managed_add_with_replayed_boundaries() {
    let central = InMemoryComponents::new(NOW);
    let control = Arc::new(central.control_plane());
    let actor = principal();
    let tenant_id = TenantId::new("tenant-e2e").unwrap();
    let project_id = ProjectId::new("project-e2e").unwrap();
    let artifact_id = ArtifactId::new("artifact-e2e").unwrap();
    let playground_id = PlaygroundId::new("playground-e2e").unwrap();
    let index_key = IndexKey {
        tenant_id: tenant_id.clone(),
        project_id: project_id.clone(),
        artifact_id: artifact_id.clone(),
        playground_id: playground_id.clone(),
    };
    let expected = central.publisher.current_version(&index_key).await.unwrap();
    let mut spec = AddJobSpec {
        job_id: neoengram_protocol::JobId::new("job-e2e").unwrap(),
        principal: actor.clone(),
        tenant_id: tenant_id.clone(),
        project_id,
        artifact_id: artifact_id.clone(),
        playground_id: playground_id.clone(),
        expected_index_version: expected.clone(),
        request_digest: ContentDigest::from_bytes([0; 32]),
        deadline_unix_ms: UnixMillis::new(NOW + 100_000),
        paths: vec![LogicalPath::parse("dataset/value.bin").unwrap()],
        all: false,
        extensions: Extensions::new(),
    };
    spec.request_digest = spec.computed_request_digest().unwrap();
    control
        .create_add_job(CreateAddJobRequest {
            actor: actor.clone(),
            spec: spec.clone(),
        })
        .await
        .unwrap();
    let target = assignment_target();
    let assigned = control
        .assign_job(AssignJobRequest {
            actor: actor.clone(),
            tenant_id: tenant_id.clone(),
            job_id: spec.job_id.clone(),
            target: target.clone(),
        })
        .await
        .unwrap();
    let AssignmentOperation::Add {
        input: assignment, ..
    } = assigned.assignment.assignment;

    let metadata = metadata_for(&assignment);
    let ledger = Arc::new(InMemoryLedger::new());
    let executor = Arc::new(FakeAddExecutor::new(metadata.prepared.clone()));
    let receipt = TransferReceipt::new(metadata.descriptors.clone(), metadata.pages.clone());
    let reports = Arc::new(CentralReportSink::new(
        control.clone(),
        tenant_id.clone(),
        target.agent_id.clone(),
    ));
    let transfer = Arc::new(CentralObjectTransfer::new(
        control.clone(),
        central.objects.clone(),
        receipt,
        // The first staging attempt commits two descriptors and then loses its response.
        [Some(2), None],
    ));
    let agent = Agent::new(
        ledger.clone(),
        Arc::new(BasicAssignmentValidator),
        executor.clone(),
        transfer.clone(),
        reports.clone(),
        Arc::new(FakeClock::new(NOW)),
    );

    let accepted = agent.accept_assignment(assignment.clone()).unwrap();
    assert_eq!(accepted.disposition, ClaimDisposition::New);
    let accepted_replay = agent.accept_assignment(assignment.clone()).unwrap();
    assert_eq!(accepted_replay.disposition, ClaimDisposition::Replayed);
    assert_eq!(ledger.records().unwrap().len(), 1);
    let accepted_results = reports.results().unwrap();
    assert!(!accepted_results[0].replayed);
    assert!(accepted_results[1].replayed);

    let key = AssignmentKey::from_assignment(&assignment);
    let staging_error = agent.execute(&key).unwrap_err();
    assert_eq!(staging_error.code(), AgentErrorCode::ObjectTransferFailed);
    let prepared = agent.assignment(&key).unwrap().unwrap();
    assert_eq!(prepared.state, AgentAssignmentState::Prepared);
    assert_eq!(
        prepared.prepared.as_ref().unwrap().transfer.metadata_pages,
        metadata.pages
    );
    assert_eq!(
        prepared
            .prepared
            .as_ref()
            .unwrap()
            .transfer
            .metadata_batches,
        metadata.descriptors
    );
    assert_eq!(central.jobs.all().unwrap()[0].state, JobState::Prepared);
    let partial_batches = central.metadata.batches().unwrap();
    assert_eq!(partial_batches.len(), 2);
    assert!(partial_batches.iter().all(|batch| !batch.is_complete()));

    // Recovery repeats JobPrepared and every submission from durable material. The two already
    // staged descriptors are accepted as exact idempotent replays.
    let awaiting = agent.recover(&key).unwrap();
    assert_eq!(awaiting.state, AgentAssignmentState::AwaitingDecision);
    assert_eq!(transfer.staging_attempts().unwrap(), 2);
    assert_eq!(executor.calls().unwrap().len(), 1);
    assert_eq!(transfer.transfers().unwrap().len(), 1);
    let staging_results = transfer.staging_results().unwrap();
    assert_eq!(
        staging_results.len(),
        metadata.descriptors.len() + metadata.pages.len() + 2
    );
    assert_eq!(
        staging_results
            .iter()
            .filter(|result| result.replayed)
            .count(),
        2
    );
    assert!(central
        .metadata
        .batches()
        .unwrap()
        .iter()
        .all(neoengramd::StagedMetadataBatch::is_complete));

    let published = control
        .finalize_add(FinalizeAddRequest {
            actor,
            tenant_id: tenant_id.clone(),
            job_id: assignment.job_id.clone(),
        })
        .await
        .unwrap();
    assert_eq!(published.job.state, JobState::Succeeded);

    let completed = agent
        .handle_decision(&tenant_id, published.decision.clone())
        .unwrap();
    assert_eq!(completed.state, AgentAssignmentState::Completed);
    let duplicate_decision = agent
        .handle_decision(&tenant_id, published.decision.clone())
        .unwrap();
    assert_eq!(duplicate_decision.revision, completed.revision);
    assert_eq!(executor.calls().unwrap().len(), 1);
    assert_eq!(transfer.transfers().unwrap().len(), 1);
    assert_eq!(transfer.finalizations().unwrap().len(), 1);

    assert_eq!(central.jobs.all().unwrap()[0].state, JobState::Succeeded);

    let snapshot = central.publisher.snapshot(&index_key).unwrap();
    assert_eq!(snapshot.version.revision.get(), 1);
    assert_eq!(snapshot.records.len(), 1);
    assert_eq!(snapshot.records[0].path.as_str(), "dataset/value.bin");
}

#[tokio::test]
async fn agent_failure_round_trips_over_wire_into_the_control_plane() {
    let central = InMemoryComponents::new(NOW);
    let control = Arc::new(central.control_plane());
    let actor = principal();
    let tenant_id = TenantId::new("tenant-failure-e2e").unwrap();
    let project_id = ProjectId::new("project-failure-e2e").unwrap();
    let artifact_id = ArtifactId::new("artifact-failure-e2e").unwrap();
    let playground_id = PlaygroundId::new("playground-failure-e2e").unwrap();
    let expected = central
        .publisher
        .current_version(&IndexKey {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: playground_id.clone(),
        })
        .await
        .unwrap();
    let mut spec = AddJobSpec {
        job_id: neoengram_protocol::JobId::new("job-failure-e2e").unwrap(),
        principal: actor.clone(),
        tenant_id: tenant_id.clone(),
        project_id,
        artifact_id,
        playground_id,
        expected_index_version: expected,
        request_digest: ContentDigest::from_bytes([0; 32]),
        deadline_unix_ms: UnixMillis::new(NOW + 100_000),
        paths: vec![LogicalPath::parse("dataset/failure.bin").unwrap()],
        all: false,
        extensions: Extensions::new(),
    };
    spec.request_digest = spec.computed_request_digest().unwrap();
    control
        .create_add_job(CreateAddJobRequest {
            actor: actor.clone(),
            spec: spec.clone(),
        })
        .await
        .unwrap();
    let target = assignment_target();
    let assigned = control
        .assign_job(AssignJobRequest {
            actor,
            tenant_id: tenant_id.clone(),
            job_id: spec.job_id.clone(),
            target: target.clone(),
        })
        .await
        .unwrap();
    let AssignmentOperation::Add {
        input: assignment, ..
    } = assigned.assignment.assignment;

    let ledger = Arc::new(InMemoryLedger::new());
    let reports = Arc::new(CentralReportSink::new(control, tenant_id, target.agent_id));
    let agent = Agent::new(
        ledger.clone(),
        Arc::new(BasicAssignmentValidator),
        Arc::new(FakeAddExecutor::with_results([Err(AgentError::new(
            AgentErrorCode::ExecutionFailed,
            "injected wire failure",
        ))])),
        Arc::new(FakeObjectTransfer::new(TransferReceipt::default())),
        reports,
        Arc::new(FakeClock::new(NOW)),
    );
    let key = AssignmentKey::from_assignment(&assignment);
    agent.accept_assignment(assignment).unwrap();

    let error = agent.execute(&key).unwrap_err();
    assert_eq!(error.code(), AgentErrorCode::ExecutionFailed);

    let local_failure = ledger
        .load(&key)
        .unwrap()
        .unwrap()
        .failure
        .expect("Agent ledger persists the exact wire failure");
    let central_job = central.jobs.all().unwrap().pop().unwrap();
    assert_eq!(central_job.state, JobState::Failed);
    assert_eq!(central_job.failure.as_ref(), Some(&local_failure));
    assert_eq!(local_failure.tenant_id, key.tenant_id);
    assert_eq!(local_failure.final_state, JobState::Failed);
}

struct CentralReportSink {
    control: Arc<ControlPlane>,
    tenant_id: TenantId,
    agent_id: AgentId,
    results: Mutex<Vec<ReceiveReportResult>>,
}

impl CentralReportSink {
    fn new(control: Arc<ControlPlane>, tenant_id: TenantId, agent_id: AgentId) -> Self {
        Self {
            control,
            tenant_id,
            agent_id,
            results: Mutex::new(Vec::new()),
        }
    }

    fn results(&self) -> AgentResult<Vec<ReceiveReportResult>> {
        self.results
            .lock()
            .map(|results| results.clone())
            .map_err(|_| agent_internal("central report result lock poisoned"))
    }
}

impl fmt::Debug for CentralReportSink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CentralReportSink")
            .finish_non_exhaustive()
    }
}

impl ReportSink for CentralReportSink {
    fn send(&self, report: LocalReport) -> AgentResult<()> {
        let envelope = ControlEnvelope {
            protocol_version: PROTOCOL_VERSION_V1,
            message_id: MessageId::new("message-agent-e2e").map_err(protocol_report_error)?,
            session_generation: SessionGeneration::new(1),
            resource_version: None,
            request_id: None,
            trace_id: None,
            sent_at_unix_ms: UnixMillis::new(NOW),
            message: report.into_control_message(),
            extensions: Extensions::new(),
        };
        let encoded = serde_json::to_vec(&envelope)
            .map_err(|error| AgentError::new(AgentErrorCode::ReportFailed, error.to_string()))?;
        let decoded = ControlEnvelope::decode_json(&encoded).map_err(protocol_report_error)?;
        let report = match decoded.message {
            ControlMessage::Accepted(report) => CentralReport::Accepted(report),
            ControlMessage::Progress(report) => CentralReport::Progress(report),
            ControlMessage::Prepared(report) => CentralReport::Prepared(report),
            ControlMessage::Finalized(report) => CentralReport::Finalized(report),
            ControlMessage::Failed(report) => CentralReport::Failed(report),
            message => {
                return Err(AgentError::new(
                    AgentErrorCode::ReportFailed,
                    format!("unexpected outbound Agent message: {message:?}"),
                ));
            }
        };
        let result = block_on(self.control.receive_report(ReceiveReportRequest {
            tenant_id: self.tenant_id.clone(),
            agent_id: self.agent_id.clone(),
            report,
        }))
        .map_err(|error| AgentError::new(AgentErrorCode::ReportFailed, error.to_string()))?;
        self.results
            .lock()
            .map_err(|_| agent_internal("central report result lock poisoned"))?
            .push(result);
        Ok(())
    }
}

struct CentralObjectTransfer {
    control: Arc<ControlPlane>,
    objects: Arc<InMemoryObjectCatalog>,
    inner: FakeObjectTransfer,
    fail_after: Mutex<VecDeque<Option<usize>>>,
    staging_attempts: Mutex<usize>,
    staging_results: Mutex<Vec<StageMetadataBatchResult>>,
}

impl CentralObjectTransfer {
    fn new(
        control: Arc<ControlPlane>,
        objects: Arc<InMemoryObjectCatalog>,
        receipt: TransferReceipt,
        fail_after: impl IntoIterator<Item = Option<usize>>,
    ) -> Self {
        Self {
            control,
            objects,
            inner: FakeObjectTransfer::new(receipt),
            fail_after: Mutex::new(fail_after.into_iter().collect()),
            staging_attempts: Mutex::new(0),
            staging_results: Mutex::new(Vec::new()),
        }
    }

    fn transfers(&self) -> AgentResult<Vec<AssignmentKey>> {
        self.inner.transfers()
    }

    fn finalizations(&self) -> AgentResult<Vec<(AssignmentKey, neoengram_protocol::JobDecision)>> {
        self.inner.finalizations()
    }

    fn staging_attempts(&self) -> AgentResult<usize> {
        self.staging_attempts
            .lock()
            .map(|attempts| *attempts)
            .map_err(|_| agent_internal("staging attempt lock poisoned"))
    }

    fn staging_results(&self) -> AgentResult<Vec<StageMetadataBatchResult>> {
        self.staging_results
            .lock()
            .map(|results| results.clone())
            .map_err(|_| agent_internal("staging result lock poisoned"))
    }
}

impl fmt::Debug for CentralObjectTransfer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CentralObjectTransfer")
            .finish_non_exhaustive()
    }
}

impl ObjectTransfer for CentralObjectTransfer {
    fn transfer(
        &self,
        assignment: &neoengram_protocol::AddAssignment,
        prepared: &PreparedAdd,
    ) -> AgentResult<TransferReceipt> {
        let receipt = self.inner.transfer(assignment, prepared)?;
        for object in &prepared.object_specs {
            block_on(self.objects.record_durability(&ObjectDurabilityReceipt {
                tenant_id: assignment.tenant_id.clone(),
                artifact_id: assignment.artifact_id.clone(),
                session_id: SessionId::new("session-e2e").unwrap(),
                job_id: assignment.job_id.clone(),
                object: WireObjectSpec {
                    object_id: object.id,
                    size: DecimalU64::new(object.size),
                    extensions: Extensions::new(),
                },
                state: DurabilityState::Durable {
                    verified_digest: object.id.digest(),
                    storage_version: "s3-e2e-version".to_owned(),
                    extensions: Extensions::new(),
                },
                checked_at_unix_ms: UnixMillis::new(NOW + 10),
                extensions: Extensions::new(),
            }))
            .map_err(transfer_error)?;
        }
        Ok(receipt)
    }

    fn stage_metadata(
        &self,
        assignment: &neoengram_protocol::AddAssignment,
        receipt: &TransferReceipt,
    ) -> AgentResult<()> {
        receipt.validate(assignment)?;
        *self
            .staging_attempts
            .lock()
            .map_err(|_| agent_internal("staging attempt lock poisoned"))? += 1;
        let fail_after = self
            .fail_after
            .lock()
            .map_err(|_| agent_internal("staging failure script lock poisoned"))?
            .pop_front()
            .flatten();
        let submissions = receipt
            .metadata_batches
            .iter()
            .cloned()
            .map(MetadataBatchSubmission::Descriptor)
            .chain(
                receipt
                    .metadata_pages
                    .iter()
                    .cloned()
                    .map(MetadataBatchSubmission::Page),
            );
        for (index, submission) in submissions.enumerate() {
            let result = block_on(
                self.control
                    .stage_metadata_batch(StageMetadataBatchRequest {
                        tenant_id: assignment.tenant_id.clone(),
                        job_id: assignment.job_id.clone(),
                        agent_id: assignment.agent_id.clone(),
                        submission,
                    }),
            )
            .map_err(transfer_error)?;
            self.staging_results
                .lock()
                .map_err(|_| agent_internal("staging result lock poisoned"))?
                .push(result);
            if fail_after == Some(index + 1) {
                return Err(AgentError::new(
                    AgentErrorCode::ObjectTransferFailed,
                    "injected response loss after partial metadata staging",
                ));
            }
        }
        Ok(())
    }

    fn finalize(
        &self,
        assignment: &neoengram_protocol::AddAssignment,
        prepared: &PreparedAdd,
        decision: &neoengram_protocol::JobDecision,
    ) -> AgentResult<()> {
        self.inner.finalize(assignment, prepared, decision)
    }
}

fn transfer_error(error: neoengramd::CentralError) -> AgentError {
    AgentError::new(AgentErrorCode::ObjectTransferFailed, error.to_string())
}

fn protocol_report_error(error: neoengram_protocol::ProtocolError) -> AgentError {
    AgentError::new(AgentErrorCode::ReportFailed, error.to_string())
}

fn agent_internal(message: &'static str) -> AgentError {
    AgentError::new(AgentErrorCode::Internal, message)
}

struct TestMetadata {
    prepared: PreparedAdd,
    descriptors: Vec<MetadataBatchDescriptor>,
    pages: Vec<MetadataBatchPage>,
}

fn metadata_for(assignment: &neoengram_protocol::AddAssignment) -> TestMetadata {
    let payload = b"cross-component-payload";
    let object_id = ObjectId::for_bytes(payload);
    let object_size = payload.len() as u64;
    let chunk = ChunkRef::new(object_id, 0, object_size).unwrap();
    let manifest = Manifest::new(object_size, ChunkingStrategy::WholeFile, vec![chunk]).unwrap();
    let manifest_id = manifest.canonical_id().unwrap();
    let path = LogicalPath::parse("dataset/value.bin").unwrap();
    let file = FileRecord::from_manifest(path.clone(), &manifest).unwrap();
    let base = IndexVersion::new(
        assignment.expected_index_version.revision.get(),
        assignment.expected_index_version.digest,
    );
    let result_digest = IndexVersion::from_snapshot(1, std::slice::from_ref(&file))
        .unwrap()
        .digest;
    let index_delta = IndexDelta::new(
        base,
        vec![IndexMutation::Upsert {
            record: file.clone(),
        }],
        result_digest,
    )
    .unwrap();
    let prepared = PreparedAdd::new(
        ManagedResource {
            tenant_id: assignment.tenant_id.to_string(),
            project_id: assignment.project_id.to_string(),
            artifact_id: assignment.artifact_id.to_string(),
            playground_id: assignment.playground_id.to_string(),
            job_id: assignment.job_id.to_string(),
        },
        base,
        "candidate-e2e",
        index_delta,
        vec![manifest],
        vec![ObjectSpec::new(object_id, object_size)],
        AddStatistics {
            files_scanned: 1,
            files_upserted: 1,
            bytes_scanned: object_size,
            chunks_observed: 1,
            objects_created: 1,
            bytes_published: object_size,
            ..AddStatistics::default()
        },
        NOW + 1,
    )
    .unwrap();
    let scope = MetadataBatchScope {
        tenant_id: assignment.tenant_id.clone(),
        project_id: assignment.project_id.clone(),
        artifact_id: assignment.artifact_id.clone(),
        playground_id: assignment.playground_id.clone(),
        job_id: assignment.job_id.clone(),
        base_index_version: assignment.expected_index_version.clone(),
        extensions: Extensions::new(),
    };
    let manifest_page = MetadataBatchPage::new(
        MetadataBatchId::new("manifest-e2e").unwrap(),
        scope.clone(),
        0,
        1,
        MetadataBatchRecords::Manifest(vec![ManifestRecord {
            manifest_id,
            total_size: DecimalU64::new(object_size),
            chunking: WireChunkingStrategy::WholeFile,
            chunk_start: DecimalU64::new(0),
            chunks: vec![WireChunkRef {
                object_id,
                offset: DecimalU64::new(0),
                size: DecimalU64::new(object_size),
                extensions: Extensions::new(),
            }],
            extensions: Extensions::new(),
        }]),
        Extensions::new(),
    )
    .unwrap();
    let index_page = MetadataBatchPage::new(
        MetadataBatchId::new("index-e2e").unwrap(),
        scope.clone(),
        0,
        1,
        MetadataBatchRecords::IndexDelta(vec![IndexDeltaRecord::Upsert {
            path,
            manifest_id,
            total_size: DecimalU64::new(object_size),
            chunk_count: DecimalU64::new(1),
            extensions: Extensions::new(),
        }]),
        Extensions::new(),
    )
    .unwrap();
    let receipt_page = MetadataBatchPage::new(
        MetadataBatchId::new("receipt-e2e").unwrap(),
        scope.clone(),
        0,
        1,
        MetadataBatchRecords::ObjectReceipt(vec![ObjectReceiptRecord {
            receipt_id: ObjectReceiptId::new("receipt-e2e").unwrap(),
            tenant_id: assignment.tenant_id.clone(),
            artifact_id: assignment.artifact_id.clone(),
            job_id: assignment.job_id.clone(),
            storage_volume_id: assignment.storage_volume_id.clone(),
            artifact_placement_id: assignment.artifact_placement_id.clone(),
            object_id,
            size: DecimalU64::new(object_size),
            verified_at_unix_ms: UnixMillis::new(NOW + 2),
            extensions: Extensions::new(),
        }]),
        Extensions::new(),
    )
    .unwrap();
    let pages = vec![manifest_page, index_page, receipt_page];
    let descriptors = pages
        .iter()
        .map(|page| {
            MetadataBatchDescriptor::from_pages(
                page.batch_id.clone(),
                scope.clone(),
                std::slice::from_ref(page),
                Extensions::new(),
            )
            .unwrap()
        })
        .collect();
    TestMetadata {
        prepared,
        descriptors,
        pages,
    }
}

fn principal() -> PrincipalRef {
    PrincipalRef {
        kind: PrincipalKind::Service,
        id: PrincipalId::new("service-e2e").unwrap(),
        extensions: Extensions::new(),
    }
}

fn assignment_target() -> AssignmentTarget {
    AssignmentTarget {
        assignment_id: AssignmentId::new("assignment-e2e").unwrap(),
        assignment_generation: AssignmentGeneration::new(1),
        agent_id: AgentId::new("agent-e2e").unwrap(),
        edge_cluster_id: EdgeClusterId::new("cluster-e2e").unwrap(),
        storage_volume_id: StorageVolumeId::new("volume-e2e").unwrap(),
        artifact_placement_id: ArtifactPlacementId::new("placement-e2e").unwrap(),
        placement_generation: PlacementGeneration::new(1),
        agent_mount_id: AgentMountId::new("mount-e2e").unwrap(),
        mount_generation: MountGeneration::new(1),
        owner_generation: OwnerGeneration::new(1),
        lease: None,
    }
}
