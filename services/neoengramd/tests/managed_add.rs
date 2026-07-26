use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Barrier,
};

use neoengram_core::{
    ChunkRef, ChunkingStrategy, ContentDigest, FileRecord, IndexVersion, LogicalPath, Manifest,
    ObjectId,
};
use neoengram_protocol::{
    AgentId, AgentMountId, ArtifactId, ArtifactPlacementId, AssignmentGeneration, AssignmentId,
    ControlError, DecimalU64, DurabilityState, EdgeClusterId, ErrorCode, Extensions, FencingToken,
    IndexDeltaRecord, JobAccepted, JobFailed, JobFailureStage, JobPrepared, JobProgress, JobState,
    LeaseGrant, LeaseId, LeaseMode, ManifestRecord, MetadataBatchDescriptor, MetadataBatchId,
    MetadataBatchKind, MetadataBatchPage, MetadataBatchRecords, MetadataBatchScope,
    MetadataPublication, MountGeneration, ObjectDurabilityReceipt, ObjectReceiptId,
    ObjectReceiptRecord, OwnerGeneration, PlacementGeneration, PlaygroundId, PrincipalId,
    PrincipalKind, PrincipalRef, ProjectId, PublishDecision, SessionId, StorageVolumeId, TenantId,
    UnixMillis, WireChunkRef, WireChunkingStrategy, WireIndexVersion, WireObjectSpec,
};
use neoengramd::{
    Action, Actor, AddJobSpec, AgentReport, AssignJobRequest, AssignmentOutbox,
    AssignmentPublishOutcome, AssignmentReserveOutcome, AssignmentTarget, AuthorizationRequest,
    Authorizer, CentralError, CentralErrorCode, CentralResult, ControlPlane, CreateAddJobRequest,
    ExpireAddJobRequest, FinalizeAddRequest, InMemoryAssignmentOutbox, InMemoryComponents,
    InMemoryJobRepository, IndexKey, IndexPublishOutcome, IndexPublishRequest, IndexPublisher,
    JobInsertOutcome, JobKey, JobRecord, JobRepository, MetadataBatchStager,
    MetadataBatchSubmission, ObjectCatalog, ReceiveReportRequest, ResumePublicationRequest,
    StageMetadataBatchRequest,
};

struct Scenario {
    components: InMemoryComponents,
    control: ControlPlane,
    actor: PrincipalRef,
    spec: AddJobSpec,
    target: AssignmentTarget,
    accepted: JobAccepted,
    progress: JobProgress,
    prepared: JobPrepared,
    descriptors: Vec<MetadataBatchDescriptor>,
    pages: Vec<MetadataBatchPage>,
    object_id: ObjectId,
    object_size: u64,
}

impl Scenario {
    fn stage_all(&self) {
        for descriptor in &self.descriptors {
            let first = self
                .control
                .stage_metadata_batch(StageMetadataBatchRequest {
                    tenant_id: self.spec.tenant_id.clone(),
                    job_id: self.spec.job_id.clone(),
                    agent_id: self.target.agent_id.clone(),
                    submission: MetadataBatchSubmission::Descriptor(descriptor.clone()),
                })
                .unwrap();
            assert!(!first.replayed);
            self.components.clock.advance(1).unwrap();
            let replay = self
                .control
                .stage_metadata_batch(StageMetadataBatchRequest {
                    tenant_id: self.spec.tenant_id.clone(),
                    job_id: self.spec.job_id.clone(),
                    agent_id: self.target.agent_id.clone(),
                    submission: MetadataBatchSubmission::Descriptor(descriptor.clone()),
                })
                .unwrap();
            assert!(replay.replayed);
        }
        for page in &self.pages {
            let first = self
                .control
                .stage_metadata_batch(StageMetadataBatchRequest {
                    tenant_id: self.spec.tenant_id.clone(),
                    job_id: self.spec.job_id.clone(),
                    agent_id: self.target.agent_id.clone(),
                    submission: MetadataBatchSubmission::Page(page.clone()),
                })
                .unwrap();
            assert!(!first.replayed);
            assert_eq!(first.complete, page.page_number + 1 == page.page_count);
            self.components.clock.advance(1).unwrap();
            let replay = self
                .control
                .stage_metadata_batch(StageMetadataBatchRequest {
                    tenant_id: self.spec.tenant_id.clone(),
                    job_id: self.spec.job_id.clone(),
                    agent_id: self.target.agent_id.clone(),
                    submission: MetadataBatchSubmission::Page(page.clone()),
                })
                .unwrap();
            assert!(replay.replayed);
        }
    }

    fn mark_object_durable(&self) {
        let objects = self
            .pages
            .iter()
            .filter_map(|page| match &page.records {
                MetadataBatchRecords::ObjectReceipt(records) => Some(records),
                MetadataBatchRecords::Manifest(_) | MetadataBatchRecords::IndexDelta(_) => None,
            })
            .flatten()
            .map(|record| (record.object_id, record.size.get()))
            .collect::<Vec<_>>();
        for (object_id, object_size) in objects {
            self.observe_object_spec(
                object_id,
                object_size,
                DurabilityState::Durable {
                    verified_digest: object_id.digest(),
                    storage_version: "s3-version-1".to_owned(),
                    extensions: Extensions::new(),
                },
            );
        }
    }

    fn observe_object(&self, state: DurabilityState) {
        self.observe_object_spec(self.object_id, self.object_size, state);
    }

    fn observe_object_spec(&self, object_id: ObjectId, object_size: u64, state: DurabilityState) {
        self.components
            .objects
            .record_durability(&ObjectDurabilityReceipt {
                tenant_id: self.spec.tenant_id.clone(),
                artifact_id: self.spec.artifact_id.clone(),
                session_id: SessionId::new("session-a").unwrap(),
                job_id: self.spec.job_id.clone(),
                object: WireObjectSpec {
                    object_id,
                    size: DecimalU64::new(object_size),
                    extensions: Extensions::new(),
                },
                state,
                checked_at_unix_ms: UnixMillis::new(400),
                extensions: Extensions::new(),
            })
            .unwrap();
    }

    fn finalize_request(&self) -> FinalizeAddRequest {
        FinalizeAddRequest {
            actor: self.actor.clone(),
            tenant_id: self.spec.tenant_id.clone(),
            job_id: self.spec.job_id.clone(),
        }
    }

    fn index_key(&self) -> IndexKey {
        IndexKey {
            tenant_id: self.spec.tenant_id.clone(),
            artifact_id: self.spec.artifact_id.clone(),
            playground_id: self.spec.playground_id.clone(),
        }
    }
}

#[test]
fn create_rejects_a_request_digest_that_does_not_bind_the_operation() {
    let components = InMemoryComponents::new(100);
    let control = components.control_plane();
    let (actor, mut spec, _) = inputs(&components);
    spec.paths = vec![LogicalPath::parse("dataset/tampered.bin").unwrap()];

    let error = control
        .create_add_job(CreateAddJobRequest { actor, spec })
        .unwrap_err();

    assert_eq!(error.code(), CentralErrorCode::MetadataInvalid);
    assert!(components.jobs.all().unwrap().is_empty());
}

#[test]
fn concurrent_identical_create_is_insert_once_and_replay_elsewhere() {
    const CALLERS: usize = 8;

    let components = InMemoryComponents::new(100);
    let jobs = Arc::new(CreateRaceRepository::new(components.jobs.clone(), CALLERS));
    let control = Arc::new(ControlPlane::new(
        components.authorizer.clone(),
        jobs,
        components.outbox.clone(),
        components.metadata.clone(),
        components.objects.clone(),
        components.publisher.clone(),
        components.audit.clone(),
        components.clock.clone(),
    ));
    let (actor, spec, _) = inputs(&components);

    let handles = (0..CALLERS)
        .map(|_| {
            let control = control.clone();
            let actor = actor.clone();
            let spec = spec.clone();
            std::thread::spawn(move || control.create_add_job(CreateAddJobRequest { actor, spec }))
        })
        .collect::<Vec<_>>();
    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(results.iter().filter(|result| !result.replayed).count(), 1);
    assert_eq!(
        results.iter().filter(|result| result.replayed).count(),
        CALLERS - 1
    );
    assert!(results.iter().all(|result| result.job.spec == spec));
    assert_eq!(components.jobs.all().unwrap().len(), 1);
}

#[test]
fn concurrent_create_with_another_spec_is_a_stable_job_id_conflict() {
    let components = InMemoryComponents::new(100);
    let jobs = Arc::new(CreateRaceRepository::new(components.jobs.clone(), 2));
    let control = Arc::new(ControlPlane::new(
        components.authorizer.clone(),
        jobs,
        components.outbox.clone(),
        components.metadata.clone(),
        components.objects.clone(),
        components.publisher.clone(),
        components.audit.clone(),
        components.clock.clone(),
    ));
    let (actor, first_spec, _) = inputs(&components);
    let mut second_spec = first_spec.clone();
    second_spec.paths = vec![LogicalPath::parse("dataset/other.bin").unwrap()];
    second_spec.request_digest = second_spec.computed_request_digest().unwrap();

    let handles = [first_spec.clone(), second_spec.clone()].map(|spec| {
        let control = control.clone();
        let actor = actor.clone();
        std::thread::spawn(move || control.create_add_job(CreateAddJobRequest { actor, spec }))
    });
    let results = handles.map(|handle| handle.join().unwrap());
    let winner = results
        .iter()
        .find_map(|result| result.as_ref().ok())
        .unwrap();
    let conflict = results
        .iter()
        .find_map(|result| result.as_ref().err())
        .unwrap();

    assert!(!winner.replayed);
    assert_eq!(conflict.code(), CentralErrorCode::JobIdReused);
    assert_eq!(components.jobs.all().unwrap(), vec![winner.job.clone()]);

    let losing_spec = if winner.job.spec == first_spec {
        second_spec
    } else {
        first_spec
    };
    let repeated = control
        .create_add_job(CreateAddJobRequest {
            actor,
            spec: losing_spec,
        })
        .unwrap_err();
    assert_eq!(repeated.code(), CentralErrorCode::JobIdReused);
}

#[test]
fn assignment_id_conflict_leaves_job_queued_and_a_new_id_recovers() {
    let components = InMemoryComponents::new(100);
    let control = components.control_plane();
    let (actor, first_spec, target) = inputs(&components);
    let mut second_spec = first_spec.clone();
    second_spec.job_id = neoengram_protocol::JobId::new("job-b").unwrap();
    second_spec.request_digest = second_spec.computed_request_digest().unwrap();

    for spec in [&first_spec, &second_spec] {
        control
            .create_add_job(CreateAddJobRequest {
                actor: actor.clone(),
                spec: spec.clone(),
            })
            .unwrap();
    }
    control
        .assign_job(AssignJobRequest {
            actor: actor.clone(),
            tenant_id: first_spec.tenant_id.clone(),
            job_id: first_spec.job_id.clone(),
            target: target.clone(),
        })
        .unwrap();

    let conflict = control
        .assign_job(AssignJobRequest {
            actor: actor.clone(),
            tenant_id: second_spec.tenant_id.clone(),
            job_id: second_spec.job_id.clone(),
            target: target.clone(),
        })
        .unwrap_err();
    assert_eq!(conflict.code(), CentralErrorCode::JobIdReused);
    let still_queued = components
        .jobs
        .get(&JobKey::new(
            second_spec.tenant_id.clone(),
            second_spec.job_id.clone(),
        ))
        .unwrap()
        .unwrap();
    assert_eq!(still_queued.state, JobState::Queued);
    assert_eq!(still_queued.resource_version.get(), 1);
    assert!(still_queued.assignment.is_none());

    let mut retry_target = target;
    retry_target.assignment_id = AssignmentId::new("assignment-b").unwrap();
    let recovered = control
        .assign_job(AssignJobRequest {
            actor,
            tenant_id: second_spec.tenant_id,
            job_id: second_spec.job_id,
            target: retry_target,
        })
        .unwrap();
    assert!(!recovered.replayed);
    assert_eq!(recovered.job.state, JobState::Assigned);
    assert_eq!(components.outbox.messages().unwrap().len(), 2);
}

#[test]
fn concurrent_identical_assignment_converges_to_one_delivery() {
    let components = InMemoryComponents::new(100);
    let (actor, spec, target) = inputs(&components);
    components
        .control_plane()
        .create_add_job(CreateAddJobRequest {
            actor: actor.clone(),
            spec: spec.clone(),
        })
        .unwrap();
    let jobs = Arc::new(AssignmentRaceRepository::new(components.jobs.clone(), 2));
    let control = Arc::new(ControlPlane::new(
        components.authorizer.clone(),
        jobs,
        components.outbox.clone(),
        components.metadata.clone(),
        components.objects.clone(),
        components.publisher.clone(),
        components.audit.clone(),
        components.clock.clone(),
    ));

    let handles = (0..2)
        .map(|_| {
            let control = control.clone();
            let request = AssignJobRequest {
                actor: actor.clone(),
                tenant_id: spec.tenant_id.clone(),
                job_id: spec.job_id.clone(),
                target: target.clone(),
            };
            std::thread::spawn(move || control.assign_job(request))
        })
        .collect::<Vec<_>>();
    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(results.iter().filter(|result| !result.replayed).count(), 1);
    assert_eq!(results.iter().filter(|result| result.replayed).count(), 1);
    assert!(results
        .iter()
        .all(|result| result.job.state == JobState::Assigned));
    assert_eq!(components.outbox.messages().unwrap().len(), 1);
}

#[test]
fn reserved_assignment_is_invisible_until_the_job_write_succeeds() {
    let components = InMemoryComponents::new(100);
    let jobs = Arc::new(FailAssignmentWriteOnce::new(components.jobs.clone()));
    let control = ControlPlane::new(
        components.authorizer.clone(),
        jobs,
        components.outbox.clone(),
        components.metadata.clone(),
        components.objects.clone(),
        components.publisher.clone(),
        components.audit.clone(),
        components.clock.clone(),
    );
    let (actor, spec, mut target) = inputs(&components);
    control
        .create_add_job(CreateAddJobRequest {
            actor: actor.clone(),
            spec: spec.clone(),
        })
        .unwrap();

    let error = control
        .assign_job(AssignJobRequest {
            actor: actor.clone(),
            tenant_id: spec.tenant_id.clone(),
            job_id: spec.job_id.clone(),
            target: target.clone(),
        })
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::StorageFailure);
    assert!(components.outbox.messages().unwrap().is_empty());
    let queued = components
        .jobs
        .get(&JobKey::new(spec.tenant_id.clone(), spec.job_id.clone()))
        .unwrap()
        .unwrap();
    assert_eq!(queued.state, JobState::Queued);
    assert!(queued.assignment.is_none());

    target.assignment_id = AssignmentId::new("assignment-after-retry").unwrap();
    let assigned = control
        .assign_job(AssignJobRequest {
            actor,
            tenant_id: spec.tenant_id,
            job_id: spec.job_id,
            target,
        })
        .unwrap();
    assert_eq!(assigned.job.state, JobState::Assigned);
    assert_eq!(
        components.outbox.messages().unwrap(),
        vec![assigned.assignment]
    );
}

#[test]
fn create_and_assign_preserve_digest_bound_operation_extensions() {
    let components = InMemoryComponents::new(100);
    let control = components.control_plane();
    let (actor, mut spec, target) = inputs(&components);
    spec.extensions
        .insert("future_add_mode".to_owned(), "strict".into());
    spec.request_digest = spec.computed_request_digest().unwrap();

    control
        .create_add_job(CreateAddJobRequest {
            actor: actor.clone(),
            spec: spec.clone(),
        })
        .unwrap();
    let assigned = control
        .assign_job(AssignJobRequest {
            actor,
            tenant_id: spec.tenant_id,
            job_id: spec.job_id,
            target,
        })
        .unwrap();
    let neoengram_protocol::AssignmentOperation::Add { input, .. } = assigned.assignment.assignment;

    assert_eq!(
        input
            .extensions
            .get("future_add_mode")
            .and_then(|value| value.as_str()),
        Some("strict")
    );
    input.validate().unwrap();
}

#[test]
fn metadata_staging_scopes_reused_batch_ids_by_tenant() {
    let stager = neoengramd::InMemoryMetadataBatchStager::default();
    let batch_id = MetadataBatchId::new("shared-batch-id").unwrap();
    for tenant in ["tenant-a", "tenant-b"] {
        let scope = MetadataBatchScope {
            tenant_id: TenantId::new(tenant).unwrap(),
            project_id: ProjectId::new("project-a").unwrap(),
            artifact_id: ArtifactId::new("artifact-a").unwrap(),
            playground_id: PlaygroundId::new("playground-a").unwrap(),
            job_id: neoengram_protocol::JobId::new("job-a").unwrap(),
            base_index_version: WireIndexVersion {
                revision: neoengram_protocol::IndexRevision::new(0),
                digest: ContentDigest::from_bytes([0; 32]),
                extensions: Extensions::new(),
            },
            extensions: Extensions::new(),
        };
        let page = MetadataBatchPage::new(
            batch_id.clone(),
            scope.clone(),
            0,
            1,
            MetadataBatchRecords::IndexDelta(Vec::new()),
            Extensions::new(),
        )
        .unwrap();
        let descriptor = MetadataBatchDescriptor::from_pages(
            batch_id.clone(),
            scope,
            std::slice::from_ref(&page),
            Extensions::new(),
        )
        .unwrap();
        assert!(!stager.stage_descriptor(descriptor).unwrap());
    }

    assert!(stager
        .get(&TenantId::new("tenant-a").unwrap(), &batch_id)
        .unwrap()
        .is_some());
    assert!(stager
        .get(&TenantId::new("tenant-b").unwrap(), &batch_id)
        .unwrap()
        .is_some());
}

#[test]
fn complete_managed_add_is_idempotent_at_every_boundary() {
    let scenario = scenario();

    let create_replay = scenario
        .control
        .create_add_job(CreateAddJobRequest {
            actor: scenario.actor.clone(),
            spec: scenario.spec.clone(),
        })
        .unwrap();
    assert!(create_replay.replayed);

    let assign_replay = scenario
        .control
        .assign_job(AssignJobRequest {
            actor: scenario.actor.clone(),
            tenant_id: scenario.spec.tenant_id.clone(),
            job_id: scenario.spec.job_id.clone(),
            target: scenario.target.clone(),
        })
        .unwrap();
    assert!(assign_replay.replayed);
    assert_eq!(scenario.components.outbox.messages().unwrap().len(), 1);

    assert!(
        scenario
            .control
            .receive_report(ReceiveReportRequest {
                tenant_id: scenario.spec.tenant_id.clone(),
                agent_id: scenario.target.agent_id.clone(),
                report: AgentReport::Accepted(scenario.accepted.clone()),
            })
            .unwrap()
            .replayed
    );
    assert!(
        scenario
            .control
            .receive_report(ReceiveReportRequest {
                tenant_id: scenario.spec.tenant_id.clone(),
                agent_id: scenario.target.agent_id.clone(),
                report: AgentReport::Progress(scenario.progress.clone()),
            })
            .unwrap()
            .replayed
    );
    assert!(
        scenario
            .control
            .receive_report(ReceiveReportRequest {
                tenant_id: scenario.spec.tenant_id.clone(),
                agent_id: scenario.target.agent_id.clone(),
                report: AgentReport::Prepared(scenario.prepared.clone()),
            })
            .unwrap()
            .replayed
    );

    scenario.stage_all();
    scenario.mark_object_durable();
    let result = scenario
        .control
        .finalize_add(scenario.finalize_request())
        .unwrap();
    assert_eq!(result.job.state, JobState::Succeeded);
    assert!(!result.replayed);

    let snapshot = scenario
        .components
        .publisher
        .snapshot(&result.job.index_key())
        .unwrap();
    assert_eq!(snapshot.version.revision.get(), 1);
    assert_eq!(snapshot.records.len(), 1);
    assert_eq!(snapshot.records[0].path.as_str(), "dataset/file.bin");
    let manifest_id = snapshot.records[0].manifest_id;

    scenario.components.metadata.clear().unwrap();
    assert!(scenario.components.metadata.batches().unwrap().is_empty());
    let manifest = scenario
        .components
        .publisher
        .manifest(
            &scenario.spec.tenant_id,
            &scenario.spec.artifact_id,
            manifest_id,
        )
        .unwrap()
        .expect("published Manifest survives staging cleanup");
    assert_eq!(manifest.canonical_id().unwrap(), manifest_id);
    assert_eq!(manifest.total_size, snapshot.records[0].total_size);
    assert_eq!(
        manifest.chunk_count().unwrap(),
        snapshot.records[0].chunk_count
    );

    let finalize_replay = scenario
        .control
        .finalize_add(scenario.finalize_request())
        .unwrap();
    assert!(finalize_replay.replayed);
    assert_eq!(finalize_replay.decision, result.decision);
    assert_eq!(finalize_replay.finalized, result.finalized);

    let first_ack = scenario
        .control
        .receive_report(ReceiveReportRequest {
            tenant_id: scenario.spec.tenant_id.clone(),
            agent_id: scenario.target.agent_id.clone(),
            report: AgentReport::Finalized(result.finalized.clone()),
        })
        .unwrap();
    assert!(!first_ack.replayed);
    let second_ack = scenario
        .control
        .receive_report(ReceiveReportRequest {
            tenant_id: scenario.spec.tenant_id.clone(),
            agent_id: scenario.target.agent_id.clone(),
            report: AgentReport::Finalized(result.finalized),
        })
        .unwrap();
    assert!(second_ack.replayed);
}

#[test]
fn finalize_reassembles_manifest_fragments_across_metadata_pages() {
    let scenario = scenario_with_fragmented_manifest();
    let manifest_pages = scenario
        .pages
        .iter()
        .filter(|page| matches!(page.records, MetadataBatchRecords::Manifest(_)))
        .count();
    assert_eq!(manifest_pages, 2);

    scenario.stage_all();
    scenario.mark_object_durable();
    let result = scenario
        .control
        .finalize_add(scenario.finalize_request())
        .unwrap();

    assert_eq!(result.job.state, JobState::Succeeded);
    let snapshot = scenario
        .components
        .publisher
        .snapshot(&scenario.index_key())
        .unwrap();
    assert_eq!(snapshot.records.len(), 1);
    assert_eq!(snapshot.records[0].total_size, 7);
    assert_eq!(snapshot.records[0].chunk_count, 2);
}

#[test]
fn finalize_rejects_an_expired_assignment_lease_before_cas() {
    let scenario = scenario_with_expiring_lease();
    scenario.stage_all();
    scenario.mark_object_durable();
    let version_before = scenario
        .components
        .publisher
        .current_version(&scenario.index_key())
        .unwrap();
    let lease_expiry = scenario.target.lease.as_ref().unwrap().expires_at_unix_ms;
    scenario.components.clock.set(lease_expiry.get());

    let error = scenario
        .control
        .finalize_add(scenario.finalize_request())
        .unwrap_err();

    assert_eq!(error.code(), CentralErrorCode::DeadlineExceeded);
    assert_eq!(
        scenario
            .components
            .publisher
            .current_version(&scenario.index_key())
            .unwrap(),
        version_before
    );
    assert_eq!(
        scenario
            .components
            .jobs
            .get(&JobKey::new(
                scenario.spec.tenant_id.clone(),
                scenario.spec.job_id.clone(),
            ))
            .unwrap()
            .unwrap()
            .state,
        JobState::Prepared
    );
}

#[test]
fn finalize_rechecks_the_lease_after_metadata_validation() {
    let components = InMemoryComponents::new(100);
    let lease_expiry = 200;
    let expiring_objects = Arc::new(ExpireLeaseOnObjectRead {
        inner: components.objects.clone(),
        clock: components.clock.clone(),
        lease_expiry,
        armed: AtomicBool::new(true),
    });
    let control = ControlPlane::new(
        components.authorizer.clone(),
        components.jobs.clone(),
        components.outbox.clone(),
        components.metadata.clone(),
        expiring_objects,
        components.publisher.clone(),
        components.audit.clone(),
        components.clock.clone(),
    );
    let scenario = build_scenario_with_options(
        components,
        control,
        false,
        "dataset/file.bin",
        false,
        Some(LeaseGrant {
            lease_id: LeaseId::new("lease-a").unwrap(),
            mode: LeaseMode::ExclusiveWrite,
            fencing_token: Some(FencingToken::new(1)),
            expires_at_unix_ms: UnixMillis::new(lease_expiry),
            extensions: Extensions::new(),
        }),
    );
    scenario.stage_all();
    scenario.mark_object_durable();
    let version_before = scenario
        .components
        .publisher
        .current_version(&scenario.index_key())
        .unwrap();

    let error = scenario
        .control
        .finalize_add(scenario.finalize_request())
        .unwrap_err();

    assert_eq!(error.code(), CentralErrorCode::DeadlineExceeded);
    assert_eq!(
        scenario
            .components
            .publisher
            .current_version(&scenario.index_key())
            .unwrap(),
        version_before
    );
    assert_eq!(
        scenario
            .components
            .jobs
            .get(&JobKey::new(
                scenario.spec.tenant_id.clone(),
                scenario.spec.job_id.clone(),
            ))
            .unwrap()
            .unwrap()
            .state,
        JobState::Prepared
    );
}

#[test]
fn persisted_job_can_be_replayed_after_its_deadline() {
    let components = InMemoryComponents::new(100);
    let control = components.control_plane();
    let (actor, spec, _) = inputs(&components);
    control
        .create_add_job(CreateAddJobRequest {
            actor: actor.clone(),
            spec: spec.clone(),
        })
        .unwrap();
    components.clock.set(spec.deadline_unix_ms.get() + 1);

    let replay = control
        .create_add_job(CreateAddJobRequest { actor, spec })
        .unwrap();

    assert!(replay.replayed);
    assert_eq!(replay.job.state, JobState::Queued);
}

#[test]
fn queued_job_expires_without_an_assignment_decision() {
    let components = InMemoryComponents::new(100);
    let control = components.control_plane();
    let (actor, spec, _) = inputs(&components);
    control
        .create_add_job(CreateAddJobRequest {
            actor: actor.clone(),
            spec: spec.clone(),
        })
        .unwrap();
    components.clock.set(spec.deadline_unix_ms.get());

    let expired = control
        .expire_add_job(ExpireAddJobRequest {
            actor,
            tenant_id: spec.tenant_id,
            job_id: spec.job_id,
        })
        .unwrap();

    assert!(!expired.replayed);
    assert_eq!(expired.job.state, JobState::TimedOut);
    assert!(expired.job.assignment.is_none());
    assert!(expired.decision.is_none());
    assert!(expired.finalized.is_none());
}

#[test]
fn assigned_job_timeout_decision_replays_exactly() {
    let components = InMemoryComponents::new(100);
    let control = components.control_plane();
    let (actor, spec, target) = inputs(&components);
    control
        .create_add_job(CreateAddJobRequest {
            actor: actor.clone(),
            spec: spec.clone(),
        })
        .unwrap();
    control
        .assign_job(AssignJobRequest {
            actor: actor.clone(),
            tenant_id: spec.tenant_id.clone(),
            job_id: spec.job_id.clone(),
            target,
        })
        .unwrap();
    components.clock.set(spec.deadline_unix_ms.get() + 1);
    let request = ExpireAddJobRequest {
        actor,
        tenant_id: spec.tenant_id,
        job_id: spec.job_id,
    };

    let expired = control.expire_add_job(request.clone()).unwrap();

    assert!(!expired.replayed);
    assert_eq!(expired.job.state, JobState::TimedOut);
    let decision = expired.decision.as_ref().unwrap();
    assert_eq!(decision.final_state, JobState::TimedOut);
    let PublishDecision::Reject { error, .. } = &decision.decision else {
        panic!("timeout must produce a Reject decision");
    };
    assert_eq!(error.code.as_str(), "JOB_DEADLINE_EXCEEDED");
    assert_eq!(
        expired.finalized.as_ref().unwrap().final_state,
        JobState::TimedOut
    );

    let replay = control.expire_add_job(request).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.job, expired.job);
    assert_eq!(replay.decision, expired.decision);
    assert_eq!(replay.finalized, expired.finalized);
}

#[test]
fn prepared_with_many_same_kind_batches_is_rejected_without_overflow() {
    let scenario = scenario();
    let manifest = scenario
        .descriptors
        .iter()
        .find(|descriptor| descriptor.kind == MetadataBatchKind::Manifest)
        .unwrap();
    let metadata_batches = (0..256)
        .map(|index| {
            let mut descriptor = manifest.clone();
            descriptor.batch_id = MetadataBatchId::new(format!("manifest-overflow-{index}"))
                .expect("generated batch ID is valid");
            descriptor.batch_digest = descriptor.computed_digest().unwrap();
            descriptor
        })
        .collect();
    let mut prepared = scenario.prepared.clone();
    prepared.metadata_batches = metadata_batches;
    prepared.candidate_digest = prepared.computed_candidate_digest().unwrap();

    let error = scenario
        .control
        .receive_report(ReceiveReportRequest {
            tenant_id: scenario.spec.tenant_id.clone(),
            agent_id: scenario.target.agent_id.clone(),
            report: AgentReport::Prepared(prepared),
        })
        .unwrap_err();

    assert_eq!(error.stable_code(), "METADATA_INVALID");
    assert!(error.message().contains("same kind"));
}

#[test]
fn prepared_candidate_digest_rejects_a_replaced_valid_descriptor() {
    let scenario = scenario();
    let mut prepared = scenario.prepared.clone();
    let mut replacement = prepared.metadata_batches[0].clone();
    replacement.batch_id = MetadataBatchId::new("replacement-valid-batch").unwrap();
    replacement.batch_digest = replacement.computed_digest().unwrap();
    replacement.validate().unwrap();
    prepared.metadata_batches[0] = replacement;

    let error = scenario
        .control
        .receive_report(ReceiveReportRequest {
            tenant_id: scenario.spec.tenant_id.clone(),
            agent_id: scenario.target.agent_id.clone(),
            report: AgentReport::Prepared(prepared),
        })
        .unwrap_err();

    assert_eq!(error.stable_code(), "METADATA_BATCH_TAMPERED");
    assert!(error.message().contains("candidate digest mismatch"));
}

#[test]
fn failed_report_rejects_invalid_generation_state_and_tenant_scope() {
    let scenario = scenario();
    let failure = JobFailed {
        tenant_id: scenario.spec.tenant_id.clone(),
        job_id: scenario.spec.job_id.clone(),
        assignment_id: scenario.target.assignment_id.clone(),
        assignment_generation: scenario.target.assignment_generation,
        final_state: JobState::Failed,
        failed_at_unix_ms: UnixMillis::new(500),
        stage: JobFailureStage::Execution,
        error: ControlError {
            code: ErrorCode::new("EXECUTION_FAILED").unwrap(),
            message: "executor failed".to_owned(),
            retryable: true,
            retry_after_ms: None,
            extensions: Extensions::new(),
        },
        extensions: Extensions::new(),
    };

    let mut zero_generation = failure.clone();
    zero_generation.assignment_generation = AssignmentGeneration::new(0);
    let error = scenario
        .control
        .receive_report(ReceiveReportRequest {
            tenant_id: scenario.spec.tenant_id.clone(),
            agent_id: scenario.target.agent_id.clone(),
            report: AgentReport::Failed(zero_generation),
        })
        .unwrap_err();
    assert_eq!(error.stable_code(), "PROTOCOL_INVALID");

    let mut invalid_terminal = failure.clone();
    invalid_terminal.final_state = JobState::Succeeded;
    let error = scenario
        .control
        .receive_report(ReceiveReportRequest {
            tenant_id: scenario.spec.tenant_id.clone(),
            agent_id: scenario.target.agent_id.clone(),
            report: AgentReport::Failed(invalid_terminal),
        })
        .unwrap_err();
    assert_eq!(error.stable_code(), "PROTOCOL_INVALID");

    let mut wrong_tenant = failure;
    wrong_tenant.tenant_id = TenantId::new("tenant-other").unwrap();
    let error = scenario
        .control
        .receive_report(ReceiveReportRequest {
            tenant_id: scenario.spec.tenant_id.clone(),
            agent_id: scenario.target.agent_id.clone(),
            report: AgentReport::Failed(wrong_tenant),
        })
        .unwrap_err();
    assert_eq!(error.stable_code(), "ASSIGNMENT_MISMATCH");
}

#[test]
fn finalize_is_blocked_until_every_declared_object_is_centrally_durable() {
    let scenario = scenario();
    scenario.stage_all();

    let error = scenario
        .control
        .finalize_add(scenario.finalize_request())
        .unwrap_err();
    assert_eq!(error.stable_code(), "OBJECT_NOT_DURABLE");
    assert_eq!(
        scenario
            .components
            .jobs
            .get(&JobKey::new(
                scenario.spec.tenant_id.clone(),
                scenario.spec.job_id.clone()
            ))
            .unwrap()
            .unwrap()
            .state,
        JobState::Prepared
    );
}

#[test]
fn pending_and_rejected_observations_do_not_downgrade_a_durable_object() {
    let scenario = scenario();
    scenario.stage_all();
    scenario.mark_object_durable();
    scenario.observe_object(DurabilityState::Pending {
        extensions: Extensions::new(),
    });
    scenario.observe_object(DurabilityState::Rejected {
        code: "UPLOAD_REJECTED".to_owned(),
        message: "a later upload attempt was rejected".to_owned(),
        extensions: Extensions::new(),
    });

    let durable = scenario
        .components
        .objects
        .durable_object(
            &scenario.spec.tenant_id,
            &scenario.spec.artifact_id,
            scenario.object_id,
        )
        .unwrap()
        .unwrap();
    assert_eq!(durable.object_id, scenario.object_id);
    assert_eq!(durable.storage_version, "s3-version-1");
    assert_eq!(
        scenario
            .control
            .finalize_add(scenario.finalize_request())
            .unwrap()
            .job
            .state,
        JobState::Succeeded
    );
}

#[test]
fn finalize_rejects_incomplete_and_tampered_batches() {
    let incomplete = scenario();
    for descriptor in &incomplete.descriptors {
        incomplete
            .control
            .stage_metadata_batch(StageMetadataBatchRequest {
                tenant_id: incomplete.spec.tenant_id.clone(),
                job_id: incomplete.spec.job_id.clone(),
                agent_id: incomplete.target.agent_id.clone(),
                submission: MetadataBatchSubmission::Descriptor(descriptor.clone()),
            })
            .unwrap();
    }
    for page in incomplete.pages.iter().take(2) {
        incomplete
            .control
            .stage_metadata_batch(StageMetadataBatchRequest {
                tenant_id: incomplete.spec.tenant_id.clone(),
                job_id: incomplete.spec.job_id.clone(),
                agent_id: incomplete.target.agent_id.clone(),
                submission: MetadataBatchSubmission::Page(page.clone()),
            })
            .unwrap();
    }
    incomplete.mark_object_durable();
    let error = incomplete
        .control
        .finalize_add(incomplete.finalize_request())
        .unwrap_err();
    assert_eq!(error.stable_code(), "METADATA_BATCH_INCOMPLETE");

    let tampered = scenario();
    let descriptor = tampered.descriptors[0].clone();
    tampered
        .control
        .stage_metadata_batch(StageMetadataBatchRequest {
            tenant_id: tampered.spec.tenant_id.clone(),
            job_id: tampered.spec.job_id.clone(),
            agent_id: tampered.target.agent_id.clone(),
            submission: MetadataBatchSubmission::Descriptor(descriptor),
        })
        .unwrap();
    let mut page = tampered.pages[0].clone();
    page.page_digest = ContentDigest::hash(b"tampered");
    let error = tampered
        .control
        .stage_metadata_batch(StageMetadataBatchRequest {
            tenant_id: tampered.spec.tenant_id.clone(),
            job_id: tampered.spec.job_id.clone(),
            agent_id: tampered.target.agent_id.clone(),
            submission: MetadataBatchSubmission::Page(page),
        })
        .unwrap_err();
    assert_eq!(error.stable_code(), "METADATA_BATCH_TAMPERED");
}

#[test]
fn finalize_rejects_metadata_not_referenced_by_the_index_delta() {
    let scenario = scenario_with_unreferenced_manifest();
    scenario.stage_all();
    scenario.mark_object_durable();

    let error = scenario
        .control
        .finalize_add(scenario.finalize_request())
        .unwrap_err();
    assert_eq!(error.stable_code(), "METADATA_INVALID");
    assert!(error
        .message()
        .contains("exact set referenced by IndexDelta"));
    assert_eq!(
        scenario
            .components
            .jobs
            .get(&JobKey::new(
                scenario.spec.tenant_id.clone(),
                scenario.spec.job_id.clone()
            ))
            .unwrap()
            .unwrap()
            .state,
        JobState::Prepared
    );
}

#[test]
fn finalize_rejects_index_mutations_outside_the_assignment_scope() {
    let scenario = scenario_with_out_of_scope_mutation();
    scenario.stage_all();
    scenario.mark_object_durable();

    let error = scenario
        .control
        .finalize_add(scenario.finalize_request())
        .unwrap_err();

    assert_eq!(error.stable_code(), "METADATA_INVALID");
    assert!(error.message().contains("outside the Add assignment scope"));
    assert_eq!(
        scenario
            .components
            .jobs
            .get(&JobKey::new(
                scenario.spec.tenant_id.clone(),
                scenario.spec.job_id.clone()
            ))
            .unwrap()
            .unwrap()
            .state,
        JobState::Prepared
    );
}

#[test]
fn finalize_rejects_a_different_internally_valid_publication() {
    let original_publication_digest = scenario().prepared.publication_digest;
    let components = InMemoryComponents::new(100);
    let control = components.control_plane();
    let substituted = build_scenario_with_candidate_overrides(
        components,
        control,
        false,
        "dataset/file.bin",
        false,
        None,
        CandidateOverrides {
            publication_digest: Some(original_publication_digest),
            payload: Some(b"substituted-payload".to_vec()),
            ..CandidateOverrides::default()
        },
    );
    assert_ne!(
        substituted.prepared.publication_digest,
        MetadataPublication::from_pages(&substituted.pages)
            .and_then(|publication| {
                publication.publication_digest(
                    &substituted.pages[0].scope,
                    substituted.prepared.result_index_digest,
                )
            })
            .unwrap()
    );
    substituted.stage_all();
    substituted.mark_object_durable();

    let first = substituted
        .control
        .finalize_add(substituted.finalize_request())
        .unwrap_err();
    let replay = substituted
        .control
        .finalize_add(substituted.finalize_request())
        .unwrap_err();

    assert_eq!(first.stable_code(), "METADATA_INVALID");
    assert_eq!(replay, first);
    assert!(
        first.message().contains("publication digest"),
        "unexpected validation error: {}",
        first.message()
    );
    let job = substituted
        .components
        .jobs
        .get(&JobKey::new(
            substituted.spec.tenant_id.clone(),
            substituted.spec.job_id.clone(),
        ))
        .unwrap()
        .unwrap();
    assert_eq!(job.state, JobState::Prepared);
    let snapshot = substituted
        .components
        .publisher
        .snapshot(&substituted.index_key())
        .unwrap();
    assert_eq!(snapshot.version, substituted.spec.expected_index_version);
    assert!(snapshot.records.is_empty());
}

#[test]
fn wrong_result_digest_becomes_a_stable_failed_decision() {
    let components = InMemoryComponents::new(100);
    let control = components.control_plane();
    let wrong_result_digest = ContentDigest::from_bytes([0x99; 32]);
    let scenario = build_scenario_with_candidate_overrides(
        components,
        control,
        false,
        "dataset/file.bin",
        false,
        None,
        CandidateOverrides {
            result_index_digest: Some(wrong_result_digest),
            ..CandidateOverrides::default()
        },
    );
    scenario.stage_all();
    scenario.mark_object_durable();

    let first = scenario
        .control
        .finalize_add(scenario.finalize_request())
        .unwrap();
    let replay = scenario
        .control
        .finalize_add(scenario.finalize_request())
        .unwrap();

    assert_eq!(first.job.state, JobState::Failed);
    assert_eq!(first.finalized.final_state, JobState::Failed);
    let PublishDecision::Reject { error, .. } = &first.decision.decision else {
        panic!("result digest mismatch must produce a Reject decision");
    };
    assert_eq!(error.code.as_str(), "INDEX_RESULT_DIGEST_MISMATCH");
    assert!(!error.retryable);
    assert!(replay.replayed);
    assert_eq!(replay.decision, first.decision);
    let snapshot = scenario
        .components
        .publisher
        .snapshot(&scenario.index_key())
        .unwrap();
    assert_eq!(snapshot.version, scenario.spec.expected_index_version);
    assert!(snapshot.records.is_empty());
}

#[test]
fn invalid_resulting_snapshot_becomes_a_stable_failed_decision() {
    let components = InMemoryComponents::new(100);
    let key = IndexKey {
        tenant_id: TenantId::new("tenant-a").unwrap(),
        artifact_id: ArtifactId::new("artifact-a").unwrap(),
        playground_id: PlaygroundId::new("playground-a").unwrap(),
    };
    let prefix = FileRecord::new(
        LogicalPath::parse("dataset").unwrap(),
        Manifest::new(0, ChunkingStrategy::FastCdc, Vec::new())
            .unwrap()
            .canonical_id()
            .unwrap(),
        0,
        0,
    )
    .unwrap();
    let base_version = components
        .publisher
        .seed(key.clone(), 7, vec![prefix.clone()])
        .unwrap();
    let control = components.control_plane();
    let scenario = build_scenario(components, control);
    assert_eq!(scenario.spec.expected_index_version, base_version);
    scenario.stage_all();
    scenario.mark_object_durable();

    let first = scenario
        .control
        .finalize_add(scenario.finalize_request())
        .unwrap();
    let replay = scenario
        .control
        .finalize_add(scenario.finalize_request())
        .unwrap();

    assert_eq!(first.job.state, JobState::Failed);
    let PublishDecision::Reject { error, .. } = &first.decision.decision else {
        panic!("invalid resulting snapshot must produce a Reject decision");
    };
    assert_eq!(error.code.as_str(), "METADATA_INVALID");
    assert!(error.message.contains("published Index is invalid"));
    assert!(replay.replayed);
    assert_eq!(replay.decision, first.decision);
    let snapshot = scenario.components.publisher.snapshot(&key).unwrap();
    assert_eq!(snapshot.version, base_version);
    assert_eq!(snapshot.records, vec![prefix]);
}

#[test]
fn cas_mismatch_becomes_a_stable_conflicted_decision_and_replays() {
    let scenario = scenario();
    scenario.stage_all();
    scenario.mark_object_durable();
    scenario
        .components
        .publisher
        .seed(
            IndexKey {
                tenant_id: scenario.spec.tenant_id.clone(),
                artifact_id: scenario.spec.artifact_id.clone(),
                playground_id: scenario.spec.playground_id.clone(),
            },
            1,
            Vec::new(),
        )
        .unwrap();

    let result = scenario
        .control
        .finalize_add(scenario.finalize_request())
        .unwrap();
    assert_eq!(result.job.state, JobState::Conflicted);
    assert_eq!(result.finalized.final_state, JobState::Conflicted);
    let replay = scenario
        .control
        .finalize_add(scenario.finalize_request())
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.decision, result.decision);
}

#[test]
fn publishing_replay_converges_after_terminal_persist_failure_and_deadline() {
    let components = InMemoryComponents::new(100);
    let jobs = Arc::new(FailTerminalOnce::new(components.jobs.clone()));
    let control = ControlPlane::new(
        components.authorizer.clone(),
        jobs.clone(),
        components.outbox.clone(),
        components.metadata.clone(),
        components.objects.clone(),
        components.publisher.clone(),
        components.audit.clone(),
        components.clock.clone(),
    );
    let scenario = build_scenario(components, control);
    scenario.stage_all();
    scenario.mark_object_durable();
    jobs.arm();

    let error = scenario
        .control
        .finalize_add(scenario.finalize_request())
        .unwrap_err();
    assert_eq!(error.stable_code(), "STORAGE_FAILURE");
    let key = JobKey::new(
        scenario.spec.tenant_id.clone(),
        scenario.spec.job_id.clone(),
    );
    assert_eq!(
        scenario.components.jobs.get(&key).unwrap().unwrap().state,
        JobState::Publishing
    );
    assert_eq!(
        scenario
            .components
            .publisher
            .snapshot(&scenario.index_key())
            .unwrap()
            .version
            .revision
            .get(),
        1
    );
    scenario.components.metadata.clear().unwrap();

    scenario
        .components
        .clock
        .set(scenario.spec.deadline_unix_ms.get() + 1);
    let timeout_error = scenario
        .control
        .expire_add_job(ExpireAddJobRequest {
            actor: scenario.actor.clone(),
            tenant_id: scenario.spec.tenant_id.clone(),
            job_id: scenario.spec.job_id.clone(),
        })
        .unwrap_err();
    assert_eq!(timeout_error.stable_code(), "JOB_INVALID_STATE");
    assert_eq!(
        scenario.components.jobs.get(&key).unwrap().unwrap().state,
        JobState::Publishing
    );

    let terminal_error = scenario
        .control
        .receive_report(ReceiveReportRequest {
            tenant_id: scenario.spec.tenant_id.clone(),
            agent_id: scenario.target.agent_id.clone(),
            report: AgentReport::Failed(JobFailed {
                tenant_id: scenario.spec.tenant_id.clone(),
                job_id: scenario.spec.job_id.clone(),
                assignment_id: scenario.target.assignment_id.clone(),
                assignment_generation: scenario.target.assignment_generation,
                final_state: JobState::RecoveryRequired,
                failed_at_unix_ms: UnixMillis::new(500),
                stage: JobFailureStage::Execution,
                error: ControlError {
                    code: ErrorCode::new("AGENT_RECOVERY_REQUIRED").unwrap(),
                    message: "injected stale terminal report".to_owned(),
                    retryable: true,
                    retry_after_ms: None,
                    extensions: Extensions::new(),
                },
                extensions: Extensions::new(),
            }),
        })
        .unwrap_err();
    assert_eq!(terminal_error.stable_code(), "JOB_INVALID_STATE");
    assert_eq!(
        scenario.components.jobs.get(&key).unwrap().unwrap().state,
        JobState::Publishing
    );

    let recovered = scenario
        .control
        .finalize_add(scenario.finalize_request())
        .unwrap();
    assert!(recovered.replayed);
    assert_eq!(recovered.job.state, JobState::Succeeded);
    assert_eq!(
        scenario
            .components
            .publisher
            .snapshot(&recovered.job.index_key())
            .unwrap()
            .version
            .revision
            .get(),
        1
    );
}

#[test]
fn publishing_recovery_uses_the_frozen_candidate_after_staging_gc_and_revocation() {
    let components = InMemoryComponents::new(100);
    let authorizer = Arc::new(RevocableAuthorizer::default());
    let publisher = Arc::new(FailPublisherOnce::new(components.publisher.clone()));
    let control = ControlPlane::new(
        authorizer.clone(),
        components.jobs.clone(),
        components.outbox.clone(),
        components.metadata.clone(),
        components.objects.clone(),
        publisher,
        components.audit.clone(),
        components.clock.clone(),
    );
    let scenario = build_scenario(components, control);
    scenario.stage_all();
    scenario.mark_object_durable();

    let error = scenario
        .control
        .finalize_add(scenario.finalize_request())
        .unwrap_err();
    assert_eq!(error.stable_code(), "STORAGE_FAILURE");
    let key = JobKey::new(
        scenario.spec.tenant_id.clone(),
        scenario.spec.job_id.clone(),
    );
    let publishing = scenario.components.jobs.get(&key).unwrap().unwrap();
    assert_eq!(publishing.state, JobState::Publishing);
    assert!(publishing.publication_candidate.is_some());
    assert_eq!(
        scenario
            .components
            .publisher
            .snapshot(&scenario.index_key())
            .unwrap()
            .version,
        scenario.spec.expected_index_version
    );

    let mut wrong_actor_request = scenario.finalize_request();
    wrong_actor_request.actor.id = PrincipalId::new("another-user").unwrap();
    let wrong_actor = scenario
        .control
        .finalize_add(wrong_actor_request)
        .unwrap_err();
    assert_eq!(wrong_actor.stable_code(), "AUTHORIZATION_DENIED");

    scenario.components.metadata.clear().unwrap();
    authorizer.revoke();
    scenario
        .components
        .clock
        .set(scenario.spec.deadline_unix_ms.get() + 1);

    let revoked = scenario
        .control
        .finalize_add(scenario.finalize_request())
        .unwrap_err();
    assert_eq!(revoked.stable_code(), "AUTHORIZATION_DENIED");
    let recovered = scenario
        .control
        .resume_publication(ResumePublicationRequest {
            tenant_id: scenario.spec.tenant_id.clone(),
            job_id: scenario.spec.job_id.clone(),
        })
        .unwrap();
    assert!(recovered.replayed);
    assert_eq!(recovered.job.state, JobState::Succeeded);
    assert_eq!(
        scenario
            .components
            .publisher
            .snapshot(&scenario.index_key())
            .unwrap()
            .version
            .revision
            .get(),
        1
    );
}

#[test]
fn persisted_assignment_can_be_reenqueued_after_its_deadline() {
    let components = InMemoryComponents::new(100);
    let outbox = Arc::new(FlakyOutbox::default());
    let control = ControlPlane::new(
        components.authorizer.clone(),
        components.jobs.clone(),
        outbox.clone(),
        components.metadata.clone(),
        components.objects.clone(),
        components.publisher.clone(),
        components.audit.clone(),
        components.clock.clone(),
    );
    let (actor, spec, target) = inputs(&components);
    control
        .create_add_job(CreateAddJobRequest {
            actor: actor.clone(),
            spec: spec.clone(),
        })
        .unwrap();
    let error = control
        .assign_job(AssignJobRequest {
            actor: actor.clone(),
            tenant_id: spec.tenant_id.clone(),
            job_id: spec.job_id.clone(),
            target: target.clone(),
        })
        .unwrap_err();
    assert_eq!(error.stable_code(), "STORAGE_FAILURE");
    let persisted = components
        .jobs
        .get(&JobKey::new(spec.tenant_id.clone(), spec.job_id.clone()))
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, JobState::Assigned);
    assert!(persisted.assignment.is_some());

    components.clock.set(spec.deadline_unix_ms.get() + 1);
    outbox.allow_delivery();
    let replay = control
        .assign_job(AssignJobRequest {
            actor,
            tenant_id: spec.tenant_id,
            job_id: spec.job_id,
            target,
        })
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(outbox.delivered.messages().unwrap().len(), 1);
}

struct CreateRaceRepository {
    inner: Arc<InMemoryJobRepository>,
    get_calls: AtomicUsize,
    initial_reads: usize,
    initial_read_barrier: Barrier,
}

struct ExpireLeaseOnObjectRead {
    inner: Arc<neoengramd::InMemoryObjectCatalog>,
    clock: Arc<neoengramd::InMemoryClock>,
    lease_expiry: u64,
    armed: AtomicBool,
}

impl ObjectCatalog for ExpireLeaseOnObjectRead {
    fn durable_object(
        &self,
        tenant_id: &TenantId,
        artifact_id: &ArtifactId,
        object_id: ObjectId,
    ) -> CentralResult<Option<neoengramd::DurableObject>> {
        let object = self
            .inner
            .durable_object(tenant_id, artifact_id, object_id)?;
        if self.armed.swap(false, Ordering::SeqCst) {
            self.clock.set(self.lease_expiry);
        }
        Ok(object)
    }
}

impl CreateRaceRepository {
    fn new(inner: Arc<InMemoryJobRepository>, initial_reads: usize) -> Self {
        Self {
            inner,
            get_calls: AtomicUsize::new(0),
            initial_reads,
            initial_read_barrier: Barrier::new(initial_reads),
        }
    }
}

impl JobRepository for CreateRaceRepository {
    fn get(&self, key: &JobKey) -> CentralResult<Option<JobRecord>> {
        let result = self.inner.get(key)?;
        if self.get_calls.fetch_add(1, Ordering::SeqCst) < self.initial_reads {
            self.initial_read_barrier.wait();
        }
        Ok(result)
    }

    fn insert_or_load(&self, job: JobRecord) -> CentralResult<JobInsertOutcome> {
        self.inner.insert_or_load(job)
    }

    fn replace(&self, expected: u64, job: JobRecord) -> CentralResult<JobRecord> {
        self.inner.replace(expected, job)
    }
}

struct AssignmentRaceRepository {
    inner: Arc<InMemoryJobRepository>,
    replace_calls: AtomicUsize,
    participants: usize,
    replace_barrier: Barrier,
}

impl AssignmentRaceRepository {
    fn new(inner: Arc<InMemoryJobRepository>, participants: usize) -> Self {
        Self {
            inner,
            replace_calls: AtomicUsize::new(0),
            participants,
            replace_barrier: Barrier::new(participants),
        }
    }
}

impl JobRepository for AssignmentRaceRepository {
    fn get(&self, key: &JobKey) -> CentralResult<Option<JobRecord>> {
        self.inner.get(key)
    }

    fn insert_or_load(&self, job: JobRecord) -> CentralResult<JobInsertOutcome> {
        self.inner.insert_or_load(job)
    }

    fn replace(&self, expected: u64, job: JobRecord) -> CentralResult<JobRecord> {
        if job.state == JobState::Assigned
            && self.replace_calls.fetch_add(1, Ordering::SeqCst) < self.participants
        {
            self.replace_barrier.wait();
        }
        self.inner.replace(expected, job)
    }
}

struct FailAssignmentWriteOnce {
    inner: Arc<InMemoryJobRepository>,
    armed: AtomicBool,
}

impl FailAssignmentWriteOnce {
    fn new(inner: Arc<InMemoryJobRepository>) -> Self {
        Self {
            inner,
            armed: AtomicBool::new(true),
        }
    }
}

impl JobRepository for FailAssignmentWriteOnce {
    fn get(&self, key: &JobKey) -> CentralResult<Option<JobRecord>> {
        self.inner.get(key)
    }

    fn insert_or_load(&self, job: JobRecord) -> CentralResult<JobInsertOutcome> {
        self.inner.insert_or_load(job)
    }

    fn replace(&self, expected: u64, job: JobRecord) -> CentralResult<JobRecord> {
        if job.state == JobState::Assigned && self.armed.swap(false, Ordering::SeqCst) {
            return Err(CentralError::new(
                CentralErrorCode::StorageFailure,
                "injected assignment JobRepository write failure",
            ));
        }
        self.inner.replace(expected, job)
    }
}

struct FailTerminalOnce {
    inner: Arc<InMemoryJobRepository>,
    armed: AtomicBool,
}

#[derive(Default)]
struct RevocableAuthorizer {
    revoked: AtomicBool,
}

impl RevocableAuthorizer {
    fn revoke(&self) {
        self.revoked.store(true, Ordering::SeqCst);
    }
}

impl Authorizer for RevocableAuthorizer {
    fn authorize(&self, request: &AuthorizationRequest) -> CentralResult<()> {
        if request.action != Action::FinalizeAdd {
            return Ok(());
        }
        let expected = PrincipalId::new("user-a").unwrap();
        let is_original_principal = matches!(
            &request.actor,
            Actor::Principal(principal)
                if principal.kind == PrincipalKind::User && principal.id == expected
        );
        if is_original_principal && !self.revoked.load(Ordering::SeqCst) {
            return Ok(());
        }
        Err(CentralError::new(
            CentralErrorCode::Unauthorized,
            "authorization was revoked or belongs to another principal",
        ))
    }
}

struct FailPublisherOnce {
    inner: Arc<neoengramd::InMemoryIndexPublisher>,
    armed: AtomicBool,
}

impl FailPublisherOnce {
    fn new(inner: Arc<neoengramd::InMemoryIndexPublisher>) -> Self {
        Self {
            inner,
            armed: AtomicBool::new(true),
        }
    }
}

impl IndexPublisher for FailPublisherOnce {
    fn compare_and_swap(&self, request: IndexPublishRequest) -> CentralResult<IndexPublishOutcome> {
        if self.armed.swap(false, Ordering::SeqCst) {
            return Err(CentralError::new(
                CentralErrorCode::StorageFailure,
                "injected publication failure before CAS",
            ));
        }
        self.inner.compare_and_swap(request)
    }

    fn current_version(&self, key: &IndexKey) -> CentralResult<WireIndexVersion> {
        self.inner.current_version(key)
    }
}

impl FailTerminalOnce {
    fn new(inner: Arc<InMemoryJobRepository>) -> Self {
        Self {
            inner,
            armed: AtomicBool::new(false),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
}

impl JobRepository for FailTerminalOnce {
    fn get(&self, key: &JobKey) -> CentralResult<Option<JobRecord>> {
        self.inner.get(key)
    }

    fn insert_or_load(&self, job: JobRecord) -> CentralResult<JobInsertOutcome> {
        self.inner.insert_or_load(job)
    }

    fn replace(&self, expected: u64, job: JobRecord) -> CentralResult<JobRecord> {
        if job.state.is_terminal() && self.armed.swap(false, Ordering::SeqCst) {
            return Err(CentralError::new(
                CentralErrorCode::StorageFailure,
                "injected terminal JobRepository write failure",
            ));
        }
        self.inner.replace(expected, job)
    }
}

struct FlakyOutbox {
    delivered: InMemoryAssignmentOutbox,
    reject_delivery: AtomicBool,
}

impl Default for FlakyOutbox {
    fn default() -> Self {
        Self {
            delivered: InMemoryAssignmentOutbox::default(),
            reject_delivery: AtomicBool::new(true),
        }
    }
}

impl FlakyOutbox {
    fn allow_delivery(&self) {
        self.reject_delivery.store(false, Ordering::SeqCst);
    }
}

impl AssignmentOutbox for FlakyOutbox {
    fn reserve(
        &self,
        assignment: neoengram_protocol::JobAssignment,
    ) -> CentralResult<AssignmentReserveOutcome> {
        self.delivered.reserve(assignment)
    }

    fn publish(
        &self,
        assignment: neoengram_protocol::JobAssignment,
    ) -> CentralResult<AssignmentPublishOutcome> {
        if self.reject_delivery.load(Ordering::SeqCst) {
            return Err(CentralError::new(
                CentralErrorCode::StorageFailure,
                "injected outbox delivery failure",
            ));
        }
        self.delivered.publish(assignment)
    }
}

fn scenario() -> Scenario {
    let components = InMemoryComponents::new(100);
    let control = components.control_plane();
    build_scenario(components, control)
}

fn scenario_with_expiring_lease() -> Scenario {
    let components = InMemoryComponents::new(100);
    let control = components.control_plane();
    build_scenario_with_options(
        components,
        control,
        false,
        "dataset/file.bin",
        false,
        Some(LeaseGrant {
            lease_id: LeaseId::new("lease-a").unwrap(),
            mode: LeaseMode::ExclusiveWrite,
            fencing_token: Some(FencingToken::new(1)),
            expires_at_unix_ms: UnixMillis::new(200),
            extensions: Extensions::new(),
        }),
    )
}

fn scenario_with_unreferenced_manifest() -> Scenario {
    let components = InMemoryComponents::new(100);
    let control = components.control_plane();
    build_scenario_with_options(components, control, true, "dataset/file.bin", false, None)
}

fn scenario_with_out_of_scope_mutation() -> Scenario {
    let components = InMemoryComponents::new(100);
    let control = components.control_plane();
    build_scenario_with_options(
        components,
        control,
        false,
        "unrelated/file.bin",
        false,
        None,
    )
}

fn scenario_with_fragmented_manifest() -> Scenario {
    let components = InMemoryComponents::new(100);
    let control = components.control_plane();
    build_scenario_with_options(components, control, false, "dataset/file.bin", true, None)
}

fn build_scenario(components: InMemoryComponents, control: ControlPlane) -> Scenario {
    build_scenario_with_options(components, control, false, "dataset/file.bin", false, None)
}

fn build_scenario_with_options(
    components: InMemoryComponents,
    control: ControlPlane,
    include_unreferenced_manifest: bool,
    index_path: &str,
    fragment_manifest: bool,
    lease: Option<LeaseGrant>,
) -> Scenario {
    build_scenario_with_candidate_overrides(
        components,
        control,
        include_unreferenced_manifest,
        index_path,
        fragment_manifest,
        lease,
        CandidateOverrides::default(),
    )
}

#[derive(Debug, Default)]
struct CandidateOverrides {
    result_index_digest: Option<ContentDigest>,
    publication_digest: Option<ContentDigest>,
    payload: Option<Vec<u8>>,
}

#[allow(clippy::too_many_arguments)]
fn build_scenario_with_candidate_overrides(
    components: InMemoryComponents,
    control: ControlPlane,
    include_unreferenced_manifest: bool,
    index_path: &str,
    fragment_manifest: bool,
    lease: Option<LeaseGrant>,
    overrides: CandidateOverrides,
) -> Scenario {
    let (actor, spec, mut target) = inputs(&components);
    target.lease = lease;
    control
        .create_add_job(CreateAddJobRequest {
            actor: actor.clone(),
            spec: spec.clone(),
        })
        .unwrap();
    control
        .assign_job(AssignJobRequest {
            actor: actor.clone(),
            tenant_id: spec.tenant_id.clone(),
            job_id: spec.job_id.clone(),
            target: target.clone(),
        })
        .unwrap();

    let accepted = JobAccepted {
        job_id: spec.job_id.clone(),
        assignment_id: target.assignment_id.clone(),
        assignment_generation: target.assignment_generation,
        accepted_at_unix_ms: UnixMillis::new(200),
        request_digest: spec.request_digest,
        extensions: Extensions::new(),
    };
    control
        .receive_report(ReceiveReportRequest {
            tenant_id: spec.tenant_id.clone(),
            agent_id: target.agent_id.clone(),
            report: AgentReport::Accepted(accepted.clone()),
        })
        .unwrap();
    let progress = JobProgress {
        job_id: spec.job_id.clone(),
        assignment_id: target.assignment_id.clone(),
        assignment_generation: target.assignment_generation,
        state: JobState::Running,
        phase: "prepare".to_owned(),
        files_completed: DecimalU64::new(0),
        bytes_completed: DecimalU64::new(0),
        retry_after_ms: None,
        extensions: Extensions::new(),
    };
    control
        .receive_report(ReceiveReportRequest {
            tenant_id: spec.tenant_id.clone(),
            agent_id: target.agent_id.clone(),
            report: AgentReport::Progress(progress.clone()),
        })
        .unwrap();

    let payloads = if fragment_manifest {
        vec![b"pay".to_vec(), b"load".to_vec()]
    } else {
        vec![overrides.payload.unwrap_or_else(|| b"payload".to_vec())]
    };
    let mut next_offset = 0_u64;
    let chunks = payloads
        .into_iter()
        .map(|payload| {
            let size = u64::try_from(payload.len()).unwrap();
            let chunk = ChunkRef::new(ObjectId::for_bytes(&payload), next_offset, size).unwrap();
            next_offset += size;
            chunk
        })
        .collect::<Vec<_>>();
    let file_size = next_offset;
    let chunking = if fragment_manifest {
        ChunkingStrategy::FastCdc
    } else {
        ChunkingStrategy::WholeFile
    };
    let wire_chunking = if fragment_manifest {
        WireChunkingStrategy::FastCdc
    } else {
        WireChunkingStrategy::WholeFile
    };
    let manifest = Manifest::new(file_size, chunking, chunks.clone()).unwrap();
    let manifest_id = manifest.canonical_id().unwrap();
    let object_id = chunks[0].object_id;
    let object_size = chunks[0].size;
    let scope = MetadataBatchScope {
        tenant_id: spec.tenant_id.clone(),
        project_id: spec.project_id.clone(),
        artifact_id: spec.artifact_id.clone(),
        playground_id: spec.playground_id.clone(),
        job_id: spec.job_id.clone(),
        base_index_version: spec.expected_index_version.clone(),
        extensions: Extensions::new(),
    };
    let mut manifest_records = if fragment_manifest {
        chunks
            .iter()
            .enumerate()
            .map(|(index, chunk)| ManifestRecord {
                manifest_id,
                total_size: DecimalU64::new(file_size),
                chunking: wire_chunking,
                chunk_start: DecimalU64::new(u64::try_from(index).unwrap()),
                chunks: vec![WireChunkRef {
                    object_id: chunk.object_id,
                    offset: DecimalU64::new(chunk.offset),
                    size: DecimalU64::new(chunk.size),
                    extensions: Extensions::new(),
                }],
                extensions: Extensions::new(),
            })
            .collect::<Vec<_>>()
    } else {
        vec![ManifestRecord {
            manifest_id,
            total_size: DecimalU64::new(file_size),
            chunking: wire_chunking,
            chunk_start: DecimalU64::new(0),
            chunks: chunks
                .iter()
                .map(|chunk| WireChunkRef {
                    object_id: chunk.object_id,
                    offset: DecimalU64::new(chunk.offset),
                    size: DecimalU64::new(chunk.size),
                    extensions: Extensions::new(),
                })
                .collect(),
            extensions: Extensions::new(),
        }]
    };
    let mut receipt_records = chunks
        .iter()
        .enumerate()
        .map(|(index, chunk)| ObjectReceiptRecord {
            receipt_id: ObjectReceiptId::new(format!("receipt-{index}")).unwrap(),
            tenant_id: spec.tenant_id.clone(),
            artifact_id: spec.artifact_id.clone(),
            job_id: spec.job_id.clone(),
            storage_volume_id: target.storage_volume_id.clone(),
            artifact_placement_id: target.artifact_placement_id.clone(),
            object_id: chunk.object_id,
            size: DecimalU64::new(chunk.size),
            verified_at_unix_ms: UnixMillis::new(300),
            extensions: Extensions::new(),
        })
        .collect::<Vec<_>>();
    if include_unreferenced_manifest {
        let extra_payload = b"unreferenced-payload";
        let extra_object_id = ObjectId::for_bytes(extra_payload);
        let extra_size = extra_payload.len() as u64;
        let extra_manifest = Manifest::new(
            extra_size,
            ChunkingStrategy::WholeFile,
            vec![ChunkRef::new(extra_object_id, 0, extra_size).unwrap()],
        )
        .unwrap();
        manifest_records.push(ManifestRecord {
            manifest_id: extra_manifest.canonical_id().unwrap(),
            total_size: DecimalU64::new(extra_size),
            chunking: WireChunkingStrategy::WholeFile,
            chunk_start: DecimalU64::new(0),
            chunks: vec![WireChunkRef {
                object_id: extra_object_id,
                offset: DecimalU64::new(0),
                size: DecimalU64::new(extra_size),
                extensions: Extensions::new(),
            }],
            extensions: Extensions::new(),
        });
        receipt_records.push(ObjectReceiptRecord {
            receipt_id: ObjectReceiptId::new("receipt-unreferenced").unwrap(),
            tenant_id: spec.tenant_id.clone(),
            artifact_id: spec.artifact_id.clone(),
            job_id: spec.job_id.clone(),
            storage_volume_id: target.storage_volume_id.clone(),
            artifact_placement_id: target.artifact_placement_id.clone(),
            object_id: extra_object_id,
            size: DecimalU64::new(extra_size),
            verified_at_unix_ms: UnixMillis::new(300),
            extensions: Extensions::new(),
        });
    }
    let manifest_batch_id = MetadataBatchId::new("batch-manifest").unwrap();
    let manifest_page_count = u32::try_from(manifest_records.len()).unwrap();
    let manifest_pages = manifest_records
        .into_iter()
        .enumerate()
        .map(|(page_number, record)| {
            MetadataBatchPage::new(
                manifest_batch_id.clone(),
                scope.clone(),
                u32::try_from(page_number).unwrap(),
                manifest_page_count,
                MetadataBatchRecords::Manifest(vec![record]),
                Extensions::new(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let index_page = MetadataBatchPage::new(
        MetadataBatchId::new("batch-index").unwrap(),
        scope.clone(),
        0,
        1,
        MetadataBatchRecords::IndexDelta(vec![IndexDeltaRecord::Upsert {
            path: LogicalPath::parse(index_path).unwrap(),
            manifest_id,
            total_size: DecimalU64::new(file_size),
            chunk_count: DecimalU64::new(u64::try_from(chunks.len()).unwrap()),
            extensions: Extensions::new(),
        }]),
        Extensions::new(),
    )
    .unwrap();
    let receipt_page = MetadataBatchPage::new(
        MetadataBatchId::new("batch-receipt").unwrap(),
        scope.clone(),
        0,
        1,
        MetadataBatchRecords::ObjectReceipt(receipt_records),
        Extensions::new(),
    )
    .unwrap();
    let manifest_descriptor = MetadataBatchDescriptor::from_pages(
        manifest_batch_id,
        scope.clone(),
        &manifest_pages,
        Extensions::new(),
    )
    .unwrap();
    let index_descriptor = MetadataBatchDescriptor::from_pages(
        index_page.batch_id.clone(),
        scope.clone(),
        std::slice::from_ref(&index_page),
        Extensions::new(),
    )
    .unwrap();
    let receipt_descriptor = MetadataBatchDescriptor::from_pages(
        receipt_page.batch_id.clone(),
        scope.clone(),
        std::slice::from_ref(&receipt_page),
        Extensions::new(),
    )
    .unwrap();
    let descriptors = vec![manifest_descriptor, index_descriptor, receipt_descriptor];
    let mut pages = manifest_pages;
    pages.push(index_page);
    pages.push(receipt_page);
    let result_record = FileRecord::new(
        LogicalPath::parse(index_path).unwrap(),
        manifest_id,
        file_size,
        u64::try_from(chunks.len()).unwrap(),
    )
    .unwrap();
    let result_index_digest = overrides.result_index_digest.unwrap_or_else(|| {
        IndexVersion::from_snapshot(1, &[result_record])
            .unwrap()
            .digest
    });
    let computed_publication_digest = MetadataPublication::from_pages(&pages)
        .and_then(|publication| publication.publication_digest(&scope, result_index_digest))
        .unwrap_or_else(|_| ContentDigest::from_bytes([0x55; 32]));
    let publication_digest = overrides
        .publication_digest
        .unwrap_or(computed_publication_digest);
    let prepared = JobPrepared::new(
        spec.job_id.clone(),
        target.assignment_id.clone(),
        target.assignment_generation,
        spec.expected_index_version.clone(),
        result_index_digest,
        publication_digest,
        descriptors.clone(),
        Extensions::new(),
    )
    .unwrap();
    control
        .receive_report(ReceiveReportRequest {
            tenant_id: spec.tenant_id.clone(),
            agent_id: target.agent_id.clone(),
            report: AgentReport::Prepared(prepared.clone()),
        })
        .unwrap();

    Scenario {
        components,
        control,
        actor,
        spec,
        target,
        accepted,
        progress,
        prepared,
        descriptors,
        pages,
        object_id,
        object_size,
    }
}

fn inputs(components: &InMemoryComponents) -> (PrincipalRef, AddJobSpec, AssignmentTarget) {
    let actor = PrincipalRef {
        kind: PrincipalKind::User,
        id: PrincipalId::new("user-a").unwrap(),
        extensions: Extensions::new(),
    };
    let tenant_id = TenantId::new("tenant-a").unwrap();
    let artifact_id = ArtifactId::new("artifact-a").unwrap();
    let playground_id = PlaygroundId::new("playground-a").unwrap();
    let expected_index_version = components
        .publisher
        .current_version(&IndexKey {
            tenant_id: tenant_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: playground_id.clone(),
        })
        .unwrap();
    let mut spec = AddJobSpec {
        job_id: neoengram_protocol::JobId::new("job-a").unwrap(),
        principal: actor.clone(),
        tenant_id,
        project_id: ProjectId::new("project-a").unwrap(),
        artifact_id,
        playground_id,
        expected_index_version,
        request_digest: ContentDigest::from_bytes([0; 32]),
        deadline_unix_ms: UnixMillis::new(10_000),
        paths: vec![LogicalPath::parse("dataset/file.bin").unwrap()],
        all: false,
        extensions: Extensions::new(),
    };
    spec.request_digest = spec.computed_request_digest().unwrap();
    let target = AssignmentTarget {
        assignment_id: AssignmentId::new("assignment-a").unwrap(),
        assignment_generation: AssignmentGeneration::new(1),
        agent_id: AgentId::new("agent-a").unwrap(),
        edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
        storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
        artifact_placement_id: ArtifactPlacementId::new("placement-a").unwrap(),
        placement_generation: PlacementGeneration::new(1),
        agent_mount_id: AgentMountId::new("mount-a").unwrap(),
        mount_generation: MountGeneration::new(1),
        owner_generation: OwnerGeneration::new(1),
        lease: None,
    };
    (actor, spec, target)
}
