#![cfg(feature = "authority-sqlite")]

use std::{path::Path, sync::Arc};

use neoengram_core::{
    ChunkRef, ChunkingStrategy, ContentDigest, FileRecord, IndexVersion, LogicalPath, Manifest,
    ManifestId, ObjectId,
};
use neoengram_protocol::{
    AgentId, AgentMountId, ArtifactId, ArtifactPlacementId, AssignmentGeneration, AssignmentId,
    AssignmentOperation, DecimalU64, DurabilityState, EdgeClusterId, Extensions, IndexDeltaRecord,
    JobPrepared, JobState, MetadataBatchDescriptor, MetadataBatchId, MetadataBatchPage,
    MetadataBatchRecords, MetadataBatchScope, MountGeneration, ObjectDurabilityReceipt,
    OwnerGeneration, PlacementGeneration, PlaygroundId, PrincipalId, PrincipalKind, PrincipalRef,
    ProjectId, ResourceVersion, SessionId, StorageVolumeId, TenantId, UnixMillis, WireIndexVersion,
    WireObjectSpec,
};
use neoengramd::{
    open_sqlite_authority, AddJobSpec, AllowAllAuthorizer, AssignJobRequest, AssignmentTarget,
    AuditEvent, AuditKind, AuthorityCapabilities, AuthorityStore, CentralErrorCode, ControlPlane,
    CreateAddJobRequest, FinalizeAddRequest, InMemoryComponents, IndexKey, IndexPublishOutcome,
    IndexPublishRejection, IndexPublishRequest, JobKey, JobRecord, PublicationCandidate,
    SqliteAuthorityConfig,
};
use sqlx::{sqlite::SqliteConnectOptions, Connection, SqliteConnection};
use tempfile::TempDir;

struct ContractFixture {
    job_key: JobKey,
    index_key: IndexKey,
    batch_id: MetadataBatchId,
    object_id: ObjectId,
    manifest_id: ManifestId,
    assignment: neoengram_protocol::JobAssignment,
    reserved_assignment: neoengram_protocol::JobAssignment,
    published_version: WireIndexVersion,
    publishing_job: JobRecord,
}

#[tokio::test]
async fn authority_contract_runs_against_in_memory_and_sqlite() {
    let memory = InMemoryComponents::new(100);
    let memory_fixture = run_contract(memory.authority_store()).await;
    assert_contract_state(&memory.authority_store(), &memory_fixture).await;

    let directory = TempDir::new().unwrap();
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    assert_eq!(
        authority.authority_store().capabilities(),
        AuthorityCapabilities::SQLITE
    );
    let fixture = run_contract(authority.authority_store()).await;
    assert_contract_state(&authority.authority_store(), &fixture).await;
    authority.integrity_check().await.unwrap();
}

#[tokio::test]
async fn sqlite_reopen_recovers_every_authority_port() {
    let directory = TempDir::new().unwrap();
    let fixture = {
        let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
            .await
            .unwrap();
        let fixture = run_contract(authority.authority_store()).await;
        assert_eq!(authority.published_assignments().await.unwrap().len(), 1);
        assert!(authority.audit_events().await.unwrap().len() >= 3);
        fixture
    };

    let reopened = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    assert_contract_state(&reopened.authority_store(), &fixture).await;
    assert_eq!(
        reopened.published_assignments().await.unwrap(),
        vec![fixture.assignment.clone()]
    );
    assert!(matches!(
        reopened
            .authority_store()
            .outbox()
            .publish(fixture.reserved_assignment.clone())
            .await
            .unwrap(),
        neoengramd::AssignmentPublishOutcome::Published
    ));
    assert_eq!(reopened.published_assignments().await.unwrap().len(), 2);
    assert!(reopened.audit_events().await.unwrap().len() >= 3);
    assert_eq!(
        reopened
            .manifest(
                &fixture.index_key.tenant_id,
                &fixture.index_key.artifact_id,
                fixture.manifest_id,
            )
            .await
            .unwrap()
            .unwrap()
            .canonical_id()
            .unwrap(),
        fixture.manifest_id
    );
}

#[tokio::test]
async fn tenant_scoped_ids_are_isolated_in_both_backends() {
    let memory = InMemoryComponents::new(100);
    assert_tenant_isolation(memory.authority_store()).await;

    let directory = TempDir::new().unwrap();
    let sqlite = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    assert_tenant_isolation(sqlite.authority_store()).await;
}

#[tokio::test]
async fn sqlite_allows_only_one_authority_instance() {
    let directory = TempDir::new().unwrap();
    let first = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let error = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .err()
        .expect("second authority must fail while the first lock is alive");
    assert_eq!(error.code(), CentralErrorCode::StorageFailure);
    drop(first);
    open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
}

#[tokio::test]
async fn sqlite_rejects_wrong_application_id_and_user_version() {
    let app_id_directory = TempDir::new().unwrap();
    initialize_and_close(app_id_directory.path()).await;
    execute_raw(app_id_directory.path(), "PRAGMA application_id = 305419896").await;
    assert_open_storage_failure(app_id_directory.path()).await;

    let version_directory = TempDir::new().unwrap();
    initialize_and_close(version_directory.path()).await;
    execute_raw(version_directory.path(), "PRAGMA user_version = 2").await;
    assert_open_storage_failure(version_directory.path()).await;
}

#[tokio::test]
async fn sqlite_rejects_unknown_nonempty_and_symbolic_link_databases() {
    let unknown = TempDir::new().unwrap();
    std::fs::write(
        unknown.path().join("authority.sqlite3"),
        b"not a sqlite database",
    )
    .unwrap();
    assert_open_storage_failure(unknown.path()).await;

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let directory = TempDir::new().unwrap();
        let target = directory.path().join("target.sqlite3");
        std::fs::write(&target, []).unwrap();
        symlink(&target, directory.path().join("authority.sqlite3")).unwrap();
        assert_open_storage_failure(directory.path()).await;
    }
}

#[tokio::test]
async fn sqlite_reports_corrupt_json_and_decimal_records() {
    let json_directory = TempDir::new().unwrap();
    let fixture = {
        let authority = open_sqlite_authority(SqliteAuthorityConfig::new(json_directory.path()))
            .await
            .unwrap();
        run_contract(authority.authority_store()).await
    };
    execute_raw(
        json_directory.path(),
        "UPDATE control_jobs SET payload = X'7b7d'",
    )
    .await;
    let reopened = open_sqlite_authority(SqliteAuthorityConfig::new(json_directory.path()))
        .await
        .unwrap();
    let error = reopened
        .authority_store()
        .jobs()
        .get(&fixture.job_key)
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::StorageFailure);
    drop(reopened);

    let decimal_directory = TempDir::new().unwrap();
    initialize_and_close(decimal_directory.path()).await;
    execute_raw(
        decimal_directory.path(),
        "PRAGMA ignore_check_constraints = ON; \
         INSERT INTO playground_indexes \
         (tenant_id, artifact_id, playground_id, revision, digest) \
         VALUES ('tenant-a', 'artifact-a', 'playground-a', '00', zeroblob(32))",
    )
    .await;
    let reopened = open_sqlite_authority(SqliteAuthorityConfig::new(decimal_directory.path()))
        .await
        .unwrap();
    let error = reopened
        .authority_store()
        .publisher()
        .current_version(&index_key("tenant-a"))
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::StorageFailure);
}

#[tokio::test]
async fn sqlite_foreign_key_check_rejects_dangling_rows() {
    let directory = TempDir::new().unwrap();
    initialize_and_close(directory.path()).await;
    execute_raw(
        directory.path(),
        "PRAGMA foreign_keys = OFF; \
         INSERT INTO metadata_batch_pages \
         (tenant_id, batch_id, page_number, payload) \
         VALUES ('tenant-a', 'missing', 0, X'7b7d')",
    )
    .await;
    assert_open_storage_failure(directory.path()).await;
}

#[tokio::test]
async fn sqlite_cas_has_one_winner_for_one_expected_version() {
    let directory = TempDir::new().unwrap();
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let store = authority.authority_store();
    let key = index_key("tenant-a");
    let expected = store.publisher().current_version(&key).await.unwrap();
    let first = publication("tenant-a", "job-cas-a", &key, expected.clone());
    let second = publication("tenant-a", "job-cas-b", &key, expected);
    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let first_task = {
        let publisher = store.publisher();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            publisher.compare_and_swap(first).await.unwrap()
        })
    };
    let second_task = {
        let publisher = store.publisher();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            publisher.compare_and_swap(second).await.unwrap()
        })
    };
    let outcomes = [first_task.await.unwrap(), second_task.await.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, IndexPublishOutcome::Published(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, IndexPublishOutcome::Conflict(_)))
            .count(),
        1
    );
    assert_eq!(
        authority.published_index(&key).await.unwrap().records.len(),
        1
    );
}

#[tokio::test]
async fn sqlite_concurrent_create_and_assign_are_idempotent() {
    const CALLERS: usize = 8;

    let directory = TempDir::new().unwrap();
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let store = authority.authority_store();
    let control = Arc::new(ControlPlane::new(
        Arc::new(AllowAllAuthorizer),
        store,
        Arc::new(neoengramd::InMemoryClock::new(100)),
    ));
    let spec = job_spec("tenant-a", "job-concurrent");
    let barrier = Arc::new(tokio::sync::Barrier::new(CALLERS));
    let mut tasks = Vec::new();
    for _ in 0..CALLERS {
        let control = control.clone();
        let spec = spec.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            control
                .create_add_job(CreateAddJobRequest {
                    actor: spec.principal.clone(),
                    spec,
                })
                .await
                .unwrap()
        }));
    }
    let mut create_replays = 0;
    for task in tasks {
        create_replays += usize::from(task.await.unwrap().replayed);
    }
    assert_eq!(create_replays, CALLERS - 1);

    let barrier = Arc::new(tokio::sync::Barrier::new(CALLERS));
    let mut tasks = Vec::new();
    for _ in 0..CALLERS {
        let control = control.clone();
        let spec = spec.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            control
                .assign_job(AssignJobRequest {
                    actor: spec.principal.clone(),
                    tenant_id: spec.tenant_id,
                    job_id: spec.job_id,
                    target: assignment_target(),
                })
                .await
                .unwrap()
        }));
    }
    let mut assign_replays = 0;
    for task in tasks {
        assign_replays += usize::from(task.await.unwrap().replayed);
    }
    assert_eq!(assign_replays, CALLERS - 1);
    assert_eq!(authority.published_assignments().await.unwrap().len(), 1);
}

#[tokio::test]
async fn sqlite_concurrent_finalize_converges_from_a_publishing_candidate() {
    let directory = TempDir::new().unwrap();
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let store = authority.authority_store();
    let fixture = run_contract(store.clone()).await;
    let control = Arc::new(ControlPlane::new(
        Arc::new(AllowAllAuthorizer),
        store,
        Arc::new(neoengramd::InMemoryClock::new(600)),
    ));
    let request = FinalizeAddRequest {
        actor: fixture.publishing_job.spec.principal.clone(),
        tenant_id: fixture.job_key.tenant_id.clone(),
        job_id: fixture.job_key.job_id.clone(),
    };
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let mut tasks = Vec::new();
    for _ in 0..2 {
        let control = control.clone();
        let request = request.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            control.finalize_add(request).await
        }));
    }
    let mut successes = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(result) => {
                successes += 1;
                assert_eq!(result.job.state, JobState::Succeeded);
            }
            Err(error) => assert!(matches!(
                error.code(),
                CentralErrorCode::ConcurrentUpdate | CentralErrorCode::InvalidState
            )),
        }
    }
    assert_eq!(successes, 1);
    let replay = control.finalize_add(request).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.job.state, JobState::Succeeded);
}

#[tokio::test]
async fn sqlite_u64_max_revision_replays_exhaustion() {
    let directory = TempDir::new().unwrap();
    initialize_and_close(directory.path()).await;
    let empty_digest = IndexVersion::from_snapshot(u64::MAX, &[]).unwrap().digest;
    let sql = format!(
        "INSERT INTO playground_indexes \
         (tenant_id, artifact_id, playground_id, revision, digest) \
         VALUES ('tenant-a', 'artifact-a', 'playground-a', '{}', X'{}')",
        u64::MAX,
        empty_digest
    );
    execute_raw(directory.path(), &sql).await;

    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let store = authority.authority_store();
    let key = index_key("tenant-a");
    let expected = store.publisher().current_version(&key).await.unwrap();
    assert_eq!(expected.revision.get(), u64::MAX);
    let request = publication("tenant-a", "job-max", &key, expected);
    let first = store
        .publisher()
        .compare_and_swap(request.clone())
        .await
        .unwrap();
    let replay = store.publisher().compare_and_swap(request).await.unwrap();
    assert_eq!(
        first,
        IndexPublishOutcome::Rejected(IndexPublishRejection::RevisionExhausted)
    );
    assert_eq!(replay, first);
}

async fn run_contract(store: AuthorityStore) -> ContractFixture {
    assert!(store.capabilities().single_process);
    assert!(!store.capabilities().database_enforced_tenant_isolation);
    assert!(!store.capabilities().high_availability);

    let spec = job_spec("tenant-a", "job-a");
    let job_key = JobKey::new(spec.tenant_id.clone(), spec.job_id.clone());
    let index_key = IndexKey {
        tenant_id: spec.tenant_id.clone(),
        artifact_id: spec.artifact_id.clone(),
        playground_id: spec.playground_id.clone(),
    };
    let control = ControlPlane::new(
        Arc::new(AllowAllAuthorizer),
        store.clone(),
        Arc::new(neoengramd::InMemoryClock::new(100)),
    );
    assert!(
        !control
            .create_add_job(CreateAddJobRequest {
                actor: spec.principal.clone(),
                spec: spec.clone(),
            })
            .await
            .unwrap()
            .replayed
    );
    assert!(
        control
            .create_add_job(CreateAddJobRequest {
                actor: spec.principal.clone(),
                spec: spec.clone(),
            })
            .await
            .unwrap()
            .replayed
    );
    let assigned = control
        .assign_job(AssignJobRequest {
            actor: spec.principal.clone(),
            tenant_id: spec.tenant_id.clone(),
            job_id: spec.job_id.clone(),
            target: assignment_target(),
        })
        .await
        .unwrap();
    assert!(
        control
            .assign_job(AssignJobRequest {
                actor: spec.principal.clone(),
                tenant_id: spec.tenant_id.clone(),
                job_id: spec.job_id.clone(),
                target: assignment_target(),
            })
            .await
            .unwrap()
            .replayed
    );
    let mut reserved_assignment = assigned.assignment.clone();
    let AssignmentOperation::Add { input, .. } = &mut reserved_assignment.assignment;
    input.assignment_id = AssignmentId::new("assignment-reserved").unwrap();
    assert!(matches!(
        store
            .outbox()
            .reserve(reserved_assignment.clone())
            .await
            .unwrap(),
        neoengramd::AssignmentReserveOutcome::Reserved
    ));
    assert!(matches!(
        store
            .outbox()
            .reserve(reserved_assignment.clone())
            .await
            .unwrap(),
        neoengramd::AssignmentReserveOutcome::Existing
    ));

    let (descriptor, page) = metadata_batch(&spec);
    assert!(!store
        .metadata()
        .stage_descriptor(descriptor.clone())
        .await
        .unwrap());
    assert!(store
        .metadata()
        .stage_descriptor(descriptor.clone())
        .await
        .unwrap());
    assert!(!store
        .metadata()
        .stage_page(&descriptor, page.clone())
        .await
        .unwrap());
    assert!(store
        .metadata()
        .stage_page(&descriptor, page)
        .await
        .unwrap());

    let object_id = ObjectId::for_bytes(b"payload");
    store
        .objects()
        .record_durability(&durability_receipt(&spec, object_id, u64::MAX))
        .await
        .unwrap();
    store
        .objects()
        .record_durability(&durability_receipt(&spec, object_id, u64::MAX))
        .await
        .unwrap();

    let request = publication(
        "tenant-a",
        "job-a",
        &index_key,
        spec.expected_index_version.clone(),
    );
    let manifest_id = request.manifests[0].canonical_id().unwrap();
    let outcome = store
        .publisher()
        .compare_and_swap(request.clone())
        .await
        .unwrap();
    let IndexPublishOutcome::Published(published_version) = outcome else {
        panic!("empty Index CAS must publish");
    };
    assert_eq!(
        store.publisher().compare_and_swap(request).await.unwrap(),
        IndexPublishOutcome::Published(published_version.clone())
    );

    let event = AuditEvent {
        event_id: "manual-event".to_owned(),
        kind: AuditKind::MetadataStaged,
        job_key: job_key.clone(),
        state: JobState::Assigned,
        occurred_at_unix_ms: UnixMillis::new(500),
    };
    assert!(!store.audit().record(event.clone()).await.unwrap());
    let mut replay = event;
    replay.occurred_at_unix_ms = UnixMillis::new(999);
    assert!(store.audit().record(replay).await.unwrap());

    let mut publishing_job = store.jobs().get(&job_key).await.unwrap().unwrap();
    let previous = publishing_job.resource_version.get();
    publishing_job.resource_version = ResourceVersion::new(previous + 1);
    publishing_job.state = JobState::Publishing;
    let assignment = publishing_job.assignment.as_ref().unwrap();
    let publication_digest = ContentDigest::hash(b"publication-candidate");
    publishing_job.prepared = Some(JobPrepared {
        job_id: publishing_job.spec.job_id.clone(),
        assignment_id: assignment.assignment_id.clone(),
        assignment_generation: assignment.assignment_generation,
        base_index_version: spec.expected_index_version.clone(),
        result_index_digest: published_version.digest,
        publication_digest,
        candidate_digest: ContentDigest::hash(b"candidate-report"),
        metadata_batches: Vec::new(),
        extensions: Extensions::new(),
    });
    publishing_job.publication_candidate = Some(PublicationCandidate {
        expected_index_version: spec.expected_index_version.clone(),
        result_index_digest: published_version.digest,
        publication_digest,
        manifests: publication_manifests(),
        mutations: publication_mutations(),
    });
    store
        .jobs()
        .replace(previous, publishing_job.clone())
        .await
        .unwrap();

    ContractFixture {
        job_key,
        index_key,
        batch_id: descriptor.batch_id,
        object_id,
        manifest_id,
        assignment: assigned.assignment,
        reserved_assignment,
        published_version,
        publishing_job,
    }
}

async fn assert_contract_state(store: &AuthorityStore, fixture: &ContractFixture) {
    assert_eq!(
        store.jobs().get(&fixture.job_key).await.unwrap(),
        Some(fixture.publishing_job.clone())
    );
    let batch = store
        .metadata()
        .get(&fixture.job_key.tenant_id, &fixture.batch_id)
        .await
        .unwrap()
        .unwrap();
    assert!(batch.is_complete());
    let durable = store
        .objects()
        .durable_object(
            &fixture.job_key.tenant_id,
            &fixture.index_key.artifact_id,
            fixture.object_id,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(durable.size, u64::MAX);
    assert_eq!(
        store
            .publisher()
            .current_version(&fixture.index_key)
            .await
            .unwrap(),
        fixture.published_version
    );
}

async fn assert_tenant_isolation(store: AuthorityStore) {
    for tenant in ["tenant-a", "tenant-b"] {
        let spec = job_spec(tenant, "shared-job");
        let job = queued_job(spec.clone());
        store.jobs().insert_or_load(job).await.unwrap();
        let control = ControlPlane::new(
            Arc::new(AllowAllAuthorizer),
            store.clone(),
            Arc::new(neoengramd::InMemoryClock::new(100)),
        );
        control
            .assign_job(AssignJobRequest {
                actor: spec.principal.clone(),
                tenant_id: spec.tenant_id.clone(),
                job_id: spec.job_id.clone(),
                target: assignment_target(),
            })
            .await
            .unwrap();

        let (descriptor, page) = metadata_batch(&spec);
        store
            .metadata()
            .stage_descriptor(descriptor.clone())
            .await
            .unwrap();
        store
            .metadata()
            .stage_page(&descriptor, page)
            .await
            .unwrap();
        let object_id = ObjectId::for_bytes(b"shared-object");
        store
            .objects()
            .record_durability(&durability_receipt(&spec, object_id, 13))
            .await
            .unwrap();

        let key = index_key(tenant);
        let request = publication(
            tenant,
            "shared-publication",
            &key,
            spec.expected_index_version.clone(),
        );
        assert!(matches!(
            store.publisher().compare_and_swap(request).await.unwrap(),
            IndexPublishOutcome::Published(_)
        ));

        let event = AuditEvent {
            event_id: "shared-event".to_owned(),
            kind: AuditKind::JobCreated,
            job_key: JobKey::new(
                TenantId::new(tenant).unwrap(),
                neoengram_protocol::JobId::new("shared-job").unwrap(),
            ),
            state: JobState::Queued,
            occurred_at_unix_ms: UnixMillis::new(1),
        };
        assert!(!store.audit().record(event).await.unwrap());
    }
    let first = store
        .jobs()
        .get(&JobKey::new(
            TenantId::new("tenant-a").unwrap(),
            neoengram_protocol::JobId::new("shared-job").unwrap(),
        ))
        .await
        .unwrap()
        .unwrap();
    let second = store
        .jobs()
        .get(&JobKey::new(
            TenantId::new("tenant-b").unwrap(),
            neoengram_protocol::JobId::new("shared-job").unwrap(),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(first.spec.tenant_id, second.spec.tenant_id);
    for tenant in ["tenant-a", "tenant-b"] {
        let tenant_id = TenantId::new(tenant).unwrap();
        assert!(store
            .metadata()
            .get(&tenant_id, &MetadataBatchId::new("batch-a").unwrap())
            .await
            .unwrap()
            .unwrap()
            .is_complete());
        assert!(store
            .objects()
            .durable_object(
                &tenant_id,
                &ArtifactId::new("artifact-a").unwrap(),
                ObjectId::for_bytes(b"shared-object"),
            )
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            store
                .publisher()
                .current_version(&index_key(tenant))
                .await
                .unwrap()
                .revision
                .get(),
            1
        );
    }
}

fn queued_job(spec: AddJobSpec) -> JobRecord {
    JobRecord {
        spec,
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
    }
}

fn job_spec(tenant: &str, job: &str) -> AddJobSpec {
    let principal = PrincipalRef {
        kind: PrincipalKind::User,
        id: PrincipalId::new("user-a").unwrap(),
        extensions: Extensions::new(),
    };
    let mut spec = AddJobSpec {
        job_id: neoengram_protocol::JobId::new(job).unwrap(),
        principal,
        tenant_id: TenantId::new(tenant).unwrap(),
        project_id: ProjectId::new("project-a").unwrap(),
        artifact_id: ArtifactId::new("artifact-a").unwrap(),
        playground_id: PlaygroundId::new("playground-a").unwrap(),
        expected_index_version: WireIndexVersion::from(
            IndexVersion::from_snapshot(0, &[]).unwrap(),
        ),
        request_digest: ContentDigest::from_bytes([0; 32]),
        deadline_unix_ms: UnixMillis::new(10_000),
        paths: vec![LogicalPath::parse("dataset/file.bin").unwrap()],
        all: false,
        extensions: Extensions::new(),
    };
    spec.request_digest = spec.computed_request_digest().unwrap();
    spec
}

fn assignment_target() -> AssignmentTarget {
    AssignmentTarget {
        assignment_id: AssignmentId::new("assignment-a").unwrap(),
        assignment_generation: AssignmentGeneration::new(1),
        agent_id: AgentId::new("agent-a").unwrap(),
        edge_cluster_id: EdgeClusterId::new("edge-a").unwrap(),
        storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
        artifact_placement_id: ArtifactPlacementId::new("placement-a").unwrap(),
        placement_generation: PlacementGeneration::new(1),
        agent_mount_id: AgentMountId::new("mount-a").unwrap(),
        mount_generation: MountGeneration::new(1),
        owner_generation: OwnerGeneration::new(1),
        lease: None,
    }
}

fn metadata_batch(spec: &AddJobSpec) -> (MetadataBatchDescriptor, MetadataBatchPage) {
    let batch_id = MetadataBatchId::new("batch-a").unwrap();
    let scope = MetadataBatchScope {
        tenant_id: spec.tenant_id.clone(),
        project_id: spec.project_id.clone(),
        artifact_id: spec.artifact_id.clone(),
        playground_id: spec.playground_id.clone(),
        job_id: spec.job_id.clone(),
        base_index_version: spec.expected_index_version.clone(),
        extensions: Extensions::new(),
    };
    let page = MetadataBatchPage::new(
        batch_id.clone(),
        scope.clone(),
        0,
        1,
        MetadataBatchRecords::IndexDelta(vec![IndexDeltaRecord::Delete {
            path: LogicalPath::parse("old.bin").unwrap(),
            extensions: Extensions::new(),
        }]),
        Extensions::new(),
    )
    .unwrap();
    let descriptor = MetadataBatchDescriptor::from_pages(
        batch_id,
        scope,
        std::slice::from_ref(&page),
        Extensions::new(),
    )
    .unwrap();
    (descriptor, page)
}

fn durability_receipt(
    spec: &AddJobSpec,
    object_id: ObjectId,
    size: u64,
) -> ObjectDurabilityReceipt {
    ObjectDurabilityReceipt {
        tenant_id: spec.tenant_id.clone(),
        artifact_id: spec.artifact_id.clone(),
        session_id: SessionId::new("session-a").unwrap(),
        job_id: spec.job_id.clone(),
        object: WireObjectSpec {
            object_id,
            size: DecimalU64::new(size),
            extensions: Extensions::new(),
        },
        state: DurabilityState::Durable {
            verified_digest: object_id.digest(),
            storage_version: "storage-version-a".to_owned(),
            extensions: Extensions::new(),
        },
        checked_at_unix_ms: UnixMillis::new(200),
        extensions: Extensions::new(),
    }
}

fn index_key(tenant: &str) -> IndexKey {
    IndexKey {
        tenant_id: TenantId::new(tenant).unwrap(),
        artifact_id: ArtifactId::new("artifact-a").unwrap(),
        playground_id: PlaygroundId::new("playground-a").unwrap(),
    }
}

fn publication(
    tenant: &str,
    job: &str,
    key: &IndexKey,
    expected: WireIndexVersion,
) -> IndexPublishRequest {
    let records = vec![FileRecord::from_manifest(
        LogicalPath::parse("dataset/file.bin").unwrap(),
        &publication_manifests()[0],
    )
    .unwrap()];
    IndexPublishRequest {
        job_key: JobKey::new(
            TenantId::new(tenant).unwrap(),
            neoengram_protocol::JobId::new(job).unwrap(),
        ),
        index_key: key.clone(),
        expected_index_version: expected,
        expected_result_digest: IndexVersion::from_snapshot(1, &records).unwrap().digest,
        manifests: publication_manifests(),
        mutations: publication_mutations(),
    }
}

fn publication_manifests() -> Vec<Manifest> {
    let object_id = ObjectId::for_bytes(b"payload");
    vec![Manifest::new(
        7,
        ChunkingStrategy::WholeFile,
        vec![ChunkRef::new(object_id, 0, 7).unwrap()],
    )
    .unwrap()]
}

fn publication_mutations() -> Vec<IndexDeltaRecord> {
    let manifest = publication_manifests().remove(0);
    vec![IndexDeltaRecord::Upsert {
        path: LogicalPath::parse("dataset/file.bin").unwrap(),
        manifest_id: manifest.canonical_id().unwrap(),
        total_size: DecimalU64::new(manifest.total_size),
        chunk_count: DecimalU64::new(manifest.chunk_count().unwrap()),
        extensions: Extensions::new(),
    }]
}

async fn initialize_and_close(path: &Path) {
    open_sqlite_authority(SqliteAuthorityConfig::new(path))
        .await
        .unwrap();
}

async fn assert_open_storage_failure(path: &Path) {
    let error = open_sqlite_authority(SqliteAuthorityConfig::new(path))
        .await
        .err()
        .expect("invalid authority database must be rejected");
    assert_eq!(error.code(), CentralErrorCode::StorageFailure);
}

async fn execute_raw(root: &Path, sql: &str) {
    let options = SqliteConnectOptions::new().filename(root.join("authority.sqlite3"));
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::raw_sql(sql).execute(&mut connection).await.unwrap();
    connection.close().await.unwrap();
}
