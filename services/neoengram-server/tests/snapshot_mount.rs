use std::{
    fs,
    path::Path,
    sync::{atomic::AtomicBool, Arc},
};

use neoengram_agentd::{AgentRequestSigner, AgentSessionFence};
use neoengram_core::{
    ChunkRef, ChunkingStrategy, Commit, CommitId, ContentDigest, DirectoryId, FileRecord,
    IndexVersion, LogicalPath, Manifest, ObjectId,
};
use neoengram_protocol::{
    AgentActionAcceptedResponse, AgentBootId, AgentBootstrapProbe, AgentBootstrapProof,
    AgentBootstrapRequest, AgentEnrollmentApprovalRequest, AgentEnrollmentDecision,
    AgentEnrollmentId, AgentEnrollmentTokenCreateRequest, AgentEnrollmentTokenId, AgentHeartbeat,
    AgentHeartbeatReportPayload, AgentHeartbeatReportResponse, AgentId, AgentIndexPageQueryPayload,
    AgentIndexPageQueryResponse, AgentInstallationId, AgentJobReportCreatePayload,
    AgentManifestPageQueryPayload, AgentManifestPageQueryResponse, AgentMessageListQueryPayload,
    AgentMessageListQueryResponse, AgentMountId, AgentMountIdentityDigest, AgentMountStatusReport,
    AgentSessionOpenPayload, AgentSessionOpenResponse, AssignmentGeneration, AssignmentId,
    AssignmentOperation, ControlEnvelope, ControlMessage, DecimalU64, DecisionGeneration,
    Ed25519PublicKeySpki, Ed25519Signature, EdgeClusterId, Extensions, IndexDeltaRecord,
    JobAccepted, JobDecision, JobId, JobProgress, JobState, MessageId, MountAccessMode,
    PrincipalId, PrincipalKind, PrincipalRef, ProtocolVersion, PublishDecision, PvcIdentityDigest,
    RequestId, ResourceHealth, ResourceVersion, SequenceNumber, SessionGeneration, SnapshotId,
    StorageVolumeId, TenantId, TraceId, UnixMillis, VolumeMarkerId, WireIndexVersion,
    AGENT_JOB_INDEX_PAGE_QUERY_PATH, AGENT_JOB_MANIFEST_PAGE_QUERY_PATH,
    AGENT_JOB_REPORT_CREATE_PATH, AGENT_SESSION_HEARTBEAT_REPORT_PATH,
    AGENT_SESSION_MESSAGE_LIST_QUERY_PATH, AGENT_SESSION_OPEN_PATH, PROTOCOL_VERSION_V1,
};
use neoengram_server::{
    agent_transport::AgentAction,
    dto::{CreateSnapshotRequest, JsonExtensions, QuerySnapshotRequest},
    AgentApiHandler, AgentDataPlaneService, AuthenticatedIdentity, CatalogService, JobCoordinator,
    Permission, RegistryAgentApiHandler, StaticRbacPolicy,
};
use neoengramd::{
    open_sqlite_authority, AdvancePlaygroundCommitRequest, AgentRegistryService,
    AllowAllAuthorizer, ArtifactInitialization, ArtifactRecord, BootstrapTokenMetadata,
    CommitRecord, ControlPlane, CreateStorageEnrollmentIntentRequest, FrozenPvcReference,
    FrozenStorageDescriptor, InMemoryClock, IndexKey, IndexPublishOutcome, IndexPublishRequest,
    IndexPublisher, JobKey, JobOperation, JobRecord, PlaygroundRecord, PlaygroundState,
    PreCommitCommitRequest, PreCommitId, PreCommitKey, PreCommitRepository, PreCommitStartRequest,
    PublishedIndex, SqliteAuthorityConfig, StorageEnrollmentAccessMode, TenantRecord,
};
use ring::{
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair as _},
};
use serde::{de::DeserializeOwned, Serialize};
use tempfile::TempDir;

const NOW_MS: u64 = 10_000;
const TENANT: &str = "tenant-snapshot-e2e";
const PROJECT: &str = "project-snapshot-e2e";
const ARTIFACT: &str = "artifact-snapshot-e2e";
const PLAYGROUND: &str = "playground-snapshot-e2e";
const VOLUME: &str = "volume-snapshot-e2e";
const AGENT: &str = "agent-snapshot-e2e";
const INSTALLATION: &str = "installation-snapshot-e2e";
const BOOT: &str = "boot-snapshot-e2e";
const MOUNT: &str = "mount-snapshot-e2e";
const ENROLLMENT: &str = "enrollment-snapshot-e2e";
const BOOTSTRAP_TOKEN: &str = "snapshot-e2e-bootstrap-token-at-least-32-bytes";
const HISTORICAL_CHUNK_A: &[u8] = b"historical-volume-only-chunk-A:";
const HISTORICAL_CHUNK_B: &[u8] = b"historical-volume-only-chunk-B";
const CURRENT_CHUNK: &[u8] = b"current-head-volume-only-chunk";

#[tokio::test]
async fn historical_artifact_commit_mounts_as_a_ready_snapshot_without_server_chunk_bytes() {
    let authority_dir = TempDir::new().unwrap();
    let volume_dir = TempDir::new().unwrap();
    let authority = Arc::new(
        open_sqlite_authority(SqliteAuthorityConfig::new(authority_dir.path()))
            .await
            .unwrap(),
    );
    let store = authority.authority_store();
    let clock = Arc::new(InMemoryClock::new(NOW_MS));
    let tenant_id = TenantId::new(TENANT).unwrap();
    let project_id = neoengram_protocol::ProjectId::new(PROJECT).unwrap();
    let artifact_id = neoengram_protocol::ArtifactId::new(ARTIFACT).unwrap();
    let playground_id = neoengram_protocol::PlaygroundId::new(PLAYGROUND).unwrap();
    let storage_volume_id = StorageVolumeId::new(VOLUME).unwrap();

    let catalog = store.control_catalog().unwrap();
    catalog
        .insert_tenant(TenantRecord {
            tenant_id: tenant_id.clone(),
            display_name: "Snapshot E2E tenant".to_owned(),
            description: None,
            resource_version: 1,
            created_at_unix_ms: UnixMillis::new(NOW_MS),
            updated_at_unix_ms: UnixMillis::new(NOW_MS),
        })
        .await
        .unwrap();

    let signing_key = signing_key();
    let registry =
        Arc::new(AgentRegistryService::from_authority(&store, clock.clone(), 30_000).unwrap());
    let approved = enroll_agent(&registry, &signing_key).await;

    let control = Arc::new(ControlPlane::new(
        Arc::new(AllowAllAuthorizer),
        store.clone(),
        clock.clone(),
    ));
    let coordinator = Arc::new(
        JobCoordinator::from_authority(control.clone(), &store, clock.clone(), 30_000).unwrap(),
    );
    let data_plane = Arc::new(AgentDataPlaneService::new(authority.clone()));
    let handler = RegistryAgentApiHandler::with_transport(
        registry,
        control,
        data_plane,
        Arc::new(AtomicBool::new(true)),
    );
    let signer = AgentRequestSigner::new(
        AgentId::new(AGENT).unwrap(),
        AgentInstallationId::new(INSTALLATION).unwrap(),
        AgentBootId::new(BOOT).unwrap(),
        signing_key,
    )
    .unwrap();

    let opened: AgentSessionOpenResponse = signed_action(
        &handler,
        &signer,
        None,
        AGENT_SESSION_OPEN_PATH,
        AgentAction::SessionOpen,
        AgentSessionOpenPayload {
            mount_identity_digest: mount_identity_digest(),
            expected_resource_version: approved.resource_version,
            extensions: Extensions::new(),
        },
    )
    .await;
    let fence = AgentSessionFence {
        session_id: opened.session_id.clone(),
        session_generation: opened.session_generation,
    };
    let heartbeat: AgentHeartbeatReportResponse = signed_action(
        &handler,
        &signer,
        Some(&fence),
        AGENT_SESSION_HEARTBEAT_REPORT_PATH,
        AgentAction::SessionHeartbeatReport,
        AgentHeartbeatReportPayload {
            heartbeat: AgentHeartbeat {
                agent_id: AgentId::new(AGENT).unwrap(),
                observed_at_unix_ms: UnixMillis::new(NOW_MS),
                acknowledged_resource_version: opened.resource_version,
                sequence: SequenceNumber::new(1),
                running_jobs: Vec::new(),
                mount_observations: Vec::new(),
                extensions: Extensions::new(),
            },
            mount_report: AgentMountStatusReport {
                agent_id: AgentId::new(AGENT).unwrap(),
                installation_id: AgentInstallationId::new(INSTALLATION).unwrap(),
                boot_id: AgentBootId::new(BOOT).unwrap(),
                session_generation: opened.session_generation,
                sequence: SequenceNumber::new(1),
                agent_mount_id: AgentMountId::new(MOUNT).unwrap(),
                storage_volume_id: storage_volume_id.clone(),
                mount_generation: opened.mount_generation,
                owner_generation: opened.owner_generation,
                observed_volume_marker: VolumeMarkerId::new(VOLUME).unwrap(),
                mount_identity_digest: mount_identity_digest(),
                access_mode: MountAccessMode::ReadWrite,
                health: ResourceHealth::Ready,
                observed_at_unix_ms: UnixMillis::new(NOW_MS),
                extensions: Extensions::new(),
            },
            extensions: Extensions::new(),
        },
    )
    .await;
    assert!(!heartbeat.replayed);

    catalog
        .insert_artifact(ArtifactRecord {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            display_name: "Snapshot E2E artifact".to_owned(),
            description: None,
            initialization: ArtifactInitialization::Empty,
            head_commit_id: None,
            resource_version: 1,
            created_at_unix_ms: UnixMillis::new(NOW_MS),
            updated_at_unix_ms: UnixMillis::new(NOW_MS),
        })
        .await
        .unwrap();
    catalog
        .insert_playground(PlaygroundRecord {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: playground_id.clone(),
            storage_volume_id: storage_volume_id.clone(),
            region: "local".to_owned(),
            display_name: "Snapshot source".to_owned(),
            base_commit_id: None,
            head_commit_id: None,
            state: PlaygroundState::Ready,
            relative_root: format!("playgrounds/{PROJECT}/{ARTIFACT}/{PLAYGROUND}"),
            created_at_unix_ms: UnixMillis::new(NOW_MS),
            updated_at_unix_ms: UnixMillis::new(NOW_MS),
        })
        .await
        .unwrap();

    let historical_manifest = Manifest::new(
        (HISTORICAL_CHUNK_A.len() + HISTORICAL_CHUNK_B.len()) as u64,
        ChunkingStrategy::FastCdc,
        vec![
            ChunkRef::new(
                ObjectId::for_bytes(HISTORICAL_CHUNK_A),
                0,
                HISTORICAL_CHUNK_A.len() as u64,
            )
            .unwrap(),
            ChunkRef::new(
                ObjectId::for_bytes(HISTORICAL_CHUNK_B),
                HISTORICAL_CHUNK_A.len() as u64,
                HISTORICAL_CHUNK_B.len() as u64,
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let current_manifest = Manifest::new(
        CURRENT_CHUNK.len() as u64,
        ChunkingStrategy::FastCdc,
        vec![ChunkRef::new(
            ObjectId::for_bytes(CURRENT_CHUNK),
            0,
            CURRENT_CHUNK.len() as u64,
        )
        .unwrap()],
    )
    .unwrap();
    write_volume_objects(volume_dir.path(), &historical_manifest, &current_manifest);

    let index_key = IndexKey {
        tenant_id: tenant_id.clone(),
        project_id: project_id.clone(),
        artifact_id: artifact_id.clone(),
        playground_id: playground_id.clone(),
    };
    let empty_version = WireIndexVersion::from(IndexVersion::from_snapshot(0, &[]).unwrap());
    store
        .publisher()
        .initialize_snapshot(neoengramd::InitializeIndexSnapshotRequest {
            index_key: index_key.clone(),
            version: empty_version.clone(),
            records: Vec::new(),
        })
        .await
        .unwrap();
    let historical_records = vec![FileRecord::from_manifest(
        LogicalPath::parse("dataset/value.bin").unwrap(),
        &historical_manifest,
    )
    .unwrap()];
    let historical_version = publish_index(
        store.publisher(),
        &index_key,
        "publish-historical-snapshot-e2e",
        empty_version.clone(),
        historical_records.clone(),
        historical_manifest.clone(),
    )
    .await;
    let historical_commit = create_commit(
        store.precommits().unwrap(),
        &tenant_id,
        &project_id,
        &artifact_id,
        &playground_id,
        &storage_volume_id,
        "historical",
        empty_version,
        historical_version.clone(),
        historical_records.clone(),
        None,
        11_000,
    )
    .await;
    catalog
        .advance_playground_commit(AdvancePlaygroundCommitRequest {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: playground_id.clone(),
            expected_head_commit_id: None,
            commit_id: ContentDigest::from(historical_commit.commit_id),
            updated_at_unix_ms: UnixMillis::new(11_000),
        })
        .await
        .unwrap();

    let current_records = vec![FileRecord::from_manifest(
        LogicalPath::parse("dataset/value.bin").unwrap(),
        &current_manifest,
    )
    .unwrap()];
    let current_version = publish_index(
        store.publisher(),
        &index_key,
        "publish-current-snapshot-e2e",
        historical_version.clone(),
        current_records.clone(),
        current_manifest,
    )
    .await;
    let current_commit = create_commit(
        store.precommits().unwrap(),
        &tenant_id,
        &project_id,
        &artifact_id,
        &playground_id,
        &storage_volume_id,
        "current",
        historical_version,
        current_version,
        current_records,
        Some(historical_commit.commit_id),
        12_000,
    )
    .await;
    catalog
        .advance_playground_commit(AdvancePlaygroundCommitRequest {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: playground_id.clone(),
            expected_head_commit_id: Some(ContentDigest::from(historical_commit.commit_id)),
            commit_id: ContentDigest::from(current_commit.commit_id),
            updated_at_unix_ms: UnixMillis::new(12_000),
        })
        .await
        .unwrap();

    let policy = Arc::new(
        StaticRbacPolicy::one_principal(
            "snapshot-user",
            [tenant_id.to_string()],
            [Permission::SnapshotCreate, Permission::SnapshotRead],
        )
        .unwrap(),
    );
    let snapshots = CatalogService::new(catalog.clone(), store.publisher(), policy, clock.clone())
        .with_precommits(store.precommits().unwrap())
        .with_coordinator(coordinator);
    let identity = AuthenticatedIdentity::new(
        "snapshot-user",
        PrincipalKind::User,
        "test",
        "snapshot-user",
    )
    .unwrap();
    let created = snapshots
        .create_snapshot(
            &identity,
            CreateSnapshotRequest {
                tenant_id: tenant_id.to_string(),
                project_id: project_id.to_string(),
                artifact_id: artifact_id.to_string(),
                commit_id: historical_commit.commit_id.to_string(),
                storage_volume_id: storage_volume_id.to_string(),
                snapshot_request_id: "snapshot-request-historical-e2e".to_owned(),
                extensions: JsonExtensions::default(),
            },
        )
        .await
        .unwrap();
    assert_eq!(created.snapshot.state, "creating");
    assert_eq!(
        created.snapshot.commit_id,
        historical_commit.commit_id.to_string()
    );
    assert_ne!(
        created.snapshot.commit_id,
        current_commit.commit_id.to_string(),
        "the Snapshot must remain bound to the selected historical Commit"
    );
    let snapshot_id = SnapshotId::new(created.snapshot.snapshot_id.clone()).unwrap();

    let polled: AgentMessageListQueryResponse = signed_action(
        &handler,
        &signer,
        Some(&fence),
        AGENT_SESSION_MESSAGE_LIST_QUERY_PATH,
        AgentAction::SessionMessageListQuery,
        AgentMessageListQueryPayload {
            max_messages: 32,
            extensions: Extensions::new(),
        },
    )
    .await;
    let assignment = polled
        .messages
        .into_iter()
        .find_map(|envelope| match envelope.message {
            ControlMessage::Assignment(assignment) => match assignment.assignment {
                AssignmentOperation::SnapshotMount { input, .. } => Some(input),
                _ => None,
            },
            _ => None,
        })
        .expect("Snapshot create did not dispatch a SnapshotMount assignment");
    assert_eq!(assignment.snapshot_id, snapshot_id);
    assert_eq!(
        assignment.commit_id,
        ContentDigest::from(historical_commit.commit_id)
    );
    assert_eq!(
        assignment.index_version,
        historical_version_from(&historical_records, 1)
    );

    let index_page: AgentIndexPageQueryResponse = signed_action(
        &handler,
        &signer,
        Some(&fence),
        AGENT_JOB_INDEX_PAGE_QUERY_PATH,
        AgentAction::JobIndexPageQuery,
        AgentIndexPageQueryPayload {
            tenant_id: tenant_id.clone(),
            job_id: assignment.job_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: None,
            snapshot_id: Some(snapshot_id.clone()),
            index_version: assignment.index_version.clone(),
            page_number: 0,
            max_records: 32,
            extensions: Extensions::new(),
        },
    )
    .await;
    assert_eq!(index_page.index_version, assignment.index_version);
    assert_eq!(index_page.records.len(), 1);
    let IndexDeltaRecord::Upsert { manifest_id, .. } = &index_page.records[0] else {
        panic!("historical Snapshot Index returned a deletion");
    };
    assert_eq!(*manifest_id, historical_manifest.canonical_id().unwrap());

    let first_manifest_page: AgentManifestPageQueryResponse = signed_action(
        &handler,
        &signer,
        Some(&fence),
        AGENT_JOB_MANIFEST_PAGE_QUERY_PATH,
        AgentAction::JobManifestPageQuery,
        AgentManifestPageQueryPayload {
            tenant_id: tenant_id.clone(),
            job_id: assignment.job_id.clone(),
            artifact_id: artifact_id.clone(),
            manifest_id: *manifest_id,
            page_number: 0,
            max_chunks: 1,
            extensions: Extensions::new(),
        },
    )
    .await;
    let second_manifest_page: AgentManifestPageQueryResponse = signed_action(
        &handler,
        &signer,
        Some(&fence),
        AGENT_JOB_MANIFEST_PAGE_QUERY_PATH,
        AgentAction::JobManifestPageQuery,
        AgentManifestPageQueryPayload {
            tenant_id: tenant_id.clone(),
            job_id: assignment.job_id.clone(),
            artifact_id: artifact_id.clone(),
            manifest_id: *manifest_id,
            page_number: 1,
            max_chunks: 1,
            extensions: Extensions::new(),
        },
    )
    .await;
    assert_eq!(first_manifest_page.page_count, 2);
    assert_eq!(second_manifest_page.page_count, 2);
    assert_eq!(
        first_manifest_page.chunks[0].object_id,
        ObjectId::for_bytes(HISTORICAL_CHUNK_A)
    );
    assert_eq!(
        second_manifest_page.chunks[0].object_id,
        ObjectId::for_bytes(HISTORICAL_CHUNK_B)
    );
    for page in [&first_manifest_page, &second_manifest_page] {
        let metadata_only = serde_json::to_vec(page).unwrap();
        assert!(!contains_bytes(&metadata_only, HISTORICAL_CHUNK_A));
        assert!(!contains_bytes(&metadata_only, HISTORICAL_CHUNK_B));
    }

    let accepted = JobAccepted {
        job_id: assignment.job_id.clone(),
        assignment_id: assignment.assignment_id.clone(),
        assignment_generation: assignment.assignment_generation,
        accepted_at_unix_ms: UnixMillis::new(NOW_MS),
        request_digest: assignment.request_digest,
        extensions: Extensions::new(),
    };
    let _: AgentActionAcceptedResponse = signed_action(
        &handler,
        &signer,
        Some(&fence),
        AGENT_JOB_REPORT_CREATE_PATH,
        AgentAction::JobReportCreate,
        AgentJobReportCreatePayload {
            tenant_id: tenant_id.clone(),
            report: report_envelope(
                opened.session_generation,
                "snapshot-accepted-e2e",
                ControlMessage::Accepted(accepted),
            ),
            extensions: Extensions::new(),
        },
    )
    .await;
    let mut mounted_extensions = Extensions::new();
    mounted_extensions.insert(
        "snapshot_mount_observed_at_unix_ms".to_owned(),
        serde_json::Value::String(NOW_MS.to_string()),
    );
    mounted_extensions.insert(
        "snapshot_mount_session_generation".to_owned(),
        serde_json::Value::String(opened.session_generation.get().to_string()),
    );
    let _: AgentActionAcceptedResponse = signed_action(
        &handler,
        &signer,
        Some(&fence),
        AGENT_JOB_REPORT_CREATE_PATH,
        AgentAction::JobReportCreate,
        AgentJobReportCreatePayload {
            tenant_id: tenant_id.clone(),
            report: report_envelope(
                opened.session_generation,
                "snapshot-mounted-e2e",
                ControlMessage::Progress(JobProgress {
                    job_id: assignment.job_id,
                    assignment_id: assignment.assignment_id,
                    assignment_generation: assignment.assignment_generation,
                    state: JobState::Succeeded,
                    phase: "mounted".to_owned(),
                    files_completed: DecimalU64::new(1),
                    bytes_completed: DecimalU64::new(
                        (HISTORICAL_CHUNK_A.len() + HISTORICAL_CHUNK_B.len()) as u64,
                    ),
                    retry_after_ms: None,
                    extensions: mounted_extensions,
                }),
            ),
            extensions: Extensions::new(),
        },
    )
    .await;

    let ready = snapshots
        .query_snapshot(
            &identity,
            QuerySnapshotRequest {
                tenant_id: tenant_id.to_string(),
                snapshot_id: snapshot_id.to_string(),
            },
        )
        .await
        .unwrap();
    assert_eq!(ready.snapshot.state, "ready");
    assert_eq!(ready.snapshot.phase, "idle");
    assert_eq!(ready.snapshot.integrity.state, "verified");
    assert_eq!(
        ready.snapshot.commit_id,
        historical_commit.commit_id.to_string()
    );

    authority.integrity_check().await.unwrap();
    authority.close().await;
    assert_authority_has_no_chunk_payloads(authority_dir.path());
}

async fn enroll_agent(
    registry: &AgentRegistryService,
    signing_key: &Arc<Ed25519KeyPair>,
) -> neoengramd::AgentRegistryRecord {
    let tenant_id = TenantId::new(TENANT).unwrap();
    let storage_volume_id = StorageVolumeId::new(VOLUME).unwrap();
    let descriptor = FrozenStorageDescriptor {
        display_name: "Snapshot E2E volume".to_owned(),
        region: "local".to_owned(),
        access_mode: StorageEnrollmentAccessMode::ReadWriteOnce,
        pvc_reference: FrozenPvcReference {
            namespace: "default".to_owned(),
            claim_name: "snapshot-e2e-volume".to_owned(),
        },
    };
    let descriptor_digest = ContentDigest::hash(b"snapshot-e2e-volume-descriptor");
    let token = registry
        .create_storage_enrollment_intent(CreateStorageEnrollmentIntentRequest {
            request: AgentEnrollmentTokenCreateRequest {
                token_id: AgentEnrollmentTokenId::new("token-snapshot-e2e").unwrap(),
                token_request_id: RequestId::new("token-request-snapshot-e2e").unwrap(),
                enrollment_id: AgentEnrollmentId::new(ENROLLMENT).unwrap(),
                tenant_id: tenant_id.clone(),
                edge_cluster_id: EdgeClusterId::new("edge-snapshot-e2e").unwrap(),
                storage_volume_id: storage_volume_id.clone(),
                volume_descriptor_digest: descriptor_digest,
                pvc_identity_digest: PvcIdentityDigest::derive("default", "snapshot-e2e-volume")
                    .unwrap(),
                agent_id: AgentId::new(AGENT).unwrap(),
                agent_mount_id: AgentMountId::new(MOUNT).unwrap(),
                expected_volume_marker: VolumeMarkerId::new(VOLUME).unwrap(),
                desired_access_mode: MountAccessMode::ReadWrite,
                bootstrap_token: BOOTSTRAP_TOKEN.to_owned(),
                created_at_unix_ms: UnixMillis::new(NOW_MS),
                expires_at_unix_ms: UnixMillis::new(NOW_MS + 60_000),
                extensions: Extensions::new(),
            },
            descriptor,
            token: BootstrapTokenMetadata {
                key_id: "snapshot-e2e-key".to_owned(),
            },
        })
        .await
        .unwrap();
    let public_key: [u8; 32] = signing_key.public_key().as_ref().try_into().unwrap();
    let public_key_spki = Ed25519PublicKeySpki::from_public_key_bytes(public_key);
    let mut bootstrap = AgentBootstrapRequest {
        bootstrap_request_id: RequestId::new("bootstrap-snapshot-e2e").unwrap(),
        bootstrap_token: BOOTSTRAP_TOKEN.to_owned(),
        installation_id: AgentInstallationId::new(INSTALLATION).unwrap(),
        tenant_id,
        edge_cluster_id: EdgeClusterId::new("edge-snapshot-e2e").unwrap(),
        storage_volume_id,
        volume_descriptor_digest: descriptor_digest,
        agent_version: "snapshot-e2e".to_owned(),
        supported_protocol_versions: vec![PROTOCOL_VERSION_V1],
        capabilities: vec![
            "single_volume_v1".to_owned(),
            "snapshot_fuse_mount_v1".to_owned(),
        ],
        public_key_fingerprint: public_key_spki.fingerprint(),
        proof: AgentBootstrapProof::new(public_key_spki, Ed25519Signature::from_bytes([0; 64])),
        probe: AgentBootstrapProbe {
            observed_volume_marker: Some(VolumeMarkerId::new(VOLUME).unwrap()),
            marker_matches: true,
            mount_boundary_detected: true,
            mount_identity_digest: mount_identity_digest(),
            access_mode: Some(MountAccessMode::ReadWrite),
            rename_supported: true,
            fsync_supported: true,
            health: ResourceHealth::Ready,
            observed_at_unix_ms: UnixMillis::new(NOW_MS),
            extensions: Extensions::new(),
        },
        extensions: Extensions::new(),
    };
    bootstrap.proof.signature = Ed25519Signature::new(
        signing_key
            .sign(&bootstrap.signing_bytes().unwrap())
            .as_ref()
            .to_vec(),
    )
    .unwrap();
    let pending = registry
        .bootstrap_agent_with_proof(bootstrap)
        .await
        .unwrap();
    assert_eq!(
        pending.record.enrollment.enrollment_id,
        token.record.enrollment.enrollment_id
    );
    registry
        .decide_enrollment(
            AgentEnrollmentApprovalRequest {
                enrollment_id: AgentEnrollmentId::new(ENROLLMENT).unwrap(),
                decision_request_id: RequestId::new("approve-snapshot-e2e").unwrap(),
                expected_resource_version: pending.record.resource_version,
                decision: AgentEnrollmentDecision::Approve,
                confirm_replacement: false,
                extensions: Extensions::new(),
            },
            PrincipalRef {
                kind: PrincipalKind::User,
                id: PrincipalId::new("snapshot-admin").unwrap(),
                extensions: Extensions::new(),
            },
        )
        .await
        .unwrap()
        .record
}

async fn publish_index(
    publisher: Arc<dyn IndexPublisher>,
    index_key: &IndexKey,
    job_id: &str,
    expected: WireIndexVersion,
    records: Vec<FileRecord>,
    manifest: Manifest,
) -> WireIndexVersion {
    let record = &records[0];
    let result = publisher
        .compare_and_swap(IndexPublishRequest {
            job_key: JobKey::new(index_key.tenant_id.clone(), JobId::new(job_id).unwrap()),
            index_key: index_key.clone(),
            expected_index_version: expected,
            expected_result_digest: IndexVersion::from_snapshot(0, &records).unwrap().digest,
            manifests: vec![manifest],
            mutations: vec![IndexDeltaRecord::Upsert {
                path: record.path.clone(),
                manifest_id: record.manifest_id,
                total_size: DecimalU64::new(record.total_size),
                chunk_count: DecimalU64::new(record.chunk_count),
                extensions: Extensions::new(),
            }],
        })
        .await
        .unwrap();
    let IndexPublishOutcome::Published(version) = result else {
        panic!("test fixture Index publication was rejected: {result:?}");
    };
    version
}

#[allow(clippy::too_many_arguments)]
async fn create_commit(
    precommits: Arc<dyn PreCommitRepository>,
    tenant_id: &TenantId,
    project_id: &neoengram_protocol::ProjectId,
    artifact_id: &neoengram_protocol::ArtifactId,
    playground_id: &neoengram_protocol::PlaygroundId,
    storage_volume_id: &StorageVolumeId,
    label: &str,
    source_version: WireIndexVersion,
    candidate_version: WireIndexVersion,
    records: Vec<FileRecord>,
    parent_commit_id: Option<CommitId>,
    created_at: u64,
) -> CommitRecord {
    let precommit_id = PreCommitId::new(format!("precommit-{label}-snapshot-e2e")).unwrap();
    let key = PreCommitKey::new(tenant_id.clone(), precommit_id.clone());
    let job_id = JobId::new(format!("precommit-job-{label}-snapshot-e2e")).unwrap();
    precommits
        .start(PreCommitStartRequest {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: playground_id.clone(),
            precommit_id: precommit_id.clone(),
            precommit_request_id: RequestId::new(format!("precommit-request-{label}-snapshot-e2e"))
                .unwrap(),
            source_index_version: source_version.clone(),
            frozen_head_commit_id: parent_commit_id,
            job_id: job_id.clone(),
            created_at_unix_ms: UnixMillis::new(created_at - 100),
        })
        .await
        .unwrap();
    precommits
        .sync_job(
            terminal_job(
                tenant_id,
                project_id,
                artifact_id,
                playground_id,
                job_id,
                source_version,
                candidate_version.clone(),
            ),
            Some(PublishedIndex {
                version: candidate_version.clone(),
                records: records.clone(),
            }),
            UnixMillis::new(created_at - 50),
        )
        .await
        .unwrap();
    let root_directory_id = DirectoryId::from_bytes([label.as_bytes()[0]; 32]);
    let message = format!("{label} snapshot fixture");
    let commit_id = Commit::new(root_directory_id, parent_commit_id, &message, created_at)
        .unwrap()
        .canonical_id()
        .unwrap();
    precommits
        .commit(PreCommitCommitRequest {
            key,
            expected_candidate_index_version: candidate_version.clone(),
            commit: CommitRecord {
                tenant_id: tenant_id.clone(),
                project_id: project_id.clone(),
                artifact_id: artifact_id.clone(),
                source_playground_id: playground_id.clone(),
                source_storage_volume_id: Some(storage_volume_id.clone()),
                source_precommit_id: precommit_id,
                commit_request_id: RequestId::new(format!("commit-request-{label}-snapshot-e2e"))
                    .unwrap(),
                commit_id,
                root_directory_id,
                parent_commit_id,
                index_version: candidate_version,
                records,
                message,
                description: None,
                tag_names: vec![label.to_owned()],
                created_at_unix_ms: UnixMillis::new(created_at),
            },
        })
        .await
        .unwrap()
        .commit
}

#[allow(clippy::too_many_arguments)]
fn terminal_job(
    tenant_id: &TenantId,
    project_id: &neoengram_protocol::ProjectId,
    artifact_id: &neoengram_protocol::ArtifactId,
    playground_id: &neoengram_protocol::PlaygroundId,
    job_id: JobId,
    expected_index_version: WireIndexVersion,
    published_index_version: WireIndexVersion,
) -> JobRecord {
    JobRecord {
        spec: neoengramd::AddJobSpec {
            job_id: job_id.clone(),
            principal: PrincipalRef {
                kind: PrincipalKind::User,
                id: PrincipalId::new("snapshot-user").unwrap(),
                extensions: Extensions::new(),
            },
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: playground_id.clone(),
            expected_index_version,
            request_digest: ContentDigest::hash(job_id.as_str().as_bytes()),
            deadline_unix_ms: UnixMillis::new(u64::MAX),
            paths: Vec::new(),
            all: true,
            extensions: Extensions::new(),
        },
        operation: JobOperation::Add,
        workspace_spec: None,
        snapshot_spec: None,
        state: JobState::Succeeded,
        resource_version: ResourceVersion::new(1),
        assignment: None,
        workspace_assignment: None,
        snapshot_assignment: None,
        accepted: None,
        progress: None,
        prepared: None,
        publication_candidate: None,
        decision: Some(JobDecision {
            job_id,
            assignment_id: AssignmentId::new("fixture-assignment-snapshot-e2e").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            decision_generation: DecisionGeneration::new(1),
            decision: PublishDecision::Publish {
                published_index_version,
                extensions: Extensions::new(),
            },
            final_state: JobState::Succeeded,
            extensions: Extensions::new(),
        }),
        finalized: None,
        finalized_ack: None,
        failure: None,
    }
}

async fn signed_action<P, R>(
    handler: &RegistryAgentApiHandler,
    signer: &AgentRequestSigner,
    fence: Option<&AgentSessionFence>,
    path: &'static str,
    action: AgentAction,
    payload: P,
) -> R
where
    P: Serialize,
    R: DeserializeOwned,
{
    let request = signer
        .sign(path, fence, UnixMillis::new(NOW_MS), payload)
        .unwrap();
    let body = serde_json::to_vec(&request).unwrap();
    let response = handler.handle(action, &body).await.unwrap();
    serde_json::from_slice(&response).unwrap()
}

fn report_envelope(
    session_generation: SessionGeneration,
    message_id: &str,
    message: ControlMessage,
) -> ControlEnvelope {
    ControlEnvelope {
        protocol_version: ProtocolVersion::V1,
        message_id: MessageId::new(message_id).unwrap(),
        session_generation,
        resource_version: None,
        request_id: None,
        trace_id: Some(TraceId::new(format!("trace-{message_id}")).unwrap()),
        sent_at_unix_ms: UnixMillis::new(NOW_MS),
        message,
        extensions: Extensions::new(),
    }
}

fn signing_key() -> Arc<Ed25519KeyPair> {
    let document = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
    Arc::new(Ed25519KeyPair::from_pkcs8(document.as_ref()).unwrap())
}

fn mount_identity_digest() -> AgentMountIdentityDigest {
    AgentMountIdentityDigest::new(ContentDigest::hash(b"snapshot-e2e-mounted-filesystem"))
}

fn write_volume_objects(root: &Path, historical: &Manifest, current: &Manifest) {
    let object_root = root
        .join("tenants")
        .join(TENANT)
        .join("artifacts")
        .join(ARTIFACT)
        .join("objects");
    fs::create_dir_all(&object_root).unwrap();
    for (object_id, bytes) in [
        (historical.chunks[0].object_id, HISTORICAL_CHUNK_A),
        (historical.chunks[1].object_id, HISTORICAL_CHUNK_B),
        (current.chunks[0].object_id, CURRENT_CHUNK),
    ] {
        fs::write(object_root.join(object_id.to_hex()), bytes).unwrap();
    }
}

fn historical_version_from(records: &[FileRecord], revision: u64) -> WireIndexVersion {
    WireIndexVersion::from(IndexVersion::from_snapshot(revision, records).unwrap())
}

fn assert_authority_has_no_chunk_payloads(root: &Path) {
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                pending.push(entry.path());
                continue;
            }
            let bytes = fs::read(entry.path()).unwrap();
            for payload in [HISTORICAL_CHUNK_A, HISTORICAL_CHUNK_B, CURRENT_CHUNK] {
                assert!(
                    !contains_bytes(&bytes, payload),
                    "Server authority unexpectedly contains raw Chunk bytes in {}",
                    entry.path().display()
                );
            }
        }
    }
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}
