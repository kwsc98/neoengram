use neoengram_central::{
    AgentRegistryRepository, AgentRegistryService, CatalogInsertOutcome, CatalogPvcReference,
    CentralErrorCode, CloseAgentSessionRequest, ControlCatalogRepository, CreateDeletionRequest,
    DeletionImpactQuery, InMemoryComponents, LifecycleAssignmentInsertOutcome,
    LifecycleAssignmentOutboxRecord, OpenAgentSessionRequest, StorageAccessMode,
    StorageBackendType, StorageVolumeRecord, StorageVolumeState, TenantRecord,
};
use neoengram_domain::core::ContentDigest;
use neoengram_domain::protocol::{
    AgentBootId, AgentBootstrapProbe, AgentBootstrapProof, AgentBootstrapRequest,
    AgentEnrollmentApprovalRequest, AgentEnrollmentDecision, AgentEnrollmentId,
    AgentEnrollmentTokenCreateRequest, AgentEnrollmentTokenId, AgentId, AgentInstallationId,
    AgentMountId, AgentMountIdentityDigest, AgentResourceLifecycleAssignment,
    AgentResourceLifecycleScope, DecimalU64, DeletionId, Ed25519PublicKeySpki, Ed25519Signature,
    EdgeClusterId, Extensions, Generation, LifecycleAssignmentId, MountAccessMode, MountGeneration,
    OwnerGeneration, PrincipalId, PrincipalKind, PrincipalRef, PvcIdentityDigest, RequestId,
    ResourceHealth, ResourceLifecycle, ResourceLifecycleAction, ResourceLifecycleAssignment,
    ResourceLifecycleEvidence, ResourceLifecycleReport, ResourceLifecycleReportState, ResourceRef,
    SessionGeneration, StorageVolumeId, TaskExecutionFence, TaskId, TenantId, UnixMillis,
    VolumeMarkerId, CURRENT_WIRE_VERSION,
};
use ring::signature::{Ed25519KeyPair, KeyPair as _};

const INITIAL_TOKEN: &str = "lifecycle-initial-bootstrap-token-0001";
const REPLACEMENT_TOKEN: &str = "lifecycle-replacement-bootstrap-token-0002";

fn task_fence(deletion_id: &DeletionId, stage_key: &str) -> TaskExecutionFence {
    TaskExecutionFence::new(
        TaskId::new(format!("task-{deletion_id}")).unwrap(),
        Generation::new(1),
        stage_key,
        Generation::new(1),
        Generation::new(1),
    )
}

#[derive(Debug, Clone, Copy)]
enum AgentKind {
    Initial,
    Replacement,
}

struct LifecycleFixture {
    components: InMemoryComponents,
    control: neoengram_central::ControlPlane,
    registry: AgentRegistryService,
    command: AgentResourceLifecycleAssignment,
    terminal: ResourceLifecycleReport,
    opened_resource_version: neoengram_domain::protocol::ResourceVersion,
}

#[tokio::test]
async fn stale_terminal_report_is_rejected_after_session_and_owner_generations_advance() {
    let fixture = lifecycle_fixture().await;

    let closed = fixture
        .registry
        .close_session(CloseAgentSessionRequest {
            agent_id: agent_id(AgentKind::Initial),
            boot_id: boot_id("boot-initial"),
            session_generation: SessionGeneration::new(1),
            expected_resource_version: fixture.opened_resource_version,
        })
        .await
        .unwrap();
    let reopened = fixture
        .registry
        .open_session(OpenAgentSessionRequest {
            agent_id: agent_id(AgentKind::Initial),
            installation_id: installation_id(AgentKind::Initial),
            boot_id: boot_id("boot-restarted"),
            mount_identity_digest: mount_identity_digest(),
            expected_resource_version: closed.resource_version,
            capabilities: None,
        })
        .await
        .unwrap();
    assert_eq!(reopened.session_generation, SessionGeneration::new(2));

    let stale_session = fixture
        .control
        .receive_lifecycle_report(
            &tenant_id(),
            &agent_id(AgentKind::Initial),
            SessionGeneration::new(1),
            fixture.terminal.clone(),
        )
        .await
        .unwrap_err();
    assert_eq!(stale_session.code(), CentralErrorCode::AssignmentMismatch);

    let replacement = replace_owner(&fixture.registry).await;
    assert_eq!(replacement.mount.mount_generation, MountGeneration::new(2));
    assert_eq!(replacement.owner.owner_generation, OwnerGeneration::new(2));
    let current = fixture
        .components
        .agent_registry
        .get_current_by_volume(&tenant_id(), &storage_volume_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current, replacement);

    let stale_owner = fixture
        .control
        .receive_lifecycle_report(
            &tenant_id(),
            &agent_id(AgentKind::Initial),
            SessionGeneration::new(1),
            fixture.terminal,
        )
        .await
        .unwrap_err();
    assert_eq!(stale_owner.code(), CentralErrorCode::AssignmentMismatch);

    let outbox = fixture
        .components
        .control_catalog
        .get_lifecycle_assignment(&tenant_id(), &fixture.command.assignment.assignment_id)
        .await
        .unwrap()
        .unwrap();
    assert!(!outbox.retired);
    assert!(outbox.terminal_report_digest.is_none());
    assert!(fixture
        .components
        .control_catalog
        .deletion_proofs()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn identical_retired_terminal_report_replays_after_its_owner_is_revoked() {
    let fixture = lifecycle_fixture().await;
    let first = fixture
        .control
        .receive_lifecycle_report(
            &tenant_id(),
            &agent_id(AgentKind::Initial),
            SessionGeneration::new(1),
            fixture.terminal.clone(),
        )
        .await
        .unwrap();
    assert!(first.1);
    assert_eq!(
        fixture
            .components
            .control_catalog
            .deletion_proofs()
            .unwrap()
            .len(),
        1
    );

    let replacement = replace_owner(&fixture.registry).await;
    assert_eq!(replacement.owner.owner_generation, OwnerGeneration::new(2));
    assert_eq!(
        replacement.owner.active_agent_id.as_ref(),
        Some(&agent_id(AgentKind::Replacement))
    );

    let replay = fixture
        .control
        .receive_lifecycle_report(
            &tenant_id(),
            &agent_id(AgentKind::Initial),
            SessionGeneration::new(1),
            fixture.terminal,
        )
        .await
        .unwrap();
    assert_eq!(replay, first);
    assert_eq!(
        fixture
            .components
            .control_catalog
            .deletion_proofs()
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn conflicting_terminal_replay_is_rejected_without_duplicating_the_proof() {
    let fixture = lifecycle_fixture().await;
    fixture
        .control
        .receive_lifecycle_report(
            &tenant_id(),
            &agent_id(AgentKind::Initial),
            SessionGeneration::new(1),
            fixture.terminal.clone(),
        )
        .await
        .unwrap();
    let original_proofs = fixture
        .components
        .control_catalog
        .deletion_proofs()
        .unwrap();
    assert_eq!(original_proofs.len(), 1);

    let mut conflicting = fixture.terminal.clone();
    conflicting.evidence.as_mut().unwrap().byte_count = DecimalU64::new(999);
    let error = fixture
        .control
        .receive_lifecycle_report(
            &tenant_id(),
            &agent_id(AgentKind::Initial),
            SessionGeneration::new(1),
            conflicting,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::AssignmentMismatch);
    assert_eq!(
        fixture
            .components
            .control_catalog
            .deletion_proofs()
            .unwrap(),
        original_proofs
    );

    assert!(
        fixture
            .control
            .receive_lifecycle_report(
                &tenant_id(),
                &agent_id(AgentKind::Initial),
                SessionGeneration::new(1),
                fixture.terminal,
            )
            .await
            .unwrap()
            .1
    );
    assert_eq!(
        fixture
            .components
            .control_catalog
            .deletion_proofs()
            .unwrap()
            .len(),
        1
    );
}

async fn lifecycle_fixture() -> LifecycleFixture {
    let components = InMemoryComponents::new(200);
    let registry = AgentRegistryService::new(
        components.agent_registry.clone(),
        components.clock.clone(),
        100,
    );
    registry
        .create_token_intent(token_request(AgentKind::Initial))
        .await
        .unwrap();
    let pending = registry
        .bootstrap_agent_with_proof(bootstrap_request(AgentKind::Initial))
        .await
        .unwrap();
    let approved = registry
        .decide_enrollment(
            approval_request(AgentKind::Initial, pending.record.resource_version, false),
            actor(),
        )
        .await
        .unwrap()
        .record;
    let opened = registry
        .open_session(OpenAgentSessionRequest {
            agent_id: agent_id(AgentKind::Initial),
            installation_id: installation_id(AgentKind::Initial),
            boot_id: boot_id("boot-initial"),
            mount_identity_digest: mount_identity_digest(),
            expected_resource_version: approved.resource_version,
            capabilities: None,
        })
        .await
        .unwrap();

    components
        .control_catalog
        .insert_tenant(TenantRecord {
            tenant_id: tenant_id(),
            display_name: "Lifecycle tenant".to_owned(),
            description: None,
            resource_version: 1,
            created_at_unix_ms: UnixMillis::new(100),
            updated_at_unix_ms: UnixMillis::new(100),
        })
        .await
        .unwrap();
    components
        .control_catalog
        .insert_storage_volume(StorageVolumeRecord {
            tenant_id: tenant_id(),
            storage_volume_id: storage_volume_id(),
            display_name: "Lifecycle volume".to_owned(),
            edge_cluster_id: edge_cluster_id(),
            region: "test-region-1".to_owned(),
            backend_type: StorageBackendType::Pvc,
            access_mode: StorageAccessMode::ReadWriteMany,
            allowed_delivery_modes: vec![
                neoengram_domain::protocol::SnapshotDeliveryMode::Fuse,
                neoengram_domain::protocol::SnapshotDeliveryMode::Copy,
            ],
            hardlink_policy: neoengram_domain::protocol::HardlinkPolicy::Disabled,
            max_whole_file_bytes: neoengram_domain::protocol::DecimalU64::new(u64::MAX),
            copy_reserve_bytes: neoengram_domain::protocol::DecimalU64::new(0),
            pvc_reference: Some(CatalogPvcReference {
                namespace: "neoengram".to_owned(),
                claim_name: "lifecycle-volume".to_owned(),
            }),
            nfs_reference: None,
            state: StorageVolumeState::Ready,
            resource_version: 1,
            lifecycle: ResourceLifecycle::active(),
            created_at_unix_ms: UnixMillis::new(100),
            updated_at_unix_ms: UnixMillis::new(100),
        })
        .await
        .unwrap();
    let root = ResourceRef::StorageVolume {
        storage_volume_id: storage_volume_id(),
    };
    let impact = components
        .control_catalog
        .query_deletion_impact(DeletionImpactQuery {
            tenant_id: tenant_id(),
            root: root.clone(),
            cascade: false,
            confirm_managed_data_erase: true,
            additional_blockers: Vec::new(),
            authority_impact: None,
            now_unix_ms: UnixMillis::new(210),
        })
        .await
        .unwrap();
    let request_digest = ContentDigest::hash(b"lifecycle-report-deletion");
    let operation = match components
        .control_catalog
        .create_deletion_idempotent(CreateDeletionRequest {
            deletion_id: DeletionId::new("deletion-report-fixture").unwrap(),
            tenant_id: tenant_id(),
            root: root.clone(),
            cascade: false,
            confirm_managed_data_erase: true,
            request_id: RequestId::new("deletion-report-request").unwrap(),
            request_digest,
            impact_digest: impact.impact_digest,
            expected_resource_version: 1,
            now_unix_ms: UnixMillis::new(211),
        })
        .await
        .unwrap()
    {
        CatalogInsertOutcome::Inserted(operation) => operation,
        CatalogInsertOutcome::Existing(_) => panic!("fixture deletion must be new"),
    };
    let lifecycle_generation = operation
        .targets
        .iter()
        .find(|target| target.resource == root)
        .unwrap()
        .lifecycle_generation;
    let command = AgentResourceLifecycleAssignment {
        assignment: ResourceLifecycleAssignment {
            assignment_id: LifecycleAssignmentId::new("lifecycle-report-assignment").unwrap(),
            tenant_id: tenant_id(),
            deletion_id: operation.deletion_id.clone(),
            resource: root,
            action: ResourceLifecycleAction::Purge,
            lifecycle_generation,
            request_digest,
            deadline_unix_ms: UnixMillis::new(10_000),
        },
        task_fence: task_fence(&operation.deletion_id, "purge"),
        resource_scope: AgentResourceLifecycleScope::StorageVolume {
            storage_volume_id: storage_volume_id(),
        },
        agent_id: agent_id(AgentKind::Initial),
        edge_cluster_id: edge_cluster_id(),
        agent_mount_id: agent_mount_id(AgentKind::Initial),
        volume_marker_id: VolumeMarkerId::new(storage_volume_id().as_str()).unwrap(),
        session_generation: opened.session_generation,
        mount_generation: opened.record.mount.mount_generation,
        owner_generation: opened.record.owner.owner_generation,
        extensions: Extensions::new(),
    };
    assert!(matches!(
        components
            .control_catalog
            .enqueue_lifecycle_assignment(LifecycleAssignmentOutboxRecord {
                assignment: command.clone(),
                published: false,
                retired: false,
                terminal_report_digest: None,
            })
            .await
            .unwrap(),
        LifecycleAssignmentInsertOutcome::Inserted(_)
    ));
    components
        .control_catalog
        .publish_lifecycle_assignment(&tenant_id(), &command.assignment.assignment_id)
        .await
        .unwrap();

    let mut terminal = ResourceLifecycleReport::accepted(&command, UnixMillis::new(300));
    terminal.state = ResourceLifecycleReportState::Purged;
    terminal.evidence = Some(ResourceLifecycleEvidence {
        volume_marker_id: VolumeMarkerId::new(storage_volume_id().as_str()).unwrap(),
        file_count: DecimalU64::new(3),
        object_count: DecimalU64::new(2),
        byte_count: DecimalU64::new(42),
        object_set_digest: ContentDigest::hash(b"purged-object-set"),
        extensions: Extensions::new(),
    });
    terminal.validate_for_assignment(&command).unwrap();

    let control = components.control_plane();
    LifecycleFixture {
        components,
        control,
        registry,
        command,
        terminal,
        opened_resource_version: opened.record.resource_version,
    }
}

async fn replace_owner(registry: &AgentRegistryService) -> neoengram_central::AgentRegistryRecord {
    registry
        .create_token_intent(token_request(AgentKind::Replacement))
        .await
        .unwrap();
    let pending = registry
        .bootstrap_agent_with_proof(bootstrap_request(AgentKind::Replacement))
        .await
        .unwrap();
    registry
        .decide_enrollment(
            approval_request(
                AgentKind::Replacement,
                pending.record.resource_version,
                true,
            ),
            actor(),
        )
        .await
        .unwrap()
        .record
}

fn token_request(kind: AgentKind) -> AgentEnrollmentTokenCreateRequest {
    let created_at = match kind {
        AgentKind::Initial => 100,
        AgentKind::Replacement => 120,
    };
    AgentEnrollmentTokenCreateRequest {
        token_id: AgentEnrollmentTokenId::new(match kind {
            AgentKind::Initial => "token-lifecycle-initial",
            AgentKind::Replacement => "token-lifecycle-replacement",
        })
        .unwrap(),
        token_request_id: RequestId::new(match kind {
            AgentKind::Initial => "token-request-lifecycle-initial",
            AgentKind::Replacement => "token-request-lifecycle-replacement",
        })
        .unwrap(),
        enrollment_id: enrollment_id(kind),
        tenant_id: tenant_id(),
        edge_cluster_id: edge_cluster_id(),
        storage_volume_id: storage_volume_id(),
        volume_descriptor_digest: ContentDigest::hash(b"lifecycle-volume-descriptor"),
        pvc_identity_digest: PvcIdentityDigest::derive("neoengram", "lifecycle-volume").unwrap(),
        agent_id: agent_id(kind),
        agent_mount_id: agent_mount_id(kind),
        expected_volume_marker: VolumeMarkerId::new(storage_volume_id().as_str()).unwrap(),
        desired_access_mode: MountAccessMode::ReadWrite,
        bootstrap_token: match kind {
            AgentKind::Initial => INITIAL_TOKEN,
            AgentKind::Replacement => REPLACEMENT_TOKEN,
        }
        .to_owned(),
        created_at_unix_ms: UnixMillis::new(created_at),
        expires_at_unix_ms: UnixMillis::new(1_000),
        extensions: Extensions::new(),
    }
}

fn bootstrap_request(kind: AgentKind) -> AgentBootstrapRequest {
    let key_pair = bootstrap_key_pair(kind);
    let proof = bootstrap_proof(&key_pair);
    let mut request = AgentBootstrapRequest {
        bootstrap_request_id: RequestId::new(match kind {
            AgentKind::Initial => "bootstrap-lifecycle-initial",
            AgentKind::Replacement => "bootstrap-lifecycle-replacement",
        })
        .unwrap(),
        bootstrap_token: match kind {
            AgentKind::Initial => INITIAL_TOKEN,
            AgentKind::Replacement => REPLACEMENT_TOKEN,
        }
        .to_owned(),
        installation_id: installation_id(kind),
        tenant_id: tenant_id(),
        edge_cluster_id: edge_cluster_id(),
        storage_volume_id: storage_volume_id(),
        volume_descriptor_digest: ContentDigest::hash(b"lifecycle-volume-descriptor"),
        agent_version: "test-agent".to_owned(),
        wire_version: CURRENT_WIRE_VERSION,
        capabilities: vec!["single_volume_v1".to_owned()],
        public_key_fingerprint: proof.public_key_fingerprint(),
        proof,
        probe: AgentBootstrapProbe {
            observed_volume_marker: Some(
                VolumeMarkerId::new(storage_volume_id().as_str()).unwrap(),
            ),
            marker_matches: true,
            mount_boundary_detected: true,
            mount_identity_digest: mount_identity_digest(),
            access_mode: Some(MountAccessMode::ReadWrite),
            rename_supported: true,
            fsync_supported: true,
            health: ResourceHealth::Ready,
            observed_at_unix_ms: UnixMillis::new(199),
            extensions: Extensions::new(),
        },
        extensions: Extensions::new(),
    };
    let signature = key_pair.sign(&request.signing_bytes().unwrap());
    request.proof.signature = Ed25519Signature::new(signature.as_ref().to_vec()).unwrap();
    request.verify().unwrap();
    request
}

fn approval_request(
    kind: AgentKind,
    expected_resource_version: neoengram_domain::protocol::ResourceVersion,
    confirm_replacement: bool,
) -> AgentEnrollmentApprovalRequest {
    AgentEnrollmentApprovalRequest {
        enrollment_id: enrollment_id(kind),
        decision_request_id: RequestId::new(match kind {
            AgentKind::Initial => "approve-lifecycle-initial",
            AgentKind::Replacement => "approve-lifecycle-replacement",
        })
        .unwrap(),
        expected_resource_version,
        decision: AgentEnrollmentDecision::Approve,
        confirm_replacement,
        extensions: Extensions::new(),
    }
}

fn bootstrap_key_pair(kind: AgentKind) -> Ed25519KeyPair {
    let seed = match kind {
        AgentKind::Initial => 1,
        AgentKind::Replacement => 2,
    };
    Ed25519KeyPair::from_seed_unchecked(&[seed; 32]).unwrap()
}

fn bootstrap_proof(key_pair: &Ed25519KeyPair) -> AgentBootstrapProof {
    let public_key = key_pair.public_key().as_ref().try_into().unwrap();
    AgentBootstrapProof::new(
        Ed25519PublicKeySpki::from_public_key_bytes(public_key),
        Ed25519Signature::from_bytes([0; 64]),
    )
}

fn enrollment_id(kind: AgentKind) -> AgentEnrollmentId {
    AgentEnrollmentId::new(match kind {
        AgentKind::Initial => "enrollment-lifecycle-initial",
        AgentKind::Replacement => "enrollment-lifecycle-replacement",
    })
    .unwrap()
}

fn agent_id(kind: AgentKind) -> AgentId {
    AgentId::new(match kind {
        AgentKind::Initial => "agent-lifecycle-initial",
        AgentKind::Replacement => "agent-lifecycle-replacement",
    })
    .unwrap()
}

fn installation_id(kind: AgentKind) -> AgentInstallationId {
    AgentInstallationId::new(match kind {
        AgentKind::Initial => "installation-lifecycle-initial",
        AgentKind::Replacement => "installation-lifecycle-replacement",
    })
    .unwrap()
}

fn agent_mount_id(kind: AgentKind) -> AgentMountId {
    AgentMountId::new(match kind {
        AgentKind::Initial => "mount-lifecycle-initial",
        AgentKind::Replacement => "mount-lifecycle-replacement",
    })
    .unwrap()
}

fn actor() -> PrincipalRef {
    PrincipalRef {
        kind: PrincipalKind::User,
        id: PrincipalId::new("lifecycle-admin").unwrap(),
        extensions: Extensions::new(),
    }
}

fn tenant_id() -> TenantId {
    TenantId::new("tenant-lifecycle-report").unwrap()
}

fn storage_volume_id() -> StorageVolumeId {
    StorageVolumeId::new("volume-lifecycle-report").unwrap()
}

fn edge_cluster_id() -> EdgeClusterId {
    EdgeClusterId::new("cluster-lifecycle-report").unwrap()
}

fn mount_identity_digest() -> AgentMountIdentityDigest {
    AgentMountIdentityDigest::new(ContentDigest::hash(b"lifecycle-volume-mount"))
}

fn boot_id(value: &str) -> AgentBootId {
    AgentBootId::new(value).unwrap()
}
