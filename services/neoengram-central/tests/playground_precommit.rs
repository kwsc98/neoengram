use std::sync::Arc;

use fusen_rs::ErrorCategory;
use neoengram_central::{
    dto::{
        CancelPreCommitRequest, CreateAddJobRequest, DataLayout, IndexVersionBody,
        QueryPreCommitRequest, RestartPreCommitRequest, StartPreCommitRequest,
    },
    AuthenticatedIdentity, CatalogService, JobCoordinator, JobService, Permission,
    StaticRbacPolicy,
};
use neoengram_central::{
    ArtifactInitialization, ArtifactRecord, CatalogPvcReference, Clock, ControlCatalogRepository,
    ControlPlane, InMemoryComponents, IndexKey, IndexPublisher, JobKey, JobOperation,
    JobRepository, PreCommitId, PreCommitRepository,
    PreCommitStartRequest as DomainPreCommitStartRequest, StorageAccessMode, StorageBackendType,
    StorageVolumeRecord, StorageVolumeState, TenantRecord, WorkspaceRecord, WorkspaceState,
};
use neoengram_domain::protocol::{
    ArtifactId, CommitDataLayout, EdgeClusterId, JobId, PrincipalKind, ProjectId, RequestId,
    StorageVolumeId, TenantId, UnixMillis, WorkspaceId,
};

mod support;
use support::ReadyStorageAvailability;

#[tokio::test]
async fn precommit_actions_create_real_jobs_and_enforce_active_scope() {
    let fixture = fixture().await;
    let request = fixture.start_request("precommit-request-a").await;

    let started = fixture
        .catalog
        .start_workspace_precommit(&fixture.identity, request.clone())
        .await
        .unwrap();

    assert!(!started.request_replayed);
    assert_eq!(started.precommit.state, "running");
    assert_eq!(started.precommit.phase, "queued");
    assert_eq!(
        started.workspace.active_precommit_id.as_deref(),
        Some(started.precommit.precommit_id.as_str())
    );
    let precommit = fixture
        .components
        .precommits
        .get(&neoengram_central::PreCommitKey::new(
            fixture.tenant_id.clone(),
            PreCommitId::new(started.precommit.precommit_id.clone()).unwrap(),
        ))
        .await
        .unwrap()
        .unwrap();
    let job = fixture
        .components
        .jobs
        .get(&JobKey::new(
            fixture.tenant_id.clone(),
            precommit.job_id.clone(),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.operation, JobOperation::Add);
    assert!(job.spec.all);
    assert!(job.spec.paths.is_empty());

    let replayed = fixture
        .catalog
        .start_workspace_precommit(&fixture.identity, request)
        .await
        .unwrap();
    assert!(replayed.request_replayed);
    assert_eq!(
        replayed.precommit.precommit_id,
        started.precommit.precommit_id
    );

    let conflict = fixture
        .catalog
        .start_workspace_precommit(
            &fixture.identity,
            fixture.start_request("precommit-request-b").await,
        )
        .await
        .unwrap_err();
    assert_eq!(conflict.code().as_str(), "precommit_conflict");

    let public_job = JobService::from_authority(
        fixture.control.clone(),
        &fixture.components.authority_store(),
    )
    .unwrap();
    let active_guard = public_job
        .create_add_job(
            &fixture.identity,
            fixture.add_request("public-job-during-precommit").await,
        )
        .await
        .unwrap_err();
    assert_eq!(active_guard.code().as_str(), "precommit_already_active");

    let hidden = fixture
        .catalog
        .query_workspace_precommit(
            &AuthenticatedIdentity::new("user-b", PrincipalKind::User, "test", "subject-b")
                .unwrap(),
            QueryPreCommitRequest {
                tenant_id: fixture.tenant_id.to_string(),
                precommit_id: started.precommit.precommit_id.clone(),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(hidden.status().as_u16(), 404);

    let cancelled = fixture
        .catalog
        .cancel_workspace_precommit(
            &fixture.identity,
            CancelPreCommitRequest {
                tenant_id: fixture.tenant_id.to_string(),
                precommit_id: started.precommit.precommit_id.clone(),
                cancel_request_id: "cancel-request-a".to_owned(),
            },
        )
        .await
        .unwrap();
    assert_eq!(cancelled.precommit.state, "cancelled");
    assert!(cancelled.workspace.active_precommit_id.is_none());

    let restarted = fixture
        .catalog
        .restart_workspace_precommit(
            &fixture.identity,
            RestartPreCommitRequest {
                tenant_id: fixture.tenant_id.to_string(),
                precommit_id: started.precommit.precommit_id,
                restart_request_id: "restart-request-a".to_owned(),
                expected_index_version: fixture.index_version().await,
            },
        )
        .await
        .unwrap();
    assert_eq!(restarted.precommit.attempt, 2);
    assert_eq!(restarted.precommit.state, "running");
    assert_eq!(
        restarted.workspace.active_precommit_id,
        Some(restarted.precommit.precommit_id)
    );
}

#[tokio::test]
async fn coordinator_recovers_precommit_committed_before_its_job() {
    let fixture = fixture().await;
    let source_index_version = fixture
        .components
        .publisher
        .current_version(&fixture.index_key())
        .await
        .unwrap();
    let job_id = JobId::new("precommit-recovery-job").unwrap();
    let precommit_id = PreCommitId::new("precommit-recovery").unwrap();
    fixture
        .components
        .precommits
        .start(DomainPreCommitStartRequest {
            tenant_id: fixture.tenant_id.clone(),
            project_id: fixture.project_id.clone(),
            artifact_id: fixture.artifact_id.clone(),
            workspace_id: fixture.workspace_id.clone(),
            precommit_id,
            precommit_request_id: RequestId::new("precommit-recovery-request").unwrap(),
            source_index_version,
            frozen_head_commit_id: None,
            data_layout: CommitDataLayout::FastCdc,
            job_id: job_id.clone(),
            created_at_unix_ms: fixture.components.clock.now(),
        })
        .await
        .unwrap();
    assert!(fixture
        .components
        .jobs
        .get(&JobKey::new(fixture.tenant_id.clone(), job_id.clone()))
        .await
        .unwrap()
        .is_none());

    fixture.coordinator.reconcile_once().await.unwrap();

    let recovered = fixture
        .components
        .jobs
        .get(&JobKey::new(fixture.tenant_id.clone(), job_id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.operation, JobOperation::Add);
    assert!(recovered.spec.all);
}

#[tokio::test]
async fn precommit_freezes_the_workspace_head_not_the_artifact_head() {
    let fixture = fixture().await;
    let workspace_head = neoengram_domain::core::ContentDigest::from_bytes([0x11; 32]);
    let artifact_head = neoengram_domain::core::ContentDigest::from_bytes([0x22; 32]);
    fixture
        .components
        .control_catalog
        .advance_workspace_commit(neoengram_central::AdvanceWorkspaceCommitRequest {
            tenant_id: fixture.tenant_id.clone(),
            project_id: fixture.project_id.clone(),
            artifact_id: fixture.artifact_id.clone(),
            workspace_id: fixture.workspace_id.clone(),
            expected_head_commit_id: None,
            commit_id: workspace_head,
            updated_at_unix_ms: UnixMillis::new(1_001),
        })
        .await
        .unwrap();
    let sibling_id = WorkspaceId::new("workspace-sibling").unwrap();
    fixture
        .components
        .control_catalog
        .insert_workspace(WorkspaceRecord {
            tenant_id: fixture.tenant_id.clone(),
            project_id: fixture.project_id.clone(),
            artifact_id: fixture.artifact_id.clone(),
            workspace_id: sibling_id.clone(),
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            region: "cn-shanghai".to_owned(),
            display_name: "Sibling".to_owned(),
            base_commit_id: Some(workspace_head),
            head_commit_id: Some(workspace_head),
            state: WorkspaceState::Ready,
            resource_version: 1,
            lifecycle: neoengram_domain::protocol::ResourceLifecycle::active(),
            relative_root: "workspaces/project-a/artifact-a/workspace-sibling".to_owned(),
            created_at_unix_ms: UnixMillis::new(1_001),
            updated_at_unix_ms: UnixMillis::new(1_001),
        })
        .await
        .unwrap();
    fixture
        .components
        .control_catalog
        .advance_workspace_commit(neoengram_central::AdvanceWorkspaceCommitRequest {
            tenant_id: fixture.tenant_id.clone(),
            project_id: fixture.project_id.clone(),
            artifact_id: fixture.artifact_id.clone(),
            workspace_id: sibling_id,
            expected_head_commit_id: Some(workspace_head),
            commit_id: artifact_head,
            updated_at_unix_ms: UnixMillis::new(1_002),
        })
        .await
        .unwrap();

    let started = fixture
        .catalog
        .start_workspace_precommit(
            &fixture.identity,
            fixture.start_request("precommit-request-branch").await,
        )
        .await
        .unwrap();
    let stored = fixture
        .components
        .precommits
        .get(&neoengram_central::PreCommitKey::new(
            fixture.tenant_id.clone(),
            PreCommitId::new(started.precommit.precommit_id).unwrap(),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.frozen_head_commit_id,
        Some(neoengram_domain::core::CommitId::from_digest(
            workspace_head
        ))
    );
}

#[tokio::test]
async fn precommit_start_fails_closed_without_live_storage_availability() {
    let fixture = fixture().await;
    let catalog = CatalogService::new(
        fixture.components.control_catalog.clone(),
        fixture.components.publisher.clone(),
        Arc::new(
            StaticRbacPolicy::one_principal(
                "user-a",
                [fixture.tenant_id.to_string()],
                [
                    Permission::WorkspaceRead,
                    Permission::WorkspaceCreate,
                    Permission::CreateAddJob,
                ],
            )
            .unwrap(),
        ),
        fixture.components.clock.clone(),
    )
    .with_precommits(fixture.components.precommits.clone())
    .with_coordinator(fixture.coordinator.clone());

    let error = catalog
        .start_workspace_precommit(
            &fixture.identity,
            fixture.start_request("precommit-request-no-registry").await,
        )
        .await
        .unwrap_err();
    assert_eq!(error.category(), ErrorCategory::Unavailable);
    assert_eq!(error.code().as_str(), "storage_availability_unavailable");
}

struct Fixture {
    components: InMemoryComponents,
    catalog: CatalogService,
    coordinator: Arc<JobCoordinator>,
    control: Arc<ControlPlane>,
    identity: AuthenticatedIdentity,
    tenant_id: TenantId,
    project_id: ProjectId,
    artifact_id: ArtifactId,
    workspace_id: WorkspaceId,
}

impl Fixture {
    fn index_key(&self) -> IndexKey {
        IndexKey {
            tenant_id: self.tenant_id.clone(),
            project_id: self.project_id.clone(),
            artifact_id: self.artifact_id.clone(),
            workspace_id: self.workspace_id.clone(),
        }
    }

    async fn index_version(&self) -> IndexVersionBody {
        let version = self
            .components
            .publisher
            .current_version(&self.index_key())
            .await
            .unwrap();
        IndexVersionBody {
            revision: version.revision.to_string(),
            digest: version.digest.to_string(),
        }
    }

    async fn start_request(&self, request_id: &str) -> StartPreCommitRequest {
        StartPreCommitRequest {
            tenant_id: self.tenant_id.to_string(),
            project_id: self.project_id.to_string(),
            artifact_id: self.artifact_id.to_string(),
            workspace_id: self.workspace_id.to_string(),
            precommit_request_id: request_id.to_owned(),
            expected_index_version: self.index_version().await,
            data_layout: DataLayout::FastCdc,
        }
    }

    async fn add_request(&self, job_id: &str) -> CreateAddJobRequest {
        CreateAddJobRequest {
            tenant_id: self.tenant_id.to_string(),
            project_id: self.project_id.to_string(),
            artifact_id: self.artifact_id.to_string(),
            workspace_id: self.workspace_id.to_string(),
            job_id: job_id.to_owned(),
            expected_index_version: self.index_version().await,
            deadline_unix_ms: "86401000".to_owned(),
            paths: Vec::new(),
            all: true,
        }
    }
}

async fn fixture() -> Fixture {
    let components = InMemoryComponents::new(1_000);
    let tenant_id = TenantId::new("tenant-a").unwrap();
    let project_id = ProjectId::new("project-a").unwrap();
    let artifact_id = ArtifactId::new("artifact-a").unwrap();
    let workspace_id = WorkspaceId::new("workspace-a").unwrap();
    components
        .control_catalog
        .insert_tenant(TenantRecord {
            tenant_id: tenant_id.clone(),
            display_name: "Tenant A".to_owned(),
            description: None,
            resource_version: 1,
            created_at_unix_ms: UnixMillis::new(1_000),
            updated_at_unix_ms: UnixMillis::new(1_000),
        })
        .await
        .unwrap();
    components
        .control_catalog
        .insert_artifact(ArtifactRecord {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            display_name: "Artifact A".to_owned(),
            description: None,
            initialization: ArtifactInitialization::Empty,
            head_commit_id: None,
            resource_version: 1,
            lifecycle: neoengram_domain::protocol::ResourceLifecycle::active(),
            created_at_unix_ms: UnixMillis::new(1_000),
            updated_at_unix_ms: UnixMillis::new(1_000),
        })
        .await
        .unwrap();
    components
        .control_catalog
        .insert_storage_volume(StorageVolumeRecord {
            tenant_id: tenant_id.clone(),
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            display_name: "Volume A".to_owned(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            region: "cn-shanghai".to_owned(),
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
                claim_name: "volume-a".to_owned(),
            }),
            nfs_reference: None,
            state: StorageVolumeState::Ready,
            resource_version: 1,
            lifecycle: neoengram_domain::protocol::ResourceLifecycle::active(),
            created_at_unix_ms: UnixMillis::new(1_000),
            updated_at_unix_ms: UnixMillis::new(1_000),
        })
        .await
        .unwrap();
    components
        .control_catalog
        .insert_workspace(WorkspaceRecord {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            workspace_id: workspace_id.clone(),
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            region: "cn-shanghai".to_owned(),
            display_name: "Workspace A".to_owned(),
            base_commit_id: None,
            head_commit_id: None,
            state: WorkspaceState::Ready,
            resource_version: 1,
            lifecycle: neoengram_domain::protocol::ResourceLifecycle::active(),
            relative_root: "workspaces/project-a/artifact-a/workspace-a".to_owned(),
            created_at_unix_ms: UnixMillis::new(1_000),
            updated_at_unix_ms: UnixMillis::new(1_000),
        })
        .await
        .unwrap();
    let policy = Arc::new(
        StaticRbacPolicy::one_principal(
            "user-a",
            [tenant_id.to_string()],
            [
                Permission::WorkspaceRead,
                Permission::WorkspaceCreate,
                Permission::CreateAddJob,
            ],
        )
        .unwrap(),
    );
    let authority = components.authority_store();
    let control = Arc::new(ControlPlane::new(
        policy.clone(),
        authority.clone(),
        components.clock.clone(),
    ));
    let coordinator = Arc::new(
        JobCoordinator::from_authority(
            control.clone(),
            &authority,
            components.clock.clone(),
            30_000,
        )
        .unwrap(),
    );
    let catalog = CatalogService::new(
        components.control_catalog.clone(),
        components.publisher.clone(),
        policy,
        components.clock.clone(),
    )
    .with_precommits(components.precommits.clone())
    .with_coordinator(coordinator.clone())
    .with_storage_availability_provider(Arc::new(ReadyStorageAvailability));
    Fixture {
        components,
        catalog,
        coordinator,
        control,
        identity: AuthenticatedIdentity::new("user-a", PrincipalKind::User, "test", "subject-a")
            .unwrap(),
        tenant_id,
        project_id,
        artifact_id,
        workspace_id,
    }
}
