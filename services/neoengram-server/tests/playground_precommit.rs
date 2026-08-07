use std::sync::Arc;

use neoengram_protocol::{
    ArtifactId, EdgeClusterId, JobId, PlaygroundId, PrincipalKind, ProjectId, RequestId,
    StorageVolumeId, TenantId, UnixMillis,
};
use neoengram_server::{
    dto::{
        CancelPreCommitRequest, CreateAddJobRequest, IndexVersionBody, JsonExtensions,
        QueryPreCommitRequest, RestartPreCommitRequest, StartPreCommitRequest,
    },
    AuthenticatedIdentity, CatalogService, JobCoordinator, JobService, Permission,
    StaticRbacPolicy,
};
use neoengramd::{
    ArtifactInitialization, ArtifactRecord, CatalogPvcReference, Clock, ControlCatalogRepository,
    ControlPlane, InMemoryComponents, IndexKey, IndexPublisher, JobKey, JobOperation,
    JobRepository, PlaygroundRecord, PlaygroundState, PreCommitId, PreCommitRepository,
    PreCommitStartRequest as DomainPreCommitStartRequest, StorageAccessMode, StorageBackendType,
    StorageVolumeRecord, StorageVolumeState, TenantRecord,
};

#[tokio::test]
async fn precommit_actions_create_real_jobs_and_enforce_active_scope() {
    let fixture = fixture().await;
    let request = fixture.start_request("precommit-request-a").await;

    let started = fixture
        .catalog
        .start_playground_precommit(&fixture.identity, request.clone())
        .await
        .unwrap();

    assert!(!started.replayed);
    assert_eq!(started.precommit.state, "running");
    assert_eq!(started.precommit.phase, "queued");
    assert_eq!(
        started.playground.active_precommit_id.as_deref(),
        Some(started.precommit.precommit_id.as_str())
    );
    let precommit = fixture
        .components
        .precommits
        .get(&neoengramd::PreCommitKey::new(
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
        .start_playground_precommit(&fixture.identity, request)
        .await
        .unwrap();
    assert!(replayed.replayed);
    assert_eq!(
        replayed.precommit.precommit_id,
        started.precommit.precommit_id
    );

    let conflict = fixture
        .catalog
        .start_playground_precommit(
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
        .query_playground_precommit(
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
        .cancel_playground_precommit(
            &fixture.identity,
            CancelPreCommitRequest {
                tenant_id: fixture.tenant_id.to_string(),
                precommit_id: started.precommit.precommit_id.clone(),
                cancel_request_id: "cancel-request-a".to_owned(),
                extensions: JsonExtensions::default(),
            },
        )
        .await
        .unwrap();
    assert_eq!(cancelled.precommit.state, "cancelled");
    assert!(cancelled.playground.active_precommit_id.is_none());

    let restarted = fixture
        .catalog
        .restart_playground_precommit(
            &fixture.identity,
            RestartPreCommitRequest {
                tenant_id: fixture.tenant_id.to_string(),
                precommit_id: started.precommit.precommit_id,
                restart_request_id: "restart-request-a".to_owned(),
                expected_index_version: fixture.index_version().await,
                extensions: JsonExtensions::default(),
            },
        )
        .await
        .unwrap();
    assert_eq!(restarted.precommit.attempt, 2);
    assert_eq!(restarted.precommit.state, "running");
    assert_eq!(
        restarted.playground.active_precommit_id,
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
            playground_id: fixture.playground_id.clone(),
            precommit_id,
            precommit_request_id: RequestId::new("precommit-recovery-request").unwrap(),
            source_index_version,
            frozen_head_commit_id: None,
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

struct Fixture {
    components: InMemoryComponents,
    catalog: CatalogService,
    coordinator: Arc<JobCoordinator>,
    control: Arc<ControlPlane>,
    identity: AuthenticatedIdentity,
    tenant_id: TenantId,
    project_id: ProjectId,
    artifact_id: ArtifactId,
    playground_id: PlaygroundId,
}

impl Fixture {
    fn index_key(&self) -> IndexKey {
        IndexKey {
            tenant_id: self.tenant_id.clone(),
            project_id: self.project_id.clone(),
            artifact_id: self.artifact_id.clone(),
            playground_id: self.playground_id.clone(),
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
            playground_id: self.playground_id.to_string(),
            precommit_request_id: request_id.to_owned(),
            expected_index_version: self.index_version().await,
            extensions: JsonExtensions::default(),
        }
    }

    async fn add_request(&self, job_id: &str) -> CreateAddJobRequest {
        CreateAddJobRequest {
            tenant_id: self.tenant_id.to_string(),
            project_id: self.project_id.to_string(),
            artifact_id: self.artifact_id.to_string(),
            playground_id: self.playground_id.to_string(),
            job_id: job_id.to_owned(),
            expected_index_version: self.index_version().await,
            deadline_unix_ms: "86401000".to_owned(),
            paths: Vec::new(),
            all: true,
            extensions: JsonExtensions::default(),
        }
    }
}

async fn fixture() -> Fixture {
    let components = InMemoryComponents::new(1_000);
    let tenant_id = TenantId::new("tenant-a").unwrap();
    let project_id = ProjectId::new("project-a").unwrap();
    let artifact_id = ArtifactId::new("artifact-a").unwrap();
    let playground_id = PlaygroundId::new("playground-a").unwrap();
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
            pvc_reference: Some(CatalogPvcReference {
                namespace: "neoengram".to_owned(),
                claim_name: "volume-a".to_owned(),
            }),
            nfs_reference: None,
            state: StorageVolumeState::Ready,
            resource_version: 1,
            created_at_unix_ms: UnixMillis::new(1_000),
            updated_at_unix_ms: UnixMillis::new(1_000),
        })
        .await
        .unwrap();
    components
        .control_catalog
        .insert_playground(PlaygroundRecord {
            tenant_id: tenant_id.clone(),
            project_id: project_id.clone(),
            artifact_id: artifact_id.clone(),
            playground_id: playground_id.clone(),
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            region: "cn-shanghai".to_owned(),
            display_name: "Playground A".to_owned(),
            base_commit_id: None,
            head_commit_id: None,
            state: PlaygroundState::Ready,
            relative_root: "playgrounds/project-a/artifact-a/playground-a".to_owned(),
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
                Permission::PlaygroundRead,
                Permission::PlaygroundCreate,
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
    .with_coordinator(coordinator.clone());
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
        playground_id,
    }
}
