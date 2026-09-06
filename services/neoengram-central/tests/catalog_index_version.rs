use std::sync::Arc;

use neoengram_central::{
    dto::{
        CreateAddJobRequest, CreateWorkspaceRequest, IndexVersionBody, QueryWorkspaceListRequest,
        QueryWorkspaceRequest,
    },
    AuthenticatedIdentity, CatalogService, JobService, Permission, StaticRbacPolicy,
};
use neoengram_central::{
    ArtifactInitialization, ArtifactRecord, CatalogPvcReference, ControlCatalogRepository,
    ControlPlane, InMemoryComponents, IndexKey, IndexPublishOutcome, IndexPublishRequest,
    IndexPublisher, JobKey, JobRepository, StorageAccessMode, StorageBackendType,
    StorageVolumeRecord, StorageVolumeState, TenantRecord,
};
use neoengram_domain::core::{ChunkingStrategy, FileRecord, IndexVersion, LogicalPath, Manifest};
use neoengram_domain::protocol::{
    ArtifactId, DecimalU64, EdgeClusterId, Extensions, IndexDeltaRecord, JobId, PrincipalKind,
    ProjectId, StorageVolumeId, TenantId, UnixMillis, WorkspaceId,
};

mod support;
use support::ReadyStorageAvailability;

#[tokio::test]
async fn job_authorization_precedes_missing_scope_validation() {
    let components = InMemoryComponents::new(1_000);
    let tenant_id = TenantId::new("tenant-a").unwrap();
    let policy = Arc::new(
        StaticRbacPolicy::one_principal(
            "user-a",
            [tenant_id.to_string()],
            std::iter::empty::<Permission>(),
        )
        .unwrap(),
    );
    let authority_store = components.authority_store();
    let control = Arc::new(ControlPlane::new(
        policy,
        authority_store.clone(),
        components.clock.clone(),
    ));
    let jobs = JobService::from_authority(control, &authority_store).unwrap();
    let identity =
        AuthenticatedIdentity::new("user-a", PrincipalKind::User, "test", "subject-a").unwrap();
    let index = IndexVersion::from_snapshot(0, &[]).unwrap();
    let job_id = JobId::new("job-unauthorized-scope").unwrap();

    let error = jobs
        .create_add_job(
            &identity,
            CreateAddJobRequest {
                tenant_id: tenant_id.to_string(),
                project_id: "project-missing".to_owned(),
                artifact_id: "artifact-missing".to_owned(),
                workspace_id: "workspace-missing".to_owned(),
                job_id: job_id.to_string(),
                expected_index_version: IndexVersionBody {
                    revision: index.revision.to_string(),
                    digest: index.digest.to_string(),
                },
                deadline_unix_ms: "10000".to_owned(),
                paths: Vec::new(),
                all: true,
            },
        )
        .await
        .unwrap_err();

    assert_eq!(error.code().as_str(), "authorization_denied");
    assert!(components
        .jobs
        .get(&JobKey::new(tenant_id, job_id))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn job_scope_validation_does_not_depend_on_agent_scheduling() {
    let components = InMemoryComponents::new(1_000);
    let tenant_id = TenantId::new("tenant-a").unwrap();
    let policy = Arc::new(
        StaticRbacPolicy::one_principal(
            "user-a",
            [tenant_id.to_string()],
            [Permission::CreateAddJob],
        )
        .unwrap(),
    );
    let authority_store = components.authority_store();
    let control = Arc::new(ControlPlane::new(
        policy,
        authority_store.clone(),
        components.clock.clone(),
    ));
    let jobs = JobService::from_authority(control, &authority_store).unwrap();
    let identity =
        AuthenticatedIdentity::new("user-a", PrincipalKind::User, "test", "subject-a").unwrap();
    let index = IndexVersion::from_snapshot(0, &[]).unwrap();

    let error = jobs
        .create_add_job(
            &identity,
            CreateAddJobRequest {
                tenant_id: tenant_id.to_string(),
                project_id: "project-missing".to_owned(),
                artifact_id: "artifact-missing".to_owned(),
                workspace_id: "workspace-missing".to_owned(),
                job_id: "job-missing-scope".to_owned(),
                expected_index_version: IndexVersionBody {
                    revision: index.revision.to_string(),
                    digest: index.digest.to_string(),
                },
                deadline_unix_ms: "10000".to_owned(),
                paths: Vec::new(),
                all: true,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.code().as_str(), "job_not_found");
    assert!(components
        .jobs
        .get(&JobKey::new(
            tenant_id,
            JobId::new("job-missing-scope").unwrap()
        ))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn workspace_responses_use_the_current_published_index_version() {
    let components = InMemoryComponents::new(1_000);
    let tenant_id = TenantId::new("tenant-a").unwrap();
    let project_id = ProjectId::new("project-a").unwrap();
    let artifact_id = ArtifactId::new("artifact-a").unwrap();
    let workspace_id = WorkspaceId::new("workspace-a").unwrap();
    let storage_volume_id = StorageVolumeId::new("volume-a").unwrap();
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
            storage_volume_id: storage_volume_id.clone(),
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
                claim_name: "data-a".to_owned(),
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
    let service = CatalogService::new(
        components.control_catalog.clone(),
        components.publisher.clone(),
        policy.clone(),
        components.clock.clone(),
    )
    .with_storage_availability_provider(Arc::new(ReadyStorageAvailability));
    let authority_store = components.authority_store();
    let control = Arc::new(ControlPlane::new(
        policy,
        authority_store.clone(),
        components.clock.clone(),
    ));
    let jobs = JobService::from_authority(control, &authority_store).unwrap();
    let identity =
        AuthenticatedIdentity::new("user-a", PrincipalKind::User, "test", "subject-a").unwrap();
    let create_request = CreateWorkspaceRequest {
        tenant_id: tenant_id.to_string(),
        project_id: project_id.to_string(),
        artifact_id: artifact_id.to_string(),
        workspace_id: workspace_id.to_string(),
        storage_volume_id: storage_volume_id.to_string(),
        display_name: "Workspace A".to_owned(),
        base_commit_id: None,
    };
    let created = service
        .create_workspace(&identity, create_request.clone())
        .await
        .unwrap();
    assert_eq!(created.workspace.index_version.revision, "0");

    let index_key = IndexKey {
        tenant_id: tenant_id.clone(),
        project_id: project_id.clone(),
        artifact_id: artifact_id.clone(),
        workspace_id: workspace_id.clone(),
    };
    let initial_version = components
        .publisher
        .current_version(&index_key)
        .await
        .unwrap();
    let add_request = CreateAddJobRequest {
        tenant_id: tenant_id.to_string(),
        project_id: project_id.to_string(),
        artifact_id: artifact_id.to_string(),
        workspace_id: workspace_id.to_string(),
        job_id: "job-replay".to_owned(),
        expected_index_version: IndexVersionBody {
            revision: initial_version.revision.to_string(),
            digest: initial_version.digest.to_string(),
        },
        deadline_unix_ms: "10000".to_owned(),
        paths: Vec::new(),
        all: true,
    };
    let first_job = jobs
        .create_add_job(&identity, add_request.clone())
        .await
        .unwrap();
    assert!(!first_job.replayed);
    let manifest = Manifest::new(0, ChunkingStrategy::FastCdc, Vec::new()).unwrap();
    let manifest_id = manifest.canonical_id().unwrap();
    let path = LogicalPath::parse("data.csv").unwrap();
    let file = FileRecord::from_manifest(path.clone(), &manifest).unwrap();
    let result_digest = IndexVersion::from_snapshot(0, std::slice::from_ref(&file))
        .unwrap()
        .digest;
    let published = components
        .publisher
        .compare_and_swap(IndexPublishRequest {
            job_key: JobKey::new(tenant_id.clone(), JobId::new("job-a").unwrap()),
            index_key,
            expected_index_version: initial_version,
            expected_result_digest: result_digest,
            manifests: vec![manifest],
            mutations: vec![IndexDeltaRecord::Upsert {
                path,
                manifest_id,
                total_size: DecimalU64::new(0),
                chunk_count: DecimalU64::new(0),
                extensions: Extensions::new(),
            }],
        })
        .await
        .unwrap();
    let IndexPublishOutcome::Published(published_version) = published else {
        panic!("the empty Index CAS must publish");
    };
    assert_eq!(published_version.revision.to_string(), "1");

    let queried = service
        .query_workspace(
            &identity,
            QueryWorkspaceRequest {
                tenant_id: tenant_id.to_string(),
                project_id: project_id.to_string(),
                artifact_id: artifact_id.to_string(),
                workspace_id: workspace_id.to_string(),
            },
        )
        .await
        .unwrap();
    assert_eq!(queried.workspace.index_version.revision, "1");
    assert_eq!(
        queried.workspace.index_version.digest,
        published_version.digest.to_string()
    );

    let listed = service
        .list_workspaces(
            &identity,
            QueryWorkspaceListRequest {
                tenant_id: tenant_id.to_string(),
                project_id: Some(project_id.to_string()),
                artifact_id: Some(artifact_id.to_string()),
                region: None,
                state: None,
                cursor: None,
                page_size: None,
                query: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(listed.items[0].index_version.revision, "1");

    let replayed = service
        .create_workspace(&identity, create_request)
        .await
        .unwrap();
    assert!(replayed.request_replayed);
    assert_eq!(replayed.workspace.index_version.revision, "1");

    let replayed_job = jobs.create_add_job(&identity, add_request).await.unwrap();
    assert!(replayed_job.replayed);
    assert_eq!(replayed_job.job.job_id, "job-replay");
}
