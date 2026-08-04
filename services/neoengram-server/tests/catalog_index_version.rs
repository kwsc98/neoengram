use std::sync::Arc;

use neoengram_core::{ChunkingStrategy, FileRecord, IndexVersion, LogicalPath, Manifest};
use neoengram_protocol::{
    ArtifactId, DecimalU64, EdgeClusterId, Extensions, IndexDeltaRecord, JobId, PlaygroundId,
    PrincipalKind, ProjectId, StorageVolumeId, TenantId, UnixMillis,
};
use neoengram_server::{
    dto::{
        CreateAddJobRequest, CreatePlaygroundRequest, IndexVersionBody, JsonExtensions,
        QueryPlaygroundListRequest, QueryPlaygroundRequest,
    },
    AuthenticatedIdentity, CatalogService, JobCoordinator, JobService, Permission,
    StaticRbacPolicy,
};
use neoengramd::{
    CatalogPvcReference, ControlCatalogRepository, ControlPlane, InMemoryComponents, IndexKey,
    IndexPublishOutcome, IndexPublishRequest, IndexPublisher, JobKey, StorageAccessMode,
    StorageBackendType, StorageVolumeRecord, StorageVolumeState, TenantRecord,
};

#[tokio::test]
async fn playground_responses_use_the_current_published_index_version() {
    let components = InMemoryComponents::new(1_000);
    let tenant_id = TenantId::new("tenant-a").unwrap();
    let project_id = ProjectId::new("project-a").unwrap();
    let artifact_id = ArtifactId::new("artifact-a").unwrap();
    let playground_id = PlaygroundId::new("playground-a").unwrap();
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
        .insert_storage_volume(StorageVolumeRecord {
            tenant_id: tenant_id.clone(),
            storage_volume_id: storage_volume_id.clone(),
            display_name: "Volume A".to_owned(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            region: "cn-shanghai".to_owned(),
            backend_type: StorageBackendType::Pvc,
            access_mode: StorageAccessMode::ReadWriteMany,
            pvc_reference: Some(CatalogPvcReference {
                namespace: "neoengram".to_owned(),
                claim_name: "data-a".to_owned(),
            }),
            nfs_reference: None,
            state: StorageVolumeState::Ready,
            resource_version: 1,
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
    let service = CatalogService::new(
        components.control_catalog.clone(),
        components.publisher.clone(),
        policy.clone(),
        components.clock.clone(),
    );
    let authority_store = components.authority_store();
    let control = Arc::new(ControlPlane::new(
        policy,
        authority_store.clone(),
        components.clock.clone(),
    ));
    let coordinator = Arc::new(
        JobCoordinator::from_authority(
            control.clone(),
            &authority_store,
            components.clock.clone(),
            30_000,
        )
        .unwrap(),
    );
    let jobs = JobService::new(control).with_coordinator(coordinator);
    let identity =
        AuthenticatedIdentity::new("user-a", PrincipalKind::User, "test", "subject-a").unwrap();
    let create_request = CreatePlaygroundRequest {
        tenant_id: tenant_id.to_string(),
        project_id: project_id.to_string(),
        artifact_id: artifact_id.to_string(),
        playground_id: playground_id.to_string(),
        storage_volume_id: storage_volume_id.to_string(),
        display_name: "Playground A".to_owned(),
        base_commit_id: None,
        extensions: JsonExtensions::default(),
    };
    let created = service
        .create_playground(&identity, create_request.clone())
        .await
        .unwrap();
    assert_eq!(created.playground.index_version.revision, "0");

    let index_key = IndexKey {
        tenant_id: tenant_id.clone(),
        project_id: project_id.clone(),
        artifact_id: artifact_id.clone(),
        playground_id: playground_id.clone(),
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
        playground_id: playground_id.to_string(),
        job_id: "job-replay".to_owned(),
        expected_index_version: IndexVersionBody {
            revision: initial_version.revision.to_string(),
            digest: initial_version.digest.to_string(),
        },
        deadline_unix_ms: "10000".to_owned(),
        paths: Vec::new(),
        all: true,
        extensions: JsonExtensions::default(),
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
        .query_playground(
            &identity,
            QueryPlaygroundRequest {
                tenant_id: tenant_id.to_string(),
                project_id: project_id.to_string(),
                artifact_id: artifact_id.to_string(),
                playground_id: playground_id.to_string(),
            },
        )
        .await
        .unwrap();
    assert_eq!(queried.playground.index_version.revision, "1");
    assert_eq!(
        queried.playground.index_version.digest,
        published_version.digest.to_string()
    );

    let listed = service
        .list_playgrounds(
            &identity,
            QueryPlaygroundListRequest {
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
        .create_playground(&identity, create_request)
        .await
        .unwrap();
    assert!(replayed.replayed);
    assert_eq!(replayed.playground.index_version.revision, "1");

    let replayed_job = jobs.create_add_job(&identity, add_request).await.unwrap();
    assert!(replayed_job.replayed);
    assert_eq!(replayed_job.job.job_id, "job-replay");
}
