use std::sync::Arc;

use async_trait::async_trait;
use neoengram_central::dto::{
    CreateCommitReplicationRequest, QueryCommitAvailabilityRequest,
    QueryCommitPlacementListRequest, QueryCommitReplicationListRequest,
    QueryCommitReplicationRequest, QueryCommitReplicationTicketRequest,
    QueryStorageVolumeListRequest,
};
use neoengram_central::{
    AuthenticatedIdentity, CatalogPvcReference, CatalogService, CentralResult, CommitRecord,
    ControlCatalogRepository, InMemoryComponents, JobRecord, Permission, PlacementRepository,
    PreCommitCancelRequest, PreCommitCommitOutcome, PreCommitCommitRequest, PreCommitKey,
    PreCommitMutationOutcome, PreCommitRecord, PreCommitRepository, PreCommitRestartRequest,
    PreCommitStartRequest, PublishedIndex, ReplicationRecord, StaticRbacPolicy, StorageAccessMode,
    StorageBackendType, StorageVolumeRecord, StorageVolumeState, TenantRecord,
};
use neoengram_domain::core::{CommitId, ContentDigest, DirectoryId};
use neoengram_domain::protocol::{
    ArtifactId, CommitDataLayout, EdgeClusterId, Extensions, HardlinkPolicy, IndexRevision,
    PrincipalKind, ProjectId, RequestId, ResourceLifecycle, SnapshotDeliveryMode, StorageVolumeId,
    TenantId, UnixMillis, WireIndexVersion,
};

#[tokio::test]
async fn replication_create_binds_commit_scope_digest_and_writable_target() {
    let components = InMemoryComponents::new(100);
    let tenant_id = TenantId::new("tenant-service").unwrap();
    components
        .control_catalog
        .insert_tenant(tenant(&tenant_id))
        .await
        .unwrap();
    components
        .control_catalog
        .insert_storage_volume(read_only_volume(&tenant_id))
        .await
        .unwrap();
    let object_set = neoengram_domain::protocol::ObjectSet::new(Vec::new()).unwrap();
    let commit_id = ContentDigest::from_bytes([51; 32]);
    components
        .placement
        .insert_commit_object_set(neoengram_domain::protocol::CommitObjectSet {
            tenant_id: tenant_id.clone(),
            commit_id: CommitId::from_digest(commit_id),
            object_set: object_set.clone(),
        })
        .await
        .unwrap();
    let commit = commit_record(&tenant_id, commit_id, object_set.object_set_digest);
    let catalog = service(&components, &tenant_id, commit.clone());

    let targets = catalog
        .list_storage_volumes(
            &identity(),
            QueryStorageVolumeListRequest {
                tenant_id: tenant_id.to_string(),
                region: None,
                backend_type: None,
                cursor: None,
                page_size: None,
                query: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(targets.items.len(), 1);
    assert!(targets.items[0].pvc_reference.is_none());

    let wrong_project = catalog
        .create_commit_replication(
            &identity(),
            create_request(
                &tenant_id,
                "project-other",
                commit_id,
                "request-wrong-project",
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(wrong_project.code().as_str(), "resource_not_found");

    let mismatched = service(
        &components,
        &tenant_id,
        CommitRecord {
            object_set_digest: ContentDigest::from_bytes([52; 32]),
            ..commit.clone()
        },
    )
    .create_commit_replication(
        &identity(),
        create_request(&tenant_id, "project-a", commit_id, "request-mismatch"),
    )
    .await
    .unwrap_err();
    assert_eq!(mismatched.code().as_str(), "commit_object_set_mismatch");

    let read_only = catalog
        .create_commit_replication(
            &identity(),
            create_request(&tenant_id, "project-a", commit_id, "request-read-only"),
        )
        .await
        .unwrap_err();
    assert_eq!(read_only.code().as_str(), "storage_volume_not_writable");
}

#[tokio::test]
async fn replicate_permission_reads_replication_views_and_failed_ticket_is_rejected() {
    let components = InMemoryComponents::new(100);
    let tenant_id = TenantId::new("tenant-replication-read").unwrap();
    components
        .control_catalog
        .insert_tenant(tenant(&tenant_id))
        .await
        .unwrap();
    let commit_id = ContentDigest::from_bytes([53; 32]);
    let object_set = neoengram_domain::protocol::ObjectSet::new(Vec::new()).unwrap();
    components
        .placement
        .insert_commit_object_set(neoengram_domain::protocol::CommitObjectSet {
            tenant_id: tenant_id.clone(),
            commit_id: CommitId::from_digest(commit_id),
            object_set: object_set.clone(),
        })
        .await
        .unwrap();
    let record = failed_replication(&tenant_id, commit_id, object_set.object_set_digest);
    components
        .placement
        .insert_replication(record.clone())
        .await
        .unwrap();
    let service = service(
        &components,
        &tenant_id,
        commit_record(&tenant_id, commit_id, object_set.object_set_digest),
    );

    service
        .query_commit_replication(
            &identity(),
            QueryCommitReplicationRequest {
                tenant_id: tenant_id.to_string(),
                replication_id: record.replication_id.to_string(),
            },
        )
        .await
        .unwrap();
    service
        .query_commit_replication_list(
            &identity(),
            QueryCommitReplicationListRequest {
                tenant_id: tenant_id.to_string(),
                commit_id: commit_id.to_string(),
            },
        )
        .await
        .unwrap();
    service
        .query_commit_placement_list(
            &identity(),
            QueryCommitPlacementListRequest {
                tenant_id: tenant_id.to_string(),
                commit_id: commit_id.to_string(),
            },
        )
        .await
        .unwrap();
    service
        .query_commit_availability(
            &identity(),
            QueryCommitAvailabilityRequest {
                tenant_id: tenant_id.to_string(),
                commit_id: commit_id.to_string(),
            },
        )
        .await
        .unwrap();

    let ticket = service
        .query_commit_replication_ticket(
            &identity(),
            QueryCommitReplicationTicketRequest {
                tenant_id: tenant_id.to_string(),
                replication_id: record.replication_id.to_string(),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(ticket.code().as_str(), "replication_not_active");
}

fn service(
    components: &InMemoryComponents,
    tenant_id: &TenantId,
    commit: CommitRecord,
) -> CatalogService {
    let policy = Arc::new(
        StaticRbacPolicy::one_principal(
            "user-a",
            [tenant_id.to_string()],
            [Permission::ArtifactCommitReplicate],
        )
        .unwrap(),
    );
    CatalogService::new(
        components.control_catalog.clone(),
        components.publisher.clone(),
        policy,
        components.clock.clone(),
    )
    .with_precommits(Arc::new(CommitLookupRepository { commit }))
    .with_placement_repository(components.placement.clone())
}

fn identity() -> AuthenticatedIdentity {
    AuthenticatedIdentity::new("user-a", PrincipalKind::User, "test", "subject-a").unwrap()
}

fn tenant(tenant_id: &TenantId) -> TenantRecord {
    TenantRecord {
        tenant_id: tenant_id.clone(),
        display_name: "Tenant".to_owned(),
        description: None,
        resource_version: 1,
        created_at_unix_ms: UnixMillis::new(1),
        updated_at_unix_ms: UnixMillis::new(1),
    }
}

fn read_only_volume(tenant_id: &TenantId) -> StorageVolumeRecord {
    StorageVolumeRecord {
        tenant_id: tenant_id.clone(),
        storage_volume_id: StorageVolumeId::new("volume-read-only").unwrap(),
        display_name: "Read-only".to_owned(),
        edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
        region: "local".to_owned(),
        backend_type: StorageBackendType::Pvc,
        access_mode: StorageAccessMode::ReadOnlyMany,
        allowed_delivery_modes: vec![SnapshotDeliveryMode::Fuse],
        hardlink_policy: HardlinkPolicy::Disabled,
        max_whole_file_bytes: neoengram_domain::protocol::DecimalU64::new(u64::MAX),
        copy_reserve_bytes: neoengram_domain::protocol::DecimalU64::new(0),
        pvc_reference: Some(CatalogPvcReference {
            namespace: "neoengram".to_owned(),
            claim_name: "read-only".to_owned(),
        }),
        nfs_reference: None,
        state: StorageVolumeState::Ready,
        resource_version: 1,
        lifecycle: ResourceLifecycle::active(),
        created_at_unix_ms: UnixMillis::new(1),
        updated_at_unix_ms: UnixMillis::new(1),
    }
}

fn create_request(
    tenant_id: &TenantId,
    project_id: &str,
    commit_id: ContentDigest,
    request_id: &str,
) -> CreateCommitReplicationRequest {
    CreateCommitReplicationRequest {
        tenant_id: tenant_id.to_string(),
        project_id: project_id.to_owned(),
        artifact_id: "artifact-a".to_owned(),
        commit_id: commit_id.to_string(),
        target_storage_volume_id: "volume-read-only".to_owned(),
        request_id: request_id.to_owned(),
    }
}

fn commit_record(
    tenant_id: &TenantId,
    commit_id: ContentDigest,
    object_set_digest: ContentDigest,
) -> CommitRecord {
    CommitRecord {
        tenant_id: tenant_id.clone(),
        project_id: ProjectId::new("project-a").unwrap(),
        artifact_id: ArtifactId::new("artifact-a").unwrap(),
        source_playground_id: neoengram_domain::protocol::PlaygroundId::new("playground-a")
            .unwrap(),
        source_precommit_id: neoengram_central::PreCommitId::new("precommit-a").unwrap(),
        commit_request_id: RequestId::new("commit-request-a").unwrap(),
        commit_id: CommitId::from_digest(commit_id),
        object_set_digest,
        root_directory_id: DirectoryId::from_bytes([54; 32]),
        parent_commit_id: None,
        index_version: WireIndexVersion {
            revision: IndexRevision::new(1),
            digest: ContentDigest::from_bytes([55; 32]),
            extensions: Extensions::new(),
        },
        data_layout: CommitDataLayout::FastCdc,
        records: Vec::new(),
        message: "Commit".to_owned(),
        description: None,
        tag_names: Vec::new(),
        created_at_unix_ms: UnixMillis::new(1),
    }
}

fn failed_replication(
    tenant_id: &TenantId,
    commit_id: ContentDigest,
    object_set_digest: ContentDigest,
) -> ReplicationRecord {
    ReplicationRecord {
        tenant_id: tenant_id.clone(),
        replication_id: neoengram_domain::protocol::ReplicationId::new("replication-failed")
            .unwrap(),
        artifact_id: Some(ArtifactId::new("artifact-a").unwrap()),
        commit_id,
        target_backend_id: "volume-target".to_owned(),
        target_storage_volume_id: StorageVolumeId::new("volume-target").unwrap(),
        source_placement_set_id: None,
        source_backend_id: None,
        source_storage_volume_id: None,
        source_edge_cluster_id: None,
        source_gateway_pool_id: None,
        source_placement_generation: None,
        source_agent_id: None,
        source_session_generation: None,
        source_mount_generation: None,
        source_route_generation: None,
        target_edge_cluster_id: None,
        target_gateway_pool_id: None,
        target_placement_generation: None,
        target_agent_id: None,
        target_session_generation: None,
        target_mount_generation: None,
        target_route_generation: None,
        transfer_route_id: None,
        transfer_id: None,
        target_placement_set_id: None,
        staging_id: None,
        object_set_digest,
        state: neoengram_domain::protocol::ReplicationState::Failed,
        request_id: RequestId::new("replication-request-failed").unwrap(),
        attempt: 1,
        completed_objects: 0,
        total_objects: 0,
        completed_bytes: 0,
        total_bytes: 0,
        issue_code: Some("FAILED".to_owned()),
        issue_message: Some("failed".to_owned()),
        created_at_unix_ms: UnixMillis::new(1),
        updated_at_unix_ms: UnixMillis::new(1),
    }
}

struct CommitLookupRepository {
    commit: CommitRecord,
}

impl CommitLookupRepository {
    fn unused<T>() -> CentralResult<T> {
        panic!("unexpected PreCommitRepository operation")
    }
}

#[async_trait]
impl PreCommitRepository for CommitLookupRepository {
    async fn start(
        &self,
        _request: PreCommitStartRequest,
    ) -> CentralResult<PreCommitMutationOutcome> {
        Self::unused()
    }

    async fn get(&self, _key: &PreCommitKey) -> CentralResult<Option<PreCommitRecord>> {
        Self::unused()
    }

    async fn get_active(
        &self,
        _tenant_id: &TenantId,
        _project_id: &ProjectId,
        _artifact_id: &ArtifactId,
        _playground_id: &neoengram_domain::protocol::PlaygroundId,
    ) -> CentralResult<Option<PreCommitRecord>> {
        Self::unused()
    }

    async fn list_running(
        &self,
        _after: Option<&PreCommitKey>,
        _limit: usize,
    ) -> CentralResult<Vec<PreCommitRecord>> {
        Self::unused()
    }

    async fn list_unpublished_commits(
        &self,
        _after: Option<&PreCommitKey>,
        _limit: usize,
    ) -> CentralResult<Vec<PreCommitRecord>> {
        Self::unused()
    }

    async fn find_restart_result(
        &self,
        _tenant_id: &TenantId,
        _restart_request_id: &RequestId,
    ) -> CentralResult<Option<PreCommitRecord>> {
        Self::unused()
    }

    async fn restart(
        &self,
        _request: PreCommitRestartRequest,
    ) -> CentralResult<PreCommitMutationOutcome> {
        Self::unused()
    }

    async fn cancel(
        &self,
        _request: PreCommitCancelRequest,
    ) -> CentralResult<PreCommitMutationOutcome> {
        Self::unused()
    }

    async fn sync_job(
        &self,
        _job: JobRecord,
        _published_index: Option<PublishedIndex>,
        _observed_at_unix_ms: UnixMillis,
    ) -> CentralResult<Option<PreCommitRecord>> {
        Self::unused()
    }

    async fn commit(
        &self,
        _request: PreCommitCommitRequest,
    ) -> CentralResult<PreCommitCommitOutcome> {
        Self::unused()
    }

    async fn get_commit(
        &self,
        tenant_id: &TenantId,
        project_id: &ProjectId,
        artifact_id: &ArtifactId,
        commit_id: CommitId,
    ) -> CentralResult<Option<CommitRecord>> {
        Ok((&self.commit.tenant_id == tenant_id
            && &self.commit.project_id == project_id
            && &self.commit.artifact_id == artifact_id
            && self.commit.commit_id == commit_id)
            .then(|| self.commit.clone()))
    }

    async fn list_published_commits(
        &self,
        _tenant_id: &TenantId,
        _project_id: &ProjectId,
        _artifact_id: &ArtifactId,
    ) -> CentralResult<Vec<CommitRecord>> {
        Self::unused()
    }

    async fn acknowledge_head_publication(
        &self,
        _key: &PreCommitKey,
        _commit_id: CommitId,
        _published_at_unix_ms: UnixMillis,
    ) -> CentralResult<PreCommitRecord> {
        Self::unused()
    }
}
