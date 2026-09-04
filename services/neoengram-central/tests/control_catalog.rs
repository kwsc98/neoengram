use std::{str::FromStr, sync::Arc};

use neoengram_central::{
    open_sqlite_authority, AdvancePlaygroundCommitRequest, ArtifactHeadExpectation,
    ArtifactInitialization, ArtifactListRequest, ArtifactRecord, AuthorityLifecycleImpact,
    CatalogInsertOutcome, CatalogNfsReference, CatalogPvcReference, CentralErrorCode,
    ControlCatalogRepository, CreateDeletionRequest, CreateRetentionHoldRequest,
    DeletionImpactQuery, DeletionTransitionRequest, GatewayPoolRecord, GatewayPoolState,
    GatewayRegistryRepository, InMemoryControlCatalog, LifecycleAssignmentInsertOutcome,
    LifecycleAssignmentOutboxRecord, PlaygroundInsertRequest, PlaygroundListRequest,
    PlaygroundRecord, PlaygroundState, ReleaseRetentionHoldRequest, RestoreDeletionRequest,
    RetryDeletionRequest, S3AccessPointInsertOutcome, S3AccessPointRecord, S3AccessPointState,
    S3CredentialInsertOutcome, S3CredentialRecord, S3CredentialState, S3MutationKind,
    S3MutationRecord, SnapshotDeliveryInsertOutcome, SnapshotDeliveryInsertRequest,
    SnapshotDeliveryMutationKind, SnapshotDeliveryMutationRequest, SnapshotDeliveryRecord,
    SnapshotDeliveryRetentionRoot, SnapshotInsertRequest, SnapshotRecord, SnapshotState,
    SnapshotWithDeliveryInsertRequest, SqliteAuthorityConfig, StorageAccessMode,
    StorageBackendType, StorageVolumeListRequest, StorageVolumeRecord, StorageVolumeState,
    TenantListRequest, TenantRecord,
};
use neoengram_domain::core::{ContentDigest, LogicalPath, ObjectId};
use neoengram_domain::protocol::{
    AgentId, AgentMountId, AgentResourceLifecycleAssignment, AgentResourceLifecycleScope,
    ArtifactId, DecimalU64, DeletionId, DeletionOperationState, DeliveryGeneration, EdgeClusterId,
    Extensions, GatewayPoolId, Generation, LifecycleAssignmentId, MountGeneration, OwnerGeneration,
    PlaygroundId, PrincipalId, PrincipalKind, PrincipalRef, ProjectId, RequestId,
    ResourceLifecycle, ResourceLifecycleAction, ResourceLifecycleAssignment,
    ResourceLifecycleState, ResourceRef, ResourceVersion, RetentionHoldId, S3AccessPointId,
    S3CredentialId, SessionGeneration, SnapshotDeliveryId, SnapshotDeliveryMode,
    SnapshotDeliveryState, SnapshotId, StorageVolumeId, TenantId, UnixMillis, VolumeMarkerId,
    DELETION_RECOVERY_WINDOW_MILLIS,
};
use sqlx::{sqlite::SqliteConnectOptions, Connection, SqliteConnection};

#[tokio::test]
async fn sqlite_catalog_persists_idempotent_resources_and_keyset_pages() {
    let directory = tempfile::tempdir().unwrap();
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let repository = authority.authority_store().control_catalog().unwrap();
    let tenant_record = tenant("tenant-a", "Research");
    assert!(matches!(
        repository
            .insert_tenant(tenant_record.clone())
            .await
            .unwrap(),
        CatalogInsertOutcome::Inserted(_)
    ));
    assert!(matches!(
        repository
            .insert_tenant(tenant_record.clone())
            .await
            .unwrap(),
        CatalogInsertOutcome::Existing(_)
    ));
    assert!(repository
        .insert_tenant(tenant("tenant-a", "Different"))
        .await
        .is_err());
    repository
        .insert_tenant(tenant("tenant-b", "Second"))
        .await
        .unwrap();

    let first_page = repository
        .list_tenants(&TenantListRequest {
            visible_tenant_ids: None,
            query: None,
            after: None,
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(first_page.records.len(), 1);
    let second_page = repository
        .list_tenants(&TenantListRequest {
            visible_tenant_ids: None,
            query: None,
            after: first_page.next,
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(second_page.records.len(), 1);
    assert_ne!(
        first_page.records[0].tenant_id,
        second_page.records[0].tenant_id
    );

    let volume = pvc_volume("tenant-a", "volume-a", "claim-a");
    repository
        .insert_storage_volume(volume.clone())
        .await
        .unwrap();
    assert!(repository
        .insert_storage_volume(pvc_volume("tenant-b", "volume-b", "claim-a"))
        .await
        .is_err());
    repository
        .insert_storage_volume(nfs_volume("tenant-a", "volume-nfs"))
        .await
        .unwrap();
    let volumes = repository
        .list_storage_volumes(&StorageVolumeListRequest {
            tenant_id: id(TenantId::new, "tenant-a"),
            region: None,
            backend_type: None,
            query: None,
            after: None,
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(volumes.records.len(), 2);

    let mut playground = playground();
    playground.state = PlaygroundState::Creating;
    repository
        .insert_artifact(artifact("tenant-a", "project-a", "artifact-a", 150))
        .await
        .unwrap();
    repository
        .insert_playground(playground.clone())
        .await
        .unwrap();
    assert_eq!(
        repository
            .get_playground(
                &playground.tenant_id,
                &playground.project_id,
                &playground.artifact_id,
                &playground.playground_id,
            )
            .await
            .unwrap(),
        Some(playground.clone())
    );
    let ready = repository
        .transition_playground_state(
            &playground.tenant_id,
            &playground.project_id,
            &playground.artifact_id,
            &playground.playground_id,
            PlaygroundState::Creating,
            PlaygroundState::Ready,
            UnixMillis::new(250),
        )
        .await
        .unwrap();
    assert_eq!(ready.state, PlaygroundState::Ready);
    assert_eq!(ready.updated_at_unix_ms, UnixMillis::new(250));
    assert_eq!(
        repository
            .transition_playground_state(
                &playground.tenant_id,
                &playground.project_id,
                &playground.artifact_id,
                &playground.playground_id,
                PlaygroundState::Creating,
                PlaygroundState::Ready,
                UnixMillis::new(300),
            )
            .await
            .unwrap(),
        ready,
        "a terminal report replay must not mutate the Playground again"
    );
    let page = repository
        .list_playgrounds(&PlaygroundListRequest {
            tenant_id: playground.tenant_id.clone(),
            project_id: Some(playground.project_id.clone()),
            artifact_id: Some(playground.artifact_id.clone()),
            region: Some("cn-shanghai".to_owned()),
            state: Some(PlaygroundState::Ready),
            query: Some("label".to_owned()),
            after: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(page.records, [ready]);

    authority.close().await;
    let reopened = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    assert!(reopened
        .authority_store()
        .control_catalog()
        .unwrap()
        .get_tenant(&id(TenantId::new, "tenant-a"))
        .await
        .unwrap()
        .is_some());
    reopened.close().await;
}

#[tokio::test]
async fn clean_catalog_creates_current_snapshot_and_delivery_schema() {
    let directory = tempfile::tempdir().unwrap();
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    authority.integrity_check().await.unwrap();
    authority.close().await;

    let options = SqliteConnectOptions::new().filename(directory.path().join("authority.sqlite3"));
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(version, 20);

    let snapshot_columns: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM pragma_table_info('snapshot_catalog_records') ORDER BY cid",
    )
    .fetch_all(&mut connection)
    .await
    .unwrap();
    assert_eq!(
        snapshot_columns
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        [
            "tenant_id",
            "project_id",
            "artifact_id",
            "snapshot_id",
            "snapshot_request_id",
            "commit_digest",
            "delivery_id",
            "edge_cluster_id",
            "storage_volume_id",
            "delivery_mode",
            "state",
            "resource_version",
            "lifecycle_state",
            "lifecycle_generation",
            "active_deletion_id",
            "delete_requested_at_unix_ms",
            "purge_after_unix_ms",
            "deleted_at_unix_ms",
            "created_at_unix_ms",
            "updated_at_unix_ms",
        ]
    );
    assert!(!snapshot_columns.iter().any(|column| column == "phase"));
    assert!(!snapshot_columns
        .iter()
        .any(|column| column == "relative_root"));

    let access_point_columns: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM pragma_table_info('s3_access_point_records') ORDER BY cid",
    )
    .fetch_all(&mut connection)
    .await
    .unwrap();
    assert!(!access_point_columns
        .iter()
        .any(|column| { column == "gateway_pool_id" || column == "region" }));

    let delivery_columns: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM pragma_table_info('snapshot_delivery_records') ORDER BY cid",
    )
    .fetch_all(&mut connection)
    .await
    .unwrap();
    for required in [
        "delivery_id",
        "snapshot_id",
        "commit_digest",
        "storage_volume_id",
        "mode",
        "target_relative_root",
        "source_index_digest",
        "delivery_generation",
        "object_set_digest",
        "resource_version",
        "issue_retryable",
    ] {
        assert!(
            delivery_columns.iter().any(|column| column == required),
            "missing SnapshotDelivery column {required}"
        );
    }
    let delivery_tables: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_schema WHERE type = 'table' AND name LIKE \
         'snapshot_delivery_%' ORDER BY name",
    )
    .fetch_all(&mut connection)
    .await
    .unwrap();
    assert!(delivery_tables
        .iter()
        .any(|table| table == "snapshot_delivery_object_retention_roots"));
    assert!(delivery_tables
        .iter()
        .any(|table| table == "snapshot_delivery_mutation_records"));
    connection.close().await.unwrap();
}

#[tokio::test]
async fn snapshot_delivery_retention_and_mutation_contract_matches_memory_and_sqlite() {
    let memory: Arc<dyn ControlCatalogRepository> = Arc::new(InMemoryControlCatalog::default());
    exercise_snapshot_delivery_retention(memory).await;

    let directory = tempfile::tempdir().unwrap();
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let sqlite = authority.authority_store().control_catalog().unwrap();
    exercise_snapshot_delivery_retention(sqlite).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

#[tokio::test]
async fn memory_and_sqlite_artifact_catalog_follow_the_same_contract() {
    let memory = InMemoryControlCatalog::default();
    exercise_artifact_contract(&memory).await;

    let directory = tempfile::tempdir().unwrap();
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let sqlite = authority.authority_store().control_catalog().unwrap();
    exercise_artifact_contract(sqlite.as_ref()).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

#[tokio::test]
async fn memory_and_sqlite_playground_head_fences_follow_the_same_contract() {
    let memory = InMemoryControlCatalog::default();
    exercise_playground_head_fence_contract(&memory).await;

    let directory = tempfile::tempdir().unwrap();
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let sqlite = authority.authority_store().control_catalog().unwrap();
    exercise_playground_head_fence_contract(sqlite.as_ref()).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

#[tokio::test]
async fn sqlite_playground_commit_digests_are_null_or_exactly_32_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let repository = authority.authority_store().control_catalog().unwrap();
    repository
        .insert_tenant(tenant("tenant-a", "Digest checks"))
        .await
        .unwrap();
    repository
        .insert_artifact(artifact("tenant-a", "project-a", "artifact-a", 100))
        .await
        .unwrap();
    repository
        .insert_storage_volume(pvc_volume("tenant-a", "volume-a", "claim-a"))
        .await
        .unwrap();
    authority.close().await;

    let options = SqliteConnectOptions::new().filename(directory.path().join("authority.sqlite3"));
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    for (playground_id, base, head) in [
        ("invalid-base", Some(vec![1_u8]), None),
        ("invalid-head", None, Some(vec![2_u8; 31])),
    ] {
        let result = sqlx::query(
            "INSERT INTO playground_catalog_records \
             (tenant_id, project_id, artifact_id, playground_id, storage_volume_id, region, \
              display_name, base_commit_digest, head_commit_digest, state, relative_root, \
              created_at_unix_ms, updated_at_unix_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("tenant-a")
        .bind("project-a")
        .bind("artifact-a")
        .bind(playground_id)
        .bind("volume-a")
        .bind("cn-shanghai")
        .bind("Invalid digest")
        .bind(base)
        .bind(head)
        .bind("ready")
        .bind(format!("playgrounds/project-a/artifact-a/{playground_id}"))
        .bind(200_i64)
        .bind(200_i64)
        .execute(&mut connection)
        .await;
        assert!(result.is_err(), "{playground_id} bypassed the digest CHECK");
    }
    connection.close().await.unwrap();
}

async fn exercise_artifact_contract(repository: &dyn ControlCatalogRepository) {
    repository
        .insert_tenant(tenant("tenant-a", "Research"))
        .await
        .unwrap();
    repository
        .insert_tenant(tenant("tenant-b", "Other"))
        .await
        .unwrap();

    let source = artifact("tenant-a", "project-a", "artifact-a", 101);
    assert!(matches!(
        repository.insert_artifact(source.clone()).await.unwrap(),
        CatalogInsertOutcome::Inserted(_)
    ));
    assert!(matches!(
        repository.insert_artifact(source.clone()).await.unwrap(),
        CatalogInsertOutcome::Existing(_)
    ));
    let mut reused = source.clone();
    reused.display_name = "Different".to_owned();
    assert!(repository.insert_artifact(reused).await.is_err());
    let cross_project_reuse = ArtifactRecord {
        project_id: id(ProjectId::new, "project-b"),
        ..source.clone()
    };
    assert!(repository
        .insert_artifact(cross_project_reuse)
        .await
        .is_err());

    let derived = ArtifactRecord {
        initialization: ArtifactInitialization::Derived {
            source_project_id: source.project_id.clone(),
            source_artifact_id: source.artifact_id.clone(),
            source_commit_id: ContentDigest::from_str(&"b".repeat(64)).unwrap(),
        },
        ..artifact("tenant-a", "project-b", "artifact-b", 100)
    };
    repository.insert_artifact(derived.clone()).await.unwrap();

    let orphan_tenant = artifact("missing", "project-a", "orphan", 99);
    assert!(repository.insert_artifact(orphan_tenant).await.is_err());
    let missing_source = ArtifactRecord {
        initialization: ArtifactInitialization::Derived {
            source_project_id: id(ProjectId::new, "project-missing"),
            source_artifact_id: id(ArtifactId::new, "artifact-missing"),
            source_commit_id: ContentDigest::from_str(&"c".repeat(64)).unwrap(),
        },
        ..artifact("tenant-a", "project-a", "derived-orphan", 99)
    };
    assert!(repository.insert_artifact(missing_source).await.is_err());

    assert!(repository
        .get_artifact(
            &id(TenantId::new, "tenant-b"),
            &source.project_id,
            &source.artifact_id
        )
        .await
        .unwrap()
        .is_none());
    let first = repository
        .list_artifacts(&ArtifactListRequest {
            tenant_id: id(TenantId::new, "tenant-a"),
            project_id: None,
            query: Some("artifact".to_owned()),
            after: None,
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(first.records, [source]);
    let second = repository
        .list_artifacts(&ArtifactListRequest {
            tenant_id: id(TenantId::new, "tenant-a"),
            project_id: None,
            query: Some("artifact".to_owned()),
            after: first.next,
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(second.records, [derived]);
    assert_eq!(second.next, None);

    repository
        .insert_storage_volume(pvc_volume("tenant-a", "volume-a", "claim-a"))
        .await
        .unwrap();
    let mut orphan_playground = playground();
    orphan_playground.artifact_id = id(ArtifactId::new, "artifact-missing");
    let error = repository
        .insert_playground(orphan_playground)
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::ArtifactNotFound);

    let mut missing_volume = playground();
    missing_volume.storage_volume_id = id(StorageVolumeId::new, "volume-missing");
    let error = repository
        .insert_playground(missing_volume)
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::StorageVolumeNotFound);

    let mut historical_commit = playground();
    historical_commit.playground_id = id(PlaygroundId::new, "playground-historical");
    historical_commit.base_commit_id = Some(ContentDigest::from_bytes([0xaa; 32]));
    historical_commit.head_commit_id = historical_commit.base_commit_id;
    repository
        .insert_playground(historical_commit)
        .await
        .unwrap();

    let mut wrong_head = playground();
    wrong_head.head_commit_id = Some(ContentDigest::from_bytes([0xbb; 32]));
    let error = repository.insert_playground(wrong_head).await.unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::ArtifactHeadMismatch);

    let mut wrong_region = playground();
    wrong_region.region = "cn-beijing".to_owned();
    let error = repository
        .insert_playground(wrong_region)
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::StorageVolumeRegionMismatch);

    let mut degraded = pvc_volume("tenant-a", "volume-degraded", "claim-degraded");
    degraded.state = StorageVolumeState::Degraded;
    repository.insert_storage_volume(degraded).await.unwrap();
    let mut degraded_playground = playground();
    degraded_playground.storage_volume_id = id(StorageVolumeId::new, "volume-degraded");
    let error = repository
        .insert_playground(degraded_playground)
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::StorageVolumeNotReady);

    let valid = playground();
    assert!(matches!(
        repository.insert_playground(valid.clone()).await.unwrap(),
        CatalogInsertOutcome::Inserted(_)
    ));
    assert!(matches!(
        repository.insert_playground(valid.clone()).await.unwrap(),
        CatalogInsertOutcome::Existing(_)
    ));
    let commit_id = ContentDigest::from_bytes([0xcc; 32]);
    let advance = AdvancePlaygroundCommitRequest {
        tenant_id: valid.tenant_id.clone(),
        project_id: valid.project_id.clone(),
        artifact_id: valid.artifact_id.clone(),
        playground_id: valid.playground_id.clone(),
        expected_head_commit_id: None,
        commit_id,
        updated_at_unix_ms: UnixMillis::new(300),
    };
    let advanced = repository
        .advance_playground_commit(advance.clone())
        .await
        .unwrap();
    assert!(!advanced.replayed);
    assert_eq!(advanced.artifact.head_commit_id, Some(commit_id));
    assert_eq!(advanced.artifact.resource_version, 2);
    assert_eq!(advanced.playground.head_commit_id, Some(commit_id));
    let replayed = repository.advance_playground_commit(advance).await.unwrap();
    assert!(replayed.replayed);
    assert_eq!(replayed.artifact.resource_version, 2);

    let mut branch = valid.clone();
    branch.playground_id = PlaygroundId::new("playground-branch").unwrap();
    branch.relative_root = "playgrounds/project-a/artifact-a/playground-branch".to_owned();
    branch.head_commit_id = None;
    branch.base_commit_id = None;
    repository.insert_playground(branch.clone()).await.unwrap();
    let branch_commit_id = ContentDigest::from_bytes([0xee; 32]);
    let branched = repository
        .advance_playground_commit(AdvancePlaygroundCommitRequest {
            tenant_id: branch.tenant_id.clone(),
            project_id: branch.project_id.clone(),
            artifact_id: branch.artifact_id.clone(),
            playground_id: branch.playground_id.clone(),
            expected_head_commit_id: None,
            commit_id: branch_commit_id,
            updated_at_unix_ms: UnixMillis::new(350),
        })
        .await
        .unwrap();
    assert_eq!(branched.artifact.head_commit_id, Some(branch_commit_id));
    assert_eq!(branched.artifact.resource_version, 3);
    assert_eq!(branched.playground.head_commit_id, Some(branch_commit_id));
    let branch_replay = repository
        .advance_playground_commit(AdvancePlaygroundCommitRequest {
            tenant_id: branch.tenant_id.clone(),
            project_id: branch.project_id.clone(),
            artifact_id: branch.artifact_id.clone(),
            playground_id: branch.playground_id.clone(),
            expected_head_commit_id: None,
            commit_id: branch_commit_id,
            updated_at_unix_ms: UnixMillis::new(351),
        })
        .await
        .unwrap();
    assert!(branch_replay.replayed);
    assert_eq!(branch_replay.artifact.resource_version, 3);

    let third_commit_id = ContentDigest::from_bytes([0xef; 32]);
    repository
        .advance_playground_commit(AdvancePlaygroundCommitRequest {
            tenant_id: valid.tenant_id.clone(),
            project_id: valid.project_id.clone(),
            artifact_id: valid.artifact_id.clone(),
            playground_id: valid.playground_id.clone(),
            expected_head_commit_id: Some(commit_id),
            commit_id: third_commit_id,
            updated_at_unix_ms: UnixMillis::new(375),
        })
        .await
        .unwrap();
    let replay_after_other_branch_advanced = repository
        .advance_playground_commit(AdvancePlaygroundCommitRequest {
            tenant_id: branch.tenant_id.clone(),
            project_id: branch.project_id.clone(),
            artifact_id: branch.artifact_id.clone(),
            playground_id: branch.playground_id,
            expected_head_commit_id: None,
            commit_id: branch_commit_id,
            updated_at_unix_ms: UnixMillis::new(376),
        })
        .await
        .unwrap();
    assert!(replay_after_other_branch_advanced.replayed);
    assert_eq!(
        replay_after_other_branch_advanced.artifact.head_commit_id,
        Some(third_commit_id),
        "recovery of an older branch publication must not roll Artifact Head back"
    );
    let stale = AdvancePlaygroundCommitRequest {
        tenant_id: valid.tenant_id.clone(),
        project_id: valid.project_id.clone(),
        artifact_id: valid.artifact_id.clone(),
        playground_id: valid.playground_id.clone(),
        expected_head_commit_id: None,
        commit_id: ContentDigest::from_bytes([0xdd; 32]),
        updated_at_unix_ms: UnixMillis::new(400),
    };
    assert_eq!(
        repository
            .advance_playground_commit(stale)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::ArtifactHeadMismatch
    );
    let mut changed_region = valid;
    changed_region.region = "cn-beijing".to_owned();
    let error = repository
        .insert_playground(changed_region)
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::InvalidState);
}

async fn exercise_playground_head_fence_contract(repository: &dyn ControlCatalogRepository) {
    repository
        .insert_tenant(tenant("tenant-a", "Head fences"))
        .await
        .unwrap();
    repository
        .insert_storage_volume(pvc_volume("tenant-a", "volume-a", "claim-a"))
        .await
        .unwrap();

    let current_head = ContentDigest::from_bytes([0x22; 32]);
    let historical = ContentDigest::from_bytes([0x11; 32]);
    let mut nonempty = artifact("tenant-a", "project-a", "artifact-a", 100);
    nonempty.head_commit_id = Some(current_head);
    repository.insert_artifact(nonempty).await.unwrap();

    let explicit_historical = playground_at("explicit-historical", Some(historical));
    assert!(matches!(
        repository
            .insert_playground_fenced(PlaygroundInsertRequest {
                record: explicit_historical.clone(),
                artifact_head: ArtifactHeadExpectation::Any,
            })
            .await
            .unwrap(),
        CatalogInsertOutcome::Inserted(record) if record == explicit_historical
    ));

    let inherited = playground_at("inherited-current", Some(current_head));
    assert!(matches!(
        repository
            .insert_playground_fenced(PlaygroundInsertRequest {
                record: inherited.clone(),
                artifact_head: ArtifactHeadExpectation::Exact(Some(current_head)),
            })
            .await
            .unwrap(),
        CatalogInsertOutcome::Inserted(record) if record == inherited
    ));

    let stale_observation = playground_at("stale-nonempty", Some(historical));
    let stale = repository
        .insert_playground_fenced(PlaygroundInsertRequest {
            record: stale_observation,
            artifact_head: ArtifactHeadExpectation::Exact(Some(historical)),
        })
        .await
        .unwrap_err();
    assert_eq!(stale.code(), CentralErrorCode::ArtifactHeadMismatch);
    assert!(stale.retryable());

    let replay_with_new_observation = playground_at(
        "inherited-current",
        Some(ContentDigest::from_bytes([0x33; 32])),
    );
    assert!(matches!(
        repository
            .insert_playground_fenced(PlaygroundInsertRequest {
                record: replay_with_new_observation,
                artifact_head: ArtifactHeadExpectation::Exact(Some(ContentDigest::from_bytes(
                    [0x33; 32]
                ))),
            })
            .await
            .unwrap(),
        CatalogInsertOutcome::Existing(record) if record == inherited
    ));
    assert_eq!(
        repository
            .insert_playground_fenced(PlaygroundInsertRequest {
                record: playground_at(
                    "inherited-current",
                    Some(ContentDigest::from_bytes([0x33; 32])),
                ),
                artifact_head: ArtifactHeadExpectation::Any,
            })
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::InvalidState,
        "an explicit Commit selection must not use omitted-base wildcard replay semantics"
    );

    let mut empty = artifact("tenant-a", "project-a", "artifact-empty", 101);
    empty.artifact_id = id(ArtifactId::new, "artifact-empty");
    repository.insert_artifact(empty).await.unwrap();
    let mut empty_baseline = playground_at("inherited-empty", None);
    empty_baseline.artifact_id = id(ArtifactId::new, "artifact-empty");
    empty_baseline.relative_root =
        "playgrounds/project-a/artifact-empty/inherited-empty".to_owned();
    assert!(matches!(
        repository
            .insert_playground_fenced(PlaygroundInsertRequest {
                record: empty_baseline.clone(),
                artifact_head: ArtifactHeadExpectation::Exact(None),
            })
            .await
            .unwrap(),
        CatalogInsertOutcome::Inserted(record) if record == empty_baseline
    ));

    let stale_empty = repository
        .insert_playground_fenced(PlaygroundInsertRequest {
            record: playground_at("stale-empty", None),
            artifact_head: ArtifactHeadExpectation::Exact(None),
        })
        .await
        .unwrap_err();
    assert_eq!(stale_empty.code(), CentralErrorCode::ArtifactHeadMismatch);
    assert!(stale_empty.retryable());

    let malformed_fence = repository
        .insert_playground_fenced(PlaygroundInsertRequest {
            record: playground_at("malformed-fence", Some(current_head)),
            artifact_head: ArtifactHeadExpectation::Exact(Some(historical)),
        })
        .await
        .unwrap_err();
    assert_eq!(malformed_fence.code(), CentralErrorCode::ProtocolInvalid);
}

#[tokio::test]
async fn s3_mutation_ledger_is_replay_safe_in_memory_and_sqlite() {
    let memory: Arc<dyn ControlCatalogRepository> = Arc::new(InMemoryControlCatalog::default());
    seed_s3_catalog(&memory, None).await;
    exercise_s3_mutation_ledger(memory).await;

    let directory = tempfile::tempdir().unwrap();
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let store = authority.authority_store();
    let gateway = store.gateway_registry().unwrap();
    gateway
        .insert_pool(GatewayPoolRecord {
            gateway_pool_id: GatewayPoolId::new("pool-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            display_name: "Primary".to_owned(),
            agent_endpoint: "https://agent.example".to_owned(),
            s3_endpoint: Some("https://s3.example".to_owned()),
            desired_replicas: 1,
            minimum_ready_replicas: 1,
            state: GatewayPoolState::Ready,
            config_generation: Generation::new(1),
            resource_version: ResourceVersion::new(1),
            created_at_unix_ms: UnixMillis::new(1),
            updated_at_unix_ms: UnixMillis::new(1),
            created_by: test_principal("operator"),
            updated_by: test_principal("operator"),
        })
        .await
        .unwrap();
    let repository = store.control_catalog().unwrap();
    seed_s3_catalog(&repository, Some(&gateway)).await;
    exercise_s3_mutation_ledger(repository).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

#[tokio::test]
async fn s3_insert_replay_rechecks_current_snapshot_delivery_in_memory_and_sqlite() {
    let memory: Arc<dyn ControlCatalogRepository> = Arc::new(InMemoryControlCatalog::default());
    seed_s3_catalog(&memory, None).await;
    exercise_s3_insert_replay_delivery_gate(memory).await;

    let directory = tempfile::tempdir().unwrap();
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let repository = authority.authority_store().control_catalog().unwrap();
    seed_s3_catalog(&repository, None).await;
    exercise_s3_insert_replay_delivery_gate(repository).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

async fn exercise_s3_insert_replay_delivery_gate(repository: Arc<dyn ControlCatalogRepository>) {
    let tenant_id = id(TenantId::new, "tenant-a");
    let access_point_id = id(S3AccessPointId::new, "s3ap-insert-replay");
    let access_point = S3AccessPointRecord {
        access_point_id: access_point_id.clone(),
        tenant_id: tenant_id.clone(),
        project_id: id(ProjectId::new, "project-a"),
        artifact_id: id(ArtifactId::new, "artifact-a"),
        snapshot_id: id(SnapshotId::new, "snapshot-a"),
        commit_id: ContentDigest::from_bytes([7; 32]),
        delivery_id: id(SnapshotDeliveryId::new, "delivery-snapshot-a"),
        storage_volume_id: id(StorageVolumeId::new, "volume-a"),
        edge_cluster_id: id(EdgeClusterId::new, "cluster-a"),
        bucket_name: "insert-replay-bucket".to_owned(),
        state: S3AccessPointState::Active,
        policy_generation: 1,
        created_at_unix_ms: UnixMillis::new(300),
        updated_at_unix_ms: UnixMillis::new(300),
    };
    let credential = S3CredentialRecord {
        credential_id: id(S3CredentialId::new, "s3cred-insert-replay"),
        access_point_id: access_point_id.clone(),
        access_key_id: "NGS3INSERTREPLAY".to_owned(),
        encrypted_secret: vec![1, 2, 3],
        state: S3CredentialState::Active,
        expires_at_unix_ms: UnixMillis::new(10_300),
        created_at_unix_ms: UnixMillis::new(300),
        last_used_at_unix_ms: None,
    };

    assert!(matches!(
        repository
            .insert_s3_access_point(access_point.clone())
            .await
            .unwrap(),
        S3AccessPointInsertOutcome::Inserted(_)
    ));
    assert!(matches!(
        repository
            .insert_s3_credential(credential.clone())
            .await
            .unwrap(),
        S3CredentialInsertOutcome::Inserted(_)
    ));

    let delivery = repository
        .get_snapshot_delivery(
            &tenant_id,
            &id(SnapshotDeliveryId::new, "delivery-snapshot-a"),
        )
        .await
        .unwrap()
        .expect("seeded SnapshotDelivery");
    let mut failed_delivery = delivery.clone();
    failed_delivery.state = SnapshotDeliveryState::Failed;
    failed_delivery.issue_code = Some("TEST_FAILURE".to_owned());
    failed_delivery.issue_message = Some("test failure".to_owned());
    failed_delivery.issue_retryable = true;
    failed_delivery.updated_at_unix_ms = UnixMillis::new(400);
    repository
        .replace_snapshot_delivery(delivery.resource_version, failed_delivery)
        .await
        .unwrap();

    assert_eq!(
        repository
            .insert_s3_access_point(access_point)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::InvalidState
    );
    assert_eq!(
        repository
            .insert_s3_credential(credential)
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::InvalidState
    );
}

#[tokio::test]
async fn lifecycle_delete_restore_purge_and_outbox_match_memory_and_sqlite() {
    let memory: Arc<dyn ControlCatalogRepository> = Arc::new(InMemoryControlCatalog::default());
    seed_s3_catalog(&memory, None).await;
    create_active_s3_access_point(&memory).await;
    seed_lifecycle_hardlink_delivery(&memory).await;
    exercise_snapshot_lifecycle(memory.clone()).await;

    let outbox_memory: Arc<dyn ControlCatalogRepository> =
        Arc::new(InMemoryControlCatalog::default());
    seed_volume_only(&outbox_memory).await;
    exercise_lifecycle_outbox(outbox_memory).await;

    let directory = tempfile::tempdir().unwrap();
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let store = authority.authority_store();
    let gateway = store.gateway_registry().unwrap();
    insert_test_gateway_pool(&gateway).await;
    let repository = store.control_catalog().unwrap();
    seed_s3_catalog(&repository, Some(&gateway)).await;
    create_active_s3_access_point(&repository).await;
    seed_lifecycle_hardlink_delivery(&repository).await;
    exercise_snapshot_lifecycle(repository).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;

    let outbox_directory = tempfile::tempdir().unwrap();
    let outbox_authority =
        open_sqlite_authority(SqliteAuthorityConfig::new(outbox_directory.path()))
            .await
            .unwrap();
    let outbox_repository = outbox_authority
        .authority_store()
        .control_catalog()
        .unwrap();
    seed_volume_only(&outbox_repository).await;
    exercise_lifecycle_outbox(outbox_repository).await;
    outbox_authority.integrity_check().await.unwrap();
    outbox_authority.close().await;
}

#[tokio::test]
async fn lifecycle_retry_resumes_the_failed_phase_in_memory_and_sqlite() {
    let memory: Arc<dyn ControlCatalogRepository> = Arc::new(InMemoryControlCatalog::default());
    exercise_lifecycle_retry_resume(memory).await;

    let directory = tempfile::tempdir().unwrap();
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let repository = authority.authority_store().control_catalog().unwrap();
    exercise_lifecycle_retry_resume(repository).await;
    authority.integrity_check().await.unwrap();
    authority.close().await;
}

async fn exercise_lifecycle_retry_resume(repository: Arc<dyn ControlCatalogRepository>) {
    let tenant_id = id(TenantId::new, "tenant-retry");
    repository
        .insert_tenant(tenant("tenant-retry", "Retry lifecycle"))
        .await
        .unwrap();
    for suffix in ["delete", "restore"] {
        repository
            .insert_storage_volume(pvc_volume(
                "tenant-retry",
                &format!("volume-{suffix}"),
                &format!("claim-{suffix}"),
            ))
            .await
            .unwrap();
    }

    let mut deletion = create_volume_deletion_for_retry(&repository, "delete", 1_000).await;
    let mut deletion_now = 1_010;
    for (index, phase) in [
        DeletionOperationState::Requested,
        DeletionOperationState::Quiescing,
        DeletionOperationState::Quarantining,
        DeletionOperationState::Recoverable,
    ]
    .into_iter()
    .enumerate()
    {
        while deletion.state != phase {
            let next = match deletion.state {
                DeletionOperationState::Requested => DeletionOperationState::Quiescing,
                DeletionOperationState::Quiescing => DeletionOperationState::Quarantining,
                DeletionOperationState::Quarantining => DeletionOperationState::Recoverable,
                state => panic!("cannot advance {state:?} to {phase:?}"),
            };
            deletion = transition_deletion(&repository, deletion, next, deletion_now).await;
            deletion_now += 1;
        }
        deletion = fail_and_retry_deletion(
            &repository,
            deletion,
            phase,
            if index % 2 == 0 {
                DeletionOperationState::Blocked
            } else {
                DeletionOperationState::Failed
            },
            &format!("delete-{index}"),
            deletion_now,
        )
        .await;
        deletion_now += 2;
    }

    let purge_at = deletion.purge_after_unix_ms.get();
    deletion = transition_deletion(
        &repository,
        deletion,
        DeletionOperationState::Purging,
        purge_at,
    )
    .await;
    deletion = fail_and_retry_deletion(
        &repository,
        deletion,
        DeletionOperationState::Purging,
        DeletionOperationState::Failed,
        "delete-purging",
        purge_at + 1,
    )
    .await;
    deletion = transition_deletion(
        &repository,
        deletion,
        DeletionOperationState::Finalizing,
        purge_at + 3,
    )
    .await;
    deletion = repository
        .transition_deletion_state(DeletionTransitionRequest {
            tenant_id: deletion.tenant_id.clone(),
            deletion_id: deletion.deletion_id.clone(),
            expected_state: DeletionOperationState::Finalizing,
            next_state: DeletionOperationState::Blocked,
            expected_resource_version: deletion.resource_version.get(),
            now_unix_ms: UnixMillis::new(purge_at + 4),
            last_error: Some("finalization temporarily blocked".to_owned()),
        })
        .await
        .unwrap();
    assert_eq!(
        deletion.resume_state,
        Some(DeletionOperationState::Finalizing)
    );
    let _ = fail_and_retry_deletion(
        &repository,
        deletion,
        DeletionOperationState::Finalizing,
        DeletionOperationState::Failed,
        "delete-finalizing",
        purge_at + 5,
    )
    .await;

    let mut restoration = create_volume_deletion_for_retry(&repository, "restore", 2_000).await;
    for (offset, next) in [
        DeletionOperationState::Quiescing,
        DeletionOperationState::Quarantining,
        DeletionOperationState::Recoverable,
    ]
    .into_iter()
    .enumerate()
    {
        restoration =
            transition_deletion(&repository, restoration, next, 2_010 + offset as u64).await;
    }
    restoration = match repository
        .restore_deletion_idempotent(RestoreDeletionRequest {
            tenant_id: tenant_id.clone(),
            deletion_id: restoration.deletion_id.clone(),
            request_id: id(RequestId::new, "restore-retry-intent"),
            request_digest: ContentDigest::hash(b"restore-retry-intent"),
            expected_resource_version: restoration.resource_version.get(),
            now_unix_ms: UnixMillis::new(2_020),
        })
        .await
        .unwrap()
    {
        CatalogInsertOutcome::Inserted(operation) => operation,
        CatalogInsertOutcome::Existing(_) => panic!("restore must be inserted"),
    };
    let restored_retry = fail_and_retry_deletion(
        &repository,
        restoration,
        DeletionOperationState::Restoring,
        DeletionOperationState::Failed,
        "restore-restoring",
        2_021,
    )
    .await;
    assert_eq!(restored_retry.state, DeletionOperationState::Restoring);
}

async fn create_volume_deletion_for_retry(
    repository: &Arc<dyn ControlCatalogRepository>,
    suffix: &str,
    now_unix_ms: u64,
) -> neoengram_domain::protocol::DeletionOperation {
    let tenant_id = id(TenantId::new, "tenant-retry");
    let root = ResourceRef::StorageVolume {
        storage_volume_id: id(StorageVolumeId::new, &format!("volume-{suffix}")),
    };
    let impact = repository
        .query_deletion_impact(DeletionImpactQuery {
            tenant_id: tenant_id.clone(),
            root: root.clone(),
            cascade: false,
            confirm_managed_data_erase: true,
            additional_blockers: Vec::new(),
            authority_impact: None,
            now_unix_ms: UnixMillis::new(now_unix_ms),
        })
        .await
        .unwrap();
    match repository
        .create_deletion_idempotent(CreateDeletionRequest {
            deletion_id: id(DeletionId::new, &format!("deletion-{suffix}")),
            tenant_id,
            root,
            cascade: false,
            confirm_managed_data_erase: true,
            request_id: id(RequestId::new, &format!("delete-{suffix}-request")),
            request_digest: ContentDigest::hash(format!("delete-{suffix}").as_bytes()),
            impact_digest: impact.impact_digest,
            expected_resource_version: 1,
            now_unix_ms: UnixMillis::new(now_unix_ms + 1),
        })
        .await
        .unwrap()
    {
        CatalogInsertOutcome::Inserted(operation) => operation,
        CatalogInsertOutcome::Existing(_) => panic!("delete must be inserted"),
    }
}

async fn fail_and_retry_deletion(
    repository: &Arc<dyn ControlCatalogRepository>,
    operation: neoengram_domain::protocol::DeletionOperation,
    expected_resume_state: DeletionOperationState,
    error_state: DeletionOperationState,
    request_suffix: &str,
    now_unix_ms: u64,
) -> neoengram_domain::protocol::DeletionOperation {
    let failed = repository
        .transition_deletion_state(DeletionTransitionRequest {
            tenant_id: operation.tenant_id.clone(),
            deletion_id: operation.deletion_id.clone(),
            expected_state: operation.state,
            next_state: error_state,
            expected_resource_version: operation.resource_version.get(),
            now_unix_ms: UnixMillis::new(now_unix_ms),
            last_error: Some(format!("failure at {expected_resume_state:?}")),
        })
        .await
        .unwrap();
    assert_eq!(failed.state, error_state);
    assert_eq!(failed.resume_state, Some(expected_resume_state));

    let retry_request = RetryDeletionRequest {
        tenant_id: failed.tenant_id.clone(),
        deletion_id: failed.deletion_id.clone(),
        request_id: id(RequestId::new, &format!("retry-{request_suffix}")),
        request_digest: ContentDigest::hash(format!("retry-{request_suffix}").as_bytes()),
        expected_resource_version: failed.resource_version.get(),
        now_unix_ms: UnixMillis::new(now_unix_ms + 1),
    };
    let retried = match repository
        .retry_deletion_idempotent(retry_request.clone())
        .await
        .unwrap()
    {
        CatalogInsertOutcome::Inserted(operation) => operation,
        CatalogInsertOutcome::Existing(_) => panic!("retry must be inserted"),
    };
    assert_eq!(retried.state, expected_resume_state);
    assert_eq!(retried.resume_state, None);
    assert_eq!(retried.last_error, None);
    assert!(matches!(
        repository
            .retry_deletion_idempotent(retry_request)
            .await
            .unwrap(),
        CatalogInsertOutcome::Existing(ref operation) if operation == &retried
    ));
    retried
}

async fn exercise_snapshot_lifecycle(repository: Arc<dyn ControlCatalogRepository>) {
    let tenant_id = id(TenantId::new, "tenant-a");
    let snapshot_id = id(SnapshotId::new, "snapshot-a");
    let root = ResourceRef::Snapshot {
        snapshot_id: snapshot_id.clone(),
    };

    let expired = repository
        .query_deletion_impact(DeletionImpactQuery {
            tenant_id: tenant_id.clone(),
            root: root.clone(),
            cascade: false,
            confirm_managed_data_erase: false,
            additional_blockers: Vec::new(),
            authority_impact: None,
            now_unix_ms: UnixMillis::new(10),
        })
        .await
        .unwrap();
    let expired_error = repository
        .create_deletion_idempotent(CreateDeletionRequest {
            deletion_id: id(DeletionId::new, "deletion-expired"),
            tenant_id: tenant_id.clone(),
            root: root.clone(),
            cascade: false,
            confirm_managed_data_erase: false,
            request_id: id(RequestId::new, "delete-expired-request"),
            request_digest: ContentDigest::hash(b"delete-expired"),
            impact_digest: expired.impact_digest,
            expected_resource_version: 1,
            now_unix_ms: UnixMillis::new(expired.impact.expires_at_unix_ms.get() + 1),
        })
        .await
        .unwrap_err();
    assert_eq!(expired_error.code(), CentralErrorCode::InvalidState);

    let first_impact = repository
        .query_deletion_impact(DeletionImpactQuery {
            tenant_id: tenant_id.clone(),
            root: root.clone(),
            cascade: false,
            confirm_managed_data_erase: false,
            additional_blockers: Vec::new(),
            authority_impact: Some(AuthorityLifecycleImpact {
                active_job_count: DecimalU64::new(2),
                estimated_file_count: DecimalU64::new(3),
                estimated_bytes: DecimalU64::new(42),
            }),
            now_unix_ms: UnixMillis::new(1_000),
        })
        .await
        .unwrap();
    assert_eq!(first_impact.impact.active_s3_credential_count.get(), 1);
    assert_eq!(first_impact.impact.active_job_count.get(), 2);
    assert_eq!(first_impact.impact.estimated_file_count.get(), 3);
    assert_eq!(first_impact.impact.estimated_bytes.get(), 42);
    let mismatched_impact = repository
        .create_deletion_idempotent(CreateDeletionRequest {
            deletion_id: id(DeletionId::new, "deletion-impact-mismatch"),
            tenant_id: tenant_id.clone(),
            root: root.clone(),
            cascade: true,
            confirm_managed_data_erase: false,
            request_id: id(RequestId::new, "delete-impact-mismatch-request"),
            request_digest: ContentDigest::hash(b"delete-impact-mismatch"),
            impact_digest: first_impact.impact_digest,
            expected_resource_version: 1,
            now_unix_ms: UnixMillis::new(1_001),
        })
        .await
        .unwrap_err();
    assert_eq!(mismatched_impact.code(), CentralErrorCode::InvalidState);
    let first_request = CreateDeletionRequest {
        deletion_id: id(DeletionId::new, "deletion-restore"),
        tenant_id: tenant_id.clone(),
        root: root.clone(),
        cascade: false,
        confirm_managed_data_erase: false,
        request_id: id(RequestId::new, "delete-restore-request"),
        request_digest: ContentDigest::hash(b"delete-restore"),
        impact_digest: first_impact.impact_digest,
        expected_resource_version: 1,
        now_unix_ms: UnixMillis::new(1_001),
    };
    let mut operation = match repository
        .create_deletion_idempotent(first_request.clone())
        .await
        .unwrap()
    {
        CatalogInsertOutcome::Inserted(operation) => operation,
        CatalogInsertOutcome::Existing(_) => panic!("first delete must insert"),
    };
    assert_eq!(
        operation.purge_after_unix_ms.get(),
        1_001 + DELETION_RECOVERY_WINDOW_MILLIS
    );
    assert!(matches!(
        repository
            .create_deletion_idempotent(first_request)
            .await
            .unwrap(),
        CatalogInsertOutcome::Existing(_)
    ));
    assert!(repository
        .get_snapshot(&tenant_id, &snapshot_id)
        .await
        .unwrap()
        .is_none());
    let access_point = repository
        .get_s3_access_point_by_snapshot(&tenant_id, &snapshot_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(access_point.state, S3AccessPointState::Disabled);
    assert_eq!(access_point.policy_generation, 2);
    let credentials = repository
        .list_s3_credentials(&access_point.access_point_id)
        .await
        .unwrap();
    assert_eq!(credentials[0].state, S3CredentialState::Revoked);
    assert_ne!(credentials[0].encrypted_secret, vec![1, 2, 3]);

    operation = transition_deletion(
        &repository,
        operation,
        DeletionOperationState::Quiescing,
        1_002,
    )
    .await;
    operation = transition_deletion(
        &repository,
        operation,
        DeletionOperationState::Quarantining,
        1_003,
    )
    .await;
    operation = transition_deletion(
        &repository,
        operation,
        DeletionOperationState::Recoverable,
        1_004,
    )
    .await;
    operation = match repository
        .restore_deletion_idempotent(RestoreDeletionRequest {
            tenant_id: tenant_id.clone(),
            deletion_id: operation.deletion_id.clone(),
            request_id: id(RequestId::new, "restore-request"),
            request_digest: ContentDigest::hash(b"restore"),
            expected_resource_version: operation.resource_version.get(),
            now_unix_ms: UnixMillis::new(1_005),
        })
        .await
        .unwrap()
    {
        CatalogInsertOutcome::Inserted(operation) => operation,
        CatalogInsertOutcome::Existing(_) => panic!("first restore must insert"),
    };
    operation = transition_deletion(
        &repository,
        operation,
        DeletionOperationState::Completed,
        1_006,
    )
    .await;
    assert_eq!(
        operation.completion,
        Some(neoengram_domain::protocol::DeletionCompletion::Restored)
    );
    let restored = repository
        .get_snapshot(&tenant_id, &snapshot_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored.lifecycle.state, ResourceLifecycleState::Active);
    let lifecycle_delivery_id = id(SnapshotDeliveryId::new, "delivery-snapshot-a");
    assert_eq!(
        repository
            .get_snapshot_delivery(&tenant_id, &lifecycle_delivery_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        SnapshotDeliveryState::Ready,
        "restoring a Snapshot keeps its successfully restored Delivery available"
    );
    assert_eq!(
        repository
            .list_snapshot_delivery_retention_roots(&tenant_id, &lifecycle_delivery_id)
            .await
            .unwrap()
            .len(),
        1,
        "restore must retain the Hardlink CAS root"
    );
    assert_eq!(
        repository
            .get_s3_access_point_by_snapshot(&tenant_id, &snapshot_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        S3AccessPointState::Disabled,
        "restoring a Snapshot must not restore its old S3 credentials"
    );

    let second_impact = repository
        .query_deletion_impact(DeletionImpactQuery {
            tenant_id: tenant_id.clone(),
            root: root.clone(),
            cascade: false,
            confirm_managed_data_erase: false,
            additional_blockers: Vec::new(),
            authority_impact: None,
            now_unix_ms: UnixMillis::new(2_000),
        })
        .await
        .unwrap();
    let mut operation = match repository
        .create_deletion_idempotent(CreateDeletionRequest {
            deletion_id: id(DeletionId::new, "deletion-purge"),
            tenant_id: tenant_id.clone(),
            root,
            cascade: false,
            confirm_managed_data_erase: false,
            request_id: id(RequestId::new, "delete-purge-request"),
            request_digest: ContentDigest::hash(b"delete-purge"),
            impact_digest: second_impact.impact_digest,
            expected_resource_version: restored.resource_version,
            now_unix_ms: UnixMillis::new(2_001),
        })
        .await
        .unwrap()
    {
        CatalogInsertOutcome::Inserted(operation) => operation,
        CatalogInsertOutcome::Existing(_) => panic!("second delete must insert"),
    };
    operation = transition_deletion(
        &repository,
        operation,
        DeletionOperationState::Quiescing,
        2_002,
    )
    .await;
    operation = transition_deletion(
        &repository,
        operation,
        DeletionOperationState::Quarantining,
        2_003,
    )
    .await;
    operation = transition_deletion(
        &repository,
        operation,
        DeletionOperationState::Recoverable,
        2_004,
    )
    .await;
    repository
        .create_retention_hold_idempotent(CreateRetentionHoldRequest {
            tenant_id: tenant_id.clone(),
            deletion_id: operation.deletion_id.clone(),
            retention_hold_id: id(RetentionHoldId::new, "hold-a"),
            reason: "legal review".to_owned(),
            expires_at_unix_ms: None,
            request_id: id(RequestId::new, "hold-create-request"),
            request_digest: ContentDigest::hash(b"hold-create"),
            expected_resource_version: operation.resource_version.get(),
            now_unix_ms: UnixMillis::new(2_005),
        })
        .await
        .unwrap();
    operation = repository
        .get_deletion_operation(&tenant_id, &operation.deletion_id)
        .await
        .unwrap()
        .unwrap();
    let purge_at = operation.purge_after_unix_ms.get();
    let blocked = repository
        .transition_deletion_state(DeletionTransitionRequest {
            tenant_id: tenant_id.clone(),
            deletion_id: operation.deletion_id.clone(),
            expected_state: DeletionOperationState::Recoverable,
            next_state: DeletionOperationState::Purging,
            expected_resource_version: operation.resource_version.get(),
            now_unix_ms: UnixMillis::new(purge_at),
            last_error: None,
        })
        .await
        .unwrap_err();
    assert_eq!(blocked.code(), CentralErrorCode::InvalidState);
    repository
        .release_retention_hold_idempotent(ReleaseRetentionHoldRequest {
            tenant_id: tenant_id.clone(),
            deletion_id: operation.deletion_id.clone(),
            retention_hold_id: id(RetentionHoldId::new, "hold-a"),
            request_id: id(RequestId::new, "hold-release-request"),
            request_digest: ContentDigest::hash(b"hold-release"),
            expected_resource_version: operation.resource_version.get(),
            now_unix_ms: UnixMillis::new(purge_at + 1),
        })
        .await
        .unwrap();
    operation = repository
        .get_deletion_operation(&tenant_id, &operation.deletion_id)
        .await
        .unwrap()
        .unwrap();
    operation = transition_deletion(
        &repository,
        operation,
        DeletionOperationState::Purging,
        purge_at + 2,
    )
    .await;
    operation = transition_deletion(
        &repository,
        operation,
        DeletionOperationState::Finalizing,
        purge_at + 3,
    )
    .await;
    operation = transition_deletion(
        &repository,
        operation,
        DeletionOperationState::Completed,
        purge_at + 4,
    )
    .await;
    assert_eq!(
        operation.completion,
        Some(neoengram_domain::protocol::DeletionCompletion::Purged)
    );
    assert!(repository
        .get_snapshot(&tenant_id, &snapshot_id)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        repository
            .get_snapshot_delivery(&tenant_id, &lifecycle_delivery_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        SnapshotDeliveryState::Deleted
    );
    assert!(repository
        .list_snapshot_delivery_retention_roots(&tenant_id, &lifecycle_delivery_id)
        .await
        .unwrap()
        .is_empty());
}

async fn seed_lifecycle_hardlink_delivery(repository: &Arc<dyn ControlCatalogRepository>) {
    let tenant_id = id(TenantId::new, "tenant-a");
    let delivery_id = id(SnapshotDeliveryId::new, "delivery-snapshot-a");
    let retention_root = SnapshotDeliveryRetentionRoot {
        tenant_id: tenant_id.clone(),
        delivery_id: delivery_id.clone(),
        object_id: ObjectId::from_bytes([0x55; 32]),
    };
    repository
        .insert_snapshot_delivery_retention_roots(&[retention_root])
        .await
        .unwrap();
}

async fn transition_deletion(
    repository: &Arc<dyn ControlCatalogRepository>,
    operation: neoengram_domain::protocol::DeletionOperation,
    next_state: DeletionOperationState,
    now_unix_ms: u64,
) -> neoengram_domain::protocol::DeletionOperation {
    repository
        .transition_deletion_state(DeletionTransitionRequest {
            tenant_id: operation.tenant_id.clone(),
            deletion_id: operation.deletion_id.clone(),
            expected_state: operation.state,
            next_state,
            expected_resource_version: operation.resource_version.get(),
            now_unix_ms: UnixMillis::new(now_unix_ms),
            last_error: None,
        })
        .await
        .unwrap()
}

async fn exercise_lifecycle_outbox(repository: Arc<dyn ControlCatalogRepository>) {
    let tenant_id = id(TenantId::new, "tenant-outbox");
    let volume_id = id(StorageVolumeId::new, "volume-outbox");
    let impact = repository
        .query_deletion_impact(DeletionImpactQuery {
            tenant_id: tenant_id.clone(),
            root: ResourceRef::StorageVolume {
                storage_volume_id: volume_id.clone(),
            },
            cascade: false,
            confirm_managed_data_erase: true,
            additional_blockers: Vec::new(),
            authority_impact: None,
            now_unix_ms: UnixMillis::new(100),
        })
        .await
        .unwrap();
    let request_digest = ContentDigest::hash(b"outbox-delete");
    let operation = match repository
        .create_deletion_idempotent(CreateDeletionRequest {
            deletion_id: id(DeletionId::new, "deletion-outbox"),
            tenant_id: tenant_id.clone(),
            root: ResourceRef::StorageVolume {
                storage_volume_id: volume_id.clone(),
            },
            cascade: false,
            confirm_managed_data_erase: true,
            request_id: id(RequestId::new, "delete-outbox-request"),
            request_digest,
            impact_digest: impact.impact_digest,
            expected_resource_version: 1,
            now_unix_ms: UnixMillis::new(101),
        })
        .await
        .unwrap()
    {
        CatalogInsertOutcome::Inserted(operation) => operation,
        CatalogInsertOutcome::Existing(_) => panic!("outbox delete must insert"),
    };
    let target = operation
        .targets
        .iter()
        .find(|target| target.resource == operation.root)
        .unwrap();
    let agent_id = id(AgentId::new, "agent-outbox");
    let assignment_id = id(LifecycleAssignmentId::new, "lifecycle-assignment-outbox");
    let record = LifecycleAssignmentOutboxRecord {
        assignment: AgentResourceLifecycleAssignment {
            assignment: ResourceLifecycleAssignment {
                assignment_id: assignment_id.clone(),
                tenant_id: tenant_id.clone(),
                deletion_id: operation.deletion_id.clone(),
                resource: operation.root.clone(),
                action: ResourceLifecycleAction::Quarantine,
                lifecycle_generation: target.lifecycle_generation,
                request_digest,
                deadline_unix_ms: UnixMillis::new(10_000),
            },
            resource_scope: AgentResourceLifecycleScope::StorageVolume {
                storage_volume_id: volume_id.clone(),
            },
            agent_id: agent_id.clone(),
            edge_cluster_id: id(EdgeClusterId::new, "cluster-outbox"),
            agent_mount_id: id(AgentMountId::new, "mount-outbox"),
            volume_marker_id: id(VolumeMarkerId::new, "volume-outbox"),
            session_generation: SessionGeneration::new(1),
            mount_generation: MountGeneration::new(1),
            owner_generation: OwnerGeneration::new(1),
            extensions: Extensions::new(),
        },
        published: false,
        retired: false,
        terminal_report_digest: None,
    };
    assert!(matches!(
        repository
            .enqueue_lifecycle_assignment(record.clone())
            .await
            .unwrap(),
        LifecycleAssignmentInsertOutcome::Inserted(_)
    ));
    assert!(repository
        .pending_lifecycle_assignments_for_agent(&agent_id, 10)
        .await
        .unwrap()
        .is_empty());
    let published = repository
        .publish_lifecycle_assignment(&tenant_id, &assignment_id)
        .await
        .unwrap();
    assert!(published.published);
    assert!(!published.retired);
    assert_eq!(
        repository
            .publish_lifecycle_assignment(&tenant_id, &assignment_id)
            .await
            .unwrap(),
        published
    );
    assert_eq!(
        repository
            .pending_lifecycle_assignments_for_agent(&agent_id, 10)
            .await
            .unwrap(),
        std::slice::from_ref(&published)
    );
    assert!(matches!(
        repository
            .enqueue_lifecycle_assignment(record)
            .await
            .unwrap(),
        LifecycleAssignmentInsertOutcome::Existing(existing) if existing == published
    ));
    let terminal_digest = ContentDigest::hash(b"terminal-report-a");
    let recorded = repository
        .record_lifecycle_report(&tenant_id, &assignment_id, &terminal_digest)
        .await
        .unwrap();
    assert_eq!(
        recorded.terminal_report_digest.as_ref(),
        Some(&terminal_digest)
    );
    assert_eq!(
        repository
            .record_lifecycle_report(&tenant_id, &assignment_id, &terminal_digest)
            .await
            .unwrap(),
        recorded
    );
    let conflicting = repository
        .record_lifecycle_report(
            &tenant_id,
            &assignment_id,
            &ContentDigest::hash(b"terminal-report-b"),
        )
        .await
        .unwrap_err();
    assert_eq!(conflicting.code(), CentralErrorCode::InvalidState);
    let retired = repository
        .retire_lifecycle_assignment(&tenant_id, &assignment_id)
        .await
        .unwrap();
    assert!(retired.retired);
    assert_eq!(
        repository
            .retire_lifecycle_assignment(&tenant_id, &assignment_id)
            .await
            .unwrap(),
        retired
    );
    assert!(repository
        .pending_lifecycle_assignments_for_agent(&agent_id, 10)
        .await
        .unwrap()
        .is_empty());
}

async fn seed_volume_only(repository: &Arc<dyn ControlCatalogRepository>) {
    repository
        .insert_tenant(tenant("tenant-outbox", "Outbox"))
        .await
        .unwrap();
    repository
        .insert_storage_volume(pvc_volume("tenant-outbox", "volume-outbox", "claim-outbox"))
        .await
        .unwrap();
}

async fn create_active_s3_access_point(repository: &Arc<dyn ControlCatalogRepository>) {
    let tenant_id = id(TenantId::new, "tenant-a");
    let access_point_id = id(S3AccessPointId::new, "s3ap-lifecycle");
    repository
        .create_s3_access_point_idempotent(
            s3_mutation(
                &tenant_id,
                &id(RequestId::new, "s3-lifecycle-create"),
                S3MutationKind::AccessPointCreate,
                91,
                300,
            ),
            S3AccessPointRecord {
                access_point_id: access_point_id.clone(),
                tenant_id,
                project_id: id(ProjectId::new, "project-a"),
                artifact_id: id(ArtifactId::new, "artifact-a"),
                snapshot_id: id(SnapshotId::new, "snapshot-a"),
                commit_id: ContentDigest::from_bytes([7; 32]),
                delivery_id: id(SnapshotDeliveryId::new, "delivery-snapshot-a"),
                storage_volume_id: id(StorageVolumeId::new, "volume-a"),
                edge_cluster_id: id(EdgeClusterId::new, "cluster-a"),
                bucket_name: "lifecycle-bucket".to_owned(),
                state: S3AccessPointState::Active,
                policy_generation: 1,
                created_at_unix_ms: UnixMillis::new(300),
                updated_at_unix_ms: UnixMillis::new(300),
            },
            S3CredentialRecord {
                credential_id: id(S3CredentialId::new, "s3cred-lifecycle"),
                access_point_id,
                access_key_id: "NGS3LIFECYCLE".to_owned(),
                encrypted_secret: vec![1, 2, 3],
                state: S3CredentialState::Active,
                expires_at_unix_ms: UnixMillis::new(100_000),
                created_at_unix_ms: UnixMillis::new(300),
                last_used_at_unix_ms: None,
            },
        )
        .await
        .unwrap();
}

async fn insert_test_gateway_pool(gateway: &Arc<dyn GatewayRegistryRepository>) {
    gateway
        .insert_pool(GatewayPoolRecord {
            gateway_pool_id: GatewayPoolId::new("pool-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            display_name: "Primary".to_owned(),
            agent_endpoint: "https://agent.example".to_owned(),
            s3_endpoint: Some("https://s3.example".to_owned()),
            desired_replicas: 1,
            minimum_ready_replicas: 1,
            state: GatewayPoolState::Ready,
            config_generation: Generation::new(1),
            resource_version: ResourceVersion::new(1),
            created_at_unix_ms: UnixMillis::new(1),
            updated_at_unix_ms: UnixMillis::new(1),
            created_by: test_principal("operator"),
            updated_by: test_principal("operator"),
        })
        .await
        .unwrap();
}

async fn seed_s3_catalog(
    repository: &Arc<dyn ControlCatalogRepository>,
    _gateway: Option<&Arc<dyn GatewayRegistryRepository>>,
) {
    repository
        .insert_tenant(tenant("tenant-a", "Research"))
        .await
        .unwrap();
    repository
        .insert_artifact(artifact("tenant-a", "project-a", "artifact-a", 100))
        .await
        .unwrap();
    repository
        .insert_storage_volume(pvc_volume("tenant-a", "volume-a", "claim-a"))
        .await
        .unwrap();
    repository
        .insert_snapshot_with_delivery(SnapshotWithDeliveryInsertRequest {
            snapshot: SnapshotInsertRequest {
                record: SnapshotRecord {
                    tenant_id: id(TenantId::new, "tenant-a"),
                    project_id: id(ProjectId::new, "project-a"),
                    artifact_id: id(ArtifactId::new, "artifact-a"),
                    snapshot_id: id(SnapshotId::new, "snapshot-a"),
                    snapshot_request_id: id(RequestId::new, "snapshot-request-a"),
                    commit_id: ContentDigest::from_bytes([7; 32]),
                    delivery_id: id(SnapshotDeliveryId::new, "delivery-snapshot-a"),
                    edge_cluster_id: id(EdgeClusterId::new, "cluster-a"),
                    storage_volume_id: id(StorageVolumeId::new, "volume-a"),
                    delivery_mode: SnapshotDeliveryMode::Copy,
                    state: SnapshotState::Ready,
                    resource_version: 1,
                    lifecycle: ResourceLifecycle::active(),
                    created_at_unix_ms: UnixMillis::new(200),
                    updated_at_unix_ms: UnixMillis::new(200),
                },
                artifact_head: ArtifactHeadExpectation::Any,
            },
            delivery: SnapshotDeliveryInsertRequest {
                request_id: id(RequestId::new, "snapshot-request-a"),
                record: SnapshotDeliveryRecord {
                    tenant_id: id(TenantId::new, "tenant-a"),
                    delivery_id: id(SnapshotDeliveryId::new, "delivery-snapshot-a"),
                    create_request_id: id(RequestId::new, "snapshot-request-a"),
                    snapshot_id: id(SnapshotId::new, "snapshot-a"),
                    commit_id: ContentDigest::from_bytes([7; 32]),
                    storage_volume_id: id(StorageVolumeId::new, "volume-a"),
                    mode: SnapshotDeliveryMode::Copy,
                    target_relative_root: LogicalPath::parse(
                        "snapshots/project-a/artifact-a/snapshot-a/deliveries/delivery-snapshot-a",
                    )
                    .unwrap(),
                    state: SnapshotDeliveryState::Ready,
                    source_index_digest: ContentDigest::from_bytes([8; 32]),
                    delivery_generation: DeliveryGeneration::new(1),
                    file_count: 0,
                    size_bytes: 0,
                    object_set_digest: ContentDigest::from_bytes([9; 32]),
                    resource_version: 1,
                    issue_code: None,
                    issue_message: None,
                    issue_retryable: false,
                    created_at_unix_ms: UnixMillis::new(200),
                    updated_at_unix_ms: UnixMillis::new(200),
                },
                retention_roots: Vec::new(),
            },
        })
        .await
        .unwrap();
}

async fn exercise_snapshot_delivery_retention(repository: Arc<dyn ControlCatalogRepository>) {
    seed_snapshot_catalog_parents(&repository).await;
    let tenant_id = id(TenantId::new, "tenant-a");
    let delivery_id = id(SnapshotDeliveryId::new, "delivery-hardlink-a");
    // Snapshot and its atomically-created Delivery share one public request identity.
    let create_request_id = id(RequestId::new, "snapshot-request-a");
    let mut delivery = SnapshotDeliveryRecord {
        tenant_id: tenant_id.clone(),
        delivery_id: delivery_id.clone(),
        create_request_id: create_request_id.clone(),
        snapshot_id: id(SnapshotId::new, "snapshot-a"),
        commit_id: ContentDigest::from_bytes([7; 32]),
        storage_volume_id: id(StorageVolumeId::new, "volume-a"),
        mode: SnapshotDeliveryMode::Hardlink,
        target_relative_root: LogicalPath::parse(
            "snapshots/project-a/artifact-a/snapshot-a/deliveries/delivery-hardlink-a",
        )
        .unwrap(),
        state: SnapshotDeliveryState::Requested,
        source_index_digest: ContentDigest::from_bytes([8; 32]),
        delivery_generation: DeliveryGeneration::new(1),
        file_count: 2,
        size_bytes: 11,
        object_set_digest: ContentDigest::from_bytes([9; 32]),
        resource_version: 1,
        issue_code: None,
        issue_message: None,
        issue_retryable: false,
        created_at_unix_ms: UnixMillis::new(300),
        updated_at_unix_ms: UnixMillis::new(300),
    };
    let roots = [ObjectId::from_bytes([1; 32]), ObjectId::from_bytes([2; 32])]
        .into_iter()
        .map(|object_id| SnapshotDeliveryRetentionRoot {
            tenant_id: tenant_id.clone(),
            delivery_id: delivery_id.clone(),
            object_id,
        })
        .collect::<Vec<_>>();
    let create_request = SnapshotDeliveryInsertRequest {
        record: delivery.clone(),
        request_id: create_request_id.clone(),
        retention_roots: roots.clone(),
    };
    repository
        .insert_snapshot_with_delivery(SnapshotWithDeliveryInsertRequest {
            snapshot: SnapshotInsertRequest {
                record: SnapshotRecord {
                    tenant_id: tenant_id.clone(),
                    project_id: id(ProjectId::new, "project-a"),
                    artifact_id: id(ArtifactId::new, "artifact-a"),
                    snapshot_id: id(SnapshotId::new, "snapshot-a"),
                    snapshot_request_id: create_request_id.clone(),
                    commit_id: ContentDigest::from_bytes([7; 32]),
                    delivery_id: delivery_id.clone(),
                    edge_cluster_id: id(EdgeClusterId::new, "cluster-a"),
                    storage_volume_id: id(StorageVolumeId::new, "volume-a"),
                    delivery_mode: SnapshotDeliveryMode::Hardlink,
                    state: SnapshotState::Creating,
                    resource_version: 1,
                    lifecycle: ResourceLifecycle::active(),
                    created_at_unix_ms: UnixMillis::new(200),
                    updated_at_unix_ms: UnixMillis::new(200),
                },
                artifact_head: ArtifactHeadExpectation::Any,
            },
            delivery: create_request.clone(),
        })
        .await
        .unwrap();
    let mismatched_snapshot_id = id(SnapshotId::new, "snapshot-request-mismatch");
    let mismatched_delivery_id = id(SnapshotDeliveryId::new, "delivery-request-mismatch");
    let mut mismatched_snapshot = repository
        .get_snapshot(&tenant_id, &id(SnapshotId::new, "snapshot-a"))
        .await
        .unwrap()
        .unwrap();
    mismatched_snapshot.snapshot_id = mismatched_snapshot_id.clone();
    mismatched_snapshot.snapshot_request_id = id(RequestId::new, "snapshot-request-mismatch");
    mismatched_snapshot.delivery_id = mismatched_delivery_id.clone();
    let mut mismatched_delivery = delivery.clone();
    mismatched_delivery.delivery_id = mismatched_delivery_id;
    mismatched_delivery.snapshot_id = mismatched_snapshot_id;
    let request_identity_error = repository
        .insert_snapshot_with_delivery(SnapshotWithDeliveryInsertRequest {
            snapshot: SnapshotInsertRequest {
                record: mismatched_snapshot,
                artifact_head: ArtifactHeadExpectation::Any,
            },
            delivery: SnapshotDeliveryInsertRequest {
                record: mismatched_delivery,
                request_id: create_request.record.create_request_id.clone(),
                retention_roots: Vec::new(),
            },
        })
        .await
        .unwrap_err();
    assert_eq!(
        request_identity_error.code(),
        CentralErrorCode::ProtocolInvalid
    );
    assert!(request_identity_error
        .message()
        .contains("same create request identity"));
    repository
        .insert_snapshot_delivery_idempotent(create_request.clone())
        .await
        .unwrap();
    assert_eq!(
        repository
            .list_snapshot_delivery_retention_roots(&tenant_id, &delivery_id)
            .await
            .unwrap(),
        roots
    );

    let failed_delivery_id = id(SnapshotDeliveryId::new, "delivery-retryable-failure");
    let failed_request_id = id(RequestId::new, "snapshot-request-b");
    let mut failed_delivery = delivery.clone();
    failed_delivery.delivery_id = failed_delivery_id.clone();
    failed_delivery.create_request_id = failed_request_id.clone();
    failed_delivery.snapshot_id = id(SnapshotId::new, "snapshot-b");
    failed_delivery.commit_id = ContentDigest::from_bytes([7; 32]);
    failed_delivery.mode = SnapshotDeliveryMode::Copy;
    failed_delivery.target_relative_root = LogicalPath::parse(
        "snapshots/project-a/artifact-a/snapshot-a/deliveries/delivery-retryable-failure",
    )
    .unwrap();
    // The aggregate is created in a consistent completed state, then the Delivery is moved to a
    // retryable failure.  A `Ready` Snapshot paired with `Failed` Delivery is intentionally not a
    // valid insertion state.
    failed_delivery.state = SnapshotDeliveryState::Ready;
    failed_delivery.issue_code = None;
    failed_delivery.issue_message = None;
    failed_delivery.issue_retryable = false;
    repository
        .insert_snapshot_with_delivery(SnapshotWithDeliveryInsertRequest {
            snapshot: SnapshotInsertRequest {
                record: SnapshotRecord {
                    tenant_id: tenant_id.clone(),
                    project_id: id(ProjectId::new, "project-a"),
                    artifact_id: id(ArtifactId::new, "artifact-a"),
                    snapshot_id: id(SnapshotId::new, "snapshot-b"),
                    snapshot_request_id: failed_request_id.clone(),
                    commit_id: ContentDigest::from_bytes([7; 32]),
                    delivery_id: failed_delivery_id.clone(),
                    edge_cluster_id: id(EdgeClusterId::new, "cluster-a"),
                    storage_volume_id: id(StorageVolumeId::new, "volume-a"),
                    delivery_mode: SnapshotDeliveryMode::Copy,
                    state: SnapshotState::Ready,
                    resource_version: 1,
                    lifecycle: ResourceLifecycle::active(),
                    created_at_unix_ms: UnixMillis::new(250),
                    updated_at_unix_ms: UnixMillis::new(250),
                },
                artifact_head: ArtifactHeadExpectation::Any,
            },
            delivery: SnapshotDeliveryInsertRequest {
                record: failed_delivery.clone(),
                request_id: failed_request_id,
                retention_roots: Vec::new(),
            },
        })
        .await
        .unwrap();
    failed_delivery.state = SnapshotDeliveryState::Failed;
    failed_delivery.issue_code = Some("DELIVERY_OBJECT_UNAVAILABLE".to_owned());
    failed_delivery.issue_message = Some("CAS object is temporarily unavailable".to_owned());
    failed_delivery.issue_retryable = true;
    failed_delivery.updated_at_unix_ms = UnixMillis::new(320);
    failed_delivery = repository
        .replace_snapshot_delivery(1, failed_delivery)
        .await
        .unwrap();
    assert_eq!(
        repository
            .get_snapshot_delivery(&tenant_id, &failed_delivery_id)
            .await
            .unwrap(),
        Some(failed_delivery.clone())
    );

    let invalid_delivery_id = id(SnapshotDeliveryId::new, "delivery-inconsistent-ready");
    let invalid_request_id = id(RequestId::new, "delivery-inconsistent-ready-create");
    let mut invalid_delivery = failed_delivery.clone();
    invalid_delivery.delivery_id = invalid_delivery_id.clone();
    invalid_delivery.create_request_id = invalid_request_id.clone();
    invalid_delivery.snapshot_id = id(SnapshotId::new, "snapshot-inconsistent-ready");
    invalid_delivery.target_relative_root = LogicalPath::parse(
        "snapshots/project-a/artifact-a/snapshot-inconsistent-ready/deliveries/delivery-inconsistent-ready",
    )
    .unwrap();
    invalid_delivery.state = SnapshotDeliveryState::Failed;
    invalid_delivery.resource_version = 1;
    invalid_delivery.delivery_generation = DeliveryGeneration::new(1);
    invalid_delivery.created_at_unix_ms = UnixMillis::new(400);
    invalid_delivery.updated_at_unix_ms = UnixMillis::new(400);
    let invalid_snapshot = SnapshotRecord {
        tenant_id: tenant_id.clone(),
        project_id: id(ProjectId::new, "project-a"),
        artifact_id: id(ArtifactId::new, "artifact-a"),
        snapshot_id: invalid_delivery.snapshot_id.clone(),
        snapshot_request_id: invalid_request_id.clone(),
        commit_id: invalid_delivery.commit_id,
        delivery_id: invalid_delivery.delivery_id.clone(),
        edge_cluster_id: id(EdgeClusterId::new, "cluster-a"),
        storage_volume_id: invalid_delivery.storage_volume_id.clone(),
        delivery_mode: invalid_delivery.mode,
        state: SnapshotState::Ready,
        resource_version: 1,
        lifecycle: ResourceLifecycle::active(),
        created_at_unix_ms: UnixMillis::new(400),
        updated_at_unix_ms: UnixMillis::new(400),
    };
    let inconsistent = repository
        .insert_snapshot_with_delivery(SnapshotWithDeliveryInsertRequest {
            snapshot: SnapshotInsertRequest {
                record: invalid_snapshot.clone(),
                artifact_head: ArtifactHeadExpectation::Any,
            },
            delivery: SnapshotDeliveryInsertRequest {
                record: invalid_delivery.clone(),
                request_id: invalid_request_id,
                retention_roots: Vec::new(),
            },
        })
        .await
        .unwrap_err();
    assert_eq!(inconsistent.code(), CentralErrorCode::InvalidState);
    assert!(inconsistent.message().contains("states are inconsistent"));
    assert!(repository
        .get_snapshot(&tenant_id, &invalid_snapshot.snapshot_id)
        .await
        .unwrap()
        .is_none());
    assert!(repository
        .get_snapshot_delivery(&tenant_id, &invalid_delivery.delivery_id)
        .await
        .unwrap()
        .is_none());

    let failed_snapshot_id = id(SnapshotId::new, "snapshot-b");
    let abnormal = repository
        .transition_snapshot_state(
            &tenant_id,
            &failed_snapshot_id,
            SnapshotState::Ready,
            SnapshotState::Abnormal,
            UnixMillis::new(325),
        )
        .await
        .unwrap();
    assert_eq!(abnormal.state, SnapshotState::Abnormal);
    let retried_snapshot = repository
        .transition_snapshot_state(
            &tenant_id,
            &failed_snapshot_id,
            SnapshotState::Abnormal,
            SnapshotState::Creating,
            UnixMillis::new(326),
        )
        .await
        .unwrap();
    assert_eq!(retried_snapshot.state, SnapshotState::Creating);

    let mut invalid_copy = delivery.clone();
    invalid_copy.delivery_id = id(SnapshotDeliveryId::new, "delivery-copy-with-roots");
    invalid_copy.create_request_id = id(RequestId::new, "delivery-copy-with-roots-create");
    invalid_copy.mode = SnapshotDeliveryMode::Copy;
    invalid_copy.target_relative_root = LogicalPath::parse(
        "snapshots/project-a/artifact-a/snapshot-a/deliveries/delivery-copy-with-roots",
    )
    .unwrap();
    let error = repository
        .insert_snapshot_delivery_idempotent(SnapshotDeliveryInsertRequest {
            request_id: invalid_copy.create_request_id.clone(),
            retention_roots: vec![SnapshotDeliveryRetentionRoot {
                tenant_id: tenant_id.clone(),
                delivery_id: invalid_copy.delivery_id.clone(),
                object_id: ObjectId::from_bytes([3; 32]),
            }],
            record: invalid_copy,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::ProtocolInvalid);

    let mut invalid_scope = delivery.clone();
    invalid_scope.delivery_id = id(SnapshotDeliveryId::new, "delivery-wrong-root-scope");
    invalid_scope.create_request_id = id(RequestId::new, "delivery-wrong-root-scope-create");
    invalid_scope.target_relative_root = LogicalPath::parse(
        "snapshots/project-a/artifact-a/snapshot-a/deliveries/delivery-wrong-root-scope",
    )
    .unwrap();
    let error = repository
        .insert_snapshot_delivery_idempotent(SnapshotDeliveryInsertRequest {
            request_id: invalid_scope.create_request_id.clone(),
            retention_roots: vec![SnapshotDeliveryRetentionRoot {
                tenant_id: tenant_id.clone(),
                delivery_id: delivery_id.clone(),
                object_id: ObjectId::from_bytes([4; 32]),
            }],
            record: invalid_scope,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::ProtocolInvalid);

    let mut duplicate_roots = delivery.clone();
    duplicate_roots.delivery_id = id(SnapshotDeliveryId::new, "delivery-duplicate-roots");
    duplicate_roots.create_request_id = id(RequestId::new, "delivery-duplicate-roots-create");
    duplicate_roots.target_relative_root = LogicalPath::parse(
        "snapshots/project-a/artifact-a/snapshot-a/deliveries/delivery-duplicate-roots",
    )
    .unwrap();
    let duplicate_root = SnapshotDeliveryRetentionRoot {
        tenant_id: tenant_id.clone(),
        delivery_id: duplicate_roots.delivery_id.clone(),
        object_id: ObjectId::from_bytes([5; 32]),
    };
    let error = repository
        .insert_snapshot_delivery_idempotent(SnapshotDeliveryInsertRequest {
            request_id: duplicate_roots.create_request_id.clone(),
            retention_roots: vec![duplicate_root.clone(), duplicate_root],
            record: duplicate_roots,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::ProtocolInvalid);

    let request_id = id(RequestId::new, "delivery-retry-a");
    let no_op_request = SnapshotDeliveryMutationRequest {
        tenant_id: tenant_id.clone(),
        request_id: request_id.clone(),
        delivery_id: delivery_id.clone(),
        kind: SnapshotDeliveryMutationKind::Retry,
        request_digest: ContentDigest::hash(b"retry-a"),
        expected_resource_version: delivery.resource_version,
        desired_delivery: delivery.clone(),
    };
    let no_op_receipt = match repository
        .apply_snapshot_delivery_mutation_idempotent(no_op_request.clone())
        .await
        .unwrap()
    {
        CatalogInsertOutcome::Inserted(receipt) => receipt,
        CatalogInsertOutcome::Existing(_) => panic!("first no-op receipt must be inserted"),
    };
    assert_eq!(no_op_receipt.delivery, delivery);
    assert!(matches!(
        repository
            .apply_snapshot_delivery_mutation_idempotent(no_op_request.clone())
            .await
            .unwrap(),
        CatalogInsertOutcome::Existing(receipt) if receipt == no_op_receipt
    ));
    let mut conflicting_desired = delivery.clone();
    conflicting_desired.state = SnapshotDeliveryState::Deleting;
    conflicting_desired.delivery_generation = DeliveryGeneration::new(2);
    let conflicting = SnapshotDeliveryMutationRequest {
        kind: SnapshotDeliveryMutationKind::Delete,
        desired_delivery: conflicting_desired,
        ..no_op_request
    };
    assert!(repository
        .apply_snapshot_delivery_mutation_idempotent(conflicting)
        .await
        .is_err());
    assert_eq!(
        repository
            .get_snapshot_delivery(&tenant_id, &delivery_id)
            .await
            .unwrap(),
        Some(delivery.clone()),
        "a request-ID conflict must not apply its Delivery transition"
    );
    assert_eq!(
        repository
            .get_snapshot_delivery_mutation(&tenant_id, &request_id)
            .await
            .unwrap(),
        Some(no_op_receipt)
    );

    let mut retried_delivery = repository
        .get_snapshot_delivery(&tenant_id, &failed_delivery_id)
        .await
        .unwrap()
        .unwrap();
    retried_delivery.state = SnapshotDeliveryState::Requested;
    retried_delivery.delivery_generation = DeliveryGeneration::new(2);
    retried_delivery.issue_code = None;
    retried_delivery.issue_message = None;
    retried_delivery.issue_retryable = false;
    retried_delivery.updated_at_unix_ms = UnixMillis::new(350);
    let transition_request_id = id(RequestId::new, "delivery-retry-transition");
    let transition_request = SnapshotDeliveryMutationRequest {
        tenant_id: tenant_id.clone(),
        request_id: transition_request_id.clone(),
        delivery_id: failed_delivery_id.clone(),
        kind: SnapshotDeliveryMutationKind::Retry,
        request_digest: ContentDigest::hash(b"retry-transition"),
        expected_resource_version: failed_delivery.resource_version,
        desired_delivery: retried_delivery,
    };
    let transition_receipt = match repository
        .apply_snapshot_delivery_mutation_idempotent(transition_request.clone())
        .await
        .unwrap()
    {
        CatalogInsertOutcome::Inserted(receipt) => receipt,
        CatalogInsertOutcome::Existing(_) => panic!("first transition receipt must be inserted"),
    };
    assert_eq!(transition_receipt.delivery.resource_version, 3);
    assert_eq!(
        transition_receipt.delivery.state,
        SnapshotDeliveryState::Requested
    );
    assert_eq!(
        repository
            .get_snapshot_delivery(&tenant_id, &failed_delivery_id)
            .await
            .unwrap(),
        Some(transition_receipt.delivery.clone())
    );
    assert!(matches!(
        repository
            .apply_snapshot_delivery_mutation_idempotent(transition_request)
            .await
            .unwrap(),
        CatalogInsertOutcome::Existing(receipt) if receipt == transition_receipt
    ));

    let stale_request_id = id(RequestId::new, "delivery-stale-transition");
    let mut stale_desired = delivery.clone();
    stale_desired.state = SnapshotDeliveryState::Deleting;
    let stale = repository
        .apply_snapshot_delivery_mutation_idempotent(SnapshotDeliveryMutationRequest {
            tenant_id: tenant_id.clone(),
            request_id: stale_request_id.clone(),
            delivery_id: delivery_id.clone(),
            kind: SnapshotDeliveryMutationKind::Delete,
            request_digest: ContentDigest::hash(b"stale-transition"),
            expected_resource_version: 99,
            desired_delivery: stale_desired,
        })
        .await
        .unwrap_err();
    assert_eq!(stale.code(), CentralErrorCode::ConcurrentUpdate);
    assert!(repository
        .get_snapshot_delivery_mutation(&tenant_id, &stale_request_id)
        .await
        .unwrap()
        .is_none());

    delivery.state = SnapshotDeliveryState::Deleted;
    delivery.updated_at_unix_ms = UnixMillis::new(400);
    repository
        .replace_snapshot_delivery(1, delivery)
        .await
        .unwrap();
    assert!(repository
        .list_snapshot_delivery_retention_roots(&tenant_id, &delivery_id)
        .await
        .unwrap()
        .is_empty());
    let replay = repository
        .insert_snapshot_delivery_idempotent(create_request)
        .await
        .unwrap();
    assert!(matches!(
        replay,
        SnapshotDeliveryInsertOutcome::Existing(SnapshotDeliveryRecord {
            state: SnapshotDeliveryState::Deleted,
            ..
        })
    ));
    assert!(repository
        .list_snapshot_delivery_retention_roots(&tenant_id, &delivery_id)
        .await
        .unwrap()
        .is_empty());

    repository
        .insert_storage_volume(pvc_volume("tenant-a", "volume-b", "claim-b"))
        .await
        .unwrap();
    let mut mismatched_parent = repository
        .get_snapshot_delivery(&tenant_id, &delivery_id)
        .await
        .unwrap()
        .unwrap();
    mismatched_parent.delivery_id = id(SnapshotDeliveryId::new, "delivery-parent-mismatch");
    mismatched_parent.create_request_id = id(RequestId::new, "delivery-parent-mismatch-create");
    mismatched_parent.storage_volume_id = id(StorageVolumeId::new, "volume-b");
    mismatched_parent.commit_id = ContentDigest::from_bytes([8; 32]);
    mismatched_parent.mode = SnapshotDeliveryMode::Copy;
    mismatched_parent.target_relative_root = LogicalPath::parse(
        "snapshots/project-a/artifact-a/snapshot-a/deliveries/delivery-parent-mismatch",
    )
    .unwrap();
    mismatched_parent.state = SnapshotDeliveryState::Requested;
    mismatched_parent.delivery_generation = DeliveryGeneration::new(1);
    mismatched_parent.resource_version = 1;
    mismatched_parent.updated_at_unix_ms = UnixMillis::new(500);
    let mismatch = repository
        .insert_snapshot_delivery_idempotent(SnapshotDeliveryInsertRequest {
            request_id: mismatched_parent.create_request_id.clone(),
            record: mismatched_parent,
            retention_roots: Vec::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(mismatch.code(), CentralErrorCode::InvalidState);
    assert!(
        mismatch.message().contains("Snapshot identity")
            || mismatch
                .message()
                .contains("Snapshot already has a SnapshotDelivery")
    );

    let volume_root = ResourceRef::StorageVolume {
        storage_volume_id: id(StorageVolumeId::new, "volume-b"),
    };
    let volume_impact = repository
        .query_deletion_impact(DeletionImpactQuery {
            tenant_id: tenant_id.clone(),
            root: volume_root.clone(),
            cascade: false,
            confirm_managed_data_erase: true,
            additional_blockers: Vec::new(),
            authority_impact: None,
            now_unix_ms: UnixMillis::new(510),
        })
        .await
        .unwrap();
    repository
        .create_deletion_idempotent(CreateDeletionRequest {
            deletion_id: id(DeletionId::new, "deletion-volume-b"),
            tenant_id: tenant_id.clone(),
            root: volume_root,
            cascade: false,
            confirm_managed_data_erase: true,
            request_id: id(RequestId::new, "delete-volume-b-request"),
            request_digest: ContentDigest::hash(b"delete-volume-b"),
            impact_digest: volume_impact.impact_digest,
            expected_resource_version: 1,
            now_unix_ms: UnixMillis::new(511),
        })
        .await
        .unwrap();
    let mut fenced_volume_delivery = repository
        .get_snapshot_delivery(&tenant_id, &delivery_id)
        .await
        .unwrap()
        .unwrap();
    fenced_volume_delivery.delivery_id = id(SnapshotDeliveryId::new, "delivery-fenced-volume");
    fenced_volume_delivery.create_request_id = id(RequestId::new, "delivery-fenced-volume-create");
    fenced_volume_delivery.storage_volume_id = id(StorageVolumeId::new, "volume-b");
    fenced_volume_delivery.mode = SnapshotDeliveryMode::Copy;
    fenced_volume_delivery.target_relative_root = LogicalPath::parse(
        "snapshots/project-a/artifact-a/snapshot-a/deliveries/delivery-fenced-volume",
    )
    .unwrap();
    fenced_volume_delivery.state = SnapshotDeliveryState::Requested;
    fenced_volume_delivery.delivery_generation = DeliveryGeneration::new(1);
    fenced_volume_delivery.resource_version = 1;
    fenced_volume_delivery.updated_at_unix_ms = UnixMillis::new(520);
    let fenced_volume = repository
        .insert_snapshot_delivery_idempotent(SnapshotDeliveryInsertRequest {
            request_id: fenced_volume_delivery.create_request_id.clone(),
            record: fenced_volume_delivery,
            retention_roots: Vec::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(fenced_volume.code(), CentralErrorCode::InvalidState);
    assert!(
        fenced_volume.message().contains("StorageVolume")
            || fenced_volume
                .message()
                .contains("Snapshot already has a SnapshotDelivery")
    );

    let snapshot_root = ResourceRef::Snapshot {
        snapshot_id: id(SnapshotId::new, "snapshot-a"),
    };
    let snapshot_impact = repository
        .query_deletion_impact(DeletionImpactQuery {
            tenant_id: tenant_id.clone(),
            root: snapshot_root.clone(),
            cascade: false,
            confirm_managed_data_erase: false,
            additional_blockers: Vec::new(),
            authority_impact: None,
            now_unix_ms: UnixMillis::new(530),
        })
        .await
        .unwrap();
    repository
        .create_deletion_idempotent(CreateDeletionRequest {
            deletion_id: id(DeletionId::new, "deletion-snapshot-delivery-parent"),
            tenant_id: tenant_id.clone(),
            root: snapshot_root,
            cascade: false,
            confirm_managed_data_erase: false,
            request_id: id(RequestId::new, "delete-snapshot-delivery-parent-request"),
            request_digest: ContentDigest::hash(b"delete-snapshot-delivery-parent"),
            impact_digest: snapshot_impact.impact_digest,
            expected_resource_version: 1,
            now_unix_ms: UnixMillis::new(531),
        })
        .await
        .unwrap();
    let mut fenced_snapshot_delivery = repository
        .get_snapshot_delivery(&tenant_id, &delivery_id)
        .await
        .unwrap()
        .unwrap();
    fenced_snapshot_delivery.delivery_id = id(SnapshotDeliveryId::new, "delivery-fenced-snapshot");
    fenced_snapshot_delivery.create_request_id =
        id(RequestId::new, "delivery-fenced-snapshot-create");
    fenced_snapshot_delivery.mode = SnapshotDeliveryMode::Copy;
    fenced_snapshot_delivery.commit_id = ContentDigest::from_bytes([8; 32]);
    fenced_snapshot_delivery.target_relative_root = LogicalPath::parse(
        "snapshots/project-a/artifact-a/snapshot-a/deliveries/delivery-fenced-snapshot",
    )
    .unwrap();
    fenced_snapshot_delivery.state = SnapshotDeliveryState::Requested;
    fenced_snapshot_delivery.delivery_generation = DeliveryGeneration::new(1);
    fenced_snapshot_delivery.resource_version = 1;
    fenced_snapshot_delivery.updated_at_unix_ms = UnixMillis::new(540);
    let fenced_snapshot = repository
        .insert_snapshot_delivery_idempotent(SnapshotDeliveryInsertRequest {
            request_id: fenced_snapshot_delivery.create_request_id.clone(),
            record: fenced_snapshot_delivery,
            retention_roots: Vec::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(fenced_snapshot.code(), CentralErrorCode::InvalidState);
    assert!(fenced_snapshot.message().contains("Snapshot"));
}

async fn seed_snapshot_catalog_parents(repository: &Arc<dyn ControlCatalogRepository>) {
    repository
        .insert_tenant(tenant("tenant-a", "Research"))
        .await
        .unwrap();
    repository
        .insert_artifact(artifact("tenant-a", "project-a", "artifact-a", 100))
        .await
        .unwrap();
    repository
        .insert_storage_volume(pvc_volume("tenant-a", "volume-a", "claim-a"))
        .await
        .unwrap();
}

async fn exercise_s3_mutation_ledger(repository: Arc<dyn ControlCatalogRepository>) {
    let tenant_id = id(TenantId::new, "tenant-a");
    let access_point_id = id(S3AccessPointId::new, "s3ap-contract");
    let create_request_id = id(RequestId::new, "s3-create-request");
    let access_point = S3AccessPointRecord {
        access_point_id: access_point_id.clone(),
        tenant_id: tenant_id.clone(),
        project_id: id(ProjectId::new, "project-a"),
        artifact_id: id(ArtifactId::new, "artifact-a"),
        snapshot_id: id(SnapshotId::new, "snapshot-a"),
        commit_id: ContentDigest::from_bytes([7; 32]),
        delivery_id: id(SnapshotDeliveryId::new, "delivery-snapshot-a"),
        storage_volume_id: id(StorageVolumeId::new, "volume-a"),
        edge_cluster_id: id(EdgeClusterId::new, "cluster-a"),
        bucket_name: "contract-bucket".to_owned(),
        state: S3AccessPointState::Active,
        policy_generation: 1,
        created_at_unix_ms: UnixMillis::new(250),
        updated_at_unix_ms: UnixMillis::new(250),
    };
    let credential = S3CredentialRecord {
        credential_id: id(S3CredentialId::new, "s3cred-contract"),
        access_point_id: access_point_id.clone(),
        access_key_id: "NGS3CONTRACT".to_owned(),
        encrypted_secret: vec![1, 2, 3],
        state: S3CredentialState::Active,
        expires_at_unix_ms: UnixMillis::new(10_300),
        created_at_unix_ms: UnixMillis::new(300),
        last_used_at_unix_ms: None,
    };
    let create = s3_mutation(
        &tenant_id,
        &create_request_id,
        S3MutationKind::AccessPointCreate,
        1,
        300,
    );
    assert!(matches!(
        repository
            .create_s3_access_point_idempotent(
                create.clone(),
                access_point.clone(),
                credential.clone(),
            )
            .await
            .unwrap(),
        CatalogInsertOutcome::Inserted(_)
    ));
    let mut replay = create.clone();
    replay.created_at_unix_ms = UnixMillis::new(999);
    assert!(matches!(
        repository
            .create_s3_access_point_idempotent(replay, access_point.clone(), credential.clone())
            .await
            .unwrap(),
        CatalogInsertOutcome::Existing(_)
    ));
    let mut conflict_request = create;
    conflict_request.request_digest = ContentDigest::hash(b"different");
    assert_eq!(
        repository
            .create_s3_access_point_idempotent(
                conflict_request,
                access_point.clone(),
                credential.clone(),
            )
            .await
            .unwrap_err()
            .code(),
        CentralErrorCode::InvalidState
    );

    let disable_id = id(RequestId::new, "s3-disable-request");
    repository
        .update_s3_access_point_state_idempotent(
            s3_mutation(
                &tenant_id,
                &disable_id,
                S3MutationKind::AccessPointDisable,
                2,
                400,
            ),
            &access_point_id,
            S3AccessPointState::Disabled,
            UnixMillis::new(400),
        )
        .await
        .unwrap();
    assert!(repository
        .list_s3_credentials(&access_point_id)
        .await
        .unwrap()
        .iter()
        .all(|credential| credential.state == S3CredentialState::Revoked));
    let enable_id = id(RequestId::new, "s3-enable-request");
    repository
        .update_s3_access_point_state_idempotent(
            s3_mutation(
                &tenant_id,
                &enable_id,
                S3MutationKind::AccessPointEnable,
                3,
                500,
            ),
            &access_point_id,
            S3AccessPointState::Active,
            UnixMillis::new(500),
        )
        .await
        .unwrap();
    let replay_disable = repository
        .update_s3_access_point_state_idempotent(
            s3_mutation(
                &tenant_id,
                &disable_id,
                S3MutationKind::AccessPointDisable,
                2,
                501,
            ),
            &access_point_id,
            S3AccessPointState::Disabled,
            UnixMillis::new(501),
        )
        .await
        .unwrap();
    assert!(
        matches!(replay_disable, CatalogInsertOutcome::Existing(record) if record.state == S3AccessPointState::Active)
    );

    let revoke_id = id(RequestId::new, "s3-revoke-request");
    repository
        .revoke_s3_credential_idempotent(
            s3_mutation(
                &tenant_id,
                &revoke_id,
                S3MutationKind::CredentialRevoke,
                4,
                600,
            ),
            &access_point_id,
            &credential.credential_id,
        )
        .await
        .unwrap();
    assert!(matches!(
        repository
            .revoke_s3_credential_idempotent(
                s3_mutation(
                    &tenant_id,
                    &revoke_id,
                    S3MutationKind::CredentialRevoke,
                    4,
                    601,
                ),
                &access_point_id,
                &credential.credential_id,
            )
            .await
            .unwrap(),
        CatalogInsertOutcome::Existing(record) if record.state == S3CredentialState::Revoked
    ));
}

fn s3_mutation(
    tenant_id: &TenantId,
    request_id: &RequestId,
    operation: S3MutationKind,
    digest_seed: u64,
    created_at: u64,
) -> S3MutationRecord {
    S3MutationRecord {
        tenant_id: tenant_id.clone(),
        request_id: request_id.clone(),
        operation,
        request_digest: ContentDigest::hash(digest_seed.to_string().as_bytes()),
        created_at_unix_ms: UnixMillis::new(created_at),
    }
}

fn test_principal(id: &str) -> PrincipalRef {
    PrincipalRef {
        kind: PrincipalKind::System,
        id: PrincipalId::new(id).unwrap(),
        extensions: Extensions::new(),
    }
}

fn tenant(tenant_id: &str, display_name: &str) -> TenantRecord {
    TenantRecord {
        tenant_id: id(TenantId::new, tenant_id),
        display_name: display_name.to_owned(),
        description: Some("catalog test".to_owned()),
        resource_version: 1,
        created_at_unix_ms: UnixMillis::new(100),
        updated_at_unix_ms: UnixMillis::new(100),
    }
}

fn pvc_volume(tenant_id: &str, volume_id: &str, claim: &str) -> StorageVolumeRecord {
    StorageVolumeRecord {
        tenant_id: id(TenantId::new, tenant_id),
        storage_volume_id: id(StorageVolumeId::new, volume_id),
        display_name: volume_id.to_owned(),
        edge_cluster_id: id(EdgeClusterId::new, "cluster-a"),
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
            claim_name: claim.to_owned(),
        }),
        nfs_reference: None,
        state: StorageVolumeState::Ready,
        resource_version: 1,
        lifecycle: ResourceLifecycle::active(),
        created_at_unix_ms: UnixMillis::new(100),
        updated_at_unix_ms: UnixMillis::new(100),
    }
}

fn nfs_volume(tenant_id: &str, volume_id: &str) -> StorageVolumeRecord {
    StorageVolumeRecord {
        tenant_id: id(TenantId::new, tenant_id),
        storage_volume_id: id(StorageVolumeId::new, volume_id),
        display_name: volume_id.to_owned(),
        edge_cluster_id: id(EdgeClusterId::new, "cluster-a"),
        region: "cn-shanghai".to_owned(),
        backend_type: StorageBackendType::Nfs,
        access_mode: StorageAccessMode::ReadOnlyMany,
        allowed_delivery_modes: vec![
            neoengram_domain::protocol::SnapshotDeliveryMode::Fuse,
            neoengram_domain::protocol::SnapshotDeliveryMode::Copy,
        ],
        hardlink_policy: neoengram_domain::protocol::HardlinkPolicy::Disabled,
        max_whole_file_bytes: neoengram_domain::protocol::DecimalU64::new(u64::MAX),
        copy_reserve_bytes: neoengram_domain::protocol::DecimalU64::new(0),
        pvc_reference: None,
        nfs_reference: Some(CatalogNfsReference {
            server: "nfs.internal".to_owned(),
            export_path: "/exports/data".to_owned(),
        }),
        state: StorageVolumeState::Unavailable,
        resource_version: 1,
        lifecycle: ResourceLifecycle::active(),
        created_at_unix_ms: UnixMillis::new(101),
        updated_at_unix_ms: UnixMillis::new(101),
    }
}

fn artifact(
    tenant_id: &str,
    project_id: &str,
    artifact_id: &str,
    created_at_unix_ms: u64,
) -> ArtifactRecord {
    ArtifactRecord {
        tenant_id: id(TenantId::new, tenant_id),
        project_id: id(ProjectId::new, project_id),
        artifact_id: id(ArtifactId::new, artifact_id),
        display_name: artifact_id.to_owned(),
        description: Some("catalog test".to_owned()),
        initialization: ArtifactInitialization::Empty,
        head_commit_id: None,
        resource_version: 1,
        lifecycle: ResourceLifecycle::active(),
        created_at_unix_ms: UnixMillis::new(created_at_unix_ms),
        updated_at_unix_ms: UnixMillis::new(created_at_unix_ms),
    }
}

fn playground() -> PlaygroundRecord {
    PlaygroundRecord {
        tenant_id: id(TenantId::new, "tenant-a"),
        project_id: id(ProjectId::new, "project-a"),
        artifact_id: id(ArtifactId::new, "artifact-a"),
        playground_id: id(PlaygroundId::new, "labeling"),
        storage_volume_id: id(StorageVolumeId::new, "volume-a"),
        region: "cn-shanghai".to_owned(),
        display_name: "Labeling".to_owned(),
        base_commit_id: None,
        head_commit_id: None,
        state: PlaygroundState::Ready,
        resource_version: 1,
        lifecycle: ResourceLifecycle::active(),
        relative_root: "playgrounds/project-a/artifact-a/labeling".to_owned(),
        created_at_unix_ms: UnixMillis::new(200),
        updated_at_unix_ms: UnixMillis::new(200),
    }
}

fn playground_at(playground_id: &str, base_commit_id: Option<ContentDigest>) -> PlaygroundRecord {
    let mut record = playground();
    record.playground_id = id(PlaygroundId::new, playground_id);
    record.base_commit_id = base_commit_id;
    record.head_commit_id = base_commit_id;
    record.relative_root = format!("playgrounds/project-a/artifact-a/{playground_id}");
    record
}

fn id<T, E>(parse: impl FnOnce(String) -> Result<T, E>, value: &str) -> T
where
    E: std::fmt::Debug,
{
    parse(value.to_owned()).unwrap()
}
