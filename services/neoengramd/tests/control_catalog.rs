use std::str::FromStr;

use neoengram_core::ContentDigest;
use neoengram_protocol::{
    ArtifactId, EdgeClusterId, PlaygroundId, ProjectId, StorageVolumeId, TenantId, UnixMillis,
};
use neoengramd::{
    open_sqlite_authority, AdvancePlaygroundCommitRequest, ArtifactHeadExpectation,
    ArtifactInitialization, ArtifactListRequest, ArtifactRecord, CatalogInsertOutcome,
    CatalogNfsReference, CatalogPvcReference, CentralErrorCode, ControlCatalogRepository,
    InMemoryControlCatalog, PlaygroundInsertRequest, PlaygroundListRequest, PlaygroundRecord,
    PlaygroundState, SqliteAuthorityConfig, StorageAccessMode, StorageBackendType,
    StorageVolumeListRequest, StorageVolumeRecord, StorageVolumeState, TenantListRequest,
    TenantRecord,
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
async fn sqlite_migrates_registry_v3_into_control_catalog_v6() {
    let directory = tempfile::tempdir().unwrap();
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    authority.close().await;

    let options =
        SqliteConnectOptions::new().filename(directory.path().join("agent-registry.sqlite3"));
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::raw_sql(
        "DROP TABLE snapshot_catalog_records;
         DROP TABLE playground_catalog_records;
         DROP TABLE artifact_catalog_records;
         DROP TABLE storage_volume_catalog_records;
         DROP TABLE tenant_catalog_records;
         PRAGMA user_version = 3;",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    connection.close().await.unwrap();

    let migrated = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    migrated
        .authority_store()
        .control_catalog()
        .unwrap()
        .insert_tenant(tenant("tenant-a", "Migrated"))
        .await
        .unwrap();
    migrated.integrity_check().await.unwrap();
    migrated.close().await;
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
async fn sqlite_v4_migration_never_derives_artifact_authority_from_playgrounds() {
    let directory = tempfile::tempdir().unwrap();
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let repository = authority.authority_store().control_catalog().unwrap();
    repository
        .insert_tenant(tenant("tenant-a", "Migrated tenant"))
        .await
        .unwrap();
    repository
        .insert_storage_volume(pvc_volume("tenant-a", "volume-a", "claim-a"))
        .await
        .unwrap();
    authority.close().await;

    let options =
        SqliteConnectOptions::new().filename(directory.path().join("agent-registry.sqlite3"));
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::raw_sql(
        "DROP TABLE playground_catalog_records;
         DROP TABLE artifact_catalog_records;
         CREATE TABLE playground_catalog_records (
             tenant_id TEXT NOT NULL,
             project_id TEXT NOT NULL,
             artifact_id TEXT NOT NULL,
             playground_id TEXT NOT NULL,
             storage_volume_id TEXT NOT NULL,
             region TEXT NOT NULL,
             display_name TEXT NOT NULL,
             base_commit_digest BLOB,
             head_commit_digest BLOB,
             index_revision TEXT NOT NULL CHECK (
                 index_revision <> '' AND index_revision NOT GLOB '*[^0-9]*'
             ),
             index_digest BLOB NOT NULL CHECK (length(index_digest) = 32),
             state TEXT NOT NULL CHECK (state IN ('creating', 'ready', 'abnormal')),
             relative_root TEXT NOT NULL CHECK (
                 relative_root <> '' AND substr(relative_root, 1, 1) <> '/'
             ),
             created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
             updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= created_at_unix_ms),
             PRIMARY KEY (tenant_id, project_id, artifact_id, playground_id),
             FOREIGN KEY (tenant_id, storage_volume_id)
                 REFERENCES storage_volume_catalog_records(tenant_id, storage_volume_id)
         ) STRICT;
         CREATE INDEX playground_catalog_keyset
             ON playground_catalog_records (
                 tenant_id, created_at_unix_ms DESC, project_id ASC, artifact_id ASC,
                 playground_id ASC
             );
         CREATE INDEX playground_catalog_filter_keyset
             ON playground_catalog_records (
                 tenant_id, project_id, artifact_id, region, state,
                 created_at_unix_ms DESC, playground_id ASC
             );",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO playground_catalog_records \
         (tenant_id, project_id, artifact_id, playground_id, storage_volume_id, region, \
          display_name, base_commit_digest, head_commit_digest, index_revision, index_digest, \
          state, relative_root, created_at_unix_ms, updated_at_unix_ms) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind("tenant-a")
    .bind("project-a")
    .bind("artifact-a")
    .bind("playground-a")
    .bind("volume-a")
    .bind("cn-shanghai")
    .bind("Migrated playground")
    .bind([5_u8; 32].as_slice())
    .bind([5_u8; 32].as_slice())
    .bind("0")
    .bind([7_u8; 32].as_slice())
    .bind("ready")
    .bind("playgrounds/project-a/artifact-a/playground-a")
    .bind(200_i64)
    .bind(201_i64)
    .execute(&mut connection)
    .await
    .unwrap();
    sqlx::query("PRAGMA user_version = 4")
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();

    let migration_error = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .err()
        .expect("a derived v4 Playground must not create an Artifact authority record");
    assert!(migration_error
        .to_string()
        .contains("Artifact authority cannot be inferred from a derived v4 Playground"));

    let options =
        SqliteConnectOptions::new().filename(directory.path().join("agent-registry.sqlite3"));
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        version, 4,
        "failed migration must roll back its schema changes"
    );
    let playground_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM playground_catalog_records")
            .fetch_one(&mut connection)
            .await
            .unwrap();
    assert_eq!(
        playground_count, 1,
        "failed migration must preserve legacy data"
    );
    // Simulate the documented operator recovery after exporting the legacy Playground.
    sqlx::query("DELETE FROM playground_catalog_records")
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();

    let migrated = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let repository = migrated.authority_store().control_catalog().unwrap();
    let artifact = repository
        .get_artifact(
            &id(TenantId::new, "tenant-a"),
            &id(ProjectId::new, "project-a"),
            &id(ArtifactId::new, "artifact-a"),
        )
        .await
        .unwrap();
    assert!(
        artifact.is_none(),
        "migration must not synthesize Artifact authority"
    );
    migrated.integrity_check().await.unwrap();
    migrated.close().await;

    let options =
        SqliteConnectOptions::new().filename(directory.path().join("agent-registry.sqlite3"));
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(version, 6);
    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM pragma_table_info('playground_catalog_records') ORDER BY cid",
    )
    .fetch_all(&mut connection)
    .await
    .unwrap();
    assert!(!columns.iter().any(|column| column == "index_revision"));
    assert!(!columns.iter().any(|column| column == "index_digest"));
    connection.close().await.unwrap();
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

    let options =
        SqliteConnectOptions::new().filename(directory.path().join("agent-registry.sqlite3"));
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
        pvc_reference: Some(CatalogPvcReference {
            namespace: "neoengram".to_owned(),
            claim_name: claim.to_owned(),
        }),
        nfs_reference: None,
        state: StorageVolumeState::Ready,
        resource_version: 1,
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
        pvc_reference: None,
        nfs_reference: Some(CatalogNfsReference {
            server: "nfs.internal".to_owned(),
            export_path: "/exports/data".to_owned(),
        }),
        state: StorageVolumeState::Unavailable,
        resource_version: 1,
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
