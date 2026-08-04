use std::str::FromStr;

use neoengram_core::{ContentDigest, IndexVersion};
use neoengram_protocol::{
    ArtifactId, EdgeClusterId, PlaygroundId, ProjectId, StorageVolumeId, TenantId, UnixMillis,
    WireIndexVersion,
};
use neoengramd::{
    open_sqlite_authority, CatalogInsertOutcome, CatalogNfsReference, CatalogPvcReference,
    PlaygroundListRequest, PlaygroundRecord, PlaygroundState, SqliteAuthorityConfig,
    StorageAccessMode, StorageBackendType, StorageVolumeListRequest, StorageVolumeRecord,
    StorageVolumeState, TenantListRequest, TenantRecord,
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

    let playground = playground();
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
    assert_eq!(page.records, [playground]);

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
async fn sqlite_migrates_registry_v3_into_control_catalog_v4() {
    let directory = tempfile::tempdir().unwrap();
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    authority.close().await;

    let options =
        SqliteConnectOptions::new().filename(directory.path().join("agent-registry.sqlite3"));
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::raw_sql(
        "DROP TABLE playground_catalog_records;
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

fn playground() -> PlaygroundRecord {
    PlaygroundRecord {
        tenant_id: id(TenantId::new, "tenant-a"),
        project_id: id(ProjectId::new, "project-a"),
        artifact_id: id(ArtifactId::new, "artifact-a"),
        playground_id: id(PlaygroundId::new, "labeling"),
        storage_volume_id: id(StorageVolumeId::new, "volume-a"),
        region: "cn-shanghai".to_owned(),
        display_name: "Labeling".to_owned(),
        base_commit_id: Some(ContentDigest::from_str(&"a".repeat(64)).unwrap()),
        head_commit_id: Some(ContentDigest::from_str(&"a".repeat(64)).unwrap()),
        index_version: WireIndexVersion::from(IndexVersion::from_snapshot(0, &[]).unwrap()),
        state: PlaygroundState::Ready,
        relative_root: "playgrounds/project-a/artifact-a/labeling".to_owned(),
        created_at_unix_ms: UnixMillis::new(200),
        updated_at_unix_ms: UnixMillis::new(200),
    }
}

fn id<T, E>(parse: impl FnOnce(String) -> Result<T, E>, value: &str) -> T
where
    E: std::fmt::Debug,
{
    parse(value.to_owned()).unwrap()
}
