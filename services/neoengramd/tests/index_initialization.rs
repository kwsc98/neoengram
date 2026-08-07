use neoengram_core::{ContentDigest, FileRecord, IndexVersion, LogicalPath, ManifestId};
use neoengram_protocol::{ArtifactId, PlaygroundId, ProjectId, TenantId, WireIndexVersion};
use neoengramd::{
    CentralErrorCode, InMemoryIndexPublisher, IndexKey, IndexPublisher,
    InitializeIndexSnapshotRequest,
};

fn index_key(playground_id: &str) -> IndexKey {
    IndexKey {
        tenant_id: TenantId::new("tenant-index-init").unwrap(),
        project_id: ProjectId::new("project-index-init").unwrap(),
        artifact_id: ArtifactId::new("artifact-index-init").unwrap(),
        playground_id: PlaygroundId::new(playground_id).unwrap(),
    }
}

fn record(path: &str, manifest_byte: u8) -> FileRecord {
    FileRecord::new(
        LogicalPath::parse(path).unwrap(),
        ManifestId::from_bytes([manifest_byte; 32]),
        u64::from(manifest_byte),
        1,
    )
    .unwrap()
}

fn version(revision: u64, records: &[FileRecord]) -> WireIndexVersion {
    WireIndexVersion::from(IndexVersion::from_snapshot(revision, records).unwrap())
}

fn request(
    key: IndexKey,
    revision: u64,
    records: Vec<FileRecord>,
) -> InitializeIndexSnapshotRequest {
    InitializeIndexSnapshotRequest {
        index_key: key,
        version: version(revision, &records),
        records,
    }
}

#[tokio::test]
async fn memory_initialization_is_create_only_and_exactly_idempotent() {
    let publisher = InMemoryIndexPublisher::default();
    let key = index_key("playground-memory-init");
    let records = vec![record("a", 1), record("b/c", 2)];
    let initialization = request(key.clone(), 7, records.clone());

    let initialized = publisher
        .initialize_snapshot(initialization.clone())
        .await
        .unwrap();
    let replayed = publisher.initialize_snapshot(initialization).await.unwrap();

    assert_eq!(initialized, version(7, &records));
    assert_eq!(replayed, initialized);
    assert_eq!(publisher.snapshot(&key).unwrap().records, records);

    let error = publisher
        .initialize_snapshot(request(key.clone(), 8, vec![record("other", 3)]))
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::ConcurrentUpdate);
    assert_eq!(publisher.snapshot(&key).unwrap().version, initialized);
}

#[tokio::test]
async fn memory_empty_initialization_is_distinct_from_an_absent_virtual_empty_index() {
    let publisher = InMemoryIndexPublisher::default();
    let key = index_key("playground-empty-init");
    let empty = request(key.clone(), 0, Vec::new());

    publisher.initialize_snapshot(empty.clone()).await.unwrap();
    publisher.initialize_snapshot(empty).await.unwrap();

    let error = publisher
        .initialize_snapshot(request(key.clone(), 1, Vec::new()))
        .await
        .unwrap_err();
    assert_eq!(error.code(), CentralErrorCode::ConcurrentUpdate);
    assert_eq!(publisher.snapshot(&key).unwrap().version, version(0, &[]));
}

#[tokio::test]
async fn initialization_rejects_noncanonical_records_and_digest_without_mutation() {
    let publisher = InMemoryIndexPublisher::default();
    let key = index_key("playground-invalid-init");
    let canonical = vec![record("a", 1), record("b", 2)];

    let digest_error = publisher
        .initialize_snapshot(InitializeIndexSnapshotRequest {
            index_key: key.clone(),
            version: WireIndexVersion {
                digest: ContentDigest::from_bytes([0x55; 32]),
                ..version(4, &canonical)
            },
            records: canonical.clone(),
        })
        .await
        .unwrap_err();
    assert_eq!(digest_error.code(), CentralErrorCode::MetadataInvalid);

    let order_error = publisher
        .initialize_snapshot(InitializeIndexSnapshotRequest {
            index_key: key.clone(),
            version: version(4, &canonical),
            records: canonical.iter().cloned().rev().collect(),
        })
        .await
        .unwrap_err();
    assert_eq!(order_error.code(), CentralErrorCode::MetadataInvalid);

    publisher
        .initialize_snapshot(request(key.clone(), 4, canonical.clone()))
        .await
        .unwrap();
    assert_eq!(publisher.snapshot(&key).unwrap().records, canonical);
}

#[cfg(feature = "authority-sqlite")]
#[tokio::test]
async fn sqlite_initialization_replays_after_reopen_and_rejects_different_content() {
    use neoengramd::{open_sqlite_authority, SqliteAuthorityConfig};

    let directory = tempfile::TempDir::new().unwrap();
    let key = index_key("playground-sqlite-init");
    let records = vec![record("a", 1), record("nested/b", 2)];
    let initialization = request(key.clone(), 12, records.clone());

    {
        let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
            .await
            .unwrap();
        let initialized = authority
            .authority_store()
            .publisher()
            .initialize_snapshot(initialization.clone())
            .await
            .unwrap();
        assert_eq!(initialized, version(12, &records));
        authority.close().await;
    }

    let reopened = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    let publisher = reopened.authority_store().publisher();
    assert_eq!(
        publisher.initialize_snapshot(initialization).await.unwrap(),
        version(12, &records)
    );
    assert_eq!(
        publisher.published_index(&key).await.unwrap().records,
        records
    );

    let conflict = publisher
        .initialize_snapshot(request(key.clone(), 13, vec![record("different", 9)]))
        .await
        .unwrap_err();
    assert_eq!(conflict.code(), CentralErrorCode::ConcurrentUpdate);
    assert_eq!(
        publisher.current_version(&key).await.unwrap(),
        version(12, &[record("a", 1), record("nested/b", 2)])
    );
    reopened.close().await;
}
