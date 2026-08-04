use neoengram_core::{
    ChunkingStrategy, ContentDigest, FileRecord, IndexVersion, LogicalPath, Manifest, ManifestId,
};
use neoengram_protocol::{
    ArtifactId, DecimalU64, Extensions, IndexDeltaRecord, JobId, PlaygroundId, ProjectId, TenantId,
};
use neoengramd::{
    InMemoryIndexPublisher, IndexKey, IndexPublishOutcome, IndexPublishRejection,
    IndexPublishRequest, IndexPublisher, JobKey,
};

fn index_key() -> IndexKey {
    IndexKey {
        tenant_id: TenantId::new("tenant-a").expect("valid tenant ID"),
        project_id: ProjectId::new("project-a").expect("valid project ID"),
        artifact_id: ArtifactId::new("artifact-a").expect("valid artifact ID"),
        playground_id: PlaygroundId::new("playground-a").expect("valid playground ID"),
    }
}

fn job_key(job_id: &str) -> JobKey {
    JobKey::new(
        TenantId::new("tenant-a").expect("valid tenant ID"),
        JobId::new(job_id).expect("valid job ID"),
    )
}

fn manifest_id() -> ManifestId {
    manifest().canonical_id().expect("canonical Manifest ID")
}

fn manifest() -> Manifest {
    Manifest::new(0, ChunkingStrategy::FastCdc, Vec::new()).expect("valid empty Manifest")
}

fn file_record(path: &str) -> FileRecord {
    FileRecord::new(
        LogicalPath::parse(path).expect("valid logical path"),
        manifest_id(),
        0,
        0,
    )
    .expect("valid file record")
}

fn upsert(path: &str) -> IndexDeltaRecord {
    IndexDeltaRecord::Upsert {
        path: LogicalPath::parse(path).expect("valid logical path"),
        manifest_id: manifest_id(),
        total_size: DecimalU64::new(0),
        chunk_count: DecimalU64::new(0),
        extensions: Extensions::new(),
    }
}

fn delete(path: &str) -> IndexDeltaRecord {
    IndexDeltaRecord::Delete {
        path: LogicalPath::parse(path).expect("valid logical path"),
        extensions: Extensions::new(),
    }
}

fn result_digest(records: &[FileRecord]) -> ContentDigest {
    IndexVersion::from_snapshot(0, records)
        .expect("valid result snapshot")
        .digest
}

#[tokio::test]
async fn publisher_applies_file_directory_transitions_in_canonical_path_order() {
    let publisher = InMemoryIndexPublisher::default();
    let key = index_key();
    let file_version = publisher
        .seed(key.clone(), 3, vec![file_record("a")])
        .expect("seed file snapshot");

    let expanded = publisher
        .compare_and_swap(IndexPublishRequest {
            job_key: job_key("job-expand"),
            index_key: key.clone(),
            expected_index_version: file_version,
            expected_result_digest: result_digest(&[file_record("a/b")]),
            manifests: vec![manifest()],
            mutations: vec![delete("a"), upsert("a/b")],
        })
        .await
        .expect("publish file-to-directory transition");
    let IndexPublishOutcome::Published(directory_version) = expanded else {
        panic!("seeded version must satisfy expansion CAS");
    };
    let directory_snapshot = publisher.snapshot(&key).expect("read expanded snapshot");
    assert_eq!(directory_snapshot.records, vec![file_record("a/b")]);
    assert_eq!(
        publisher
            .manifest(&key.tenant_id, &key.artifact_id, manifest_id())
            .expect("read published Manifest"),
        Some(manifest())
    );

    let collapsed = publisher
        .compare_and_swap(IndexPublishRequest {
            job_key: job_key("job-collapse"),
            index_key: key.clone(),
            expected_index_version: directory_version,
            expected_result_digest: result_digest(&[file_record("a")]),
            manifests: vec![manifest()],
            mutations: vec![upsert("a"), delete("a/b")],
        })
        .await
        .expect("publish directory-to-file transition");
    assert!(matches!(collapsed, IndexPublishOutcome::Published(_)));
    let file_snapshot = publisher.snapshot(&key).expect("read collapsed snapshot");
    assert_eq!(file_snapshot.records, vec![file_record("a")]);
}

#[tokio::test]
async fn publisher_rejects_a_prefix_conflict_in_the_resulting_snapshot_atomically() {
    let publisher = InMemoryIndexPublisher::default();
    let key = index_key();
    let initial_version = publisher
        .current_version(&key)
        .await
        .expect("read initial Index version");

    let first = publisher
        .compare_and_swap(IndexPublishRequest {
            job_key: job_key("job-invalid-final-snapshot"),
            index_key: key.clone(),
            expected_index_version: initial_version.clone(),
            expected_result_digest: ContentDigest::from_bytes([0; 32]),
            manifests: vec![manifest()],
            mutations: vec![upsert("a"), upsert("a/b")],
        })
        .await
        .expect("invalid final snapshot is a stable publication outcome");
    assert!(matches!(
        first,
        IndexPublishOutcome::Rejected(IndexPublishRejection::InvalidMetadata { .. })
    ));

    let snapshot = publisher.snapshot(&key).expect("read unchanged snapshot");
    assert_eq!(snapshot.version, initial_version);
    assert!(snapshot.records.is_empty());
    assert!(publisher
        .manifest(&key.tenant_id, &key.artifact_id, manifest_id())
        .expect("read immutable catalog")
        .is_none());
}

#[tokio::test]
async fn publisher_rejects_an_unexpected_result_digest_atomically() {
    let publisher = InMemoryIndexPublisher::default();
    let key = index_key();
    let initial_version = publisher
        .current_version(&key)
        .await
        .expect("read initial Index version");
    let request = IndexPublishRequest {
        job_key: job_key("job-wrong-result-digest"),
        index_key: key.clone(),
        expected_index_version: initial_version.clone(),
        expected_result_digest: ContentDigest::from_bytes([99; 32]),
        manifests: vec![manifest()],
        mutations: vec![upsert("a")],
    };

    let first = publisher
        .compare_and_swap(request.clone())
        .await
        .expect("result mismatch is a stable publication outcome");
    let replay = publisher
        .compare_and_swap(request)
        .await
        .expect("result mismatch replays idempotently");

    assert!(matches!(
        first,
        IndexPublishOutcome::Rejected(IndexPublishRejection::ResultDigestMismatch { .. })
    ));
    assert_eq!(replay, first);
    let snapshot = publisher.snapshot(&key).expect("read unchanged snapshot");
    assert_eq!(snapshot.version, initial_version);
    assert!(snapshot.records.is_empty());
    assert!(publisher
        .manifest(&key.tenant_id, &key.artifact_id, manifest_id())
        .expect("read immutable catalog")
        .is_none());
}

#[tokio::test]
async fn publisher_rejects_an_open_manifest_set_atomically() {
    let publisher = InMemoryIndexPublisher::default();
    let key = index_key();
    let initial_version = publisher
        .current_version(&key)
        .await
        .expect("read initial Index version");
    let request = IndexPublishRequest {
        job_key: job_key("job-open-manifest-set"),
        index_key: key.clone(),
        expected_index_version: initial_version.clone(),
        expected_result_digest: result_digest(&[file_record("a")]),
        manifests: Vec::new(),
        mutations: vec![upsert("a")],
    };

    let first = publisher
        .compare_and_swap(request.clone())
        .await
        .expect("open publication is a stable rejection");
    let replay = publisher
        .compare_and_swap(request)
        .await
        .expect("open publication rejection replays");

    assert!(matches!(
        first,
        IndexPublishOutcome::Rejected(IndexPublishRejection::InvalidMetadata { .. })
    ));
    assert_eq!(replay, first);
    let snapshot = publisher.snapshot(&key).expect("read unchanged snapshot");
    assert_eq!(snapshot.version, initial_version);
    assert!(snapshot.records.is_empty());
    assert!(publisher
        .manifest(&key.tenant_id, &key.artifact_id, manifest_id())
        .expect("read immutable catalog")
        .is_none());
}

#[tokio::test]
async fn publisher_rejects_index_metadata_that_disagrees_with_its_manifest() {
    let publisher = InMemoryIndexPublisher::default();
    let key = index_key();
    let initial_version = publisher
        .current_version(&key)
        .await
        .expect("read initial Index version");
    let inconsistent_record =
        FileRecord::new(LogicalPath::parse("a").unwrap(), manifest_id(), 1, 1).unwrap();
    let request = IndexPublishRequest {
        job_key: job_key("job-inconsistent-manifest-metadata"),
        index_key: key.clone(),
        expected_index_version: initial_version.clone(),
        expected_result_digest: result_digest(&[inconsistent_record]),
        manifests: vec![manifest()],
        mutations: vec![IndexDeltaRecord::Upsert {
            path: LogicalPath::parse("a").unwrap(),
            manifest_id: manifest_id(),
            total_size: DecimalU64::new(1),
            chunk_count: DecimalU64::new(1),
            extensions: Extensions::new(),
        }],
    };

    let outcome = publisher
        .compare_and_swap(request)
        .await
        .expect("inconsistent metadata is a stable rejection");

    assert!(matches!(
        outcome,
        IndexPublishOutcome::Rejected(IndexPublishRejection::InvalidMetadata { .. })
    ));
    let snapshot = publisher.snapshot(&key).expect("read unchanged snapshot");
    assert_eq!(snapshot.version, initial_version);
    assert!(snapshot.records.is_empty());
    assert!(publisher
        .manifest(&key.tenant_id, &key.artifact_id, manifest_id())
        .expect("read immutable catalog")
        .is_none());
}

#[tokio::test]
async fn publisher_replays_revision_exhaustion_without_mutating_the_index() {
    let publisher = InMemoryIndexPublisher::default();
    let key = index_key();
    let exhausted_version = publisher
        .seed(key.clone(), u64::MAX, Vec::new())
        .expect("seed exhausted Index revision");
    let request = IndexPublishRequest {
        job_key: job_key("job-revision-exhausted"),
        index_key: key.clone(),
        expected_index_version: exhausted_version.clone(),
        expected_result_digest: result_digest(&[file_record("a")]),
        manifests: vec![manifest()],
        mutations: vec![upsert("a")],
    };

    let first = publisher
        .compare_and_swap(request.clone())
        .await
        .expect("revision exhaustion is a stable publication outcome");
    let replay = publisher
        .compare_and_swap(request)
        .await
        .expect("revision exhaustion replays idempotently");

    assert_eq!(
        first,
        IndexPublishOutcome::Rejected(IndexPublishRejection::RevisionExhausted)
    );
    assert_eq!(replay, first);
    let snapshot = publisher.snapshot(&key).expect("read unchanged snapshot");
    assert_eq!(snapshot.version, exhausted_version);
    assert!(snapshot.records.is_empty());
}
