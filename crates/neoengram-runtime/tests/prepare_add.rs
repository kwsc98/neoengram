use std::sync::Mutex;

use neoengram_domain::core::{
    canonical, ChunkRef, ChunkingStrategy, FileRecord, IndexMutation, IndexVersion, LogicalPath,
    Manifest, ObjectId,
};
use neoengram_runtime::engine::{
    prepare_add, ChunkingSelection, EngineResult, ErrorCode, ManagedResource, NoopFailureInjector,
    ObjectStore, PathScope, PrepareAddRequest, ProgressEvent, ProgressPhase, ProgressSink,
};
use neoengram_runtime::fs::{FixedClock, MemoryIndexSnapshot, MemoryObjectStore, MemoryWorktree};

#[derive(Debug, Default)]
struct RecordingProgress {
    events: Mutex<Vec<ProgressEvent>>,
}

impl ProgressSink for RecordingProgress {
    fn emit(&self, event: &ProgressEvent) -> EngineResult<()> {
        self.events.lock().unwrap().push(event.clone());
        Ok(())
    }
}

#[test]
fn managed_prepare_add_publishes_objects_and_returns_a_non_authoritative_candidate() {
    let deleted_payload = b"old";
    let deleted_object = ObjectId::for_bytes(deleted_payload);
    let deleted_manifest = Manifest::new(
        deleted_payload.len() as u64,
        ChunkingStrategy::WholeFile,
        vec![ChunkRef::new(deleted_object, 0, deleted_payload.len() as u64).unwrap()],
    )
    .unwrap();
    let deleted_record = FileRecord::from_manifest(
        LogicalPath::parse("deleted.bin").unwrap(),
        &deleted_manifest,
    )
    .unwrap();
    let base_version =
        IndexVersion::from_snapshot(7, std::slice::from_ref(&deleted_record)).unwrap();
    let index = MemoryIndexSnapshot::new(base_version, [deleted_record]);

    let worktree = MemoryWorktree::default();
    let new_path = LogicalPath::parse("new.bin").unwrap();
    let new_payload = b"new managed payload".to_vec();
    worktree
        .insert_file(new_path.clone(), new_payload.clone())
        .unwrap();
    let objects = MemoryObjectStore::default();
    let clock = FixedClock::new(1_234);
    let progress = RecordingProgress::default();
    let request = PrepareAddRequest {
        resource: ManagedResource {
            tenant_id: "tenant-a".to_owned(),
            project_id: "project-a".to_owned(),
            artifact_id: "artifact-a".to_owned(),
            playground_id: "playground-a".to_owned(),
            job_id: "job-a".to_owned(),
        },
        expected_index_version: base_version,
        scopes: vec![PathScope::Root],
        stage_deletions: true,
        chunking: ChunkingSelection::Explicit(ChunkingStrategy::WholeFile),
        max_whole_file_bytes: None,
        candidate_batch_id: "batch-a".to_owned(),
    };

    let prepared = prepare_add(
        &request,
        &index,
        &worktree,
        &objects,
        &clock,
        &progress,
        &NoopFailureInjector,
    )
    .unwrap();

    prepared.validate_for(&request).unwrap();
    assert_eq!(prepared.prepared_at_unix_ms, 1_234);
    assert_eq!(prepared.statistics.files_scanned, 1);
    assert_eq!(prepared.statistics.files_upserted, 1);
    assert_eq!(prepared.statistics.paths_deleted, 1);
    assert_eq!(prepared.statistics.objects_created, 1);
    assert_eq!(prepared.index_delta.mutation_count(), 2);
    assert_eq!(prepared.index_delta.page_count(), 1);
    let mutations = prepared.index_delta.iter().collect::<Vec<_>>();
    assert!(matches!(
        mutations[0],
        IndexMutation::Delete { path } if path.as_str() == "deleted.bin"
    ));
    let upserted = match mutations[1] {
        IndexMutation::Upsert { record } => record.clone(),
        other => panic!("expected an upsert, observed {other:?}"),
    };
    assert_eq!(upserted.path, new_path);
    assert_eq!(prepared.manifests.len(), 1);
    assert_eq!(prepared.object_specs.len(), 1);
    assert_eq!(
        prepared.index_delta.result_digest,
        canonical::index_snapshot_digest(std::slice::from_ref(&upserted)).unwrap()
    );
    assert_eq!(
        objects
            .stat(&prepared.object_specs[0].id)
            .unwrap()
            .unwrap()
            .size,
        new_payload.len() as u64
    );
    assert_eq!(
        prepared.candidate_digest(),
        prepared.canonical_digest().unwrap()
    );
    assert!(progress
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|event| event.phase == ProgressPhase::Prepared));
}

#[test]
fn managed_prepare_add_rejects_a_stale_index_before_publishing_objects() {
    let actual = IndexVersion::from_snapshot(2, &[]).unwrap();
    let expected = IndexVersion::from_snapshot(1, &[]).unwrap();
    let index = MemoryIndexSnapshot::new(actual, []);
    let worktree = MemoryWorktree::default();
    worktree
        .insert_file(LogicalPath::parse("file.bin").unwrap(), b"payload".to_vec())
        .unwrap();
    let objects = MemoryObjectStore::default();
    let request = PrepareAddRequest {
        resource: ManagedResource {
            tenant_id: "tenant-a".to_owned(),
            project_id: "project-a".to_owned(),
            artifact_id: "artifact-a".to_owned(),
            playground_id: "playground-a".to_owned(),
            job_id: "job-a".to_owned(),
        },
        expected_index_version: expected,
        scopes: vec![PathScope::Root],
        stage_deletions: false,
        chunking: ChunkingSelection::ArtifactDefault,
        max_whole_file_bytes: None,
        candidate_batch_id: "batch-a".to_owned(),
    };

    let error = prepare_add(
        &request,
        &index,
        &worktree,
        &objects,
        &FixedClock::new(1),
        &RecordingProgress::default(),
        &NoopFailureInjector,
    )
    .unwrap_err();
    assert_eq!(
        error.code(),
        neoengram_runtime::engine::ErrorCode::IndexVersionMismatch
    );
    assert!(objects
        .stat(&ObjectId::for_bytes(b"payload"))
        .unwrap()
        .is_none());
}

#[test]
fn whole_file_limit_is_enforced_before_object_publication() {
    let version = IndexVersion::from_snapshot(1, &[]).unwrap();
    let index = MemoryIndexSnapshot::new(version, []);
    let worktree = MemoryWorktree::default();
    let path = LogicalPath::parse("oversized.bin").unwrap();
    let payload = b"oversized whole file".to_vec();
    worktree.insert_file(path, payload.clone()).unwrap();
    let objects = MemoryObjectStore::default();
    let request = PrepareAddRequest {
        resource: ManagedResource {
            tenant_id: "tenant-a".to_owned(),
            project_id: "project-a".to_owned(),
            artifact_id: "artifact-a".to_owned(),
            playground_id: "playground-a".to_owned(),
            job_id: "job-whole-file-limit".to_owned(),
        },
        expected_index_version: version,
        scopes: vec![PathScope::Root],
        stage_deletions: true,
        chunking: ChunkingSelection::Explicit(ChunkingStrategy::WholeFile),
        max_whole_file_bytes: Some(4),
        candidate_batch_id: "batch-whole-file-limit".to_owned(),
    };

    let error = prepare_add(
        &request,
        &index,
        &worktree,
        &objects,
        &FixedClock::new(1_234),
        &RecordingProgress::default(),
        &NoopFailureInjector,
    )
    .unwrap_err();

    assert_eq!(error.code(), ErrorCode::InvalidArgument);
    assert!(objects
        .stat(&ObjectId::for_bytes(&payload))
        .unwrap()
        .is_none());
}

#[test]
fn changing_commit_layout_reencodes_every_file_in_both_directions() {
    let files = [
        ("first.bin", b"first payload".as_slice()),
        ("nested/second.bin", b"second payload".as_slice()),
        ("empty.bin", b"".as_slice()),
    ];

    for (parent_layout, target_layout) in [
        (ChunkingStrategy::FastCdc, ChunkingStrategy::WholeFile),
        (ChunkingStrategy::WholeFile, ChunkingStrategy::FastCdc),
    ] {
        let mut parent_records = Vec::new();
        let worktree = MemoryWorktree::default();
        for (path, payload) in files {
            let chunks = if payload.is_empty() {
                Vec::new()
            } else {
                vec![ChunkRef::new(ObjectId::for_bytes(payload), 0, payload.len() as u64).unwrap()]
            };
            let manifest = Manifest::new(payload.len() as u64, parent_layout, chunks).unwrap();
            let logical_path = LogicalPath::parse(path).unwrap();
            parent_records
                .push(FileRecord::from_manifest(logical_path.clone(), &manifest).unwrap());
            worktree
                .insert_file(logical_path, payload.to_vec())
                .unwrap();
        }
        parent_records.sort_by(|left, right| left.path.cmp(&right.path));

        let base_version = IndexVersion::from_snapshot(7, &parent_records).unwrap();
        let index = MemoryIndexSnapshot::new(base_version, parent_records);
        let objects = MemoryObjectStore::default();
        let request = PrepareAddRequest {
            resource: ManagedResource {
                tenant_id: "tenant-a".to_owned(),
                project_id: "project-a".to_owned(),
                artifact_id: "artifact-a".to_owned(),
                playground_id: "playground-a".to_owned(),
                job_id: format!("job-{target_layout:?}"),
            },
            expected_index_version: base_version,
            scopes: vec![PathScope::Root],
            stage_deletions: true,
            chunking: ChunkingSelection::Explicit(target_layout),
            max_whole_file_bytes: None,
            candidate_batch_id: format!("batch-{target_layout:?}"),
        };

        let prepared = prepare_add(
            &request,
            &index,
            &worktree,
            &objects,
            &FixedClock::new(1),
            &RecordingProgress::default(),
            &NoopFailureInjector,
        )
        .unwrap();

        assert_eq!(prepared.statistics.files_scanned, files.len() as u64);
        assert_eq!(prepared.statistics.files_upserted, files.len() as u64);
        assert_eq!(prepared.index_delta.mutation_count(), files.len());
        assert!(prepared
            .index_delta
            .iter()
            .all(|mutation| matches!(mutation, IndexMutation::Upsert { .. })));
        assert_eq!(prepared.manifests.len(), files.len());
        assert!(prepared
            .manifests
            .iter()
            .all(|manifest| manifest.chunking == target_layout));
        if target_layout == ChunkingStrategy::WholeFile {
            assert!(prepared.manifests.iter().all(|manifest| {
                (manifest.total_size == 0 && manifest.chunks.is_empty())
                    || (manifest.total_size > 0
                        && manifest.chunks.len() == 1
                        && manifest.chunks[0].offset == 0
                        && manifest.chunks[0].size == manifest.total_size)
            }));
        }
    }
}
