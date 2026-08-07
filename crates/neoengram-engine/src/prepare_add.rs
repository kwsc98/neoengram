use std::{
    collections::{BTreeMap, BTreeSet},
    io::Cursor,
};

use fastcdc::v2020::StreamCDC;
use neoengram_core::{
    canonical, ChunkRef, ChunkingStrategy, FileRecord, IndexDelta, IndexMutation, LogicalPath,
    Manifest, ManifestId, ObjectId, ObjectSpec,
};

use crate::{
    AddStatistics, ChunkingSelection, Clock, EngineError, EngineResult, ErrorCode, FailureInjector,
    FailurePoint, IndexSnapshotReader, ObjectPutOutcome, ObjectStore, PageRequest, PathScope,
    PrepareAddRequest, PreparedAdd, ProgressEvent, ProgressPhase, ProgressSink, ProgressUnit,
    Worktree, WorktreeEntry, WorktreeEntryKind, WorktreeScanRequest, MAX_PAGE_SIZE,
};

const MIN_CHUNK_SIZE: u32 = 256 * 1024;
const AVG_CHUNK_SIZE: u32 = 1024 * 1024;
const MAX_CHUNK_SIZE: u32 = 4 * 1024 * 1024;

struct ScannedFile {
    record: FileRecord,
    manifest: Manifest,
}

struct CandidateParts {
    mutations: Vec<IndexMutation>,
    result_digest: neoengram_core::ContentDigest,
    manifests: Vec<Manifest>,
    object_specs: Vec<ObjectSpec>,
}

/// Prepares a bounded, non-authoritative managed Add candidate through runtime-neutral ports.
///
/// Object publication is durable before the candidate is sealed. This use case never publishes an
/// IndexVersion; the caller must submit placement evidence and the returned metadata to the
/// central CAS publisher. The ObjectStore can be Volume-local and is not assumed to be reachable
/// by the central service.
#[allow(clippy::too_many_arguments)]
pub fn prepare_add(
    request: &PrepareAddRequest,
    index: &dyn IndexSnapshotReader,
    worktree: &dyn Worktree,
    objects: &dyn ObjectStore,
    clock: &dyn Clock,
    progress: &dyn ProgressSink,
    failures: &dyn FailureInjector,
) -> EngineResult<PreparedAdd> {
    request.validate()?;
    if index.version() != &request.expected_index_version {
        return Err(index_version_mismatch(
            &request.expected_index_version,
            index.version(),
        ));
    }

    failures.check(FailurePoint::BeforeScan)?;
    progress.emit(&ProgressEvent::new(
        ProgressPhase::Scanning,
        ProgressUnit::Files,
        0,
    ))?;
    let snapshot = read_index_snapshot(index)?;
    let observed = scan_worktree(request, worktree, progress)?;
    failures.check(FailurePoint::AfterScan)?;

    objects.initialize()?;
    objects.validate_layout()?;
    failures.check(FailurePoint::BeforeObjectPublication)?;
    let chunking = match request.chunking {
        ChunkingSelection::ArtifactDefault => ChunkingStrategy::default(),
        ChunkingSelection::Explicit(strategy) => strategy,
    };
    let mut statistics = AddStatistics {
        files_scanned: u64::try_from(observed.len())
            .map_err(|_| EngineError::new(ErrorCode::Internal, "scanned file count exceeds u64"))?,
        ..AddStatistics::default()
    };
    let mut scanned = BTreeMap::new();
    let total_files = statistics.files_scanned;
    for (path, entry) in observed {
        let file = chunk_file(&path, &entry, chunking, worktree, objects, &mut statistics)?;
        scanned.insert(path.clone(), file);
        progress.emit(
            &ProgressEvent::new(
                ProgressPhase::Chunking,
                ProgressUnit::Files,
                u64::try_from(scanned.len()).map_err(|_| {
                    EngineError::new(ErrorCode::Internal, "chunked file count exceeds u64")
                })?,
            )
            .with_total(total_files)
            .at_path(path),
        )?;
    }
    objects.durability_barrier()?;
    failures.check(FailurePoint::AfterObjectPublication)?;

    let candidate = build_candidate(request, &snapshot, &scanned, &mut statistics)?;
    failures.check(FailurePoint::BeforeCandidateSeal)?;
    let mutation_count = u64::try_from(candidate.mutations.len())
        .map_err(|_| EngineError::new(ErrorCode::Internal, "mutation count exceeds u64"))?;
    let index_delta = IndexDelta::new(
        request.expected_index_version,
        candidate.mutations,
        candidate.result_digest,
    )?;
    let prepared = PreparedAdd::new(
        request.resource.clone(),
        request.expected_index_version,
        request.candidate_batch_id.clone(),
        index_delta,
        candidate.manifests,
        candidate.object_specs,
        statistics,
        clock.now_unix_ms()?,
    )?;
    prepared.validate_for(request)?;
    progress.emit(&ProgressEvent::new(
        ProgressPhase::Prepared,
        ProgressUnit::Actions,
        mutation_count,
    ))?;
    Ok(prepared)
}

fn read_index_snapshot(
    index: &dyn IndexSnapshotReader,
) -> EngineResult<BTreeMap<LogicalPath, FileRecord>> {
    let mut records = BTreeMap::new();
    let mut request = PageRequest::first(MAX_PAGE_SIZE)?;
    let mut cursors = BTreeSet::new();
    loop {
        let page = index.scan_files(None, &request)?;
        if page.items.len() > request.limit as usize {
            return Err(EngineError::new(
                ErrorCode::IntegrityViolation,
                "Index reader returned more records than requested",
            ));
        }
        for record in page.items {
            record.validate()?;
            if records.insert(record.path.clone(), record).is_some() {
                return Err(EngineError::new(
                    ErrorCode::IntegrityViolation,
                    "Index reader returned a duplicate logical path",
                ));
            }
        }
        let Some(next) = page.next else {
            break;
        };
        if !cursors.insert(next.as_str().to_owned()) {
            return Err(EngineError::new(
                ErrorCode::IntegrityViolation,
                "Index reader returned a repeated pagination cursor",
            ));
        }
        request = PageRequest::new(Some(next), MAX_PAGE_SIZE)?;
    }

    let ordered = records.values().cloned().collect::<Vec<_>>();
    let observed_digest = canonical::index_snapshot_digest(&ordered)?;
    if observed_digest != index.version().digest {
        return Err(EngineError::new(
            ErrorCode::IntegrityViolation,
            "Index snapshot content does not match its declared version",
        )
        .with_context("declared_digest", index.version().digest.to_string())
        .with_context("observed_digest", observed_digest.to_string()));
    }
    Ok(records)
}

fn scan_worktree(
    request: &PrepareAddRequest,
    worktree: &dyn Worktree,
    progress: &dyn ProgressSink,
) -> EngineResult<BTreeMap<LogicalPath, WorktreeEntry>> {
    let mut files = BTreeMap::new();
    for scope in &request.scopes {
        if let PathScope::Path(path) = scope {
            match worktree.inspect(path)? {
                None if request.stage_deletions => continue,
                None => {
                    return Err(EngineError::new(
                        ErrorCode::ResourceNotFound,
                        "managed Add scope is missing from the worktree",
                    )
                    .with_context("logical_path", path.to_string()));
                }
                Some(_) => {}
            }
        }
        let scan_request = WorktreeScanRequest {
            scopes: vec![scope.clone()],
        };
        worktree.scan_files(&scan_request, &mut |entry| {
            if entry.kind != WorktreeEntryKind::File || entry.fingerprint.is_none() {
                return Err(EngineError::new(
                    ErrorCode::IntegrityViolation,
                    "Worktree scan returned a non-file or unfingerprinted entry",
                )
                .with_context("logical_path", entry.path.to_string()));
            }
            if !scope_contains(scope, &entry.path) {
                return Err(EngineError::new(
                    ErrorCode::IntegrityViolation,
                    "Worktree scan returned a file outside its requested scope",
                )
                .with_context("logical_path", entry.path.to_string()));
            }
            if let Some(previous) = files.get(&entry.path) {
                if previous != &entry {
                    return Err(EngineError::new(
                        ErrorCode::WorktreeChangedDuringScan,
                        "overlapping Worktree scans observed different file fingerprints",
                    )
                    .with_context("logical_path", entry.path.to_string()));
                }
                return Ok(());
            }
            let path = entry.path.clone();
            files.insert(path.clone(), entry);
            progress.emit(
                &ProgressEvent::new(
                    ProgressPhase::Scanning,
                    ProgressUnit::Files,
                    u64::try_from(files.len()).map_err(|_| {
                        EngineError::new(ErrorCode::Internal, "scanned file count exceeds u64")
                    })?,
                )
                .at_path(path),
            )
        })?;
    }
    Ok(files)
}

fn chunk_file(
    path: &LogicalPath,
    initial_entry: &WorktreeEntry,
    chunking: ChunkingStrategy,
    worktree: &dyn Worktree,
    objects: &dyn ObjectStore,
    statistics: &mut AddStatistics,
) -> EngineResult<ScannedFile> {
    let expected_size = initial_entry
        .fingerprint
        .as_ref()
        .ok_or_else(|| {
            EngineError::new(
                ErrorCode::IntegrityViolation,
                "Worktree file is missing its scan fingerprint",
            )
        })?
        .size;
    let chunks = match chunking {
        ChunkingStrategy::FastCdc => {
            chunk_fast_cdc(path, expected_size, worktree, objects, statistics)?
        }
        ChunkingStrategy::WholeFile => {
            chunk_whole_file(path, expected_size, worktree, objects, statistics)?
        }
    };
    let final_entry = worktree.inspect(path)?.ok_or_else(|| {
        EngineError::new(
            ErrorCode::WorktreeChangedDuringScan,
            "Worktree file disappeared while preparing Add",
        )
        .with_context("logical_path", path.to_string())
    })?;
    if &final_entry != initial_entry {
        return Err(EngineError::new(
            ErrorCode::WorktreeChangedDuringScan,
            "Worktree file changed while preparing Add",
        )
        .with_context("logical_path", path.to_string()));
    }
    let manifest = Manifest::new(expected_size, chunking, chunks)?;
    let record = FileRecord::from_manifest(path.clone(), &manifest)?;
    statistics.bytes_scanned = statistics
        .bytes_scanned
        .checked_add(expected_size)
        .ok_or_else(|| EngineError::new(ErrorCode::Internal, "scanned byte count exceeds u64"))?;
    statistics.chunks_observed = statistics
        .chunks_observed
        .checked_add(manifest.chunk_count()?)
        .ok_or_else(|| EngineError::new(ErrorCode::Internal, "chunk count exceeds u64"))?;
    Ok(ScannedFile { record, manifest })
}

fn chunk_fast_cdc(
    path: &LogicalPath,
    expected_size: u64,
    worktree: &dyn Worktree,
    objects: &dyn ObjectStore,
    statistics: &mut AddStatistics,
) -> EngineResult<Vec<ChunkRef>> {
    let source = worktree.open_file(path)?;
    let mut chunks = Vec::new();
    let mut observed_size = 0_u64;
    for result in StreamCDC::new(source, MIN_CHUNK_SIZE, AVG_CHUNK_SIZE, MAX_CHUNK_SIZE) {
        let chunk = result.map_err(|error| {
            EngineError::new(ErrorCode::Io, "FastCDC failed to read Worktree content")
                .with_source(error)
        })?;
        let size = u64::try_from(chunk.length)
            .map_err(|_| EngineError::new(ErrorCode::Internal, "FastCDC chunk size exceeds u64"))?;
        let object_id = ObjectId::for_bytes(&chunk.data);
        let spec = ObjectSpec::new(object_id, size);
        record_object_outcome(
            objects.put_from(&spec, &mut Cursor::new(chunk.data))?,
            size,
            statistics,
        )?;
        chunks.push(ChunkRef::new(object_id, chunk.offset, size)?);
        observed_size = observed_size.checked_add(size).ok_or_else(|| {
            EngineError::new(ErrorCode::Internal, "FastCDC byte count exceeds u64")
        })?;
    }
    if observed_size != expected_size {
        return Err(EngineError::new(
            ErrorCode::WorktreeChangedDuringScan,
            "FastCDC input size differs from the Worktree fingerprint",
        )
        .with_context("logical_path", path.to_string()));
    }
    Ok(chunks)
}

fn chunk_whole_file(
    path: &LogicalPath,
    expected_size: u64,
    worktree: &dyn Worktree,
    objects: &dyn ObjectStore,
    statistics: &mut AddStatistics,
) -> EngineResult<Vec<ChunkRef>> {
    let mut hash_source = worktree.open_file(path)?;
    let mut hasher = blake3::Hasher::new();
    let observed_size = std::io::copy(&mut hash_source, &mut hasher)?;
    if observed_size != expected_size {
        return Err(EngineError::new(
            ErrorCode::WorktreeChangedDuringScan,
            "WholeFile input size differs from the Worktree fingerprint",
        )
        .with_context("logical_path", path.to_string()));
    }
    if expected_size == 0 {
        return Ok(Vec::new());
    }
    let object_id = ObjectId::from_bytes(*hasher.finalize().as_bytes());
    let spec = ObjectSpec::new(object_id, expected_size);
    let mut payload_source = worktree.open_file(path)?;
    record_object_outcome(
        objects.put_from(&spec, &mut payload_source)?,
        expected_size,
        statistics,
    )?;
    Ok(vec![ChunkRef::new(object_id, 0, expected_size)?])
}

fn record_object_outcome(
    outcome: ObjectPutOutcome,
    size: u64,
    statistics: &mut AddStatistics,
) -> EngineResult<()> {
    match outcome {
        ObjectPutOutcome::Created => {
            statistics.objects_created =
                statistics.objects_created.checked_add(1).ok_or_else(|| {
                    EngineError::new(ErrorCode::Internal, "created object count exceeds u64")
                })?;
            statistics.bytes_published =
                statistics
                    .bytes_published
                    .checked_add(size)
                    .ok_or_else(|| {
                        EngineError::new(ErrorCode::Internal, "published byte count exceeds u64")
                    })?;
        }
        ObjectPutOutcome::AlreadyPresent => {
            statistics.objects_reused =
                statistics.objects_reused.checked_add(1).ok_or_else(|| {
                    EngineError::new(ErrorCode::Internal, "reused object count exceeds u64")
                })?;
        }
    }
    Ok(())
}

fn build_candidate(
    request: &PrepareAddRequest,
    snapshot: &BTreeMap<LogicalPath, FileRecord>,
    scanned: &BTreeMap<LogicalPath, ScannedFile>,
    statistics: &mut AddStatistics,
) -> EngineResult<CandidateParts> {
    let mut result = snapshot.clone();
    let mut mutations = BTreeMap::new();
    let mut referenced_manifests = BTreeSet::new();
    let mut manifests_by_id = BTreeMap::<ManifestId, Manifest>::new();

    for (path, scanned_file) in scanned {
        result.insert(path.clone(), scanned_file.record.clone());
        if snapshot.get(path) != Some(&scanned_file.record) {
            let manifest_id = scanned_file.manifest.canonical_id()?;
            referenced_manifests.insert(manifest_id);
            manifests_by_id
                .entry(manifest_id)
                .or_insert_with(|| scanned_file.manifest.clone());
            mutations.insert(
                path.clone(),
                IndexMutation::Upsert {
                    record: scanned_file.record.clone(),
                },
            );
        }
    }
    if request.stage_deletions {
        for path in snapshot.keys() {
            if request
                .scopes
                .iter()
                .any(|scope| scope_contains(scope, path))
                && !scanned.contains_key(path)
            {
                result.remove(path);
                mutations.insert(path.clone(), IndexMutation::Delete { path: path.clone() });
            }
        }
    }

    let ordered_result = result.values().cloned().collect::<Vec<_>>();
    let result_digest = canonical::index_snapshot_digest(&ordered_result)?;
    let mutations = mutations.into_values().collect::<Vec<_>>();
    statistics.files_upserted = u64::try_from(
        mutations
            .iter()
            .filter(|mutation| matches!(mutation, IndexMutation::Upsert { .. }))
            .count(),
    )
    .map_err(|_| EngineError::new(ErrorCode::Internal, "upsert count exceeds u64"))?;
    statistics.paths_deleted = u64::try_from(
        mutations
            .iter()
            .filter(|mutation| matches!(mutation, IndexMutation::Delete { .. }))
            .count(),
    )
    .map_err(|_| EngineError::new(ErrorCode::Internal, "deletion count exceeds u64"))?;

    let manifests = referenced_manifests
        .into_iter()
        .map(|id| {
            manifests_by_id.remove(&id).ok_or_else(|| {
                EngineError::new(
                    ErrorCode::Internal,
                    "referenced Manifest disappeared while sealing Add candidate",
                )
            })
        })
        .collect::<EngineResult<Vec<_>>>()?;
    let mut object_specs = BTreeMap::<ObjectId, u64>::new();
    for manifest in &manifests {
        for chunk in &manifest.chunks {
            if let Some(previous) = object_specs.insert(chunk.object_id, chunk.size) {
                if previous != chunk.size {
                    return Err(EngineError::new(
                        ErrorCode::IntegrityViolation,
                        "prepared Manifests disagree about an object size",
                    )
                    .with_context("object_id", chunk.object_id.to_string()));
                }
            }
        }
    }
    Ok(CandidateParts {
        mutations,
        result_digest,
        manifests,
        object_specs: object_specs
            .into_iter()
            .map(|(id, size)| ObjectSpec::new(id, size))
            .collect(),
    })
}

fn scope_contains(scope: &PathScope, path: &LogicalPath) -> bool {
    match scope {
        PathScope::Root => true,
        PathScope::Path(prefix) => prefix == path || prefix.is_ancestor_of(path),
    }
}

fn index_version_mismatch(
    expected: &neoengram_core::IndexVersion,
    observed: &neoengram_core::IndexVersion,
) -> EngineError {
    EngineError::new(
        ErrorCode::IndexVersionMismatch,
        "Index snapshot does not match the managed Add base version",
    )
    .with_context("expected_revision", expected.revision.to_string())
    .with_context("expected_digest", expected.digest.to_string())
    .with_context("observed_revision", observed.revision.to_string())
    .with_context("observed_digest", observed.digest.to_string())
}
