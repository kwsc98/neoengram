use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};

use neoengram_agent::{
    AddExecutor, AgentError, AgentErrorCode, AgentResult, ObjectTransfer, TransferReceipt,
};
use neoengram_core::{
    canonical, ChunkingStrategy, FileRecord, IndexMutation, IndexVersion, LogicalPath, ObjectId,
    ObjectSpec,
};
use neoengram_engine::{
    prepare_add, ChunkingSelection, EngineError, EngineResult, IndexSnapshotReader,
    ManagedResource, NoopFailureInjector, NoopProgressSink, ObjectStore, Page, PageCursor,
    PageRequest, PathScope, PrepareAddRequest, PreparedAdd,
};
use neoengram_fs::{LocalWorktree, LooseObjectStore, SystemClock, VerifiedRoot};
use neoengram_protocol::{
    AddAssignment, DecimalU64, Extensions, IndexDeltaRecord, JobDecision, ManifestRecord,
    MetadataBatchDescriptor, MetadataBatchId, MetadataBatchPage, MetadataBatchRecords,
    MetadataBatchScope, ObjectReceiptId, ObjectReceiptRecord, UnixMillis, WireChunkRef,
    WireChunkingStrategy, MAX_RECORDS_PER_PAGE,
};

/// FastCDC's configured maximum chunk size and the development Agent API's inline object limit.
pub const MAX_AGENT_OBJECT_BYTES: u64 = 4 * 1024 * 1024;

/// A complete, content-verified authoritative Index downloaded before synchronous execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoritativeIndexSnapshot {
    version: IndexVersion,
    records: Arc<Vec<FileRecord>>,
}

impl AuthoritativeIndexSnapshot {
    pub fn new(version: IndexVersion, records: Vec<FileRecord>) -> AgentResult<Self> {
        let mut previous: Option<&LogicalPath> = None;
        for record in &records {
            record
                .validate()
                .map_err(|error| execution_error(error.to_string()))?;
            if previous.is_some_and(|path| path >= &record.path) {
                return Err(execution_error(
                    "authoritative Index records are not strictly path ordered",
                ));
            }
            previous = Some(&record.path);
        }
        let observed = canonical::index_snapshot_digest(&records)
            .map_err(|error| execution_error(error.to_string()))?;
        if observed != version.digest {
            return Err(execution_error(format!(
                "authoritative Index digest mismatch: declared {}, observed {observed}",
                version.digest
            )));
        }
        Ok(Self {
            version,
            records: Arc::new(records),
        })
    }

    #[must_use]
    pub const fn version(&self) -> &IndexVersion {
        &self.version
    }

    #[must_use]
    pub fn records(&self) -> &[FileRecord] {
        self.records.as_slice()
    }
}

impl IndexSnapshotReader for AuthoritativeIndexSnapshot {
    fn version(&self) -> &IndexVersion {
        &self.version
    }

    fn get_file(&self, path: &LogicalPath) -> EngineResult<Option<FileRecord>> {
        Ok(self
            .records
            .binary_search_by(|record| record.path.cmp(path))
            .ok()
            .map(|index| self.records[index].clone()))
    }

    fn scan_files(
        &self,
        prefix: Option<&LogicalPath>,
        request: &PageRequest,
    ) -> EngineResult<Page<FileRecord>> {
        request.validate()?;
        let after = request.after.as_ref().map(PageCursor::as_str);
        let mut matching = self.records.iter().filter(|record| {
            after.is_none_or(|cursor| record.path.as_str() > cursor)
                && prefix.is_none_or(|prefix| logical_prefix_contains(prefix, &record.path))
        });
        let items = matching
            .by_ref()
            .take(request.limit as usize)
            .cloned()
            .collect::<Vec<_>>();
        let has_more = matching.next().is_some();
        let next = if has_more {
            let last = items.last().ok_or_else(|| {
                EngineError::new(
                    neoengram_engine::ErrorCode::IntegrityViolation,
                    "non-empty Index continuation has no cursor record",
                )
            })?;
            Some(PageCursor::new(last.path.as_str())?)
        } else {
            None
        };
        Ok(Page { items, next })
    }
}

/// Result of central missing-object negotiation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MissingObjectSet {
    pub missing: Vec<ObjectSpec>,
}

/// Storage-side evidence returned after one object is durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectUploadEvidence {
    pub object: ObjectSpec,
    pub verified_at_unix_ms: u64,
}

/// Synchronous boundary between the Agent state machine and an async session transport.
///
/// A session runtime can prefetch Index pages and serve `authoritative_index` from a cache, then
/// implement the remaining calls with a dedicated blocking worker/channel. No physical path is
/// ever supplied by this bridge.
pub trait ExecutionBridge: std::fmt::Debug + Send + Sync {
    fn authoritative_index(
        &self,
        assignment: &AddAssignment,
    ) -> AgentResult<AuthoritativeIndexSnapshot>;

    fn query_missing_objects(
        &self,
        assignment: &AddAssignment,
        objects: &[ObjectSpec],
    ) -> AgentResult<MissingObjectSet>;

    fn upload_object(
        &self,
        assignment: &AddAssignment,
        object: ObjectSpec,
        content: &[u8],
    ) -> AgentResult<ObjectUploadEvidence>;

    fn stage_metadata_descriptor(
        &self,
        assignment: &AddAssignment,
        descriptor: &MetadataBatchDescriptor,
    ) -> AgentResult<()>;

    fn stage_metadata_page(
        &self,
        assignment: &AddAssignment,
        page: &MetadataBatchPage,
    ) -> AgentResult<()>;

    fn now_unix_ms(&self) -> AgentResult<u64>;
}

/// Real managed-Add adapter over one configured mount and a node-local loose object cache.
#[derive(Debug, Clone)]
pub struct FilesystemExecution {
    mount_root: PathBuf,
    object_cache_root: PathBuf,
    bridge: Arc<dyn ExecutionBridge>,
}

impl FilesystemExecution {
    #[must_use]
    pub fn new(
        mount_root: impl Into<PathBuf>,
        object_cache_root: impl Into<PathBuf>,
        bridge: Arc<dyn ExecutionBridge>,
    ) -> Self {
        Self {
            mount_root: mount_root.into(),
            object_cache_root: object_cache_root.into(),
            bridge,
        }
    }

    fn worktree(&self, assignment: &AddAssignment) -> AgentResult<LocalWorktree> {
        let mount = VerifiedRoot::open(&self.mount_root).map_err(AgentError::from)?;
        let relative = LogicalPath::parse(format!(
            "playgrounds/{}/{}/{}",
            assignment.project_id, assignment.artifact_id, assignment.playground_id
        ))
        .map_err(|error| execution_error(error.to_string()))?;
        let physical = mount.resolve_existing(&relative).map_err(|error| {
            AgentError::new(
                AgentErrorCode::MountUnavailable,
                format!("managed playground directory is unavailable: {error}"),
            )
        })?;
        let root = VerifiedRoot::open(physical).map_err(AgentError::from)?;
        Ok(LocalWorktree::new(root))
    }

    fn objects(&self, assignment: &AddAssignment) -> AgentResult<LooseObjectStore> {
        let path = self
            .object_cache_root
            .join("tenants")
            .join(assignment.tenant_id.as_str())
            .join("artifacts")
            .join(assignment.artifact_id.as_str())
            .join("objects");
        LooseObjectStore::open_or_create(path).map_err(AgentError::from)
    }
}

impl AddExecutor for FilesystemExecution {
    fn prepare_add(&self, assignment: &AddAssignment) -> AgentResult<PreparedAdd> {
        let index = self.bridge.authoritative_index(assignment)?;
        let worktree = self.worktree(assignment)?;
        let objects = self.objects(assignment)?;
        let request = PrepareAddRequest {
            resource: ManagedResource {
                tenant_id: assignment.tenant_id.to_string(),
                project_id: assignment.project_id.to_string(),
                artifact_id: assignment.artifact_id.to_string(),
                playground_id: assignment.playground_id.to_string(),
                job_id: assignment.job_id.to_string(),
            },
            expected_index_version: assignment.expected_index_version.clone().into(),
            scopes: if assignment.all {
                vec![PathScope::Root]
            } else {
                assignment
                    .paths
                    .iter()
                    .cloned()
                    .map(PathScope::Path)
                    .collect()
            },
            stage_deletions: true,
            chunking: ChunkingSelection::Explicit(ChunkingStrategy::FastCdc),
            candidate_batch_id: format!("candidate-{}", assignment.request_digest),
        };
        prepare_add(
            &request,
            &index,
            &worktree,
            &objects,
            &SystemClock,
            &NoopProgressSink,
            &NoopFailureInjector,
        )
        .map_err(AgentError::from)
    }
}

impl ObjectTransfer for FilesystemExecution {
    fn transfer(
        &self,
        assignment: &AddAssignment,
        prepared: &PreparedAdd,
    ) -> AgentResult<TransferReceipt> {
        prepared.validate().map_err(AgentError::from)?;
        let objects = self.objects(assignment)?;
        let negotiated = self
            .bridge
            .query_missing_objects(assignment, &prepared.object_specs)?;
        let missing = validate_missing_set(&prepared.object_specs, negotiated)?;
        let checked_at = self.bridge.now_unix_ms()?;
        let mut verified_at = BTreeMap::<ObjectId, u64>::new();

        for spec in &prepared.object_specs {
            if missing.contains(&spec.id) {
                if spec.size > MAX_AGENT_OBJECT_BYTES {
                    return Err(transfer_error(format!(
                        "object {} is {} bytes, exceeding the {} byte Agent limit",
                        spec.id, spec.size, MAX_AGENT_OBJECT_BYTES
                    )));
                }
                let capacity = usize::try_from(spec.size)
                    .map_err(|_| transfer_error("object size does not fit memory"))?;
                let mut content = Vec::with_capacity(capacity);
                objects
                    .copy_to(spec, &mut content)
                    .map_err(|error| transfer_error(error.to_string()))?;
                let evidence = self.bridge.upload_object(assignment, *spec, &content)?;
                if evidence.object != *spec {
                    return Err(transfer_error(format!(
                        "upload evidence for object {} differs from requested object",
                        spec.id
                    )));
                }
                verified_at.insert(spec.id, evidence.verified_at_unix_ms);
            } else {
                verified_at.insert(spec.id, checked_at);
            }
        }

        let receipt = metadata_receipt(assignment, prepared, &verified_at)?;
        receipt.validate_for(assignment, prepared)?;
        Ok(receipt)
    }

    fn stage_metadata(
        &self,
        assignment: &AddAssignment,
        receipt: &TransferReceipt,
    ) -> AgentResult<()> {
        receipt.validate(assignment)?;
        for descriptor in &receipt.metadata_batches {
            self.bridge
                .stage_metadata_descriptor(assignment, descriptor)?;
        }
        for page in &receipt.metadata_pages {
            self.bridge.stage_metadata_page(assignment, page)?;
        }
        Ok(())
    }

    fn finalize(
        &self,
        _assignment: &AddAssignment,
        _prepared: &PreparedAdd,
        _decision: &JobDecision,
    ) -> AgentResult<()> {
        // Loose objects are a reusable local cache; central publication requires no local commit.
        Ok(())
    }
}

fn validate_missing_set(
    expected: &[ObjectSpec],
    negotiated: MissingObjectSet,
) -> AgentResult<BTreeSet<ObjectId>> {
    let expected = expected
        .iter()
        .map(|spec| (spec.id, spec.size))
        .collect::<BTreeMap<_, _>>();
    let mut missing = BTreeSet::new();
    for spec in negotiated.missing {
        if expected.get(&spec.id) != Some(&spec.size) {
            return Err(transfer_error(format!(
                "missing-object response contains unknown or conflicting object {}",
                spec.id
            )));
        }
        if !missing.insert(spec.id) {
            return Err(transfer_error(format!(
                "missing-object response repeats object {}",
                spec.id
            )));
        }
    }
    Ok(missing)
}

fn metadata_receipt(
    assignment: &AddAssignment,
    prepared: &PreparedAdd,
    verified_at: &BTreeMap<ObjectId, u64>,
) -> AgentResult<TransferReceipt> {
    let scope = MetadataBatchScope {
        tenant_id: assignment.tenant_id.clone(),
        project_id: assignment.project_id.clone(),
        artifact_id: assignment.artifact_id.clone(),
        playground_id: assignment.playground_id.clone(),
        job_id: assignment.job_id.clone(),
        base_index_version: assignment.expected_index_version.clone(),
        extensions: Extensions::new(),
    };
    // Publication identity excludes execution timestamps, keeping batch IDs stable on recovery.
    let digest = prepared
        .publication_digest()
        .map_err(|error| transfer_error(error.to_string()))?;
    let manifest_id = MetadataBatchId::new(format!("m-{digest}"))?;
    let index_id = MetadataBatchId::new(format!("i-{digest}"))?;
    let receipt_id = MetadataBatchId::new(format!("o-{digest}"))?;

    let mut manifests = Vec::new();
    for manifest in &prepared.manifests {
        let id = manifest
            .canonical_id()
            .map_err(|error| transfer_error(error.to_string()))?;
        if manifest.chunks.is_empty() {
            manifests.push(ManifestRecord {
                manifest_id: id,
                total_size: DecimalU64::new(manifest.total_size),
                chunking: WireChunkingStrategy::from(manifest.chunking),
                chunk_start: DecimalU64::new(0),
                chunks: Vec::new(),
                extensions: Extensions::new(),
            });
        } else {
            for (fragment_number, chunks) in
                manifest.chunks.chunks(MAX_RECORDS_PER_PAGE).enumerate()
            {
                let chunk_start = fragment_number
                    .checked_mul(MAX_RECORDS_PER_PAGE)
                    .and_then(|value| u64::try_from(value).ok())
                    .ok_or_else(|| transfer_error("Manifest chunk ordinal exceeds u64"))?;
                manifests.push(ManifestRecord {
                    manifest_id: id,
                    total_size: DecimalU64::new(manifest.total_size),
                    chunking: WireChunkingStrategy::from(manifest.chunking),
                    chunk_start: DecimalU64::new(chunk_start),
                    chunks: chunks
                        .iter()
                        .map(|chunk| WireChunkRef {
                            object_id: chunk.object_id,
                            offset: DecimalU64::new(chunk.offset),
                            size: DecimalU64::new(chunk.size),
                            extensions: Extensions::new(),
                        })
                        .collect(),
                    extensions: Extensions::new(),
                });
            }
        }
    }
    let mutations = prepared
        .index_delta
        .iter()
        .map(|mutation| match mutation {
            IndexMutation::Upsert { record } => IndexDeltaRecord::Upsert {
                path: record.path.clone(),
                manifest_id: record.manifest_id,
                total_size: DecimalU64::new(record.total_size),
                chunk_count: DecimalU64::new(record.chunk_count),
                extensions: Extensions::new(),
            },
            IndexMutation::Delete { path } => IndexDeltaRecord::Delete {
                path: path.clone(),
                extensions: Extensions::new(),
            },
        })
        .collect::<Vec<_>>();
    let receipts = prepared
        .object_specs
        .iter()
        .map(|spec| {
            let checked_at = verified_at.get(&spec.id).copied().ok_or_else(|| {
                transfer_error(format!("object {} has no durability evidence", spec.id))
            })?;
            Ok(ObjectReceiptRecord {
                receipt_id: ObjectReceiptId::new(format!("r-{}", spec.id))?,
                tenant_id: assignment.tenant_id.clone(),
                artifact_id: assignment.artifact_id.clone(),
                job_id: assignment.job_id.clone(),
                storage_volume_id: assignment.storage_volume_id.clone(),
                artifact_placement_id: assignment.artifact_placement_id.clone(),
                object_id: spec.id,
                size: DecimalU64::new(spec.size),
                verified_at_unix_ms: UnixMillis::new(checked_at),
                extensions: Extensions::new(),
            })
        })
        .collect::<AgentResult<Vec<_>>>()?;

    let manifest_pages = manifest_pages(manifest_id, &scope, manifests)?;
    let index_pages = index_pages(index_id, &scope, mutations)?;
    let receipt_pages = receipt_pages(receipt_id, &scope, receipts)?;
    let descriptors = vec![
        descriptor(&scope, &manifest_pages)?,
        descriptor(&scope, &index_pages)?,
        descriptor(&scope, &receipt_pages)?,
    ];
    let pages = manifest_pages
        .into_iter()
        .chain(index_pages)
        .chain(receipt_pages)
        .collect();
    Ok(TransferReceipt::new(descriptors, pages))
}

fn manifest_pages(
    batch_id: MetadataBatchId,
    scope: &MetadataBatchScope,
    records: Vec<ManifestRecord>,
) -> AgentResult<Vec<MetadataBatchPage>> {
    build_pages(batch_id, scope, records, MetadataBatchRecords::Manifest)
}

fn index_pages(
    batch_id: MetadataBatchId,
    scope: &MetadataBatchScope,
    records: Vec<IndexDeltaRecord>,
) -> AgentResult<Vec<MetadataBatchPage>> {
    build_pages(batch_id, scope, records, MetadataBatchRecords::IndexDelta)
}

fn receipt_pages(
    batch_id: MetadataBatchId,
    scope: &MetadataBatchScope,
    records: Vec<ObjectReceiptRecord>,
) -> AgentResult<Vec<MetadataBatchPage>> {
    build_pages(
        batch_id,
        scope,
        records,
        MetadataBatchRecords::ObjectReceipt,
    )
}

fn build_pages<T: Clone>(
    batch_id: MetadataBatchId,
    scope: &MetadataBatchScope,
    records: Vec<T>,
    wrap: impl Fn(Vec<T>) -> MetadataBatchRecords,
) -> AgentResult<Vec<MetadataBatchPage>> {
    let page_count = records.len().div_ceil(MAX_RECORDS_PER_PAGE).max(1);
    let page_count =
        u32::try_from(page_count).map_err(|_| transfer_error("metadata page count exceeds u32"))?;
    if records.is_empty() {
        return Ok(vec![MetadataBatchPage::new(
            batch_id,
            scope.clone(),
            0,
            1,
            wrap(Vec::new()),
            Extensions::new(),
        )?]);
    }
    records
        .chunks(MAX_RECORDS_PER_PAGE)
        .enumerate()
        .map(|(page_number, records)| {
            Ok(MetadataBatchPage::new(
                batch_id.clone(),
                scope.clone(),
                u32::try_from(page_number)
                    .map_err(|_| transfer_error("metadata page number exceeds u32"))?,
                page_count,
                wrap(records.to_vec()),
                Extensions::new(),
            )?)
        })
        .collect()
}

fn descriptor(
    scope: &MetadataBatchScope,
    pages: &[MetadataBatchPage],
) -> AgentResult<MetadataBatchDescriptor> {
    let batch_id = pages
        .first()
        .ok_or_else(|| transfer_error("metadata batch has no pages"))?
        .batch_id
        .clone();
    Ok(MetadataBatchDescriptor::from_pages(
        batch_id,
        scope.clone(),
        pages,
        Extensions::new(),
    )?)
}

fn logical_prefix_contains(prefix: &LogicalPath, path: &LogicalPath) -> bool {
    path == prefix
        || path
            .as_str()
            .strip_prefix(prefix.as_str())
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn execution_error(message: impl Into<String>) -> AgentError {
    AgentError::new(AgentErrorCode::ExecutionFailed, message)
}

fn transfer_error(message: impl Into<String>) -> AgentError {
    AgentError::new(AgentErrorCode::ObjectTransferFailed, message)
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::Mutex};

    use neoengram_core::{ChunkRef, ContentDigest, Manifest};
    use neoengram_protocol::{
        AgentId, AgentMountId, ArtifactId, ArtifactPlacementId, AssignmentGeneration, AssignmentId,
        EdgeClusterId, MountGeneration, OwnerGeneration, PlacementGeneration, PlaygroundId,
        PrincipalId, PrincipalKind, PrincipalRef, ProjectId, StorageVolumeId, TenantId,
        WireIndexVersion,
    };
    use tempfile::TempDir;

    use super::*;

    #[derive(Debug)]
    struct RecordingBridge {
        index: AuthoritativeIndexSnapshot,
        missing_all: bool,
        uploaded: Mutex<Vec<ObjectSpec>>,
        descriptors: Mutex<Vec<MetadataBatchDescriptor>>,
        pages: Mutex<Vec<MetadataBatchPage>>,
    }

    impl RecordingBridge {
        fn new(index: AuthoritativeIndexSnapshot, missing_all: bool) -> Self {
            Self {
                index,
                missing_all,
                uploaded: Mutex::new(Vec::new()),
                descriptors: Mutex::new(Vec::new()),
                pages: Mutex::new(Vec::new()),
            }
        }
    }

    impl ExecutionBridge for RecordingBridge {
        fn authoritative_index(
            &self,
            _assignment: &AddAssignment,
        ) -> AgentResult<AuthoritativeIndexSnapshot> {
            Ok(self.index.clone())
        }

        fn query_missing_objects(
            &self,
            _assignment: &AddAssignment,
            objects: &[ObjectSpec],
        ) -> AgentResult<MissingObjectSet> {
            Ok(MissingObjectSet {
                missing: if self.missing_all {
                    objects.to_vec()
                } else {
                    Vec::new()
                },
            })
        }

        fn upload_object(
            &self,
            _assignment: &AddAssignment,
            object: ObjectSpec,
            content: &[u8],
        ) -> AgentResult<ObjectUploadEvidence> {
            if ObjectSpec::for_bytes(content) != object {
                return Err(transfer_error(
                    "test bridge received invalid object content",
                ));
            }
            self.uploaded.lock().unwrap().push(object);
            Ok(ObjectUploadEvidence {
                object,
                verified_at_unix_ms: 1_001,
            })
        }

        fn stage_metadata_descriptor(
            &self,
            _assignment: &AddAssignment,
            descriptor: &MetadataBatchDescriptor,
        ) -> AgentResult<()> {
            self.descriptors.lock().unwrap().push(descriptor.clone());
            Ok(())
        }

        fn stage_metadata_page(
            &self,
            _assignment: &AddAssignment,
            page: &MetadataBatchPage,
        ) -> AgentResult<()> {
            self.pages.lock().unwrap().push(page.clone());
            Ok(())
        }

        fn now_unix_ms(&self) -> AgentResult<u64> {
            Ok(1_000)
        }
    }

    #[test]
    fn real_worktree_detects_add_modify_and_delete() {
        let temporary = TempDir::new().unwrap();
        let mount = temporary.path().join("mount");
        let playground = mount.join("playgrounds/project-a/artifact-a/playground-a");
        fs::create_dir_all(&playground).unwrap();
        fs::write(playground.join("added.txt"), b"added").unwrap();
        fs::write(playground.join("modified.txt"), b"new value").unwrap();

        let mut base_records = vec![
            index_record("deleted.txt", b"deleted"),
            index_record("modified.txt", b"old value"),
        ];
        base_records.sort_by(|left, right| left.path.cmp(&right.path));
        let base = IndexVersion::from_snapshot(7, &base_records).unwrap();
        let snapshot = AuthoritativeIndexSnapshot::new(base, base_records).unwrap();
        let bridge = Arc::new(RecordingBridge::new(snapshot, true));
        let assignment = assignment(base);
        let execution =
            FilesystemExecution::new(&mount, temporary.path().join("cache"), bridge.clone());

        let prepared = execution.prepare_add(&assignment).unwrap();
        let mutations = prepared.index_delta.iter().collect::<Vec<_>>();
        assert_eq!(mutations.len(), 3);
        assert!(matches!(
            mutations[0],
            IndexMutation::Upsert { record } if record.path.as_str() == "added.txt"
        ));
        assert!(matches!(
            mutations[1],
            IndexMutation::Delete { path } if path.as_str() == "deleted.txt"
        ));
        assert!(matches!(
            mutations[2],
            IndexMutation::Upsert { record } if record.path.as_str() == "modified.txt"
        ));
        assert_eq!(prepared.statistics.files_upserted, 2);
        assert_eq!(prepared.statistics.paths_deleted, 1);
    }

    #[test]
    fn duplicate_payload_is_uploaded_once_and_stages_exact_metadata_kinds() {
        let temporary = TempDir::new().unwrap();
        let mount = temporary.path().join("mount");
        let playground = mount.join("playgrounds/project-a/artifact-a/playground-a");
        fs::create_dir_all(&playground).unwrap();
        fs::write(playground.join("first.bin"), b"same payload").unwrap();
        fs::write(playground.join("second.bin"), b"same payload").unwrap();

        let base = IndexVersion::from_snapshot(0, &[]).unwrap();
        let snapshot = AuthoritativeIndexSnapshot::new(base, Vec::new()).unwrap();
        let bridge = Arc::new(RecordingBridge::new(snapshot, true));
        let assignment = assignment(base);
        let execution =
            FilesystemExecution::new(&mount, temporary.path().join("cache"), bridge.clone());

        let prepared = execution.prepare_add(&assignment).unwrap();
        assert_eq!(prepared.statistics.files_upserted, 2);
        assert_eq!(prepared.manifests.len(), 1);
        assert_eq!(prepared.object_specs.len(), 1);
        let receipt = execution.transfer(&assignment, &prepared).unwrap();
        assert_eq!(bridge.uploaded.lock().unwrap().len(), 1);
        assert_eq!(receipt.metadata_batches.len(), 3);
        for kind in [
            neoengram_protocol::MetadataBatchKind::Manifest,
            neoengram_protocol::MetadataBatchKind::IndexDelta,
            neoengram_protocol::MetadataBatchKind::ObjectReceipt,
        ] {
            assert_eq!(
                receipt
                    .metadata_batches
                    .iter()
                    .filter(|descriptor| descriptor.kind == kind)
                    .count(),
                1
            );
        }

        execution.stage_metadata(&assignment, &receipt).unwrap();
        assert_eq!(bridge.descriptors.lock().unwrap().len(), 3);
        assert_eq!(
            bridge.pages.lock().unwrap().len(),
            receipt.metadata_pages.len()
        );
    }

    #[test]
    fn authoritative_index_rejects_digest_mismatch() {
        let record = index_record("tracked.txt", b"tracked");
        let declared = IndexVersion::new(9, ContentDigest::from_bytes([9; 32]));
        let error = AuthoritativeIndexSnapshot::new(declared, vec![record]).unwrap_err();
        assert_eq!(error.code(), AgentErrorCode::ExecutionFailed);
        assert!(error.message().contains("digest mismatch"));
    }

    #[test]
    fn authoritative_index_rejects_noncanonical_page_order() {
        let records = vec![index_record("z.txt", b"z"), index_record("a.txt", b"a")];
        let error = AuthoritativeIndexSnapshot::new(
            IndexVersion::new(1, ContentDigest::from_bytes([0; 32])),
            records,
        )
        .unwrap_err();
        assert!(error.message().contains("strictly path ordered"));
    }

    fn index_record(path: &str, payload: &[u8]) -> FileRecord {
        let object_id = ObjectId::for_bytes(payload);
        let manifest = Manifest::new(
            payload.len() as u64,
            ChunkingStrategy::WholeFile,
            vec![ChunkRef::new(object_id, 0, payload.len() as u64).unwrap()],
        )
        .unwrap();
        FileRecord::from_manifest(LogicalPath::parse(path).unwrap(), &manifest).unwrap()
    }

    fn assignment(index_version: IndexVersion) -> AddAssignment {
        let mut assignment = AddAssignment {
            job_id: neoengram_protocol::JobId::new("job-a").unwrap(),
            assignment_id: AssignmentId::new("assignment-a").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            agent_id: AgentId::new("agent-a").unwrap(),
            principal: PrincipalRef {
                kind: PrincipalKind::System,
                id: PrincipalId::new("system-a").unwrap(),
                extensions: Extensions::new(),
            },
            tenant_id: TenantId::new("tenant-a").unwrap(),
            project_id: ProjectId::new("project-a").unwrap(),
            artifact_id: ArtifactId::new("artifact-a").unwrap(),
            playground_id: PlaygroundId::new("playground-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            artifact_placement_id: ArtifactPlacementId::new("placement-a").unwrap(),
            placement_generation: PlacementGeneration::new(1),
            agent_mount_id: AgentMountId::new("mount-a").unwrap(),
            mount_generation: MountGeneration::new(1),
            owner_generation: OwnerGeneration::new(1),
            expected_index_version: WireIndexVersion::from(index_version),
            lease: None,
            request_digest: ContentDigest::from_bytes([0; 32]),
            deadline_unix_ms: UnixMillis::new(u64::MAX),
            paths: Vec::new(),
            all: true,
            extensions: Extensions::new(),
        };
        assignment.request_digest = assignment.computed_request_digest().unwrap();
        assignment
    }
}
