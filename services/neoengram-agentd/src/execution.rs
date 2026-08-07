use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use neoengram_agent::{
    AddExecutor, AgentError, AgentErrorCode, AgentResult, ObjectTransfer, TransferReceipt,
};
use neoengram_core::{
    canonical, ChunkRef, ChunkingStrategy, ContentDigest, FileRecord, IndexMutation, IndexVersion,
    LogicalPath, Manifest, ObjectId, ObjectSpec,
};
use neoengram_engine::{
    prepare_add, ChunkingSelection, EngineError, EngineResult, IndexSnapshotReader,
    ManagedResource, NoopFailureInjector, NoopProgressSink, ObjectStore, Page, PageCursor,
    PageRequest, PathScope, PrepareAddRequest, PreparedAdd,
};
use neoengram_fs::{
    rename_no_replace, sync_directory, LocalWorktree, LooseObjectStore, SystemClock, VerifiedRoot,
};
use neoengram_protocol::{
    AddAssignment, AgentId, AgentMountId, DecimalU64, Extensions, IndexDeltaRecord, JobDecision,
    ManifestRecord, MetadataBatchDescriptor, MetadataBatchId, MetadataBatchPage,
    MetadataBatchRecords, MetadataBatchScope, MountGeneration, ObjectReceiptId,
    ObjectReceiptRecord, OwnerGeneration, SnapshotMountAssignment, StorageVolumeId, TenantId,
    UnixMillis, WireChunkRef, WireChunkingStrategy, WorkspaceMaterializeAssignment,
    MAX_RECORDS_PER_PAGE,
};

/// FastCDC's configured maximum chunk size and the maximum durable loose-object size.
pub const MAX_AGENT_OBJECT_BYTES: u64 = 4 * 1024 * 1024;

const VOLUME_METADATA_DIRECTORY: &str = ".neoengram";
const VOLUME_OBJECTS_DIRECTORY: &str = "objects";

/// Materializes a server-derived Playground root below one approved Volume mount.
///
/// Commit and Manifest metadata arrive over the signed Agent API, while every payload object is
/// read from the Volume-local CAS. A complete tree is staged on the same filesystem and published
/// with an atomic no-replace rename, so a failed checkout never exposes a partial Playground.
#[derive(Debug, Clone)]
pub struct WorkspaceMaterializer {
    mount_root: PathBuf,
    expected_binding: Option<WorkspaceMaterializationBinding>,
    bridge: Option<Arc<dyn ExecutionBridge>>,
}

#[derive(Debug, Clone)]
struct WorkspaceMaterializationBinding {
    agent_id: AgentId,
    tenant_id: TenantId,
    storage_volume_id: StorageVolumeId,
    agent_mount_id: AgentMountId,
    mount_generation: MountGeneration,
    owner_generation: OwnerGeneration,
}

impl WorkspaceMaterializer {
    #[cfg(test)]
    #[must_use]
    pub(crate) fn new(mount_root: impl Into<PathBuf>) -> Self {
        Self {
            mount_root: mount_root.into(),
            expected_binding: None,
            bridge: None,
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_bridge(
        mount_root: impl Into<PathBuf>,
        bridge: Arc<dyn ExecutionBridge>,
    ) -> Self {
        Self {
            mount_root: mount_root.into(),
            expected_binding: None,
            bridge: Some(bridge),
        }
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn for_binding(
        mount_root: impl Into<PathBuf>,
        agent_id: AgentId,
        tenant_id: TenantId,
        storage_volume_id: StorageVolumeId,
        agent_mount_id: AgentMountId,
        mount_generation: MountGeneration,
        owner_generation: OwnerGeneration,
        bridge: Arc<dyn ExecutionBridge>,
    ) -> Self {
        Self {
            mount_root: mount_root.into(),
            expected_binding: Some(WorkspaceMaterializationBinding {
                agent_id,
                tenant_id,
                storage_volume_id,
                agent_mount_id,
                mount_generation,
                owner_generation,
            }),
            bridge: Some(bridge),
        }
    }

    /// Materializes and atomically publishes the exact immutable baseline.
    pub fn materialize(
        &self,
        assignment: &WorkspaceMaterializeAssignment,
    ) -> AgentResult<WorkspaceMaterializationStats> {
        self.validate_assignment(assignment)?;
        let mount = VerifiedRoot::open(&self.mount_root).map_err(|error| {
            AgentError::new(
                AgentErrorCode::MountUnavailable,
                format!("approved Agent mount is unavailable: {error}"),
            )
        })?;
        let files = match assignment.base_commit_id {
            Some(_) => {
                let bridge = self.bridge.as_ref().ok_or_else(|| {
                    AgentError::new(
                        AgentErrorCode::InvalidState,
                        "Workspace materialization bridge is not configured",
                    )
                })?;
                let snapshot = bridge.workspace_materialization_snapshot(assignment)?;
                let expected = assignment.base_index_version.as_ref().ok_or_else(|| {
                    AgentError::new(
                        AgentErrorCode::InvalidAssignment,
                        "non-empty Workspace assignment has no base Index version",
                    )
                })?;
                if snapshot.version() != &IndexVersion::from(expected.clone()) {
                    return Err(AgentError::new(
                        AgentErrorCode::ProtocolInvalid,
                        "Workspace materialization snapshot differs from the assignment base Index",
                    ));
                }
                snapshot.files().to_vec()
            }
            None => Vec::new(),
        };
        let stats = WorkspaceMaterializationStats::from_files(&files)?;
        let parent = assignment.relative_root.parent().ok_or_else(|| {
            AgentError::new(
                AgentErrorCode::InvalidAssignment,
                "Workspace relative root has no parent",
            )
        })?;
        let parent_path = mount.create_dir_all(&parent).map_err(|error| {
            execution_error(format!(
                "failed to create Playground parent directory: {error}"
            ))
        })?;
        let destination = mount
            .resolve_for_create(&assignment.relative_root)
            .map_err(|error| execution_error(error.to_string()))?;

        if path_exists(&destination)? {
            verify_materialized_tree(&destination, &files)?;
            return Ok(stats);
        }

        let temporary = tempfile::Builder::new()
            .prefix(".neoengram-materialize-")
            .tempdir_in(&parent_path)
            .map_err(|error| {
                execution_error(format!("failed to create staging directory: {error}"))
            })?;
        materialize_staging_tree(temporary.path(), assignment, &files, &self.mount_root)?;
        sync_directory(temporary.path()).map_err(AgentError::from)?;
        let staging = temporary.keep();
        match rename_no_replace(&staging, &destination) {
            Ok(()) => Ok(stats),
            Err(error) if error.code() == neoengram_engine::ErrorCode::AlreadyExists => {
                let _ = fs::remove_dir_all(&staging);
                verify_materialized_tree(&destination, &files)?;
                Ok(stats)
            }
            Err(error) => {
                let _ = fs::remove_dir_all(&staging);
                Err(AgentError::from(error))
            }
        }
    }

    pub(crate) fn validate_assignment(
        &self,
        assignment: &WorkspaceMaterializeAssignment,
    ) -> AgentResult<()> {
        assignment.validate().map_err(|error| {
            AgentError::new(
                AgentErrorCode::InvalidAssignment,
                format!("invalid workspace materialize assignment: {error}"),
            )
        })?;
        if let Some(expected) = &self.expected_binding {
            if assignment.agent_id != expected.agent_id
                || assignment.tenant_id != expected.tenant_id
                || assignment.storage_volume_id != expected.storage_volume_id
                || assignment.agent_mount_id != expected.agent_mount_id
            {
                return Err(AgentError::new(
                    AgentErrorCode::ScopeMismatch,
                    "workspace materialize assignment does not match the approved Agent mount scope",
                ));
            }
            if assignment.mount_generation != expected.mount_generation
                || assignment.owner_generation != expected.owner_generation
            {
                return Err(AgentError::new(
                    AgentErrorCode::GenerationMismatch,
                    "workspace materialize assignment carries a stale mount or owner generation",
                ));
            }
        }
        Ok(())
    }
}

/// Exact immutable file recipe returned by the materialization metadata bridge.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceMaterializationFile {
    pub record: FileRecord,
    pub manifest: Manifest,
}

/// Complete Commit snapshot used for one physical checkout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceMaterializationSnapshot {
    version: IndexVersion,
    files: Arc<Vec<WorkspaceMaterializationFile>>,
}

impl WorkspaceMaterializationSnapshot {
    pub fn new(
        version: IndexVersion,
        files: Vec<WorkspaceMaterializationFile>,
    ) -> AgentResult<Self> {
        let records = files
            .iter()
            .map(|file| file.record.clone())
            .collect::<Vec<_>>();
        AuthoritativeIndexSnapshot::new(version.clone(), records)?;
        for file in &files {
            file.manifest
                .validate()
                .map_err(|error| execution_error(error.to_string()))?;
            let manifest_id = file
                .manifest
                .canonical_id()
                .map_err(|error| execution_error(error.to_string()))?;
            let chunk_count = file
                .manifest
                .chunk_count()
                .map_err(|error| execution_error(error.to_string()))?;
            if manifest_id != file.record.manifest_id
                || file.manifest.total_size != file.record.total_size
                || chunk_count != file.record.chunk_count
            {
                return Err(execution_error(format!(
                    "Manifest metadata differs from Index record {}",
                    file.record.path
                )));
            }
        }
        Ok(Self {
            version,
            files: Arc::new(files),
        })
    }

    #[must_use]
    pub const fn version(&self) -> &IndexVersion {
        &self.version
    }

    #[must_use]
    pub fn files(&self) -> &[WorkspaceMaterializationFile] {
        self.files.as_slice()
    }
}

/// Physical publication statistics reported through the durable control outbox.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkspaceMaterializationStats {
    pub files: u64,
    pub bytes: u64,
}

impl WorkspaceMaterializationStats {
    fn from_files(materialized: &[WorkspaceMaterializationFile]) -> AgentResult<Self> {
        let files = u64::try_from(materialized.len())
            .map_err(|_| execution_error("Workspace file count exceeds u64"))?;
        let bytes = files_total_size(materialized)?;
        Ok(Self { files, bytes })
    }
}

fn files_total_size(materialized: &[WorkspaceMaterializationFile]) -> AgentResult<u64> {
    materialized.iter().try_fold(0_u64, |total, file| {
        total
            .checked_add(file.record.total_size)
            .ok_or_else(|| execution_error("Workspace byte count exceeds u64"))
    })
}

fn path_exists(path: &Path) -> AgentResult<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(execution_error(format!(
            "failed to inspect Playground destination: {error}"
        ))),
    }
}

fn materialize_staging_tree(
    staging: &Path,
    assignment: &WorkspaceMaterializeAssignment,
    files: &[WorkspaceMaterializationFile],
    mount_root: &Path,
) -> AgentResult<()> {
    let staging_root = VerifiedRoot::open(staging).map_err(AgentError::from)?;
    let objects = if files.is_empty() {
        None
    } else {
        Some(workspace_object_store(mount_root, assignment)?)
    };
    let mut directories = BTreeSet::from([staging.to_path_buf()]);

    for file in files {
        let parent = file.record.path.parent();
        if let Some(parent) = parent.as_ref() {
            let path = staging_root
                .create_dir_all(parent)
                .map_err(AgentError::from)?;
            for ancestor in path.ancestors() {
                if ancestor == staging {
                    directories.insert(ancestor.to_path_buf());
                    break;
                }
                if ancestor.starts_with(staging) {
                    directories.insert(ancestor.to_path_buf());
                }
            }
        }
        let destination = staging_root
            .resolve_for_create(&file.record.path)
            .map_err(AgentError::from)?;
        let mut output = File::options()
            .write(true)
            .create_new(true)
            .open(&destination)
            .map_err(|error| {
                execution_error(format!(
                    "failed to create staged Workspace file {}: {error}",
                    file.record.path
                ))
            })?;
        let objects = objects.as_ref().ok_or_else(|| {
            execution_error("non-empty Workspace snapshot has no Volume object store")
        })?;
        for chunk in &file.manifest.chunks {
            objects
                .copy_to(&chunk.object_spec(), &mut output)
                .map_err(|error| object_materialization_error(chunk, error))?;
        }
        output.flush().map_err(|error| {
            execution_error(format!(
                "failed to flush staged Workspace file {}: {error}",
                file.record.path
            ))
        })?;
        output.sync_all().map_err(|error| {
            execution_error(format!(
                "failed to synchronize staged Workspace file {}: {error}",
                file.record.path
            ))
        })?;
    }

    let mut directories = directories.into_iter().collect::<Vec<_>>();
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        sync_directory(&directory).map_err(AgentError::from)?;
    }
    Ok(())
}

fn workspace_object_store(
    mount_root: &Path,
    assignment: &WorkspaceMaterializeAssignment,
) -> AgentResult<LooseObjectStore> {
    artifact_object_store(mount_root, &assignment.tenant_id, &assignment.artifact_id)
}

pub(crate) fn artifact_object_store(
    mount_root: &Path,
    tenant_id: &TenantId,
    artifact_id: &neoengram_protocol::ArtifactId,
) -> AgentResult<LooseObjectStore> {
    let mount = VerifiedRoot::open(mount_root).map_err(AgentError::from)?;
    let metadata_root = open_or_create_metadata_root(&mount)?;
    let relative = LogicalPath::parse(format!(
        "{VOLUME_OBJECTS_DIRECTORY}/tenants/{}/artifacts/{}/objects",
        tenant_id, artifact_id
    ))
    .map_err(|error| execution_error(error.to_string()))?;
    let path = metadata_root
        .create_dir_all(&relative)
        .map_err(AgentError::from)?;
    let objects = LooseObjectStore::open_or_create(path).map_err(AgentError::from)?;
    objects.initialize().map_err(AgentError::from)?;
    Ok(objects)
}

fn object_materialization_error(
    chunk: &ChunkRef,
    error: neoengram_engine::EngineError,
) -> AgentError {
    let message = match error.code() {
        neoengram_engine::ErrorCode::ObjectMissing => format!(
            "required Volume object {} for offset {} is missing",
            chunk.object_id, chunk.offset
        ),
        neoengram_engine::ErrorCode::ObjectCorrupt => format!(
            "required Volume object {} for offset {} is corrupt: {error}",
            chunk.object_id, chunk.offset
        ),
        _ => format!(
            "failed to read Volume object {} for offset {}: {error}",
            chunk.object_id, chunk.offset
        ),
    };
    AgentError::new(AgentErrorCode::ObjectTransferFailed, message)
}

fn verify_materialized_tree(
    root: &Path,
    files: &[WorkspaceMaterializationFile],
) -> AgentResult<()> {
    let metadata = fs::symlink_metadata(root).map_err(|error| {
        execution_error(format!("failed to inspect existing Playground: {error}"))
    })?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(execution_error(
            "existing Playground destination is not an ordinary directory",
        ));
    }

    let expected_files = files
        .iter()
        .map(|file| (file.record.path.as_str().to_owned(), file))
        .collect::<BTreeMap<_, _>>();
    let mut expected_directories = BTreeSet::new();
    for file in files {
        let components = file.record.path.components().collect::<Vec<_>>();
        for length in 1..components.len() {
            expected_directories.insert(components[..length].join("/"));
        }
    }

    let mut observed_files = BTreeSet::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(directory) = stack.pop() {
        let entries = fs::read_dir(&directory).map_err(|error| {
            execution_error(format!("failed to enumerate existing Playground: {error}"))
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| {
                execution_error(format!("failed to read existing Playground entry: {error}"))
            })?;
            let path = entry.path();
            let relative = physical_relative_path(root, &path)?;
            let metadata = fs::symlink_metadata(&path).map_err(|error| {
                execution_error(format!(
                    "failed to inspect existing Playground entry: {error}"
                ))
            })?;
            if metadata.file_type().is_symlink() {
                return Err(execution_error(format!(
                    "existing Playground contains symlink {relative}"
                )));
            }
            if metadata.is_dir() {
                if !expected_directories.contains(&relative) {
                    return Err(execution_error(format!(
                        "existing Playground contains unexpected directory {relative}"
                    )));
                }
                stack.push(path);
            } else if metadata.is_file() {
                let expected = expected_files.get(&relative).ok_or_else(|| {
                    execution_error(format!(
                        "existing Playground contains unexpected file {relative}"
                    ))
                })?;
                verify_materialized_file(&path, expected)?;
                observed_files.insert(relative);
            } else {
                return Err(execution_error(format!(
                    "existing Playground contains unsupported entry {relative}"
                )));
            }
        }
    }

    if observed_files.len() != expected_files.len() {
        let missing = expected_files
            .keys()
            .find(|path| !observed_files.contains(*path))
            .map(String::as_str)
            .unwrap_or("unknown");
        return Err(execution_error(format!(
            "existing Playground is missing expected file {missing}"
        )));
    }
    Ok(())
}

fn physical_relative_path(root: &Path, path: &Path) -> AgentResult<String> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| execution_error("existing Playground entry escaped the destination root"))?;
    let mut components = Vec::new();
    for component in relative.components() {
        let value = component
            .as_os_str()
            .to_str()
            .ok_or_else(|| execution_error("existing Playground contains a non-UTF-8 path"))?;
        components.push(value);
    }
    Ok(components.join("/"))
}

fn verify_materialized_file(
    path: &Path,
    expected: &WorkspaceMaterializationFile,
) -> AgentResult<()> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        execution_error(format!(
            "failed to inspect materialized file {}: {error}",
            expected.record.path
        ))
    })?;
    if metadata.len() != expected.record.total_size {
        return Err(execution_error(format!(
            "materialized file {} has size {}, expected {}",
            expected.record.path,
            metadata.len(),
            expected.record.total_size
        )));
    }
    let mut input = File::open(path).map_err(|error| {
        execution_error(format!(
            "failed to open materialized file {}: {error}",
            expected.record.path
        ))
    })?;
    for chunk in &expected.manifest.chunks {
        let observed = hash_exact_chunk(&mut input, chunk.size, &expected.record.path)?;
        if observed != chunk.object_id {
            return Err(execution_error(format!(
                "materialized file {} differs at offset {}",
                expected.record.path, chunk.offset
            )));
        }
    }
    let mut trailing = [0_u8; 1];
    if input.read(&mut trailing).map_err(|error| {
        execution_error(format!(
            "failed to finish verifying materialized file {}: {error}",
            expected.record.path
        ))
    })? != 0
    {
        return Err(execution_error(format!(
            "materialized file {} contains trailing bytes",
            expected.record.path
        )));
    }
    Ok(())
}

fn hash_exact_chunk(
    input: &mut File,
    size: u64,
    logical_path: &LogicalPath,
) -> AgentResult<ObjectId> {
    let mut remaining = size;
    let mut buffer = [0_u8; 64 * 1024];
    let mut hasher = blake3::Hasher::new();
    while remaining > 0 {
        let limit = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| execution_error("chunk read size exceeds usize"))?;
        let read = input.read(&mut buffer[..limit]).map_err(|error| {
            execution_error(format!(
                "failed to verify materialized file {logical_path}: {error}"
            ))
        })?;
        if read == 0 {
            return Err(execution_error(format!(
                "materialized file {logical_path} ended within a declared chunk"
            )));
        }
        hasher.update(&buffer[..read]);
        remaining -= read as u64;
    }
    Ok(ObjectId::from_bytes(*hasher.finalize().as_bytes()))
}

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

/// Synchronous boundary between the Agent state machine and an async session transport.
///
/// A session runtime can prefetch Index pages and serve `authoritative_index` from a cache, then
/// stage the metadata that describes objects durable on the assigned Volume. Object bytes never
/// cross this bridge and no physical path is supplied to the control plane.
pub trait ExecutionBridge: std::fmt::Debug + Send + Sync {
    fn authoritative_index(
        &self,
        assignment: &AddAssignment,
    ) -> AgentResult<AuthoritativeIndexSnapshot>;

    fn workspace_materialization_snapshot(
        &self,
        assignment: &WorkspaceMaterializeAssignment,
    ) -> AgentResult<WorkspaceMaterializationSnapshot>;

    fn snapshot_mount_snapshot(
        &self,
        _assignment: &SnapshotMountAssignment,
    ) -> AgentResult<WorkspaceMaterializationSnapshot> {
        Err(AgentError::new(
            AgentErrorCode::InvalidState,
            "Snapshot mount metadata bridge is not configured",
        ))
    }

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

/// Real managed-Add adapter over one configured mount and its durable loose-object CAS.
#[derive(Debug, Clone)]
pub struct FilesystemExecution {
    mount_root: PathBuf,
    bridge: Arc<dyn ExecutionBridge>,
}

impl FilesystemExecution {
    #[must_use]
    pub fn new(mount_root: impl Into<PathBuf>, bridge: Arc<dyn ExecutionBridge>) -> Self {
        Self {
            mount_root: mount_root.into(),
            bridge,
        }
    }

    /// Creates and validates the Volume-owned CAS root before the Agent becomes ready.
    pub fn initialize(&self) -> AgentResult<()> {
        self.volume_objects_root().map(|_| ())
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
        let root = self.volume_objects_root()?;
        let relative = LogicalPath::parse(format!(
            "tenants/{}/artifacts/{}/objects",
            assignment.tenant_id, assignment.artifact_id
        ))
        .map_err(|error| execution_error(error.to_string()))?;
        let path = root.create_dir_all(&relative).map_err(AgentError::from)?;
        LooseObjectStore::open_or_create(path).map_err(AgentError::from)
    }

    fn volume_objects_root(&self) -> AgentResult<VerifiedRoot> {
        let mount = VerifiedRoot::open(&self.mount_root).map_err(AgentError::from)?;
        let metadata_root = open_or_create_metadata_root(&mount)?;
        let relative = LogicalPath::parse(VOLUME_OBJECTS_DIRECTORY)
            .map_err(|error| execution_error(error.to_string()))?;
        let path = metadata_root
            .create_dir_all(&relative)
            .map_err(AgentError::from)?;
        VerifiedRoot::open(path).map_err(AgentError::from)
    }
}

fn open_or_create_metadata_root(mount: &VerifiedRoot) -> AgentResult<VerifiedRoot> {
    mount.verify_identity().map_err(AgentError::from)?;
    let path = mount.as_path().join(VOLUME_METADATA_DIRECTORY);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
        Ok(_) => {
            return Err(AgentError::new(
                AgentErrorCode::MountUnavailable,
                "Volume metadata path is not an ordinary directory",
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => match fs::create_dir(&path) {
            Ok(()) => crate::durability::sync_directory(mount.as_path())
                .map_err(|error| execution_error(error.to_string()))?,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let metadata = fs::symlink_metadata(&path).map_err(|error| {
                    AgentError::new(
                        AgentErrorCode::MountUnavailable,
                        format!("failed to inspect concurrently created metadata path: {error}"),
                    )
                })?;
                if !metadata.is_dir() || metadata.file_type().is_symlink() {
                    return Err(AgentError::new(
                        AgentErrorCode::MountUnavailable,
                        "concurrently created Volume metadata path is not an ordinary directory",
                    ));
                }
            }
            Err(error) => {
                return Err(AgentError::new(
                    AgentErrorCode::MountUnavailable,
                    format!("failed to create Volume metadata directory: {error}"),
                ));
            }
        },
        Err(error) => {
            return Err(AgentError::new(
                AgentErrorCode::MountUnavailable,
                format!("failed to inspect Volume metadata directory: {error}"),
            ));
        }
    }
    mount.verify_identity().map_err(AgentError::from)?;
    let metadata_root = VerifiedRoot::open(&path).map_err(AgentError::from)?;
    if metadata_root.as_path().parent() != Some(mount.as_path()) {
        return Err(AgentError::new(
            AgentErrorCode::MountUnavailable,
            "Volume metadata directory escaped the approved mount",
        ));
    }
    Ok(metadata_root)
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
        for spec in &prepared.object_specs {
            if spec.size > MAX_AGENT_OBJECT_BYTES {
                return Err(transfer_error(format!(
                    "object {} is {} bytes, exceeding the {} byte Agent limit",
                    spec.id, spec.size, MAX_AGENT_OBJECT_BYTES
                )));
            }
            objects
                .verify(spec)
                .map_err(|error| transfer_error(error.to_string()))?;
        }
        objects
            .durability_barrier()
            .map_err(|error| transfer_error(error.to_string()))?;
        let checked_at = self.bridge.now_unix_ms()?;
        let verified_at = prepared
            .object_specs
            .iter()
            .map(|spec| (spec.id, checked_at))
            .collect::<BTreeMap<_, _>>();

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
        // Objects are already immutable and durable on the assigned Volume before Prepared.
        Ok(())
    }
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
                receipt_id: object_receipt_id(assignment, spec)?,
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

fn object_receipt_id(
    assignment: &AddAssignment,
    object: &ObjectSpec,
) -> AgentResult<ObjectReceiptId> {
    let scope = (
        &assignment.tenant_id,
        &assignment.artifact_id,
        &assignment.job_id,
        &assignment.storage_volume_id,
        &assignment.artifact_placement_id,
        assignment.placement_generation.get().to_string(),
        object.id,
        DecimalU64::new(object.size),
    );
    let canonical =
        neoengram_protocol::domain_separated_jcs_bytes("neoengram.object-receipt-id.v1", &scope)
            .map_err(|error| transfer_error(error.to_string()))?;
    ObjectReceiptId::new(format!("r-{}", ContentDigest::hash(canonical))).map_err(AgentError::from)
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

    use neoengram_core::{ChunkRef, Manifest};
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
        workspace: Option<WorkspaceMaterializationSnapshot>,
        descriptors: Mutex<Vec<MetadataBatchDescriptor>>,
        pages: Mutex<Vec<MetadataBatchPage>>,
    }

    impl RecordingBridge {
        fn new(index: AuthoritativeIndexSnapshot) -> Self {
            Self {
                index,
                workspace: None,
                descriptors: Mutex::new(Vec::new()),
                pages: Mutex::new(Vec::new()),
            }
        }

        fn with_workspace(mut self, workspace: WorkspaceMaterializationSnapshot) -> Self {
            self.workspace = Some(workspace);
            self
        }
    }

    impl ExecutionBridge for RecordingBridge {
        fn authoritative_index(
            &self,
            _assignment: &AddAssignment,
        ) -> AgentResult<AuthoritativeIndexSnapshot> {
            Ok(self.index.clone())
        }

        fn workspace_materialization_snapshot(
            &self,
            _assignment: &WorkspaceMaterializeAssignment,
        ) -> AgentResult<WorkspaceMaterializationSnapshot> {
            self.workspace.clone().ok_or_else(|| {
                execution_error("test bridge has no Workspace materialization snapshot")
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
        let bridge = Arc::new(RecordingBridge::new(snapshot));
        let assignment = assignment(base);
        let execution = FilesystemExecution::new(&mount, bridge.clone());

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
    fn duplicate_payload_is_durable_once_on_the_volume_and_stages_exact_metadata_kinds() {
        let temporary = TempDir::new().unwrap();
        let mount = temporary.path().join("mount");
        let state = temporary.path().join("state");
        let playground = mount.join("playgrounds/project-a/artifact-a/playground-a");
        fs::create_dir_all(&playground).unwrap();
        fs::create_dir(&state).unwrap();
        fs::write(playground.join("first.bin"), b"same payload").unwrap();
        fs::write(playground.join("second.bin"), b"same payload").unwrap();

        let base = IndexVersion::from_snapshot(0, &[]).unwrap();
        let snapshot = AuthoritativeIndexSnapshot::new(base, Vec::new()).unwrap();
        let bridge = Arc::new(RecordingBridge::new(snapshot));
        let assignment = assignment(base);
        let execution = FilesystemExecution::new(&mount, bridge.clone());

        let prepared = execution.prepare_add(&assignment).unwrap();
        assert_eq!(prepared.statistics.files_upserted, 2);
        assert_eq!(prepared.manifests.len(), 1);
        assert_eq!(prepared.object_specs.len(), 1);
        let receipt = execution.transfer(&assignment, &prepared).unwrap();
        let object = prepared.object_specs[0];
        assert!(volume_object_path(&mount, &assignment, object.id).is_file());
        assert!(!state.join("objects").exists());
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
    fn transfer_rejects_a_corrupt_volume_object_before_issuing_receipts() {
        let temporary = TempDir::new().unwrap();
        let mount = temporary.path().join("mount");
        let playground = mount.join("playgrounds/project-a/artifact-a/playground-a");
        fs::create_dir_all(&playground).unwrap();
        fs::write(playground.join("payload.bin"), b"same payload").unwrap();

        let base = IndexVersion::from_snapshot(0, &[]).unwrap();
        let snapshot = AuthoritativeIndexSnapshot::new(base, Vec::new()).unwrap();
        let bridge = Arc::new(RecordingBridge::new(snapshot));
        let assignment = assignment(base);
        let execution = FilesystemExecution::new(&mount, bridge);
        let prepared = execution.prepare_add(&assignment).unwrap();
        let object = prepared.object_specs[0];
        fs::write(
            volume_object_path(&mount, &assignment, object.id),
            b"evil payload",
        )
        .unwrap();

        let error = execution.transfer(&assignment, &prepared).unwrap_err();
        assert_eq!(error.code(), AgentErrorCode::ObjectTransferFailed);
        assert!(error.message().contains("digest"));
    }

    #[test]
    fn transfer_rejects_a_deleted_volume_object_before_issuing_receipts() {
        let temporary = TempDir::new().unwrap();
        let mount = temporary.path().join("mount");
        let playground = mount.join("playgrounds/project-a/artifact-a/playground-a");
        fs::create_dir_all(&playground).unwrap();
        fs::write(playground.join("payload.bin"), b"same payload").unwrap();

        let base = IndexVersion::from_snapshot(0, &[]).unwrap();
        let snapshot = AuthoritativeIndexSnapshot::new(base, Vec::new()).unwrap();
        let bridge = Arc::new(RecordingBridge::new(snapshot));
        let assignment = assignment(base);
        let execution = FilesystemExecution::new(&mount, bridge);
        let prepared = execution.prepare_add(&assignment).unwrap();
        let object = prepared.object_specs[0];
        fs::remove_file(volume_object_path(&mount, &assignment, object.id)).unwrap();

        let error = execution.transfer(&assignment, &prepared).unwrap_err();
        assert_eq!(error.code(), AgentErrorCode::ObjectTransferFailed);
    }

    #[test]
    fn receipt_identity_is_stable_and_unique_across_artifact_job_scopes() {
        let base = IndexVersion::from_snapshot(0, &[]).unwrap();
        let first = assignment(base);
        let mut other_job = first.clone();
        other_job.job_id = neoengram_protocol::JobId::new("job-b").unwrap();
        other_job.request_digest = other_job.computed_request_digest().unwrap();
        let mut other_artifact = first.clone();
        other_artifact.artifact_id = ArtifactId::new("artifact-b").unwrap();
        other_artifact.artifact_placement_id = ArtifactPlacementId::new("placement-b").unwrap();
        other_artifact.request_digest = other_artifact.computed_request_digest().unwrap();
        let object = ObjectSpec::for_bytes(b"shared payload");

        let first_id = object_receipt_id(&first, &object).unwrap();
        assert_eq!(first_id, object_receipt_id(&first, &object).unwrap());
        assert_ne!(first_id, object_receipt_id(&other_job, &object).unwrap());
        assert_ne!(
            first_id,
            object_receipt_id(&other_artifact, &object).unwrap()
        );
    }

    #[cfg(unix)]
    #[test]
    fn volume_cas_rejects_a_symlinked_metadata_directory() {
        let temporary = TempDir::new().unwrap();
        let mount = temporary.path().join("mount");
        let outside = temporary.path().join("outside");
        fs::create_dir(&mount).unwrap();
        fs::create_dir(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, mount.join(VOLUME_METADATA_DIRECTORY)).unwrap();

        let base = IndexVersion::from_snapshot(0, &[]).unwrap();
        let snapshot = AuthoritativeIndexSnapshot::new(base, Vec::new()).unwrap();
        let execution = FilesystemExecution::new(&mount, Arc::new(RecordingBridge::new(snapshot)));

        let error = execution.initialize().unwrap_err();
        assert_eq!(error.code(), AgentErrorCode::MountUnavailable);
        assert!(!outside.join(VOLUME_OBJECTS_DIRECTORY).exists());
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

    #[test]
    fn workspace_materializer_creates_the_canonical_directory_idempotently() {
        let temporary = TempDir::new().unwrap();
        let mount = temporary.path().join("mount");
        fs::create_dir(&mount).unwrap();
        let assignment = workspace_assignment(None);
        let materializer = WorkspaceMaterializer::new(&mount);

        materializer.materialize(&assignment).unwrap();
        materializer.materialize(&assignment).unwrap();

        let expected = mount.join("playgrounds/project-a/artifact-a/playground-a");
        assert!(expected.is_dir());
    }

    #[test]
    fn workspace_materializer_rejects_a_noncanonical_relative_root() {
        let temporary = TempDir::new().unwrap();
        let mount = temporary.path().join("mount");
        fs::create_dir(&mount).unwrap();
        let mut assignment = workspace_assignment(None);
        assignment.relative_root = LogicalPath::parse("other/playground-a").unwrap();

        let error = WorkspaceMaterializer::new(&mount)
            .materialize(&assignment)
            .unwrap_err();
        assert_eq!(error.code(), AgentErrorCode::InvalidAssignment);
        assert!(!mount.join("other").exists());
    }

    #[test]
    fn workspace_materializer_restores_non_empty_baseline_and_verifies_replay() {
        let temporary = TempDir::new().unwrap();
        let mount = temporary.path().join("mount");
        fs::create_dir(&mount).unwrap();
        let first = b"first chunk";
        let second = b"second chunk";
        let chunks = vec![
            ChunkRef::new(ObjectId::for_bytes(first), 0, first.len() as u64).unwrap(),
            ChunkRef::new(
                ObjectId::for_bytes(second),
                first.len() as u64,
                second.len() as u64,
            )
            .unwrap(),
        ];
        let manifest = Manifest::new(
            (first.len() + second.len()) as u64,
            ChunkingStrategy::FastCdc,
            chunks,
        )
        .unwrap();
        let record = FileRecord::from_manifest(
            LogicalPath::parse("nested/materialized.txt").unwrap(),
            &manifest,
        )
        .unwrap();
        let version = IndexVersion::from_snapshot(9, std::slice::from_ref(&record)).unwrap();
        let snapshot = WorkspaceMaterializationSnapshot::new(
            version.clone(),
            vec![WorkspaceMaterializationFile { record, manifest }],
        )
        .unwrap();
        let empty = AuthoritativeIndexSnapshot::new(
            IndexVersion::from_snapshot(0, &[]).unwrap(),
            Vec::new(),
        )
        .unwrap();
        let bridge = Arc::new(RecordingBridge::new(empty).with_workspace(snapshot));
        let mut assignment = workspace_assignment(Some(ContentDigest::from_bytes([0x42; 32])));
        assignment.base_index_version = Some(version.into());
        assignment.request_digest = assignment.computed_request_digest().unwrap();
        let objects = workspace_object_store(&mount, &assignment).unwrap();
        objects
            .put_from(
                &ObjectSpec::for_bytes(first),
                &mut std::io::Cursor::new(first),
            )
            .unwrap();
        objects
            .put_from(
                &ObjectSpec::for_bytes(second),
                &mut std::io::Cursor::new(second),
            )
            .unwrap();
        let materializer = WorkspaceMaterializer::with_bridge(&mount, bridge);

        let stats = materializer.materialize(&assignment).unwrap();
        assert_eq!(stats.files, 1);
        assert_eq!(stats.bytes, (first.len() + second.len()) as u64);
        assert_eq!(
            fs::read(
                mount.join("playgrounds/project-a/artifact-a/playground-a/nested/materialized.txt")
            )
            .unwrap(),
            [first.as_slice(), second.as_slice()].concat()
        );
        materializer.materialize(&assignment).unwrap();

        fs::write(
            mount.join("playgrounds/project-a/artifact-a/playground-a/unexpected"),
            b"unexpected",
        )
        .unwrap();
        let error = materializer.materialize(&assignment).unwrap_err();
        assert!(error.message().contains("unexpected file"));
    }

    #[test]
    fn workspace_materializer_does_not_publish_when_a_volume_object_is_missing() {
        let temporary = TempDir::new().unwrap();
        let mount = temporary.path().join("mount");
        fs::create_dir(&mount).unwrap();
        let payload = b"missing";
        let manifest = Manifest::new(
            payload.len() as u64,
            ChunkingStrategy::WholeFile,
            vec![ChunkRef::new(ObjectId::for_bytes(payload), 0, payload.len() as u64).unwrap()],
        )
        .unwrap();
        let record =
            FileRecord::from_manifest(LogicalPath::parse("missing.txt").unwrap(), &manifest)
                .unwrap();
        let version = IndexVersion::from_snapshot(1, std::slice::from_ref(&record)).unwrap();
        let snapshot = WorkspaceMaterializationSnapshot::new(
            version.clone(),
            vec![WorkspaceMaterializationFile { record, manifest }],
        )
        .unwrap();
        let empty = AuthoritativeIndexSnapshot::new(
            IndexVersion::from_snapshot(0, &[]).unwrap(),
            Vec::new(),
        )
        .unwrap();
        let bridge = Arc::new(RecordingBridge::new(empty).with_workspace(snapshot));
        let mut assignment = workspace_assignment(Some(ContentDigest::from_bytes([0x43; 32])));
        assignment.base_index_version = Some(version.into());
        assignment.request_digest = assignment.computed_request_digest().unwrap();

        let error = WorkspaceMaterializer::with_bridge(&mount, bridge)
            .materialize(&assignment)
            .unwrap_err();
        assert_eq!(error.code(), AgentErrorCode::ObjectTransferFailed);
        assert!(error.message().contains("is missing"));
        assert!(!mount
            .join("playgrounds/project-a/artifact-a/playground-a")
            .exists());
    }

    #[test]
    fn workspace_materializer_rejects_stale_owner_generation() {
        let temporary = TempDir::new().unwrap();
        let mount = temporary.path().join("mount");
        fs::create_dir(&mount).unwrap();
        let assignment = workspace_assignment(None);
        let materializer = WorkspaceMaterializer::for_binding(
            &mount,
            assignment.agent_id.clone(),
            assignment.tenant_id.clone(),
            assignment.storage_volume_id.clone(),
            assignment.agent_mount_id.clone(),
            assignment.mount_generation,
            OwnerGeneration::new(assignment.owner_generation.get() + 1),
            Arc::new(RecordingBridge::new(
                AuthoritativeIndexSnapshot::new(
                    IndexVersion::from_snapshot(0, &[]).unwrap(),
                    Vec::new(),
                )
                .unwrap(),
            )),
        );

        let error = materializer.materialize(&assignment).unwrap_err();
        assert_eq!(error.code(), AgentErrorCode::GenerationMismatch);
        assert!(!mount.join("playgrounds").exists());
    }

    #[cfg(unix)]
    #[test]
    fn workspace_materializer_rejects_symlink_traversal() {
        let temporary = TempDir::new().unwrap();
        let mount = temporary.path().join("mount");
        let outside = temporary.path().join("outside");
        fs::create_dir(&mount).unwrap();
        fs::create_dir(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, mount.join("playgrounds")).unwrap();

        let error = WorkspaceMaterializer::new(&mount)
            .materialize(&workspace_assignment(None))
            .unwrap_err();
        assert_eq!(error.code(), AgentErrorCode::ExecutionFailed);
        assert!(!outside.join("project-a").exists());
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

    fn volume_object_path(
        mount: &std::path::Path,
        assignment: &AddAssignment,
        object_id: ObjectId,
    ) -> PathBuf {
        mount
            .join(VOLUME_METADATA_DIRECTORY)
            .join(VOLUME_OBJECTS_DIRECTORY)
            .join("tenants")
            .join(assignment.tenant_id.as_str())
            .join("artifacts")
            .join(assignment.artifact_id.as_str())
            .join("objects")
            .join(object_id.to_hex())
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

    fn workspace_assignment(
        base_commit_id: Option<ContentDigest>,
    ) -> WorkspaceMaterializeAssignment {
        let project_id = ProjectId::new("project-a").unwrap();
        let artifact_id = ArtifactId::new("artifact-a").unwrap();
        let playground_id = PlaygroundId::new("playground-a").unwrap();
        let mut assignment = WorkspaceMaterializeAssignment {
            job_id: neoengram_protocol::JobId::new("job-materialize-a").unwrap(),
            assignment_id: AssignmentId::new("assignment-materialize-a").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            agent_id: AgentId::new("agent-a").unwrap(),
            principal: PrincipalRef {
                kind: PrincipalKind::System,
                id: PrincipalId::new("system-a").unwrap(),
                extensions: Extensions::new(),
            },
            tenant_id: TenantId::new("tenant-a").unwrap(),
            project_id,
            artifact_id,
            playground_id,
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            agent_mount_id: AgentMountId::new("mount-a").unwrap(),
            mount_generation: MountGeneration::new(1),
            owner_generation: OwnerGeneration::new(1),
            relative_root: LogicalPath::parse("playgrounds/project-a/artifact-a/playground-a")
                .unwrap(),
            base_commit_id,
            base_index_version: base_commit_id
                .map(|_| WireIndexVersion::from(IndexVersion::from_snapshot(0, &[]).unwrap())),
            request_digest: ContentDigest::from_bytes([0; 32]),
            deadline_unix_ms: UnixMillis::new(u64::MAX),
            extensions: Extensions::new(),
        };
        assignment.request_digest = assignment.computed_request_digest().unwrap();
        assignment
    }
}
