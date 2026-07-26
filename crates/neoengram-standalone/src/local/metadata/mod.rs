//! SQLite-backed local metadata contracts.
//!
//! File payloads live in `ObjectStore`. The mutable Index refers to immutable file Manifests,
//! while Commits refer to immutable, hierarchical Directory objects. Directory and Manifest
//! readers are paged so a mounted snapshot never has to materialize the whole namespace.

use std::{fmt::Debug, num::NonZeroU32, path::Path, sync::Arc};

use anyhow::{ensure, Context, Result};
use neoengram_core::{
    canonical::{DirectoryHasher as CoreDirectoryHasher, ManifestHasher as CoreManifestHasher},
    ChunkRef as CoreChunkRef, Commit as CoreCommit, CommitId, DirectoryEntry as CoreDirectoryEntry,
    DirectoryId, FileRecord as CoreFileRecord, LogicalPath, ManifestId, ObjectId, PathComponent,
};
use serde::{Deserialize, Serialize};

use crate::local::{
    model::{Chunk, ChunkingStrategy, Commit, DirectoryEntry, DirectoryEntryKind},
    STORAGE_TEMP_FILE_PREFIX,
};

mod sqlite;

use sqlite::SqliteMetadataStore;

const MAX_PAGE_SIZE: u32 = 4_096;

pub(crate) type WorkspaceId = [u8; 16];
pub(crate) const DEFAULT_WORKSPACE_ID: WorkspaceId = [0; 16];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkspaceRecord {
    pub(crate) id: WorkspaceId,
    pub(crate) name: String,
    pub(crate) root_path: String,
    pub(crate) head: HeadState,
    pub(crate) base_commit_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PageRequest {
    pub(crate) after: Option<String>,
    pub(crate) limit: NonZeroU32,
}

impl PageRequest {
    pub(crate) fn new(after: Option<String>, limit: u32) -> Result<Self> {
        let limit = NonZeroU32::new(limit).context("分页大小必须大于零")?;
        let request = Self { after, limit };
        request.validate()?;
        Ok(request)
    }

    pub(crate) fn first(limit: u32) -> Result<Self> {
        Self::new(None, limit)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.limit.get() <= MAX_PAGE_SIZE,
            "分页大小不能超过 {MAX_PAGE_SIZE}: {}",
            self.limit
        );
        Ok(())
    }

    pub(crate) fn limit_usize(&self) -> Result<usize> {
        self.validate()?;
        usize::try_from(self.limit.get()).context("当前平台无法表示分页大小")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Page<T> {
    pub(crate) items: Vec<T>,
    pub(crate) next: Option<String>,
}

pub(crate) use neoengram_core::IndexVersion;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManifestRef {
    pub(crate) id: String,
    pub(crate) chunk_count: u64,
    pub(crate) chunking: ChunkingStrategy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectoryRef {
    pub(crate) id: String,
    pub(crate) entry_count: u64,
    pub(crate) file_count: u64,
    pub(crate) total_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileRecord {
    pub(crate) path: String,
    pub(crate) total_size: u64,
    pub(crate) chunk_count: u64,
    pub(crate) manifest_id: String,
}

impl FileRecord {
    pub(crate) fn from_manifest(path: String, total_size: u64, manifest: &ManifestRef) -> Self {
        Self {
            path,
            total_size,
            chunk_count: manifest.chunk_count,
            manifest_id: manifest.id.clone(),
        }
    }
}

pub(crate) trait ManifestReader: Debug {
    fn total_size(&self) -> u64;
    fn chunk_count(&self) -> u64;
    fn chunking(&self) -> ChunkingStrategy;
    fn scan_chunks(&self, request: &PageRequest) -> Result<Page<Chunk>>;

    /// Returns chunks beginning with the Chunk covering `offset`, or the first Chunk after it.
    /// A continuation cursor is the byte offset immediately after the last returned Chunk.
    #[cfg_attr(not(feature = "fuse-mount"), allow(dead_code))]
    fn scan_chunks_from_offset(&self, offset: u64, limit: NonZeroU32) -> Result<Page<Chunk>>;
}

pub(crate) trait FileSetReader: Debug {
    fn get_file(&self, path: &str) -> Result<Option<FileRecord>>;
    fn scan_files(&self, prefix: Option<&str>, request: &PageRequest) -> Result<Page<FileRecord>>;
}

pub(crate) trait IndexReader: FileSetReader {
    fn format_version(&self) -> u32;
    fn version(&self) -> &IndexVersion;
}

pub(crate) trait IndexTxn: IndexReader {
    fn upsert_file(&mut self, file: &FileRecord) -> Result<()>;
    fn delete_prefix(&mut self, prefix: &str) -> Result<u64>;
    fn commit(self: Box<Self>) -> Result<IndexVersion>;
}

pub(crate) trait DirectoryReader: Debug {
    fn entry_count(&self) -> u64;
    fn file_count(&self) -> u64;
    fn total_size(&self) -> u64;
    #[cfg_attr(not(feature = "fuse-mount"), allow(dead_code))]
    fn get_entry(&self, name: &str) -> Result<Option<DirectoryEntry>>;
    fn scan_entries(&self, request: &PageRequest) -> Result<Page<DirectoryEntry>>;
}

pub(crate) trait DirectoryWriter: Debug {
    fn append_entry(&mut self, entry: &DirectoryEntry) -> Result<()>;
    fn finish(self: Box<Self>) -> Result<DirectoryRef>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum HeadState {
    Attached { reference: String },
    Detached { commit_id: String },
}

impl HeadState {
    pub(crate) fn validate(&self) -> Result<()> {
        match self {
            Self::Attached { reference } => validate_reference_name(reference),
            Self::Detached { commit_id } => validate_metadata_object_id(commit_id),
        }
    }
}

fn normalize_head_state(state: &HeadState) -> HeadState {
    match state {
        HeadState::Attached { reference } => HeadState::Attached {
            reference: reference.trim().to_owned(),
        },
        HeadState::Detached { commit_id } => HeadState::Detached {
            commit_id: commit_id.trim().to_owned(),
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredReference {
    pub(crate) name: String,
    pub(crate) target: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReferenceCas {
    Updated,
    Mismatch { actual: Option<String> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HeadCas {
    Updated,
    Mismatch { actual: HeadState },
}

pub(crate) trait MetadataReader: Debug {
    fn read_head_state(&self) -> Result<HeadState>;
    fn get_reference(&self, name: &str) -> Result<Option<String>>;
    fn open_manifest(&self, id: &str) -> Result<Option<Box<dyn ManifestReader + '_>>>;
    fn open_directory(&self, id: &str) -> Result<Option<Box<dyn DirectoryReader + '_>>>;
    fn get_commit(&self, id: &str) -> Result<Option<Commit>>;
}

pub(crate) trait MetadataSnapshot: MetadataReader {
    #[allow(dead_code)]
    fn scan_directory_ids(&self, request: &PageRequest) -> Result<Page<String>>;
    fn scan_manifest_ids(&self, request: &PageRequest) -> Result<Page<String>>;
    fn scan_commit_ids(&self, request: &PageRequest) -> Result<Page<String>>;
    fn scan_references(&self, prefix: &str, request: &PageRequest)
        -> Result<Page<StoredReference>>;
}

/// Repository-wide SQLite lifecycle and integrity operations.
pub(crate) trait RepositoryMetadataStore: Send + Sync {
    fn initialize(&self, head_reference: &str) -> Result<()>;
    fn validate_layout(&self) -> Result<()>;
    fn verify_integrity(&self) -> Result<()>;
}

/// Mutable state for one workspace's staged index.
pub(crate) trait WorkspaceIndexStore: Send + Sync {
    fn read_index(&self) -> Result<Box<dyn IndexReader + '_>>;
    fn begin_index_transaction(
        &self,
        expected: Option<&IndexVersion>,
    ) -> Result<Box<dyn IndexTxn + '_>>;
}

/// Immutable manifests, directories and commits shared by every workspace.
pub(crate) trait ImmutableCatalogStore: MetadataReader + Send + Sync {
    fn snapshot(&self) -> Result<Box<dyn MetadataSnapshot + '_>>;

    fn put_manifest(
        &self,
        total_size: u64,
        chunking: ChunkingStrategy,
        chunks: &mut dyn Iterator<Item = Result<Chunk>>,
    ) -> Result<ManifestRef>;
    fn begin_directory_write(&self) -> Result<Box<dyn DirectoryWriter + '_>>;
    fn put_commit(&self, id: &str, commit: &Commit) -> Result<()>;
}

/// Optimistic publication of HEAD and named references.
pub(crate) trait RefStore: MetadataReader + Send + Sync {
    fn compare_exchange_head(&self, expected: &HeadState, new_state: &HeadState)
        -> Result<HeadCas>;
    fn compare_exchange_reference(
        &self,
        name: &str,
        expected: Option<&str>,
        new_target: Option<&str>,
    ) -> Result<ReferenceCas>;
}

/// Registration and ownership of local writable workspaces.
pub(crate) trait WorkspaceRegistry: Send + Sync {
    fn create_workspace(
        &self,
        name: &str,
        root_path: &str,
        commit_id: &str,
    ) -> Result<WorkspaceRecord>;
    fn attach_workspace_to_branch(
        &self,
        id: WorkspaceId,
        expected_commit_id: &str,
        reference: &str,
    ) -> Result<()>;
    fn get_workspace(&self, name: &str) -> Result<Option<WorkspaceRecord>>;
    fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>>;
    fn remove_workspace(&self, id: WorkspaceId) -> Result<bool>;
}

/// Standalone composition surface used by the repository adapter.
///
/// Keeping this as a pure composition trait lets callers depend on the narrow ports above while
/// the current repository object can still carry one trait object during the migration.
pub(crate) trait MetadataStore:
    MetadataReader
    + RepositoryMetadataStore
    + WorkspaceIndexStore
    + ImmutableCatalogStore
    + RefStore
    + WorkspaceRegistry
{
}

impl<T> MetadataStore for T where
    T: MetadataReader
        + RepositoryMetadataStore
        + WorkspaceIndexStore
        + ImmutableCatalogStore
        + RefStore
        + WorkspaceRegistry
{
}

pub(crate) fn open_workspace_metadata_store(
    metadata_root: &Path,
    workspace_id: WorkspaceId,
) -> Arc<dyn MetadataStore> {
    Arc::new(SqliteMetadataStore::for_workspace(
        metadata_root.to_path_buf(),
        workspace_id,
    ))
}

pub(crate) fn validate_metadata_object_id(id: &str) -> Result<()> {
    ensure!(
        id.len() == 64
            && id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()),
        "元数据对象 ID 不是 64 位小写十六进制: {id}"
    );
    Ok(())
}

pub(crate) fn describe_manifest(
    total_size: u64,
    chunking: ChunkingStrategy,
    chunks: &[Chunk],
) -> Result<ManifestRef> {
    let mut hasher = ManifestHasher::new(total_size, chunking);
    for chunk in chunks {
        hasher.push(chunk)?;
    }
    hasher.finish()
}

pub(crate) struct ManifestHasher {
    hasher: CoreManifestHasher,
    chunking: ChunkingStrategy,
    chunk_count: u64,
}

impl ManifestHasher {
    pub(crate) fn new(total_size: u64, chunking: ChunkingStrategy) -> Self {
        Self {
            hasher: CoreManifestHasher::new(total_size, chunking),
            chunking,
            chunk_count: 0,
        }
    }

    pub(crate) fn push(&mut self, chunk: &Chunk) -> Result<()> {
        self.hasher.push(&core_chunk_ref(chunk)?)?;
        self.chunk_count = self
            .chunk_count
            .checked_add(1)
            .context("Manifest Chunk 数量溢出")?;
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<ManifestRef> {
        Ok(ManifestRef {
            id: self.hasher.finish()?.to_string(),
            chunk_count: self.chunk_count,
            chunking: self.chunking,
        })
    }
}

pub(crate) struct DirectoryHasher {
    hasher: CoreDirectoryHasher,
}

impl DirectoryHasher {
    pub(crate) fn new() -> Self {
        Self {
            hasher: CoreDirectoryHasher::new(),
        }
    }

    pub(crate) fn push(&mut self, entry: &DirectoryEntry) -> Result<()> {
        self.hasher.push(&core_directory_entry(entry)?)?;
        Ok(())
    }

    pub(crate) fn finish(self) -> (String, u64) {
        let entry_count = self.hasher.entry_count();
        (self.hasher.finish().to_string(), entry_count)
    }
}

fn core_chunk_ref(chunk: &Chunk) -> Result<CoreChunkRef> {
    let object_id = chunk
        .hash
        .parse::<ObjectId>()
        .with_context(|| format!("Manifest Chunk ID 无效: {}", chunk.hash))?;
    CoreChunkRef::new(object_id, chunk.offset, chunk.size).map_err(Into::into)
}

fn core_directory_entry(entry: &DirectoryEntry) -> Result<CoreDirectoryEntry> {
    let name = PathComponent::parse(entry.name.clone())
        .with_context(|| format!("Directory 子项名称无效: {}", entry.name))?;
    match entry.kind {
        DirectoryEntryKind::File => Ok(CoreDirectoryEntry::file(
            entry.ordinal,
            name,
            entry
                .target_id
                .parse::<ManifestId>()
                .with_context(|| format!("Manifest ID 无效: {}", entry.target_id))?,
            entry.total_size,
        )),
        DirectoryEntryKind::Directory => {
            let core_entry = CoreDirectoryEntry::directory(
                entry.ordinal,
                name,
                entry
                    .target_id
                    .parse::<DirectoryId>()
                    .with_context(|| format!("Directory ID 无效: {}", entry.target_id))?,
            );
            ensure!(
                entry.total_size == core_entry.total_size,
                "Directory 类型子项不能携带文件大小: {}",
                entry.name
            );
            Ok(core_entry)
        }
    }
}

pub(crate) fn core_file_record(file: &FileRecord) -> Result<CoreFileRecord> {
    CoreFileRecord::new(
        LogicalPath::parse(file.path.clone())?,
        file.manifest_id.parse::<ManifestId>()?,
        file.total_size,
        file.chunk_count,
    )
    .map_err(Into::into)
}

fn core_commit(commit: &Commit) -> Result<CoreCommit> {
    CoreCommit::new(
        commit.root_directory_id.parse::<DirectoryId>()?,
        commit
            .parent
            .as_deref()
            .map(str::parse::<CommitId>)
            .transpose()?,
        commit.message.clone(),
        commit.created_at_unix_ms,
    )
    .map_err(Into::into)
}

pub(crate) fn canonical_commit_id(commit: &Commit) -> Result<String> {
    Ok(core_commit(commit)?.canonical_id()?.to_string())
}

pub(crate) fn validate_directory_entry(entry: &DirectoryEntry) -> Result<()> {
    core_directory_entry(entry)?;
    Ok(())
}

fn validate_reference_name(name: &str) -> Result<()> {
    ensure!(name.starts_with("refs/"), "引用名必须位于 refs/ 下: {name}");
    ensure!(!name.contains('\\'), "引用名必须使用 `/` 分隔: {name}");
    for component in name.split('/') {
        ensure!(
            !component.is_empty() && component != "." && component != "..",
            "引用名未规范化: {name}"
        );
        ensure!(
            !component.starts_with(STORAGE_TEMP_FILE_PREFIX),
            "引用名使用了保留的临时文件前缀: {name}"
        );
        ensure!(
            component
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')),
            "引用名包含非法字符: {name}"
        );
    }
    Ok(())
}

fn validate_reference_prefix(prefix: &str) -> Result<()> {
    if prefix == "refs" {
        return Ok(());
    }
    validate_reference_name(prefix)
}

#[cfg(test)]
mod tests;
