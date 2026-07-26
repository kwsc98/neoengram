use std::{
    fmt::Debug,
    io::{Read, Write},
};

use neoengram_core::{
    ChunkRef, ChunkingStrategy, Commit, CommitId, DirectoryEntry, DirectoryId, LogicalPath,
    ManifestId, ObjectId,
};
pub use neoengram_core::{FileRecord as IndexEntry, IndexVersion, ObjectSpec};
use serde::{Deserialize, Serialize};

use crate::{
    EngineError, EngineResult, ErrorCode, JournalId, JournalRecord, MutationApplication,
    MutationCheckpoint, MutationIdentity, MutationPlan, ProgressSink,
};

pub const MAX_PAGE_SIZE: u32 = 4_096;

/// Opaque keyset cursor owned by the port which emitted it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PageCursor(String);

impl PageCursor {
    pub fn new(value: impl Into<String>) -> EngineResult<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(EngineError::new(
                ErrorCode::InvalidArgument,
                "page cursor cannot be empty",
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn validate(&self) -> EngineResult<()> {
        if self.0.is_empty() {
            return Err(EngineError::new(
                ErrorCode::InvalidArgument,
                "page cursor cannot be empty",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageRequest {
    pub after: Option<PageCursor>,
    pub limit: u32,
}

impl PageRequest {
    pub fn new(after: Option<PageCursor>, limit: u32) -> EngineResult<Self> {
        if !(1..=MAX_PAGE_SIZE).contains(&limit) {
            return Err(EngineError::new(
                ErrorCode::InvalidArgument,
                format!("page limit must be in 1..={MAX_PAGE_SIZE}"),
            ));
        }
        Ok(Self { after, limit })
    }

    pub fn first(limit: u32) -> EngineResult<Self> {
        Self::new(None, limit)
    }

    pub fn validate(&self) -> EngineResult<()> {
        if !(1..=MAX_PAGE_SIZE).contains(&self.limit) {
            return Err(EngineError::new(
                ErrorCode::InvalidArgument,
                format!("page limit must be in 1..={MAX_PAGE_SIZE}"),
            ));
        }
        if let Some(after) = &self.after {
            after.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next: Option<PageCursor>,
}

pub trait IndexSnapshotReader: Debug + Send + Sync {
    fn version(&self) -> &IndexVersion;
    fn get_file(&self, path: &LogicalPath) -> EngineResult<Option<IndexEntry>>;
    fn scan_files(
        &self,
        prefix: Option<&LogicalPath>,
        request: &PageRequest,
    ) -> EngineResult<Page<IndexEntry>>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestMetadata {
    pub id: ManifestId,
    pub total_size: u64,
    pub chunk_count: u64,
    pub chunking: ChunkingStrategy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryMetadata {
    pub id: DirectoryId,
    pub entry_count: u64,
    pub file_count: u64,
    pub total_size: u64,
}

/// Read-only access to centrally authoritative immutable metadata.
pub trait ImmutableCatalogReader: Debug + Send + Sync {
    fn get_commit(&self, id: &CommitId) -> EngineResult<Option<Commit>>;
    fn get_manifest(&self, id: &ManifestId) -> EngineResult<Option<ManifestMetadata>>;
    fn scan_manifest_chunks(
        &self,
        id: &ManifestId,
        request: &PageRequest,
    ) -> EngineResult<Page<ChunkRef>>;
    fn get_directory(&self, id: &DirectoryId) -> EngineResult<Option<DirectoryMetadata>>;
    fn scan_directory_entries(
        &self,
        id: &DirectoryId,
        request: &PageRequest,
    ) -> EngineResult<Page<DirectoryEntry>>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectMetadata {
    pub id: ObjectId,
    pub size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectPutOutcome {
    Created,
    AlreadyPresent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectPage {
    pub objects: Vec<ObjectMetadata>,
    pub next: Option<PageCursor>,
}

/// Local immutable payload store used by an Engine execution context.
pub trait ObjectStore: Debug + Send + Sync {
    fn initialize(&self) -> EngineResult<()>;
    fn validate_layout(&self) -> EngineResult<()>;
    fn put_from(
        &self,
        expected: &ObjectSpec,
        source: &mut dyn Read,
    ) -> EngineResult<ObjectPutOutcome>;
    fn copy_to(&self, expected: &ObjectSpec, target: &mut dyn Write) -> EngineResult<()>;
    fn stat(&self, id: &ObjectId) -> EngineResult<Option<ObjectMetadata>>;
    fn list_page(&self, request: &PageRequest) -> EngineResult<ObjectPage>;
    fn remove(&self, id: &ObjectId) -> EngineResult<bool>;
    fn durability_barrier(&self) -> EngineResult<()>;

    fn verify(&self, expected: &ObjectSpec) -> EngineResult<()> {
        self.copy_to(expected, &mut std::io::sink())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorktreeEntryKind {
    File,
    Directory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileFingerprint {
    pub size: u64,
    pub modified_unix_nanos: Option<u128>,
    pub identity: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorktreeEntry {
    pub path: LogicalPath,
    pub kind: WorktreeEntryKind,
    pub fingerprint: Option<FileFingerprint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "path", rename_all = "snake_case")]
pub enum PathScope {
    Root,
    Path(LogicalPath),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorktreeScanRequest {
    pub scopes: Vec<PathScope>,
}

/// Logical worktree access. Implementations own safe translation to physical paths.
pub trait Worktree: Debug + Send + Sync {
    /// Applies one complete, already validated mutation plan.
    ///
    /// The default implementation executes the portable primitive actions below. Transactional
    /// adapters may override this entry point when the physical worktree requires an operation-
    /// wide backup/rollback protocol, but must return statistics for the complete plan.
    fn apply_mutation(
        &self,
        plan: &MutationPlan,
        catalog: &dyn ImmutableCatalogReader,
        objects: &dyn ObjectStore,
        progress: &dyn ProgressSink,
        failures: &dyn FailureInjector,
    ) -> EngineResult<MutationApplication> {
        crate::mutation::apply_worktree_actions(plan, self, catalog, objects, progress, failures)
    }

    fn inspect(&self, path: &LogicalPath) -> EngineResult<Option<WorktreeEntry>>;
    fn scan_files(
        &self,
        request: &WorktreeScanRequest,
        visitor: &mut dyn FnMut(WorktreeEntry) -> EngineResult<()>,
    ) -> EngineResult<()>;
    fn open_file(&self, path: &LogicalPath) -> EngineResult<Box<dyn Read + Send>>;
    fn create_dir_all(&self, path: &LogicalPath) -> EngineResult<()>;
    fn write_file_atomic(
        &self,
        path: &LogicalPath,
        expected_size: u64,
        source: &mut dyn Read,
    ) -> EngineResult<()>;
    fn remove_file(&self, path: &LogicalPath) -> EngineResult<bool>;
    fn rename_no_replace(
        &self,
        source: &LogicalPath,
        destination: &LogicalPath,
    ) -> EngineResult<()>;
}

pub trait JournalStore: Debug + Send + Sync {
    fn create(&self, journal: &JournalRecord) -> EngineResult<()>;
    fn load(&self, id: &JournalId) -> EngineResult<Option<JournalRecord>>;
    fn compare_exchange(&self, expected_revision: u64, journal: &JournalRecord)
        -> EngineResult<()>;
    fn list_active(&self) -> EngineResult<Vec<JournalRecord>>;
    fn remove(&self, id: &JournalId) -> EngineResult<bool>;
}

pub trait MutationGuard: Debug + Send {
    fn identity(&self) -> &MutationIdentity;
    fn checkpoint(&self, checkpoint: MutationCheckpoint) -> EngineResult<()>;
}

pub trait LockManager: Debug + Send + Sync {
    fn acquire(&self, identity: &MutationIdentity) -> EngineResult<Box<dyn MutationGuard>>;
}

pub trait Clock: Debug + Send + Sync {
    fn now_unix_ms(&self) -> EngineResult<u64>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FailurePoint {
    BeforeScan,
    AfterScan,
    BeforeObjectPublication,
    AfterObjectPublication,
    BeforeCandidateSeal,
    BeforeJournalPublish,
    BeforeMutation,
    BeforeReceipt,
    /// A stable adapter-specific point used by deterministic integration tests.
    Named(&'static str),
}

pub trait FailureInjector: Debug + Send + Sync {
    fn check(&self, point: FailurePoint) -> EngineResult<()>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopFailureInjector;

impl FailureInjector for NoopFailureInjector {
    fn check(&self, _point: FailurePoint) -> EngineResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_request_enforces_bounded_nonzero_pages() {
        assert!(PageRequest::first(1).is_ok());
        assert!(PageRequest::first(MAX_PAGE_SIZE).is_ok());
        assert_eq!(
            PageRequest::first(0).unwrap_err().code(),
            ErrorCode::InvalidArgument
        );
        assert_eq!(
            PageRequest::first(MAX_PAGE_SIZE + 1).unwrap_err().code(),
            ErrorCode::InvalidArgument
        );
        assert_eq!(
            PageRequest {
                after: None,
                limit: 0,
            }
            .validate()
            .unwrap_err()
            .code(),
            ErrorCode::InvalidArgument
        );
    }
}
