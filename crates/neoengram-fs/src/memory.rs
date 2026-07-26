use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    io::{Cursor, Read, Write},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
};

use neoengram_core::{
    ChunkRef, Commit, CommitId, DirectoryEntry, DirectoryId, LogicalPath, ManifestId, ObjectId,
};
use neoengram_engine::{
    Clock, DirectoryMetadata, EngineError, EngineResult, ErrorCode, FailureInjector, FailurePoint,
    FileFingerprint, ImmutableCatalogReader, IndexEntry, IndexSnapshotReader, IndexVersion,
    JournalId, JournalRecord, JournalStore, ManifestMetadata, ObjectMetadata, ObjectPage,
    ObjectPutOutcome, ObjectSpec, ObjectStore, Page, PageCursor, PageRequest, PathScope, Worktree,
    WorktreeEntry, WorktreeEntryKind, WorktreeScanRequest,
};

use crate::objects::copy_and_hash_exact;

#[derive(Debug)]
pub struct MemoryIndexSnapshot {
    version: IndexVersion,
    files: BTreeMap<LogicalPath, IndexEntry>,
}

impl MemoryIndexSnapshot {
    #[must_use]
    pub fn new(version: IndexVersion, files: impl IntoIterator<Item = IndexEntry>) -> Self {
        Self {
            version,
            files: files
                .into_iter()
                .map(|entry| (entry.path.clone(), entry))
                .collect(),
        }
    }
}

impl IndexSnapshotReader for MemoryIndexSnapshot {
    fn version(&self) -> &IndexVersion {
        &self.version
    }

    fn get_file(&self, path: &LogicalPath) -> EngineResult<Option<IndexEntry>> {
        Ok(self.files.get(path).cloned())
    }

    fn scan_files(
        &self,
        prefix: Option<&LogicalPath>,
        request: &PageRequest,
    ) -> EngineResult<Page<IndexEntry>> {
        request.validate()?;
        let filtered = self.files.values().filter(|entry| {
            prefix.is_none_or(|prefix| entry.path == *prefix || prefix.is_ancestor_of(&entry.path))
        });
        page_by_logical_path(filtered.cloned().collect(), request, |entry| &entry.path)
    }
}

type ManifestRecord = (ManifestMetadata, Vec<ChunkRef>);
type DirectoryRecord = (DirectoryMetadata, Vec<DirectoryEntry>);

#[derive(Debug, Default)]
pub struct MemoryCatalog {
    commits: RwLock<BTreeMap<CommitId, Commit>>,
    manifests: RwLock<BTreeMap<ManifestId, ManifestRecord>>,
    directories: RwLock<BTreeMap<DirectoryId, DirectoryRecord>>,
}

impl MemoryCatalog {
    pub fn insert_commit(&self, id: CommitId, commit: Commit) -> EngineResult<()> {
        write_lock(&self.commits)?.insert(id, commit);
        Ok(())
    }

    pub fn insert_manifest(
        &self,
        metadata: ManifestMetadata,
        chunks: Vec<ChunkRef>,
    ) -> EngineResult<()> {
        write_lock(&self.manifests)?.insert(metadata.id, (metadata, chunks));
        Ok(())
    }

    pub fn insert_directory(
        &self,
        metadata: DirectoryMetadata,
        entries: Vec<DirectoryEntry>,
    ) -> EngineResult<()> {
        write_lock(&self.directories)?.insert(metadata.id, (metadata, entries));
        Ok(())
    }
}

impl ImmutableCatalogReader for MemoryCatalog {
    fn get_commit(&self, id: &CommitId) -> EngineResult<Option<Commit>> {
        Ok(read_lock(&self.commits)?.get(id).cloned())
    }

    fn get_manifest(&self, id: &ManifestId) -> EngineResult<Option<ManifestMetadata>> {
        Ok(read_lock(&self.manifests)?
            .get(id)
            .map(|(metadata, _)| metadata.clone()))
    }

    fn scan_manifest_chunks(
        &self,
        id: &ManifestId,
        request: &PageRequest,
    ) -> EngineResult<Page<ChunkRef>> {
        request.validate()?;
        let manifests = read_lock(&self.manifests)?;
        let (_, chunks) = manifests.get(id).ok_or_else(|| {
            EngineError::new(ErrorCode::ResourceNotFound, "manifest is not present")
        })?;
        page_by_offset(chunks, request, |chunk| chunk.offset)
    }

    fn get_directory(&self, id: &DirectoryId) -> EngineResult<Option<DirectoryMetadata>> {
        Ok(read_lock(&self.directories)?
            .get(id)
            .map(|(metadata, _)| metadata.clone()))
    }

    fn scan_directory_entries(
        &self,
        id: &DirectoryId,
        request: &PageRequest,
    ) -> EngineResult<Page<DirectoryEntry>> {
        request.validate()?;
        let directories = read_lock(&self.directories)?;
        let (_, entries) = directories.get(id).ok_or_else(|| {
            EngineError::new(ErrorCode::ResourceNotFound, "directory is not present")
        })?;
        page_by_offset(entries, request, |entry| entry.ordinal)
    }
}

#[derive(Debug, Default)]
pub struct MemoryObjectStore {
    objects: RwLock<BTreeMap<ObjectId, Vec<u8>>>,
}

impl ObjectStore for MemoryObjectStore {
    fn initialize(&self) -> EngineResult<()> {
        Ok(())
    }

    fn validate_layout(&self) -> EngineResult<()> {
        Ok(())
    }

    fn put_from(
        &self,
        expected: &ObjectSpec,
        source: &mut dyn Read,
    ) -> EngineResult<ObjectPutOutcome> {
        if let Some(existing) = read_lock(&self.objects)?.get(&expected.id) {
            verify_bytes(existing, expected)?;
            return Ok(ObjectPutOutcome::AlreadyPresent);
        }
        let capacity = usize::try_from(expected.size).map_err(|_| {
            EngineError::new(ErrorCode::InvalidArgument, "object size cannot fit memory")
        })?;
        let mut bytes = Vec::with_capacity(capacity);
        copy_and_hash_exact(source, &mut bytes, expected, ErrorCode::InvalidArgument)?;
        let outcome = match write_lock(&self.objects)?.entry(expected.id) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(bytes);
                ObjectPutOutcome::Created
            }
            std::collections::btree_map::Entry::Occupied(entry) => {
                verify_bytes(entry.get(), expected)?;
                ObjectPutOutcome::AlreadyPresent
            }
        };
        Ok(outcome)
    }

    fn copy_to(&self, expected: &ObjectSpec, target: &mut dyn Write) -> EngineResult<()> {
        let objects = read_lock(&self.objects)?;
        let bytes = objects.get(&expected.id).ok_or_else(|| {
            EngineError::new(ErrorCode::ObjectMissing, "memory object is missing")
        })?;
        verify_bytes(bytes, expected)?;
        target.write_all(bytes).map_err(EngineError::from)
    }

    fn stat(&self, id: &ObjectId) -> EngineResult<Option<ObjectMetadata>> {
        read_lock(&self.objects)?
            .get(id)
            .map(|bytes| {
                Ok(ObjectMetadata {
                    id: *id,
                    size: u64::try_from(bytes.len()).map_err(|_| {
                        EngineError::new(ErrorCode::Internal, "object length cannot fit u64")
                    })?,
                })
            })
            .transpose()
    }

    fn list_page(&self, request: &PageRequest) -> EngineResult<ObjectPage> {
        request.validate()?;
        let objects = read_lock(&self.objects)?;
        let after = request
            .after
            .as_ref()
            .map(|cursor| cursor.as_str().parse::<ObjectId>())
            .transpose()
            .map_err(EngineError::from)?;
        let limit = usize::try_from(request.limit).map_err(|_| {
            EngineError::new(ErrorCode::InvalidArgument, "page limit cannot fit usize")
        })?;
        let mut iterator = objects
            .iter()
            .filter(|(id, _)| after.is_none_or(|after| **id > after));
        let mut page = Vec::with_capacity(limit);
        for (id, bytes) in iterator.by_ref().take(limit) {
            page.push(ObjectMetadata {
                id: *id,
                size: u64::try_from(bytes.len()).map_err(|_| {
                    EngineError::new(ErrorCode::Internal, "object length cannot fit u64")
                })?,
            });
        }
        let next = if iterator.next().is_some() {
            page.last()
                .map(|object| PageCursor::new(object.id.to_hex()))
                .transpose()?
        } else {
            None
        };
        Ok(ObjectPage {
            objects: page,
            next,
        })
    }

    fn remove(&self, id: &ObjectId) -> EngineResult<bool> {
        Ok(write_lock(&self.objects)?.remove(id).is_some())
    }

    fn durability_barrier(&self) -> EngineResult<()> {
        Ok(())
    }
}

#[derive(Debug, Default)]
struct MemoryWorktreeState {
    files: BTreeMap<LogicalPath, Vec<u8>>,
    directories: BTreeSet<LogicalPath>,
}

#[derive(Debug, Default)]
pub struct MemoryWorktree {
    state: RwLock<MemoryWorktreeState>,
}

impl MemoryWorktree {
    pub fn insert_file(&self, path: LogicalPath, bytes: Vec<u8>) -> EngineResult<()> {
        let mut state = write_lock(&self.state)?;
        add_parent_directories(&mut state.directories, &path);
        state.files.insert(path, bytes);
        Ok(())
    }
}

impl Worktree for MemoryWorktree {
    fn inspect(&self, path: &LogicalPath) -> EngineResult<Option<WorktreeEntry>> {
        let state = read_lock(&self.state)?;
        if let Some(bytes) = state.files.get(path) {
            return Ok(Some(memory_file_entry(path.clone(), bytes)?));
        }
        Ok(state.directories.contains(path).then(|| WorktreeEntry {
            path: path.clone(),
            kind: WorktreeEntryKind::Directory,
            fingerprint: None,
        }))
    }

    fn scan_files(
        &self,
        request: &WorktreeScanRequest,
        visitor: &mut dyn FnMut(WorktreeEntry) -> EngineResult<()>,
    ) -> EngineResult<()> {
        let state = read_lock(&self.state)?;
        for (path, bytes) in &state.files {
            if request
                .scopes
                .iter()
                .any(|scope| scope_contains(scope, path))
            {
                visitor(memory_file_entry(path.clone(), bytes)?)?;
            }
        }
        Ok(())
    }

    fn open_file(&self, path: &LogicalPath) -> EngineResult<Box<dyn Read + Send>> {
        let bytes = read_lock(&self.state)?
            .files
            .get(path)
            .cloned()
            .ok_or_else(|| EngineError::new(ErrorCode::ResourceNotFound, "file is not present"))?;
        Ok(Box::new(Cursor::new(bytes)))
    }

    fn create_dir_all(&self, path: &LogicalPath) -> EngineResult<()> {
        let mut state = write_lock(&self.state)?;
        if state.files.contains_key(path) {
            return Err(EngineError::new(
                ErrorCode::AlreadyExists,
                "a file already occupies the directory path",
            ));
        }
        add_parent_directories(&mut state.directories, path);
        state.directories.insert(path.clone());
        Ok(())
    }

    fn write_file_atomic(
        &self,
        path: &LogicalPath,
        expected_size: u64,
        source: &mut dyn Read,
    ) -> EngineResult<()> {
        let bytes = read_exact_size(source, expected_size)?;
        let mut state = write_lock(&self.state)?;
        if state.files.contains_key(path) || state.directories.contains(path) {
            return Err(EngineError::new(
                ErrorCode::AlreadyExists,
                "worktree destination already exists",
            ));
        }
        add_parent_directories(&mut state.directories, path);
        state.files.insert(path.clone(), bytes);
        Ok(())
    }

    fn remove_file(&self, path: &LogicalPath) -> EngineResult<bool> {
        Ok(write_lock(&self.state)?.files.remove(path).is_some())
    }

    fn rename_no_replace(
        &self,
        source: &LogicalPath,
        destination: &LogicalPath,
    ) -> EngineResult<()> {
        let mut state = write_lock(&self.state)?;
        if state.files.contains_key(destination) || state.directories.contains(destination) {
            return Err(EngineError::new(
                ErrorCode::AlreadyExists,
                "worktree rename destination already exists",
            ));
        }
        let bytes = state.files.remove(source).ok_or_else(|| {
            EngineError::new(
                ErrorCode::ResourceNotFound,
                "worktree rename source is missing",
            )
        })?;
        add_parent_directories(&mut state.directories, destination);
        state.files.insert(destination.clone(), bytes);
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct MemoryJournalStore {
    journals: RwLock<BTreeMap<JournalId, JournalRecord>>,
}

impl JournalStore for MemoryJournalStore {
    fn create(&self, journal: &JournalRecord) -> EngineResult<()> {
        journal.validate()?;
        if journal.revision != 0 {
            return Err(EngineError::new(
                ErrorCode::InvalidArgument,
                "new journal revision must be zero",
            ));
        }
        let mut journals = write_lock(&self.journals)?;
        if journals.contains_key(&journal.id) {
            return Err(EngineError::new(
                ErrorCode::AlreadyExists,
                "journal already exists",
            ));
        }
        journals.insert(journal.id.clone(), journal.clone());
        Ok(())
    }

    fn load(&self, id: &JournalId) -> EngineResult<Option<JournalRecord>> {
        id.validate()?;
        Ok(read_lock(&self.journals)?.get(id).cloned())
    }

    fn compare_exchange(
        &self,
        expected_revision: u64,
        journal: &JournalRecord,
    ) -> EngineResult<()> {
        let mut journals = write_lock(&self.journals)?;
        let existing = journals
            .get(&journal.id)
            .ok_or_else(|| EngineError::new(ErrorCode::ResourceNotFound, "journal is missing"))?;
        if existing.revision != expected_revision {
            return Err(EngineError::new(
                ErrorCode::Conflict,
                "journal revision compare-exchange failed",
            ));
        }
        journal.validate_successor(existing)?;
        journals.insert(journal.id.clone(), journal.clone());
        Ok(())
    }

    fn list_active(&self) -> EngineResult<Vec<JournalRecord>> {
        Ok(read_lock(&self.journals)?
            .values()
            .filter(|journal| journal.phase.is_active())
            .cloned()
            .collect())
    }

    fn remove(&self, id: &JournalId) -> EngineResult<bool> {
        id.validate()?;
        Ok(write_lock(&self.journals)?.remove(id).is_some())
    }
}

#[derive(Debug, Clone)]
pub struct FixedClock {
    now: Arc<AtomicU64>,
}

impl FixedClock {
    #[must_use]
    pub fn new(now_unix_ms: u64) -> Self {
        Self {
            now: Arc::new(AtomicU64::new(now_unix_ms)),
        }
    }

    pub fn set(&self, now_unix_ms: u64) {
        self.now.store(now_unix_ms, Ordering::SeqCst);
    }
}

impl Clock for FixedClock {
    fn now_unix_ms(&self) -> EngineResult<u64> {
        Ok(self.now.load(Ordering::SeqCst))
    }
}

#[derive(Debug, Default)]
pub struct TriggeredFailureInjector {
    armed: Mutex<HashSet<FailurePoint>>,
}

impl TriggeredFailureInjector {
    pub fn arm(&self, point: FailurePoint) -> EngineResult<()> {
        mutex_lock(&self.armed)?.insert(point);
        Ok(())
    }
}

impl FailureInjector for TriggeredFailureInjector {
    fn check(&self, point: FailurePoint) -> EngineResult<()> {
        if mutex_lock(&self.armed)?.remove(&point) {
            return Err(EngineError::new(
                ErrorCode::InjectedFailure,
                "injected engine failure",
            ));
        }
        Ok(())
    }
}

fn page_by_logical_path<T, F>(
    items: Vec<T>,
    request: &PageRequest,
    path: F,
) -> EngineResult<Page<T>>
where
    F: Fn(&T) -> &LogicalPath,
{
    let start = request.after.as_ref().map_or(Ok(0), |cursor| {
        let after = LogicalPath::parse(cursor.as_str())?;
        Ok::<_, EngineError>(items.partition_point(|item| path(item) <= &after))
    })?;
    let limit = usize::try_from(request.limit)
        .map_err(|_| EngineError::new(ErrorCode::InvalidArgument, "page limit cannot fit usize"))?;
    let end = start.saturating_add(limit).min(items.len());
    let has_more = end < items.len();
    let page = items
        .into_iter()
        .skip(start)
        .take(limit)
        .collect::<Vec<_>>();
    let next = if has_more {
        page.last()
            .map(|item| PageCursor::new(path(item).as_str()))
            .transpose()?
    } else {
        None
    };
    Ok(Page { items: page, next })
}

fn page_by_offset<T: Clone, F>(
    items: &[T],
    request: &PageRequest,
    offset: F,
) -> EngineResult<Page<T>>
where
    F: Fn(&T) -> u64,
{
    let start = request.after.as_ref().map_or(Ok(0), |cursor| {
        let after = cursor.as_str().parse::<u64>().map_err(|_| {
            EngineError::new(ErrorCode::InvalidArgument, "offset cursor is invalid")
        })?;
        Ok::<_, EngineError>(items.partition_point(|item| offset(item) <= after))
    })?;
    let limit = usize::try_from(request.limit)
        .map_err(|_| EngineError::new(ErrorCode::InvalidArgument, "page limit cannot fit usize"))?;
    let end = start.saturating_add(limit).min(items.len());
    let page = items[start..end].to_vec();
    let next = if end < items.len() {
        page.last()
            .map(|item| PageCursor::new(offset(item).to_string()))
            .transpose()?
    } else {
        None
    };
    Ok(Page { items: page, next })
}

fn memory_file_entry(path: LogicalPath, bytes: &[u8]) -> EngineResult<WorktreeEntry> {
    Ok(WorktreeEntry {
        path,
        kind: WorktreeEntryKind::File,
        fingerprint: Some(FileFingerprint {
            size: u64::try_from(bytes.len())
                .map_err(|_| EngineError::new(ErrorCode::Internal, "file length cannot fit u64"))?,
            modified_unix_nanos: None,
            identity: Some(ObjectId::for_bytes(bytes).to_hex()),
        }),
    })
}

fn scope_contains(scope: &PathScope, path: &LogicalPath) -> bool {
    match scope {
        PathScope::Root => true,
        PathScope::Path(scope) => scope == path || scope.is_ancestor_of(path),
    }
}

fn add_parent_directories(directories: &mut BTreeSet<LogicalPath>, path: &LogicalPath) {
    let mut parent = path.parent();
    while let Some(path) = parent {
        parent = path.parent();
        directories.insert(path);
    }
}

fn read_exact_size(source: &mut dyn Read, expected_size: u64) -> EngineResult<Vec<u8>> {
    let capacity = usize::try_from(expected_size)
        .map_err(|_| EngineError::new(ErrorCode::InvalidArgument, "file size cannot fit memory"))?;
    let mut bytes = Vec::with_capacity(capacity);
    source.read_to_end(&mut bytes).map_err(EngineError::from)?;
    if bytes.len() != capacity {
        return Err(EngineError::new(
            ErrorCode::WorktreeChangedDuringScan,
            "file length changed while reading",
        ));
    }
    Ok(bytes)
}

fn verify_bytes(bytes: &[u8], expected: &ObjectSpec) -> EngineResult<()> {
    if bytes.len() as u128 != u128::from(expected.size) || ObjectId::for_bytes(bytes) != expected.id
    {
        return Err(EngineError::new(
            ErrorCode::ObjectCorrupt,
            "memory object does not match its specification",
        ));
    }
    Ok(())
}

fn read_lock<T>(lock: &RwLock<T>) -> EngineResult<std::sync::RwLockReadGuard<'_, T>> {
    lock.read()
        .map_err(|_| EngineError::new(ErrorCode::Internal, "memory adapter read lock is poisoned"))
}

fn write_lock<T>(lock: &RwLock<T>) -> EngineResult<std::sync::RwLockWriteGuard<'_, T>> {
    lock.write()
        .map_err(|_| EngineError::new(ErrorCode::Internal, "memory adapter write lock is poisoned"))
}

fn mutex_lock<T>(lock: &Mutex<T>) -> EngineResult<std::sync::MutexGuard<'_, T>> {
    lock.lock()
        .map_err(|_| EngineError::new(ErrorCode::Internal, "memory adapter mutex is poisoned"))
}

#[cfg(test)]
mod tests {
    use neoengram_core::ContentDigest;

    use super::*;

    #[test]
    fn index_snapshot_uses_stable_keyset_pages() {
        let manifest_id = ManifestId::from_bytes([7; 32]);
        let entries = ["a", "b", "c"].map(|path| IndexEntry {
            path: LogicalPath::parse(path).unwrap(),
            total_size: 0,
            chunk_count: 0,
            manifest_id,
        });
        let index = MemoryIndexSnapshot::new(
            IndexVersion::new(1, ContentDigest::from_bytes([1; 32])),
            entries,
        );
        let first = index
            .scan_files(None, &PageRequest::first(2).unwrap())
            .unwrap();
        assert_eq!(first.items.len(), 2);
        let second = index
            .scan_files(None, &PageRequest::new(first.next, 2).unwrap())
            .unwrap();
        assert_eq!(second.items[0].path.as_str(), "c");
    }

    #[test]
    fn memory_worktree_never_replaces_a_destination() {
        let worktree = MemoryWorktree::default();
        let source = LogicalPath::parse("source").unwrap();
        let destination = LogicalPath::parse("destination").unwrap();
        worktree
            .insert_file(source.clone(), b"source".to_vec())
            .unwrap();
        worktree
            .insert_file(destination.clone(), b"destination".to_vec())
            .unwrap();
        assert_eq!(
            worktree
                .rename_no_replace(&source, &destination)
                .unwrap_err()
                .code(),
            ErrorCode::AlreadyExists
        );
    }
}
