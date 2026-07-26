use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use fs2::FileExt;
use neoengram_core::{LogicalPath, PathComponent};
use neoengram_engine::{
    Clock, EngineError, EngineResult, ErrorCode, FileFingerprint, JournalId, JournalRecord,
    JournalStore, LockManager, MutationCheckpoint, MutationGuard, MutationIdentity, PathScope,
    Worktree, WorktreeEntry, WorktreeEntryKind, WorktreeScanRequest,
};
use tempfile::NamedTempFile;

use crate::{io_error, rename_no_replace, sync_directory, VerifiedRoot};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

#[derive(Debug, Clone)]
pub struct LocalWorktree {
    root: VerifiedRoot,
}

impl LocalWorktree {
    #[must_use]
    pub const fn new(root: VerifiedRoot) -> Self {
        Self { root }
    }

    #[must_use]
    pub fn root(&self) -> &VerifiedRoot {
        &self.root
    }

    fn entry_for_path(&self, path: LogicalPath, physical: &Path) -> EngineResult<WorktreeEntry> {
        let metadata = fs::symlink_metadata(physical)
            .map_err(|error| io_error(error, "failed to inspect worktree entry"))?;
        if metadata.file_type().is_symlink() {
            return Err(EngineError::new(
                ErrorCode::InvalidPath,
                "worktree entry is a symlink",
            ));
        }
        if metadata.is_file() {
            Ok(WorktreeEntry {
                path,
                kind: WorktreeEntryKind::File,
                fingerprint: Some(file_fingerprint(&metadata)?),
            })
        } else if metadata.is_dir() {
            Ok(WorktreeEntry {
                path,
                kind: WorktreeEntryKind::Directory,
                fingerprint: None,
            })
        } else {
            Err(EngineError::new(
                ErrorCode::InvalidPath,
                "worktree entry is neither a regular file nor a directory",
            ))
        }
    }

    fn collect_directory(
        &self,
        physical: &Path,
        logical_parent: Option<&LogicalPath>,
        output: &mut BTreeMap<LogicalPath, WorktreeEntry>,
    ) -> EngineResult<()> {
        self.root.verify_identity()?;
        let mut entries = fs::read_dir(physical)
            .map_err(|error| io_error(error, "failed to enumerate worktree directory"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| io_error(error, "failed to read worktree directory entry"))?;
        entries.sort_unstable_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            let name = entry.file_name().into_string().map_err(|_| {
                EngineError::new(ErrorCode::InvalidPath, "worktree name is not valid UTF-8")
            })?;
            let component = PathComponent::parse(name)?;
            let logical = logical_parent.map_or_else(
                || LogicalPath::parse(component.as_str()),
                |parent| Ok(parent.join(&component)),
            )?;
            let physical = entry.path();
            let observed = self.entry_for_path(logical.clone(), &physical)?;
            match observed.kind {
                WorktreeEntryKind::File => {
                    output.insert(logical, observed);
                }
                WorktreeEntryKind::Directory => {
                    self.collect_directory(&physical, Some(&logical), output)?;
                }
            }
        }
        Ok(())
    }
}

impl Worktree for LocalWorktree {
    fn inspect(&self, path: &LogicalPath) -> EngineResult<Option<WorktreeEntry>> {
        match self.root.resolve_existing(path) {
            Ok(physical) => self.entry_for_path(path.clone(), &physical).map(Some),
            Err(error) if error.code() == ErrorCode::ResourceNotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn scan_files(
        &self,
        request: &WorktreeScanRequest,
        visitor: &mut dyn FnMut(WorktreeEntry) -> EngineResult<()>,
    ) -> EngineResult<()> {
        let mut files = BTreeMap::new();
        for scope in &request.scopes {
            match scope {
                PathScope::Root => {
                    self.collect_directory(self.root.as_path(), None, &mut files)?;
                }
                PathScope::Path(path) => {
                    let physical = self.root.resolve_existing(path)?;
                    let entry = self.entry_for_path(path.clone(), &physical)?;
                    match entry.kind {
                        WorktreeEntryKind::File => {
                            files.insert(path.clone(), entry);
                        }
                        WorktreeEntryKind::Directory => {
                            self.collect_directory(&physical, Some(path), &mut files)?;
                        }
                    }
                }
            }
        }
        for entry in files.into_values() {
            visitor(entry)?;
        }
        Ok(())
    }

    fn open_file(&self, path: &LogicalPath) -> EngineResult<Box<dyn Read + Send>> {
        let physical = self.root.resolve_existing(path)?;
        let metadata = fs::symlink_metadata(&physical)
            .map_err(|error| io_error(error, "failed to inspect worktree file"))?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(EngineError::new(
                ErrorCode::InvalidPath,
                "worktree path is not an ordinary file",
            ));
        }
        Ok(Box::new(File::open(physical).map_err(|error| {
            io_error(error, "failed to open worktree file")
        })?))
    }

    fn create_dir_all(&self, path: &LogicalPath) -> EngineResult<()> {
        let created = self.root.create_dir_all(path)?;
        let parent = created.parent().ok_or_else(|| {
            EngineError::new(ErrorCode::InvalidPath, "created directory has no parent")
        })?;
        sync_directory(parent)
    }

    fn write_file_atomic(
        &self,
        path: &LogicalPath,
        expected_size: u64,
        source: &mut dyn Read,
    ) -> EngineResult<()> {
        let destination = self.root.resolve_for_create(path)?;
        if destination.try_exists().map_err(EngineError::from)? {
            return Err(EngineError::new(
                ErrorCode::AlreadyExists,
                "worktree destination already exists",
            ));
        }
        let parent = destination.parent().ok_or_else(|| {
            EngineError::new(ErrorCode::InvalidPath, "worktree destination has no parent")
        })?;
        let mut temporary = NamedTempFile::new_in(parent)
            .map_err(|error| io_error(error, "failed to create temporary worktree file"))?;
        copy_exact_size(source, temporary.as_file_mut(), expected_size)?;
        temporary
            .as_file()
            .sync_all()
            .map_err(|error| io_error(error, "failed to synchronize temporary worktree file"))?;
        temporary.persist_noclobber(&destination).map_err(|error| {
            if error.error.kind() == io::ErrorKind::AlreadyExists {
                EngineError::new(ErrorCode::AlreadyExists, "worktree destination won a race")
            } else {
                io_error(error.error, "failed to publish worktree file")
            }
        })?;
        sync_directory(parent)
    }

    fn remove_file(&self, path: &LogicalPath) -> EngineResult<bool> {
        let physical = match self.root.resolve_existing(path) {
            Ok(path) => path,
            Err(error) if error.code() == ErrorCode::ResourceNotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        let metadata = fs::symlink_metadata(&physical)
            .map_err(|error| io_error(error, "failed to inspect worktree removal"))?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(EngineError::new(
                ErrorCode::InvalidPath,
                "worktree removal target is not an ordinary file",
            ));
        }
        fs::remove_file(&physical)
            .map_err(|error| io_error(error, "failed to remove worktree file"))?;
        let parent = physical.parent().ok_or_else(|| {
            EngineError::new(ErrorCode::InvalidPath, "worktree file has no parent")
        })?;
        sync_directory(parent)?;
        Ok(true)
    }

    fn rename_no_replace(
        &self,
        source: &LogicalPath,
        destination: &LogicalPath,
    ) -> EngineResult<()> {
        let source = self.root.resolve_existing(source)?;
        let destination = self.root.resolve_for_create(destination)?;
        rename_no_replace(&source, &destination)
    }
}

/// Durable JSON journal files with per-journal advisory locking and revision CAS.
#[derive(Debug, Clone)]
pub struct FileJournalStore {
    root: VerifiedRoot,
}

impl FileJournalStore {
    pub fn open_or_create(path: impl AsRef<Path>) -> EngineResult<Self> {
        Ok(Self {
            root: VerifiedRoot::create(path)?,
        })
    }

    fn stem(id: &JournalId) -> String {
        blake3::hash(id.as_str().as_bytes()).to_hex().to_string()
    }

    fn journal_path(&self, id: &JournalId) -> PathBuf {
        self.root
            .as_path()
            .join(format!("{}.journal.json", Self::stem(id)))
    }

    fn lock_path(&self, id: &JournalId) -> PathBuf {
        self.root
            .as_path()
            .join(format!("{}.journal.lock", Self::stem(id)))
    }

    fn acquire_lock(&self, id: &JournalId) -> EngineResult<File> {
        id.validate()?;
        self.root.verify_identity()?;
        let path = self.lock_path(id);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|error| io_error(error, "failed to open journal lock"))?;
        file.try_lock_exclusive().map_err(|error| {
            EngineError::new(
                ErrorCode::Conflict,
                "journal is locked by another operation",
            )
            .with_source(error)
        })?;
        Ok(file)
    }

    fn read_record(&self, id: &JournalId) -> EngineResult<Option<JournalRecord>> {
        id.validate()?;
        let path = self.journal_path(id);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(io_error(error, "failed to inspect journal")),
        };
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(EngineError::new(
                ErrorCode::IntegrityViolation,
                "journal is not an ordinary file",
            ));
        }
        let bytes = fs::read(path).map_err(|error| io_error(error, "failed to read journal"))?;
        let record: JournalRecord = serde_json::from_slice(&bytes).map_err(|error| {
            EngineError::new(ErrorCode::IntegrityViolation, "journal JSON is invalid")
                .with_source(error)
        })?;
        record.validate()?;
        if record.id != *id {
            return Err(EngineError::new(
                ErrorCode::IntegrityViolation,
                "journal identity does not match its filename",
            ));
        }
        Ok(Some(record))
    }

    fn encoded(record: &JournalRecord) -> EngineResult<Vec<u8>> {
        let mut bytes = serde_json::to_vec(record).map_err(|error| {
            EngineError::new(ErrorCode::Internal, "failed to encode journal").with_source(error)
        })?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    fn publish_new(&self, record: &JournalRecord) -> EngineResult<()> {
        let destination = self.journal_path(&record.id);
        let mut temporary = NamedTempFile::new_in(self.root.as_path())
            .map_err(|error| io_error(error, "failed to create temporary journal"))?;
        temporary
            .write_all(&Self::encoded(record)?)
            .map_err(|error| io_error(error, "failed to write journal"))?;
        temporary
            .as_file()
            .sync_all()
            .map_err(|error| io_error(error, "failed to synchronize journal"))?;
        temporary.persist_noclobber(destination).map_err(|error| {
            if error.error.kind() == io::ErrorKind::AlreadyExists {
                EngineError::new(ErrorCode::AlreadyExists, "journal already exists")
            } else {
                io_error(error.error, "failed to publish journal")
            }
        })?;
        sync_directory(self.root.as_path())
    }

    fn replace(&self, record: &JournalRecord) -> EngineResult<()> {
        let destination = self.journal_path(&record.id);
        let mut temporary = NamedTempFile::new_in(self.root.as_path())
            .map_err(|error| io_error(error, "failed to create temporary journal"))?;
        temporary
            .write_all(&Self::encoded(record)?)
            .map_err(|error| io_error(error, "failed to write journal"))?;
        temporary
            .as_file()
            .sync_all()
            .map_err(|error| io_error(error, "failed to synchronize journal"))?;
        temporary
            .persist(destination)
            .map_err(|error| io_error(error.error, "failed to replace journal"))?;
        sync_directory(self.root.as_path())
    }

    /// Lists every durable journal record, including records already finalized.
    pub fn list_all(&self) -> EngineResult<Vec<JournalRecord>> {
        self.root.verify_identity()?;
        let mut records = Vec::new();
        for entry in fs::read_dir(self.root.as_path())
            .map_err(|error| io_error(error, "failed to enumerate journals"))?
        {
            let entry = entry.map_err(|error| io_error(error, "failed to read journal entry"))?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                return Err(EngineError::new(
                    ErrorCode::IntegrityViolation,
                    "journal directory entry is not UTF-8",
                ));
            };
            if !name.ends_with(".journal.json") {
                continue;
            }
            let metadata = fs::symlink_metadata(entry.path())
                .map_err(|error| io_error(error, "failed to inspect journal entry"))?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(EngineError::new(
                    ErrorCode::IntegrityViolation,
                    "journal entry is not an ordinary file",
                ));
            }
            let bytes = fs::read(entry.path())
                .map_err(|error| io_error(error, "failed to read journal entry"))?;
            let record: JournalRecord = serde_json::from_slice(&bytes).map_err(|error| {
                EngineError::new(ErrorCode::IntegrityViolation, "journal JSON is invalid")
                    .with_source(error)
            })?;
            record.validate()?;
            if name != format!("{}.journal.json", Self::stem(&record.id)) {
                return Err(EngineError::new(
                    ErrorCode::IntegrityViolation,
                    "journal filename does not match its identity",
                ));
            }
            records.push(record);
        }
        records.sort_unstable_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
        Ok(records)
    }
}

impl JournalStore for FileJournalStore {
    fn create(&self, journal: &JournalRecord) -> EngineResult<()> {
        journal.validate()?;
        if journal.revision != 0 {
            return Err(EngineError::new(
                ErrorCode::InvalidArgument,
                "new journal revision must be zero",
            ));
        }
        let _lock = self.acquire_lock(&journal.id)?;
        self.publish_new(journal)
    }

    fn load(&self, id: &JournalId) -> EngineResult<Option<JournalRecord>> {
        self.root.verify_identity()?;
        self.read_record(id)
    }

    fn compare_exchange(
        &self,
        expected_revision: u64,
        journal: &JournalRecord,
    ) -> EngineResult<()> {
        let _lock = self.acquire_lock(&journal.id)?;
        let current = self
            .read_record(&journal.id)?
            .ok_or_else(|| EngineError::new(ErrorCode::ResourceNotFound, "journal is missing"))?;
        if current.revision != expected_revision {
            return Err(EngineError::new(
                ErrorCode::Conflict,
                "journal revision compare-exchange failed",
            ));
        }
        journal.validate_successor(&current)?;
        self.replace(journal)
    }

    fn list_active(&self) -> EngineResult<Vec<JournalRecord>> {
        Ok(self
            .list_all()?
            .into_iter()
            .filter(|record| record.phase.is_active())
            .collect())
    }

    fn remove(&self, id: &JournalId) -> EngineResult<bool> {
        let _lock = self.acquire_lock(id)?;
        let path = self.journal_path(id);
        match fs::remove_file(path) {
            Ok(()) => {
                sync_directory(self.root.as_path())?;
                Ok(true)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(io_error(error, "failed to remove journal")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LocalLockManager {
    root: VerifiedRoot,
}

impl LocalLockManager {
    pub fn open_or_create(path: impl AsRef<Path>) -> EngineResult<Self> {
        Ok(Self {
            root: VerifiedRoot::create(path)?,
        })
    }

    fn lock_path(&self, identity: &MutationIdentity) -> PathBuf {
        let key = format!(
            "{}\0{}\0{}",
            identity.tenant_id, identity.artifact_id, identity.playground_id
        );
        self.root.as_path().join(format!(
            "{}.mutation.lock",
            blake3::hash(key.as_bytes()).to_hex()
        ))
    }
}

impl LockManager for LocalLockManager {
    fn acquire(&self, identity: &MutationIdentity) -> EngineResult<Box<dyn MutationGuard>> {
        identity.validate()?;
        self.root.verify_identity()?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.lock_path(identity))
            .map_err(|error| io_error(error, "failed to open mutation lock"))?;
        file.try_lock_exclusive().map_err(|error| {
            EngineError::new(
                ErrorCode::Conflict,
                "playground mutation lock is already held",
            )
            .with_source(error)
        })?;
        Ok(Box::new(LocalMutationGuard {
            _file: file,
            identity: identity.clone(),
            root: self.root.clone(),
        }))
    }
}

#[derive(Debug)]
struct LocalMutationGuard {
    _file: File,
    identity: MutationIdentity,
    root: VerifiedRoot,
}

impl MutationGuard for LocalMutationGuard {
    fn identity(&self) -> &MutationIdentity {
        &self.identity
    }

    fn checkpoint(&self, _checkpoint: MutationCheckpoint) -> EngineResult<()> {
        self.root.verify_identity()
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_ms(&self) -> EngineResult<u64> {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| {
                EngineError::new(ErrorCode::Internal, "system time is before Unix epoch")
                    .with_source(error)
            })?;
        u64::try_from(duration.as_millis()).map_err(|_| {
            EngineError::new(
                ErrorCode::Internal,
                "Unix millisecond timestamp exceeds u64",
            )
        })
    }
}

fn file_fingerprint(metadata: &fs::Metadata) -> EngineResult<FileFingerprint> {
    let modified_unix_nanos = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos());
    #[cfg(unix)]
    let identity = Some(format!("{}:{}", metadata.dev(), metadata.ino()));
    #[cfg(not(unix))]
    let identity = None;
    Ok(FileFingerprint {
        size: metadata.len(),
        modified_unix_nanos,
        identity,
    })
}

fn copy_exact_size(
    source: &mut dyn Read,
    target: &mut dyn Write,
    expected_size: u64,
) -> EngineResult<()> {
    let mut remaining = expected_size;
    let mut buffer = [0_u8; 64 * 1024];
    while remaining > 0 {
        let limit = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| EngineError::new(ErrorCode::Internal, "copy length cannot fit usize"))?;
        let read = source
            .read(&mut buffer[..limit])
            .map_err(|error| io_error(error, "failed to read worktree stream"))?;
        if read == 0 {
            return Err(EngineError::new(
                ErrorCode::WorktreeChangedDuringScan,
                "worktree stream ended before its expected size",
            ));
        }
        target
            .write_all(&buffer[..read])
            .map_err(|error| io_error(error, "failed to write worktree stream"))?;
        remaining -= u64::try_from(read)
            .map_err(|_| EngineError::new(ErrorCode::Internal, "read length cannot fit u64"))?;
    }
    let mut extra = [0_u8; 1];
    if source
        .read(&mut extra)
        .map_err(|error| io_error(error, "failed to check worktree stream length"))?
        != 0
    {
        return Err(EngineError::new(
            ErrorCode::WorktreeChangedDuringScan,
            "worktree stream exceeds its expected size",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use neoengram_core::ContentDigest;
    use neoengram_engine::{IndexVersion, JournalPhase};

    use super::*;

    fn identity() -> MutationIdentity {
        MutationIdentity {
            tenant_id: "tenant-a".to_owned(),
            artifact_id: "artifact-a".to_owned(),
            playground_id: "playground-a".to_owned(),
            job_id: "job-a".to_owned(),
            storage_volume_id: "volume-a".to_owned(),
            owner_generation: 1,
            fencing_token: 2,
        }
    }

    fn journal(revision: u64, phase: JournalPhase) -> JournalRecord {
        JournalRecord {
            id: JournalId::new("journal-a").unwrap(),
            revision,
            phase,
            identity: identity(),
            plan_id: "plan-a".to_owned(),
            plan_digest: ContentDigest::from_bytes([3; 32]),
            recovery_payload: Vec::new(),
            updated_at_unix_ms: revision,
        }
    }

    #[test]
    fn local_worktree_scans_and_publishes_without_replace() {
        let temporary = tempfile::tempdir().unwrap();
        let worktree = LocalWorktree::new(VerifiedRoot::open(temporary.path()).unwrap());
        let directory = LogicalPath::parse("data").unwrap();
        let file = LogicalPath::parse("data/model.bin").unwrap();
        worktree.create_dir_all(&directory).unwrap();
        worktree
            .write_file_atomic(&file, 3, &mut Cursor::new(b"abc"))
            .unwrap();
        assert_eq!(
            worktree
                .write_file_atomic(&file, 3, &mut Cursor::new(b"abc"))
                .unwrap_err()
                .code(),
            ErrorCode::AlreadyExists
        );
        let mut observed = Vec::new();
        worktree
            .scan_files(
                &WorktreeScanRequest {
                    scopes: vec![PathScope::Root],
                },
                &mut |entry| {
                    observed.push(entry.path);
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(observed, vec![file]);
    }

    #[test]
    fn file_journal_store_enforces_revision_cas() {
        let temporary = tempfile::tempdir().unwrap();
        let store = FileJournalStore::open_or_create(temporary.path()).unwrap();
        store.create(&journal(0, JournalPhase::Prepared)).unwrap();
        assert_eq!(
            store
                .compare_exchange(1, &journal(2, JournalPhase::Applying))
                .unwrap_err()
                .code(),
            ErrorCode::Conflict
        );
        store
            .compare_exchange(0, &journal(1, JournalPhase::Applying))
            .unwrap();
        assert_eq!(store.list_active().unwrap().len(), 1);
    }

    #[test]
    fn local_lock_excludes_a_second_mutation() {
        let temporary = tempfile::tempdir().unwrap();
        let manager = LocalLockManager::open_or_create(temporary.path()).unwrap();
        let first = manager.acquire(&identity()).unwrap();
        assert_eq!(
            manager.acquire(&identity()).unwrap_err().code(),
            ErrorCode::Conflict
        );
        drop(first);
        manager.acquire(&identity()).unwrap();
    }

    #[test]
    fn core_index_version_is_available_to_local_adapters() {
        let version = IndexVersion::new(1, ContentDigest::from_bytes([1; 32]));
        assert_eq!(version.revision, 1);
    }
}
