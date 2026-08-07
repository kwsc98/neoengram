use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use neoengram_agent::{AgentError, AgentErrorCode, AgentResult};
use neoengram_core::{ContentDigest, IndexVersion, ObjectId};
use neoengram_engine::{ObjectSpec, ObjectStore};
use neoengram_fs::{sync_directory, LooseObjectStore, VerifiedRoot};
use neoengram_protocol::{
    AgentId, AgentMountId, MountGeneration, OwnerGeneration, SessionGeneration, SnapshotId,
    SnapshotMountAssignment, StorageVolumeId, TenantId,
};
use serde::{Deserialize, Serialize};

use crate::{execution::artifact_object_store, ExecutionBridge, WorkspaceMaterializationSnapshot};

const SNAPSHOT_STATE_FORMAT: u32 = 1;
const SNAPSHOT_STATE_DIRECTORY: &str = "snapshot-mounts";
const MAX_SNAPSHOT_STATE_BYTES: u64 = 256 * 1024 * 1024;

/// Counts verified immutable inputs exposed by one read-only Snapshot mount.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SnapshotMountStats {
    pub files: u64,
    pub bytes: u64,
    pub objects: u64,
}

#[derive(Debug)]
pub(crate) enum SnapshotRecoveryOutcome {
    Mounted {
        assignment: SnapshotMountAssignment,
        stats: SnapshotMountStats,
    },
    Failed {
        assignment: Option<SnapshotMountAssignment>,
        code: AgentErrorCode,
        message: String,
    },
}

#[derive(Debug, Clone)]
struct SnapshotMountBinding {
    agent_id: AgentId,
    tenant_id: TenantId,
    storage_volume_id: StorageVolumeId,
    agent_mount_id: AgentMountId,
    mount_generation: MountGeneration,
    owner_generation: OwnerGeneration,
    session_generation: SessionGeneration,
}

trait SnapshotMountGuard: std::fmt::Debug + Send {
    fn is_live(&self, _mountpoint: &Path) -> AgentResult<bool> {
        Ok(true)
    }
}

trait SnapshotMountBackend: std::fmt::Debug + Send + Sync {
    fn mount(
        &self,
        mountpoint: &Path,
        commit_id: ContentDigest,
        snapshot: WorkspaceMaterializationSnapshot,
        objects: LooseObjectStore,
    ) -> AgentResult<Box<dyn SnapshotMountGuard>>;
}

#[derive(Debug)]
struct MountedSnapshot {
    assignment: SnapshotMountAssignment,
    stats: SnapshotMountStats,
    mountpoint: PathBuf,
    guard: Box<dyn SnapshotMountGuard>,
}

/// Owns persistent Snapshot desires and the live FUSE background sessions for one Agent Volume.
///
/// The assignment is synchronized to `state_dir` before mounting. Dropping the manager drops all
/// FUSE guards, which unmounts every live Snapshot; constructing a new manager and calling
/// [`Self::recover`] replays the persisted assignments against current owner fences.
pub struct SnapshotMountManager {
    mount_root: PathBuf,
    store: SnapshotMountStore,
    binding: SnapshotMountBinding,
    bridge: Arc<dyn ExecutionBridge>,
    backend: Arc<dyn SnapshotMountBackend>,
    mounted: Mutex<BTreeMap<SnapshotId, MountedSnapshot>>,
}

impl std::fmt::Debug for SnapshotMountManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SnapshotMountManager")
            .field("mount_root", &self.mount_root)
            .field("store", &self.store)
            .field("binding", &self.binding)
            .finish_non_exhaustive()
    }
}

impl SnapshotMountManager {
    #[allow(clippy::too_many_arguments)]
    pub fn for_binding(
        mount_root: impl Into<PathBuf>,
        state_root: impl AsRef<Path>,
        agent_id: AgentId,
        tenant_id: TenantId,
        storage_volume_id: StorageVolumeId,
        agent_mount_id: AgentMountId,
        mount_generation: MountGeneration,
        owner_generation: OwnerGeneration,
        session_generation: SessionGeneration,
        bridge: Arc<dyn ExecutionBridge>,
    ) -> AgentResult<Self> {
        Ok(Self {
            mount_root: mount_root.into(),
            store: SnapshotMountStore::open(state_root.as_ref().join(SNAPSHOT_STATE_DIRECTORY))?,
            binding: SnapshotMountBinding {
                agent_id,
                tenant_id,
                storage_volume_id,
                agent_mount_id,
                mount_generation,
                owner_generation,
                session_generation,
            },
            bridge,
            backend: Arc::new(PlatformSnapshotMountBackend),
            mounted: Mutex::new(BTreeMap::new()),
        })
    }

    #[cfg(test)]
    fn with_backend(
        mount_root: impl Into<PathBuf>,
        state_root: impl AsRef<Path>,
        binding: SnapshotMountBinding,
        bridge: Arc<dyn ExecutionBridge>,
        backend: Arc<dyn SnapshotMountBackend>,
    ) -> AgentResult<Self> {
        Ok(Self {
            mount_root: mount_root.into(),
            store: SnapshotMountStore::open(state_root.as_ref().join(SNAPSHOT_STATE_DIRECTORY))?,
            binding,
            bridge,
            backend,
            mounted: Mutex::new(BTreeMap::new()),
        })
    }

    #[must_use]
    pub const fn session_generation(&self) -> SessionGeneration {
        self.binding.session_generation
    }

    pub fn validate_assignment(&self, assignment: &SnapshotMountAssignment) -> AgentResult<()> {
        assignment.validate().map_err(|error| {
            AgentError::new(
                AgentErrorCode::InvalidAssignment,
                format!("invalid Snapshot mount assignment: {error}"),
            )
        })?;
        if assignment.agent_id != self.binding.agent_id
            || assignment.tenant_id != self.binding.tenant_id
            || assignment.storage_volume_id != self.binding.storage_volume_id
            || assignment.agent_mount_id != self.binding.agent_mount_id
        {
            return Err(AgentError::new(
                AgentErrorCode::ScopeMismatch,
                "Snapshot mount assignment does not match the approved Agent mount scope",
            ));
        }
        if assignment.mount_generation != self.binding.mount_generation
            || assignment.owner_generation != self.binding.owner_generation
        {
            return Err(AgentError::new(
                AgentErrorCode::GenerationMismatch,
                "Snapshot mount assignment carries a stale mount or owner generation",
            ));
        }
        Ok(())
    }

    pub fn is_mounted(&self, assignment: &SnapshotMountAssignment) -> AgentResult<bool> {
        let mounted = self.lock_mounted()?;
        let Some(mounted) = mounted.get(&assignment.snapshot_id) else {
            return Ok(false);
        };
        if mounted.assignment != *assignment {
            return Ok(false);
        }
        mounted.guard.is_live(&mounted.mountpoint)
    }

    pub(crate) fn mounted(
        &self,
    ) -> AgentResult<Vec<(SnapshotMountAssignment, SnapshotMountStats)>> {
        let mounted = self.lock_mounted()?;
        let mut live = Vec::new();
        for mounted in mounted.values() {
            if mounted.guard.is_live(&mounted.mountpoint)? {
                live.push((mounted.assignment.clone(), mounted.stats));
            }
        }
        Ok(live)
    }

    /// Verifies and mounts one immutable Snapshot. Replaying an exact live assignment is a no-op.
    pub fn mount(&self, assignment: SnapshotMountAssignment) -> AgentResult<SnapshotMountStats> {
        self.validate_assignment(&assignment)?;
        {
            let mounted = self.lock_mounted()?;
            if let Some(existing) = mounted.get(&assignment.snapshot_id) {
                if existing.assignment != assignment {
                    return Err(AgentError::new(
                        AgentErrorCode::AssignmentMismatch,
                        "Snapshot ID is already mounted from another immutable assignment",
                    ));
                }
                return if existing.guard.is_live(&existing.mountpoint)? {
                    Ok(existing.stats)
                } else {
                    Err(snapshot_error("Snapshot FUSE mount is no longer active"))
                };
            }
        }

        let snapshot = self.bridge.snapshot_mount_snapshot(&assignment)?;
        self.mount_local(assignment, snapshot, true)
    }

    /// Remounts every persisted Snapshot whose placement fences still match this Agent owner.
    pub(crate) fn recover(&self) -> AgentResult<Vec<SnapshotRecoveryOutcome>> {
        let mut recovered = Vec::new();
        for entry in self.store.entries()? {
            let (assignment, snapshot) = match entry {
                Ok(value) => value,
                Err(error) => {
                    recovered.push(recovery_failure(None, error));
                    continue;
                }
            };
            if let Err(error) = self.validate_assignment(&assignment) {
                recovered.push(recovery_failure(Some(assignment), error));
                continue;
            }
            match self.mount_local(assignment.clone(), snapshot, false) {
                Ok(stats) => recovered.push(SnapshotRecoveryOutcome::Mounted { assignment, stats }),
                Err(error) => {
                    recovered.push(recovery_failure(Some(assignment), error));
                }
            }
        }
        Ok(recovered)
    }

    fn mount_local(
        &self,
        assignment: SnapshotMountAssignment,
        snapshot: WorkspaceMaterializationSnapshot,
        persist: bool,
    ) -> AgentResult<SnapshotMountStats> {
        if snapshot.version() != &assignment.index_version.clone().into() {
            return Err(AgentError::new(
                AgentErrorCode::ProtocolInvalid,
                "Snapshot metadata differs from the assignment Index fence",
            ));
        }
        let objects = artifact_object_store(
            &self.mount_root,
            &assignment.tenant_id,
            &assignment.artifact_id,
        )?;
        let stats = verify_snapshot_objects(snapshot.files(), &objects)?;
        let mountpoint = self.prepare_mountpoint(&assignment)?;
        if persist {
            self.store.put(&assignment, &snapshot)?;
        }
        let guard = match self
            .backend
            .mount(&mountpoint, assignment.commit_id, snapshot, objects)
        {
            Ok(guard) => guard,
            Err(error) => {
                if persist {
                    let _ = self.store.remove(&assignment.snapshot_id);
                }
                return Err(error);
            }
        };
        self.lock_mounted()?.insert(
            assignment.snapshot_id.clone(),
            MountedSnapshot {
                assignment,
                stats,
                mountpoint,
                guard,
            },
        );
        Ok(stats)
    }

    fn prepare_mountpoint(&self, assignment: &SnapshotMountAssignment) -> AgentResult<PathBuf> {
        let root = VerifiedRoot::open(&self.mount_root).map_err(AgentError::from)?;
        let mountpoint = root
            .create_dir_all(&assignment.relative_root)
            .map_err(AgentError::from)?;
        let metadata = fs::symlink_metadata(&mountpoint).map_err(snapshot_io)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(snapshot_error(
                "Snapshot mountpoint is not an ordinary directory",
            ));
        }
        if fs::read_dir(&mountpoint)
            .map_err(snapshot_io)?
            .next()
            .transpose()
            .map_err(snapshot_io)?
            .is_some()
        {
            return Err(snapshot_error(
                "Snapshot mountpoint must be empty before FUSE mount",
            ));
        }
        Ok(mountpoint)
    }

    fn lock_mounted(
        &self,
    ) -> AgentResult<std::sync::MutexGuard<'_, BTreeMap<SnapshotId, MountedSnapshot>>> {
        self.mounted
            .lock()
            .map_err(|_| snapshot_error("Snapshot mount registry lock is poisoned"))
    }

    /// Removes unhealthy guards and returns each failure exactly once. Persisted descriptors stay
    /// in place so recovery can remount only after the coordinator has observed the failure.
    pub(crate) fn check_health(&self) -> AgentResult<Vec<SnapshotRecoveryOutcome>> {
        let mut mounted = self.lock_mounted()?;
        let mut unhealthy = Vec::new();
        for (snapshot_id, snapshot) in mounted.iter() {
            match snapshot.guard.is_live(&snapshot.mountpoint) {
                Ok(true) => {}
                Ok(false) => unhealthy.push((
                    snapshot_id.clone(),
                    snapshot_error("Snapshot FUSE mount is no longer active"),
                )),
                Err(error) => unhealthy.push((snapshot_id.clone(), error)),
            }
        }
        let mut failures = Vec::with_capacity(unhealthy.len());
        for (snapshot_id, error) in unhealthy {
            let Some(snapshot) = mounted.remove(&snapshot_id) else {
                continue;
            };
            failures.push(recovery_failure(Some(snapshot.assignment), error));
            tracing::warn!(
                snapshot_id = %snapshot_id,
                "discarded an unhealthy read-only Snapshot mount; its persisted descriptor remains recoverable"
            );
        }
        Ok(failures)
    }
}

fn recovery_failure(
    assignment: Option<SnapshotMountAssignment>,
    error: AgentError,
) -> SnapshotRecoveryOutcome {
    SnapshotRecoveryOutcome::Failed {
        assignment,
        code: error.code(),
        message: error.message().to_owned(),
    }
}

fn snapshot_stats(
    files: &[crate::WorkspaceMaterializationFile],
) -> AgentResult<SnapshotMountStats> {
    let mut bytes = 0_u64;
    let mut objects = BTreeSet::new();
    for file in files {
        bytes = bytes
            .checked_add(file.record.total_size)
            .ok_or_else(|| snapshot_error("Snapshot byte count exceeds u64"))?;
        objects.extend(file.manifest.chunks.iter().map(|chunk| chunk.object_id));
    }
    Ok(SnapshotMountStats {
        files: u64::try_from(files.len())
            .map_err(|_| snapshot_error("Snapshot file count exceeds u64"))?,
        bytes,
        objects: u64::try_from(objects.len())
            .map_err(|_| snapshot_error("Snapshot object count exceeds u64"))?,
    })
}

fn verify_snapshot_objects(
    files: &[crate::WorkspaceMaterializationFile],
    objects: &LooseObjectStore,
) -> AgentResult<SnapshotMountStats> {
    let stats = snapshot_stats(files)?;
    let mut specs = BTreeMap::<ObjectId, u64>::new();
    for file in files {
        for chunk in &file.manifest.chunks {
            match specs.insert(chunk.object_id, chunk.size) {
                Some(size) if size != chunk.size => {
                    return Err(AgentError::new(
                        AgentErrorCode::ProtocolInvalid,
                        "Snapshot Manifests disagree about an Object size",
                    ));
                }
                _ => {}
            }
        }
    }
    for (id, size) in specs {
        objects
            .verify(&ObjectSpec::new(id, size))
            .map_err(|error| {
                AgentError::new(
                    AgentErrorCode::ObjectTransferFailed,
                    format!("Snapshot requires missing or corrupt Volume object {id}: {error}"),
                )
            })?;
    }
    Ok(stats)
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredSnapshotMount {
    format: u32,
    assignment: SnapshotMountAssignment,
    index_version: IndexVersion,
    files: Vec<crate::WorkspaceMaterializationFile>,
}

#[derive(Debug)]
struct SnapshotMountStore {
    root: PathBuf,
}

impl SnapshotMountStore {
    fn open(root: PathBuf) -> AgentResult<Self> {
        fs::create_dir_all(&root).map_err(snapshot_io)?;
        let metadata = fs::symlink_metadata(&root).map_err(snapshot_io)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(snapshot_error(
                "Snapshot state root is not an ordinary directory",
            ));
        }
        Ok(Self { root })
    }

    fn path(&self, snapshot_id: &SnapshotId) -> PathBuf {
        self.root.join(format!("{}.json", snapshot_id.as_str()))
    }

    fn put(
        &self,
        assignment: &SnapshotMountAssignment,
        snapshot: &WorkspaceMaterializationSnapshot,
    ) -> AgentResult<()> {
        let payload = serde_json::to_vec(&StoredSnapshotMount {
            format: SNAPSHOT_STATE_FORMAT,
            assignment: assignment.clone(),
            index_version: *snapshot.version(),
            files: snapshot.files().to_vec(),
        })
        .map_err(|error| snapshot_error(format!("failed to encode Snapshot state: {error}")))?;
        if payload.len() as u64 > MAX_SNAPSHOT_STATE_BYTES {
            return Err(snapshot_error("Snapshot state exceeds its size limit"));
        }
        let mut temporary = tempfile::NamedTempFile::new_in(&self.root).map_err(snapshot_io)?;
        temporary.write_all(&payload).map_err(snapshot_io)?;
        temporary.as_file().sync_all().map_err(snapshot_io)?;
        temporary
            .persist(self.path(&assignment.snapshot_id))
            .map_err(|error| snapshot_io(error.error))?;
        sync_directory(&self.root).map_err(AgentError::from)
    }

    #[cfg(test)]
    fn list(
        &self,
    ) -> AgentResult<Vec<(SnapshotMountAssignment, WorkspaceMaterializationSnapshot)>> {
        self.entries()?.into_iter().collect()
    }

    fn entries(
        &self,
    ) -> AgentResult<Vec<AgentResult<(SnapshotMountAssignment, WorkspaceMaterializationSnapshot)>>>
    {
        let mut paths = fs::read_dir(&self.root)
            .map_err(snapshot_io)?
            .map(|entry| entry.map(|entry| entry.path()).map_err(snapshot_io))
            .collect::<AgentResult<Vec<_>>>()?;
        paths.sort();
        Ok(paths.into_iter().map(|path| self.load(&path)).collect())
    }

    fn load(
        &self,
        path: &Path,
    ) -> AgentResult<(SnapshotMountAssignment, WorkspaceMaterializationSnapshot)> {
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            return Err(snapshot_error(
                "Snapshot state root contains an unknown file",
            ));
        }
        let metadata = fs::symlink_metadata(path).map_err(snapshot_io)?;
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.len() > MAX_SNAPSHOT_STATE_BYTES
        {
            return Err(snapshot_error("Snapshot state file is invalid"));
        }
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        File::open(path)
            .map_err(snapshot_io)?
            .take(MAX_SNAPSHOT_STATE_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(snapshot_io)?;
        let stored: StoredSnapshotMount = serde_json::from_slice(&bytes)
            .map_err(|error| snapshot_error(format!("failed to decode Snapshot state: {error}")))?;
        if stored.format != SNAPSHOT_STATE_FORMAT {
            return Err(snapshot_error("Snapshot state format is unsupported"));
        }
        stored.assignment.validate().map_err(|error| {
            snapshot_error(format!("persisted Snapshot assignment is invalid: {error}"))
        })?;
        if self.path(&stored.assignment.snapshot_id) != path {
            return Err(snapshot_error(
                "Snapshot state filename does not match its identity",
            ));
        }
        let snapshot = WorkspaceMaterializationSnapshot::new(stored.index_version, stored.files)?;
        if snapshot.version() != &stored.assignment.index_version.clone().into() {
            return Err(AgentError::new(
                AgentErrorCode::ProtocolInvalid,
                "persisted Snapshot metadata differs from its assignment Index fence",
            ));
        }
        Ok((stored.assignment, snapshot))
    }

    fn remove(&self, snapshot_id: &SnapshotId) -> AgentResult<()> {
        match fs::remove_file(self.path(snapshot_id)) {
            Ok(()) => sync_directory(&self.root).map_err(AgentError::from),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(snapshot_io(error)),
        }
    }
}

#[derive(Debug, Default)]
struct PlatformSnapshotMountBackend;

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod platform {
    use std::{
        collections::{BTreeMap, BTreeSet},
        ffi::OsStr,
        fs,
        path::Path,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    #[cfg(target_os = "macos")]
    use std::{io, process::Command};

    use fuser::{
        consts::FOPEN_KEEP_CACHE, FileAttr, FileType, Filesystem, MountOption, ReplyAttr,
        ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs,
        ReplyWrite, Request, TimeOrNow,
    };
    use libc::{EBADF, EINVAL, EIO, EISDIR, ENOENT, ENOTDIR, EROFS, O_ACCMODE, O_RDONLY, O_TRUNC};
    use neoengram_core::{ContentDigest, Manifest};
    use neoengram_engine::ObjectStore;
    use neoengram_fs::LooseObjectStore;

    use super::{
        snapshot_error, snapshot_io, AgentError, AgentErrorCode, AgentResult, SnapshotMountGuard,
        WorkspaceMaterializationSnapshot,
    };

    const ROOT_INODE: u64 = 1;
    const ATTRIBUTE_TTL: Duration = Duration::from_secs(24 * 60 * 60);
    const BLOCK_SIZE: u64 = 4096;
    const INODE_DOMAIN: &[u8] = b"neoengram-agent-snapshot-fuse-inode-v1";

    #[cfg(target_os = "macos")]
    const MACFUSE_FSKIT_UNAVAILABLE: &str = "macFUSE FSKit is unavailable; enable macFUSE under System Settings > General > Login Items & Extensions > File System Extensions, then retry the Snapshot mount";

    #[cfg(target_os = "linux")]
    pub(super) fn snapshot_mount_is_live(mountpoint: &Path) -> AgentResult<bool> {
        let contents = fs::read_to_string("/proc/self/mountinfo").map_err(|_| {
            snapshot_error("Snapshot mount health could not inspect the kernel mount table")
        })?;
        let target = mountpoint.to_string_lossy();
        Ok(contents.lines().any(|line| {
            let Some((mount_fields, filesystem_fields)) = line.split_once(" - ") else {
                return false;
            };
            let Some(encoded_mountpoint) = mount_fields.split_whitespace().nth(4) else {
                return false;
            };
            let filesystem = filesystem_fields.split_whitespace().next();
            decode_linux_mount_field(encoded_mountpoint) == target
                && filesystem == Some("fuse.neoengram")
        }))
    }

    #[cfg(target_os = "linux")]
    fn decode_linux_mount_field(field: &str) -> String {
        field
            .replace("\\040", " ")
            .replace("\\011", "\t")
            .replace("\\012", "\n")
            .replace("\\134", "\\")
    }

    #[cfg(target_os = "macos")]
    pub(super) fn snapshot_mount_is_live(mountpoint: &Path) -> AgentResult<bool> {
        let output = Command::new("/sbin/mount").output().map_err(|_| {
            snapshot_error("Snapshot mount health could not inspect the kernel mount table")
        })?;
        if !output.status.success() {
            return Err(snapshot_error(
                "Snapshot mount health could not inspect the kernel mount table",
            ));
        }
        let contents = String::from_utf8(output.stdout).map_err(|_| {
            snapshot_error("Snapshot mount health received an invalid kernel mount table")
        })?;
        let target = mountpoint.to_string_lossy();
        Ok(contents
            .lines()
            .any(|line| macos_mount_line_matches(line, &target)))
    }

    #[cfg(target_os = "macos")]
    fn macos_mount_line_matches(line: &str, target: &str) -> bool {
        let Some((_, remainder)) = line.rsplit_once(" on ") else {
            return false;
        };
        let Some(end) = remainder.rfind(" (") else {
            return false;
        };
        &remainder[..end] == target
    }

    #[cfg(target_os = "macos")]
    fn classify_macos_mount_error(error: io::Error) -> AgentError {
        let os_error = error
            .raw_os_error()
            .map_or_else(|| "none".to_owned(), |code| code.to_string());
        let message = match error.kind() {
            io::ErrorKind::PermissionDenied | io::ErrorKind::NotFound => MACFUSE_FSKIT_UNAVAILABLE,
            io::ErrorKind::Unsupported => "macFUSE FSKit is unsupported on this version of macOS",
            io::ErrorKind::TimedOut => "macFUSE FSKit mount readiness timed out",
            _ => "macFUSE FSKit mount failed",
        };
        snapshot_error(format!(
            "{message} (error_kind={:?}, os_error={os_error})",
            error.kind()
        ))
    }

    fn snapshot_mount_options() -> Vec<MountOption> {
        let options = vec![
            MountOption::FSName("neoengram".to_owned()),
            MountOption::Subtype("neoengram".to_owned()),
            MountOption::RO,
            MountOption::DefaultPermissions,
            MountOption::NoDev,
            MountOption::NoSuid,
            MountOption::NoExec,
            MountOption::NoAtime,
        ];
        #[cfg(target_os = "macos")]
        let options = {
            let mut options = options;
            options.push(MountOption::CUSTOM("backend=fskit".to_owned()));
            options.push(MountOption::CUSTOM("quiet".to_owned()));
            options
        };
        options
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum NodeKind {
        File,
        Directory,
    }

    #[derive(Debug, Clone)]
    struct Node {
        inode: u64,
        parent: u64,
        name: String,
        path: String,
        kind: NodeKind,
        size: u64,
    }

    #[derive(Debug)]
    pub(super) struct ReadOnlySnapshotFs {
        nodes: BTreeMap<u64, Node>,
        by_path: BTreeMap<String, u64>,
        children: BTreeMap<u64, Vec<u64>>,
        manifests: BTreeMap<u64, Manifest>,
        objects: LooseObjectStore,
        object_faulted: Arc<AtomicBool>,
        uid: u32,
        gid: u32,
    }

    impl ReadOnlySnapshotFs {
        pub(super) fn new(
            mountpoint: &Path,
            commit_id: ContentDigest,
            snapshot: WorkspaceMaterializationSnapshot,
            objects: LooseObjectStore,
            object_faulted: Arc<AtomicBool>,
        ) -> AgentResult<Self> {
            let metadata = fs::metadata(mountpoint).map_err(snapshot_io)?;
            #[cfg(unix)]
            use std::os::unix::fs::MetadataExt;
            let (uid, gid) = (metadata.uid(), metadata.gid());
            let root = Node {
                inode: ROOT_INODE,
                parent: ROOT_INODE,
                name: String::new(),
                path: String::new(),
                kind: NodeKind::Directory,
                size: 0,
            };
            let mut filesystem = Self {
                nodes: BTreeMap::from([(ROOT_INODE, root)]),
                by_path: BTreeMap::from([(String::new(), ROOT_INODE)]),
                children: BTreeMap::new(),
                manifests: BTreeMap::new(),
                objects,
                object_faulted,
                uid,
                gid,
            };
            for file in snapshot.files() {
                filesystem.insert_file(commit_id, &file.record.path, file.manifest.clone())?;
            }
            for children in filesystem.children.values_mut() {
                children.sort_by(|left, right| {
                    filesystem.nodes[left]
                        .name
                        .cmp(&filesystem.nodes[right].name)
                });
            }
            Ok(filesystem)
        }

        fn insert_file(
            &mut self,
            commit_id: ContentDigest,
            path: &neoengram_core::LogicalPath,
            manifest: Manifest,
        ) -> AgentResult<()> {
            let components = path.components().collect::<Vec<_>>();
            let mut parent = ROOT_INODE;
            let mut current = String::new();
            for (index, component) in components.iter().enumerate() {
                if !current.is_empty() {
                    current.push('/');
                }
                current.push_str(component);
                let is_file = index + 1 == components.len();
                if let Some(existing) = self.by_path.get(&current).copied() {
                    let expected = if is_file {
                        NodeKind::File
                    } else {
                        NodeKind::Directory
                    };
                    if self.nodes[&existing].kind != expected || is_file {
                        return Err(snapshot_error(
                            "Snapshot Index has a file/directory conflict",
                        ));
                    }
                    parent = existing;
                    continue;
                }
                let kind = if is_file {
                    NodeKind::File
                } else {
                    NodeKind::Directory
                };
                let inode = derive_inode(commit_id, &current, kind, self.nodes.keys().copied());
                let node = Node {
                    inode,
                    parent,
                    name: (*component).to_owned(),
                    path: current.clone(),
                    kind,
                    size: if is_file { manifest.total_size } else { 0 },
                };
                self.nodes.insert(inode, node);
                self.by_path.insert(current.clone(), inode);
                self.children.entry(parent).or_default().push(inode);
                if is_file {
                    self.manifests.insert(inode, manifest.clone());
                }
                parent = inode;
            }
            Ok(())
        }

        fn attr(&self, node: &Node) -> FileAttr {
            let (kind, perm, nlink) = match node.kind {
                NodeKind::File => (FileType::RegularFile, 0o444, 1),
                NodeKind::Directory => (FileType::Directory, 0o555, 2),
            };
            FileAttr {
                ino: node.inode,
                size: node.size,
                blocks: node.size.div_ceil(512),
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                crtime: UNIX_EPOCH,
                kind,
                perm,
                nlink,
                uid: self.uid,
                gid: self.gid,
                rdev: 0,
                blksize: BLOCK_SIZE as u32,
                flags: 0,
            }
        }

        #[cfg(test)]
        pub(super) fn inode_for_path(&self, path: &str) -> Option<u64> {
            self.by_path.get(path).copied()
        }

        pub(super) fn read_range(
            &self,
            inode: u64,
            offset: u64,
            size: u32,
        ) -> AgentResult<Vec<u8>> {
            let node = self
                .nodes
                .get(&inode)
                .ok_or_else(|| snapshot_error("inode is absent"))?;
            let manifest = self
                .manifests
                .get(&inode)
                .ok_or_else(|| snapshot_error("inode is not a file"))?;
            if offset >= node.size || size == 0 {
                return Ok(Vec::new());
            }
            let end = offset.saturating_add(u64::from(size)).min(node.size);
            let mut output = Vec::with_capacity((end - offset) as usize);
            for chunk in &manifest.chunks {
                let chunk_end = chunk.offset.saturating_add(chunk.size);
                if chunk_end <= offset || chunk.offset >= end {
                    continue;
                }
                let mut bytes = Vec::with_capacity(chunk.size as usize);
                self.objects
                    .copy_to(&chunk.object_spec(), &mut bytes)
                    .map_err(|error| {
                        self.object_faulted.store(true, Ordering::SeqCst);
                        let _ = error;
                        AgentError::new(
                            AgentErrorCode::ObjectTransferFailed,
                            "Snapshot object data is unavailable",
                        )
                    })?;
                let start = usize::try_from(offset.saturating_sub(chunk.offset))
                    .map_err(|_| snapshot_error("Snapshot read offset exceeds usize"))?;
                let finish = usize::try_from(end.min(chunk_end) - chunk.offset)
                    .map_err(|_| snapshot_error("Snapshot read end exceeds usize"))?;
                output.extend_from_slice(&bytes[start..finish]);
            }
            if output.len() as u64 != end - offset {
                return Err(snapshot_error(
                    "Snapshot Manifest did not cover the requested range",
                ));
            }
            Ok(output)
        }
    }

    fn derive_inode(
        commit_id: ContentDigest,
        path: &str,
        kind: NodeKind,
        used: impl Iterator<Item = u64> + Clone,
    ) -> u64 {
        let used = used.collect::<BTreeSet<_>>();
        for salt in 0_u64.. {
            let mut hasher = blake3::Hasher::new();
            hasher.update(INODE_DOMAIN);
            hasher.update(commit_id.as_bytes());
            hasher.update(&[match kind {
                NodeKind::File => 1,
                NodeKind::Directory => 2,
            }]);
            hasher.update(path.as_bytes());
            hasher.update(&salt.to_le_bytes());
            let mut bytes = [0_u8; 8];
            bytes.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
            let inode = (u64::from_le_bytes(bytes) & i64::MAX as u64).max(2);
            if !used.contains(&inode) {
                return inode;
            }
        }
        unreachable!("u64 inode salt space is exhaustive")
    }

    impl Filesystem for ReadOnlySnapshotFs {
        fn lookup(&mut self, _request: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
            let Some(name) = name.to_str() else {
                reply.error(ENOENT);
                return;
            };
            let Some(parent_node) = self.nodes.get(&parent) else {
                reply.error(ENOENT);
                return;
            };
            if parent_node.kind != NodeKind::Directory {
                reply.error(ENOTDIR);
                return;
            }
            let path = if parent_node.path.is_empty() {
                name.to_owned()
            } else {
                format!("{}/{}", parent_node.path, name)
            };
            match self
                .by_path
                .get(&path)
                .and_then(|inode| self.nodes.get(inode))
            {
                Some(node) => reply.entry(&ATTRIBUTE_TTL, &self.attr(node), 0),
                None => reply.error(ENOENT),
            }
        }

        fn getattr(
            &mut self,
            _request: &Request<'_>,
            inode: u64,
            _handle: Option<u64>,
            reply: ReplyAttr,
        ) {
            match self.nodes.get(&inode) {
                Some(node) => reply.attr(&ATTRIBUTE_TTL, &self.attr(node)),
                None => reply.error(ENOENT),
            }
        }

        fn setattr(
            &mut self,
            _request: &Request<'_>,
            _inode: u64,
            _mode: Option<u32>,
            _uid: Option<u32>,
            _gid: Option<u32>,
            _size: Option<u64>,
            _atime: Option<TimeOrNow>,
            _mtime: Option<TimeOrNow>,
            _ctime: Option<SystemTime>,
            _handle: Option<u64>,
            _crtime: Option<SystemTime>,
            _chgtime: Option<SystemTime>,
            _bkuptime: Option<SystemTime>,
            _flags: Option<u32>,
            reply: ReplyAttr,
        ) {
            reply.error(mutation_errno());
        }

        fn mknod(
            &mut self,
            _request: &Request<'_>,
            _parent: u64,
            _name: &OsStr,
            _mode: u32,
            _umask: u32,
            _rdev: u32,
            reply: ReplyEntry,
        ) {
            reply.error(mutation_errno());
        }

        fn open(&mut self, _request: &Request<'_>, inode: u64, flags: i32, reply: ReplyOpen) {
            let Some(node) = self.nodes.get(&inode) else {
                reply.error(ENOENT);
                return;
            };
            if node.kind == NodeKind::Directory {
                reply.error(EISDIR);
                return;
            }
            if flags & O_ACCMODE != O_RDONLY || flags & O_TRUNC != 0 {
                reply.error(EROFS);
                return;
            }
            reply.opened(inode, FOPEN_KEEP_CACHE);
        }

        fn read(
            &mut self,
            _request: &Request<'_>,
            inode: u64,
            handle: u64,
            offset: i64,
            size: u32,
            _flags: i32,
            _lock_owner: Option<u64>,
            reply: ReplyData,
        ) {
            if inode != handle {
                reply.error(EBADF);
                return;
            }
            if offset < 0 {
                reply.error(EINVAL);
                return;
            }
            match self.read_range(inode, offset as u64, size) {
                Ok(bytes) => reply.data(&bytes),
                Err(_) => reply.error(EIO),
            }
        }

        fn write(
            &mut self,
            _request: &Request<'_>,
            _inode: u64,
            _handle: u64,
            _offset: i64,
            _data: &[u8],
            _write_flags: u32,
            _flags: i32,
            _lock_owner: Option<u64>,
            reply: ReplyWrite,
        ) {
            reply.error(EROFS);
        }

        fn mkdir(
            &mut self,
            _request: &Request<'_>,
            _parent: u64,
            _name: &OsStr,
            _mode: u32,
            _umask: u32,
            reply: ReplyEntry,
        ) {
            reply.error(EROFS);
        }
        fn unlink(
            &mut self,
            _request: &Request<'_>,
            _parent: u64,
            _name: &OsStr,
            reply: ReplyEmpty,
        ) {
            reply.error(EROFS);
        }
        fn rmdir(
            &mut self,
            _request: &Request<'_>,
            _parent: u64,
            _name: &OsStr,
            reply: ReplyEmpty,
        ) {
            reply.error(EROFS);
        }

        fn symlink(
            &mut self,
            _request: &Request<'_>,
            _parent: u64,
            _name: &OsStr,
            _target: &Path,
            reply: ReplyEntry,
        ) {
            reply.error(mutation_errno());
        }

        fn rename(
            &mut self,
            _request: &Request<'_>,
            _parent: u64,
            _name: &OsStr,
            _new_parent: u64,
            _new_name: &OsStr,
            _flags: u32,
            reply: ReplyEmpty,
        ) {
            reply.error(EROFS);
        }

        fn link(
            &mut self,
            _request: &Request<'_>,
            _inode: u64,
            _new_parent: u64,
            _new_name: &OsStr,
            reply: ReplyEntry,
        ) {
            reply.error(mutation_errno());
        }

        fn create(
            &mut self,
            _request: &Request<'_>,
            _parent: u64,
            _name: &OsStr,
            _mode: u32,
            _umask: u32,
            _flags: i32,
            reply: ReplyCreate,
        ) {
            reply.error(mutation_errno());
        }

        fn opendir(&mut self, _request: &Request<'_>, inode: u64, flags: i32, reply: ReplyOpen) {
            let Some(node) = self.nodes.get(&inode) else {
                reply.error(ENOENT);
                return;
            };
            if node.kind != NodeKind::Directory {
                reply.error(ENOTDIR);
                return;
            }
            if flags & O_ACCMODE != O_RDONLY {
                reply.error(EROFS);
                return;
            }
            reply.opened(inode, 0);
        }

        fn readdir(
            &mut self,
            _request: &Request<'_>,
            inode: u64,
            handle: u64,
            offset: i64,
            mut reply: ReplyDirectory,
        ) {
            if inode != handle {
                reply.error(EBADF);
                return;
            }
            if offset < 0 {
                reply.error(EINVAL);
                return;
            }
            let Some(node) = self.nodes.get(&inode) else {
                reply.error(ENOENT);
                return;
            };
            let mut entries = vec![
                (inode, FileType::Directory, "."),
                (node.parent, FileType::Directory, ".."),
            ];
            for child in self.children.get(&inode).into_iter().flatten() {
                let child = &self.nodes[child];
                entries.push((
                    child.inode,
                    match child.kind {
                        NodeKind::File => FileType::RegularFile,
                        NodeKind::Directory => FileType::Directory,
                    },
                    child.name.as_str(),
                ));
            }
            for (index, (entry_inode, kind, name)) in
                entries.into_iter().enumerate().skip(offset as usize)
            {
                if reply.add(entry_inode, (index + 1) as i64, kind, name) {
                    break;
                }
            }
            reply.ok();
        }

        fn statfs(&mut self, _request: &Request<'_>, _inode: u64, reply: ReplyStatfs) {
            let files = self.manifests.len() as u64;
            reply.statfs(
                0,
                0,
                0,
                files + 1,
                0,
                BLOCK_SIZE as u32,
                255,
                BLOCK_SIZE as u32,
            );
        }

        fn setxattr(
            &mut self,
            _request: &Request<'_>,
            _inode: u64,
            _name: &OsStr,
            _value: &[u8],
            _flags: i32,
            _position: u32,
            reply: ReplyEmpty,
        ) {
            reply.error(mutation_errno());
        }

        fn removexattr(
            &mut self,
            _request: &Request<'_>,
            _inode: u64,
            _name: &OsStr,
            reply: ReplyEmpty,
        ) {
            reply.error(mutation_errno());
        }

        fn fallocate(
            &mut self,
            _request: &Request<'_>,
            _inode: u64,
            _handle: u64,
            _offset: i64,
            _length: i64,
            _mode: i32,
            reply: ReplyEmpty,
        ) {
            reply.error(mutation_errno());
        }

        fn copy_file_range(
            &mut self,
            _request: &Request<'_>,
            _inode_in: u64,
            _handle_in: u64,
            _offset_in: i64,
            _inode_out: u64,
            _handle_out: u64,
            _offset_out: i64,
            _length: u64,
            _flags: u32,
            reply: ReplyWrite,
        ) {
            reply.error(mutation_errno());
        }
    }

    const fn mutation_errno() -> i32 {
        EROFS
    }

    #[cfg(test)]
    mod tests {
        use super::mutation_errno;

        #[cfg(target_os = "macos")]
        use std::io;

        #[cfg(target_os = "macos")]
        use neoengram_agent::AgentErrorCode;

        #[cfg(target_os = "macos")]
        use super::{
            classify_macos_mount_error, macos_mount_line_matches, snapshot_mount_options,
            MACFUSE_FSKIT_UNAVAILABLE,
        };

        #[test]
        fn create_and_setattr_use_read_only_filesystem_errno() {
            assert_eq!(mutation_errno(), libc::EROFS);
        }

        #[cfg(target_os = "macos")]
        #[test]
        fn macos_snapshot_mount_uses_fskit_backend() {
            let options = snapshot_mount_options();
            assert!(options.iter().any(
                |option| matches!(option, fuser::MountOption::CUSTOM(value) if value == "backend=fskit")
            ));
            assert!(options.iter().any(
                |option| matches!(option, fuser::MountOption::CUSTOM(value) if value == "quiet")
            ));
        }

        #[cfg(target_os = "macos")]
        #[test]
        fn macos_mount_health_accepts_fskit_source_names() {
            let target = "/Users/example/Desktop/mount/snapshots/snapshot-1";
            assert!(macos_mount_line_matches(
                "neoengram on /Users/example/Desktop/mount/snapshots/snapshot-1 (macfuse-local, local, read-only)",
                target,
            ));
            assert!(macos_mount_line_matches(
                "macfuse://local-volume on /Users/example/Desktop/mount/snapshots/snapshot-1 (fskit, local, read-only)",
                target,
            ));
            assert!(!macos_mount_line_matches(
                "neoengram on /Users/example/Desktop/mount/snapshots/snapshot-2 (macfuse-local, local, read-only)",
                target,
            ));
        }

        #[cfg(target_os = "macos")]
        #[test]
        fn unavailable_macfuse_fskit_returns_sanitized_mount_error() {
            let private_path = "/Users/private/Desktop/mount";
            let error = classify_macos_mount_error(io::Error::new(
                io::ErrorKind::AlreadyExists,
                private_path,
            ));

            assert_eq!(error.code(), AgentErrorCode::MountUnavailable);
            assert!(error.message().contains("macFUSE FSKit mount failed"));
            assert!(error.message().contains("error_kind=AlreadyExists"));
            assert!(error.message().contains("os_error=none"));
            assert!(!error.message().contains(private_path));
        }

        #[cfg(target_os = "macos")]
        #[test]
        fn approval_failure_explains_how_to_enable_macfuse() {
            let error = classify_macos_mount_error(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "not enabled",
            ));

            assert!(error.message().contains(MACFUSE_FSKIT_UNAVAILABLE));
            assert!(!error.message().contains("not enabled"));
        }
    }

    #[derive(Debug)]
    pub(super) struct SnapshotMountSession {
        session: fuser::BackgroundSession,
        object_faulted: Arc<AtomicBool>,
    }

    impl SnapshotMountGuard for SnapshotMountSession {
        fn is_live(&self, mountpoint: &Path) -> AgentResult<bool> {
            if self.object_faulted.load(Ordering::SeqCst) {
                return Err(AgentError::new(
                    AgentErrorCode::ObjectTransferFailed,
                    "Snapshot object data became unavailable while mounted",
                ));
            }
            if self.session.guard.is_finished() {
                return Ok(false);
            }
            snapshot_mount_is_live(mountpoint)
        }
    }

    pub(super) fn mount(
        mountpoint: &Path,
        commit_id: ContentDigest,
        snapshot: WorkspaceMaterializationSnapshot,
        objects: LooseObjectStore,
    ) -> AgentResult<SnapshotMountSession> {
        #[cfg(target_os = "macos")]
        if !Path::new("/Library/Filesystems/macfuse.fs").exists() {
            return Err(snapshot_error("macFUSE is not installed"));
        }
        let object_faulted = Arc::new(AtomicBool::new(false));
        let filesystem = ReadOnlySnapshotFs::new(
            mountpoint,
            commit_id,
            snapshot,
            objects,
            Arc::clone(&object_faulted),
        )?;
        let options = snapshot_mount_options();
        let session = fuser::spawn_mount2(filesystem, mountpoint, &options);
        #[cfg(target_os = "macos")]
        let session = session.map_err(classify_macos_mount_error)?;
        #[cfg(target_os = "linux")]
        let session = session.map_err(snapshot_io)?;
        Ok(SnapshotMountSession {
            session,
            object_faulted,
        })
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl SnapshotMountBackend for PlatformSnapshotMountBackend {
    fn mount(
        &self,
        mountpoint: &Path,
        commit_id: ContentDigest,
        snapshot: WorkspaceMaterializationSnapshot,
        objects: LooseObjectStore,
    ) -> AgentResult<Box<dyn SnapshotMountGuard>> {
        platform::mount(mountpoint, commit_id, snapshot, objects)
            .map(|session| Box::new(session) as Box<dyn SnapshotMountGuard>)
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
impl SnapshotMountBackend for PlatformSnapshotMountBackend {
    fn mount(
        &self,
        _mountpoint: &Path,
        _commit_id: ContentDigest,
        _snapshot: WorkspaceMaterializationSnapshot,
        _objects: LooseObjectStore,
    ) -> AgentResult<Box<dyn SnapshotMountGuard>> {
        Err(snapshot_error(
            "Snapshot FUSE mounts are supported only on Linux and macOS",
        ))
    }
}

fn snapshot_error(message: impl Into<String>) -> AgentError {
    AgentError::new(AgentErrorCode::MountUnavailable, message)
}

fn snapshot_io(error: io::Error) -> AgentError {
    snapshot_error(format!("Snapshot filesystem I/O failed: {error}"))
}

#[cfg(test)]
mod tests {
    use std::{
        io::Cursor,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
    };

    use neoengram_core::{
        ChunkRef, ChunkingStrategy, FileRecord, IndexVersion, LogicalPath, Manifest, ObjectId,
    };
    use neoengram_engine::ObjectStore;
    use neoengram_protocol::{
        AssignmentGeneration, AssignmentId, Extensions, IndexRevision, PrincipalId, PrincipalKind,
        PrincipalRef, ProjectId, ProtocolVersion, SnapshotId, WireIndexVersion,
    };

    use super::*;

    #[derive(Debug)]
    struct NeverBridge;

    impl ExecutionBridge for NeverBridge {
        fn authoritative_index(
            &self,
            _assignment: &neoengram_protocol::AddAssignment,
        ) -> AgentResult<crate::AuthoritativeIndexSnapshot> {
            panic!("recovery must not query the Server")
        }

        fn workspace_materialization_snapshot(
            &self,
            _assignment: &neoengram_protocol::WorkspaceMaterializeAssignment,
        ) -> AgentResult<WorkspaceMaterializationSnapshot> {
            panic!("recovery must not query the Server")
        }

        fn stage_metadata_descriptor(
            &self,
            _assignment: &neoengram_protocol::AddAssignment,
            _descriptor: &neoengram_protocol::MetadataBatchDescriptor,
        ) -> AgentResult<()> {
            panic!("recovery must not stage metadata")
        }

        fn stage_metadata_page(
            &self,
            _assignment: &neoengram_protocol::AddAssignment,
            _page: &neoengram_protocol::MetadataBatchPage,
        ) -> AgentResult<()> {
            panic!("recovery must not stage metadata")
        }

        fn now_unix_ms(&self) -> AgentResult<u64> {
            panic!("recovery must not ask the bridge for time")
        }
    }

    #[derive(Debug)]
    struct CountingBackend(Arc<AtomicUsize>);

    #[derive(Debug)]
    struct TestGuard;

    impl SnapshotMountGuard for TestGuard {}

    impl SnapshotMountBackend for CountingBackend {
        fn mount(
            &self,
            _mountpoint: &Path,
            _commit_id: ContentDigest,
            _snapshot: WorkspaceMaterializationSnapshot,
            _objects: LooseObjectStore,
        ) -> AgentResult<Box<dyn SnapshotMountGuard>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(TestGuard))
        }
    }

    #[derive(Debug)]
    struct HealthBackend {
        mounts: Arc<AtomicUsize>,
        live: Arc<AtomicBool>,
    }

    #[derive(Debug)]
    struct HealthGuard(Arc<AtomicBool>);

    impl SnapshotMountGuard for HealthGuard {
        fn is_live(&self, _mountpoint: &Path) -> AgentResult<bool> {
            Ok(self.0.load(Ordering::SeqCst))
        }
    }

    impl SnapshotMountBackend for HealthBackend {
        fn mount(
            &self,
            _mountpoint: &Path,
            _commit_id: ContentDigest,
            _snapshot: WorkspaceMaterializationSnapshot,
            _objects: LooseObjectStore,
        ) -> AgentResult<Box<dyn SnapshotMountGuard>> {
            self.mounts.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(HealthGuard(Arc::clone(&self.live))))
        }
    }

    fn assignment() -> SnapshotMountAssignment {
        let project_id = ProjectId::new("project-a").unwrap();
        let artifact_id = neoengram_protocol::ArtifactId::new("artifact-a").unwrap();
        let snapshot_id = SnapshotId::new("snapshot-a").unwrap();
        let relative_root = SnapshotMountAssignment::canonical_relative_root(
            &project_id,
            &artifact_id,
            &snapshot_id,
        )
        .unwrap();
        let mut assignment = SnapshotMountAssignment {
            job_id: neoengram_protocol::JobId::new("job-snapshot-a").unwrap(),
            assignment_id: AssignmentId::new("assignment-snapshot-a").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            agent_id: AgentId::new("agent-a").unwrap(),
            principal: PrincipalRef {
                kind: PrincipalKind::Service,
                id: PrincipalId::new("principal-a").unwrap(),
                extensions: Extensions::new(),
            },
            tenant_id: TenantId::new("tenant-a").unwrap(),
            project_id,
            artifact_id,
            snapshot_id,
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            agent_mount_id: AgentMountId::new("mount-a").unwrap(),
            mount_generation: MountGeneration::new(1),
            owner_generation: OwnerGeneration::new(1),
            relative_root,
            commit_id: ContentDigest::from_bytes([0x11; 32]),
            index_version: WireIndexVersion {
                revision: IndexRevision::new(1),
                digest: ContentDigest::from_bytes([0x22; 32]),
                extensions: Extensions::new(),
            },
            request_digest: ContentDigest::from_bytes([0; 32]),
            deadline_unix_ms: neoengram_protocol::UnixMillis::new(10_000),
            extensions: Extensions::new(),
        };
        assignment.request_digest = assignment.computed_request_digest().unwrap();
        assignment
    }

    fn snapshot_with_objects(root: &Path) -> (WorkspaceMaterializationSnapshot, LooseObjectStore) {
        let store = LooseObjectStore::open_or_create(root).unwrap();
        let mut offset = 0_u64;
        let mut chunks = Vec::new();
        for payload in [b"abc".as_slice(), b"defgh".as_slice()] {
            let id = ObjectId::for_bytes(payload);
            let spec = ObjectSpec::new(id, payload.len() as u64);
            store.put_from(&spec, &mut Cursor::new(payload)).unwrap();
            chunks.push(ChunkRef::new(id, offset, payload.len() as u64).unwrap());
            offset += payload.len() as u64;
        }
        let manifest = Manifest::new(offset, ChunkingStrategy::FastCdc, chunks).unwrap();
        let record =
            FileRecord::from_manifest(LogicalPath::parse("nested/file.bin").unwrap(), &manifest)
                .unwrap();
        let version = IndexVersion::from_snapshot(1, std::slice::from_ref(&record)).unwrap();
        (
            WorkspaceMaterializationSnapshot::new(
                version,
                vec![crate::WorkspaceMaterializationFile { record, manifest }],
            )
            .unwrap(),
            store,
        )
    }

    #[test]
    fn persistent_assignment_round_trips_and_rejects_filename_substitution() {
        let temporary = tempfile::tempdir().unwrap();
        let store = SnapshotMountStore::open(temporary.path().join("state")).unwrap();
        let mut assignment = assignment();
        let version = IndexVersion::from_snapshot(1, &[]).unwrap();
        assignment.index_version = version.into();
        assignment.request_digest = assignment.computed_request_digest().unwrap();
        let snapshot = WorkspaceMaterializationSnapshot::new(version, Vec::new()).unwrap();
        store.put(&assignment, &snapshot).unwrap();
        let restored = store.list().unwrap();
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].0, assignment);
        assert_eq!(restored[0].1, snapshot);

        fs::rename(
            store.path(&assignment.snapshot_id),
            store.root.join("snapshot-other.json"),
        )
        .unwrap();
        assert!(store.list().is_err());
    }

    #[test]
    fn preflight_verifies_every_unique_volume_object() {
        let temporary = tempfile::tempdir().unwrap();
        let (snapshot, objects) = snapshot_with_objects(temporary.path());
        let stats = verify_snapshot_objects(snapshot.files(), &objects).unwrap();
        assert_eq!(stats.files, 1);
        assert_eq!(stats.bytes, 8);
        assert_eq!(stats.objects, 2);

        let missing = snapshot.files()[0].manifest.chunks[0].object_id;
        objects.remove(&missing).unwrap();
        assert_eq!(
            verify_snapshot_objects(snapshot.files(), &objects)
                .unwrap_err()
                .code(),
            AgentErrorCode::ObjectTransferFailed
        );
    }

    #[test]
    fn startup_recovery_uses_only_persisted_metadata_and_volume_cas() {
        let temporary = tempfile::tempdir().unwrap();
        let mount_root = temporary.path().join("mount");
        let state_root = temporary.path().join("agent-state");
        fs::create_dir(&mount_root).unwrap();
        let (snapshot, _) = snapshot_with_objects(&temporary.path().join("fixture-objects"));
        let mut assignment = assignment();
        assignment.index_version = (*snapshot.version()).into();
        assignment.request_digest = assignment.computed_request_digest().unwrap();

        let volume_objects =
            artifact_object_store(&mount_root, &assignment.tenant_id, &assignment.artifact_id)
                .unwrap();
        for payload in [b"abc".as_slice(), b"defgh".as_slice()] {
            let spec = ObjectSpec::new(ObjectId::for_bytes(payload), payload.len() as u64);
            volume_objects
                .put_from(&spec, &mut Cursor::new(payload))
                .unwrap();
        }
        let store = SnapshotMountStore::open(state_root.join(SNAPSHOT_STATE_DIRECTORY)).unwrap();
        store.put(&assignment, &snapshot).unwrap();
        fs::write(store.root.join("snapshot-bad.json"), b"{").unwrap();

        let mounts = Arc::new(AtomicUsize::new(0));
        let manager = SnapshotMountManager::with_backend(
            &mount_root,
            &state_root,
            SnapshotMountBinding {
                agent_id: assignment.agent_id.clone(),
                tenant_id: assignment.tenant_id.clone(),
                storage_volume_id: assignment.storage_volume_id.clone(),
                agent_mount_id: assignment.agent_mount_id.clone(),
                mount_generation: assignment.mount_generation,
                owner_generation: assignment.owner_generation,
                session_generation: SessionGeneration::new(7),
            },
            Arc::new(NeverBridge),
            Arc::new(CountingBackend(Arc::clone(&mounts))),
        )
        .unwrap();
        let recovered = manager.recover().unwrap();
        assert_eq!(recovered.len(), 2);
        assert!(matches!(
            recovered.iter().find(|outcome| matches!(outcome, SnapshotRecoveryOutcome::Mounted { .. })).unwrap(),
            SnapshotRecoveryOutcome::Mounted {
                assignment: restored,
                ..
            } if restored == &assignment
        ));
        assert!(recovered.iter().any(|outcome| matches!(
            outcome,
            SnapshotRecoveryOutcome::Failed {
                assignment: None,
                ..
            }
        )));
        assert_eq!(mounts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn unhealthy_guard_is_not_reported_as_mounted_and_can_be_remounted() {
        let temporary = tempfile::tempdir().unwrap();
        let mount_root = temporary.path().join("mount");
        let state_root = temporary.path().join("agent-state");
        fs::create_dir(&mount_root).unwrap();
        let (snapshot, _) = snapshot_with_objects(&temporary.path().join("fixture-objects"));
        let mut assignment = assignment();
        assignment.index_version = (*snapshot.version()).into();
        assignment.request_digest = assignment.computed_request_digest().unwrap();

        let volume_objects =
            artifact_object_store(&mount_root, &assignment.tenant_id, &assignment.artifact_id)
                .unwrap();
        for payload in [b"abc".as_slice(), b"defgh".as_slice()] {
            let spec = ObjectSpec::new(ObjectId::for_bytes(payload), payload.len() as u64);
            volume_objects
                .put_from(&spec, &mut Cursor::new(payload))
                .unwrap();
        }

        let mounts = Arc::new(AtomicUsize::new(0));
        let live = Arc::new(AtomicBool::new(true));
        let manager = SnapshotMountManager::with_backend(
            &mount_root,
            &state_root,
            SnapshotMountBinding {
                agent_id: assignment.agent_id.clone(),
                tenant_id: assignment.tenant_id.clone(),
                storage_volume_id: assignment.storage_volume_id.clone(),
                agent_mount_id: assignment.agent_mount_id.clone(),
                mount_generation: assignment.mount_generation,
                owner_generation: assignment.owner_generation,
                session_generation: SessionGeneration::new(7),
            },
            Arc::new(NeverBridge),
            Arc::new(HealthBackend {
                mounts: Arc::clone(&mounts),
                live: Arc::clone(&live),
            }),
        )
        .unwrap();

        manager
            .mount_local(assignment.clone(), snapshot.clone(), true)
            .unwrap();
        assert!(manager.is_mounted(&assignment).unwrap());
        assert_eq!(manager.mounted().unwrap().len(), 1);

        live.store(false, Ordering::SeqCst);
        assert!(!manager.is_mounted(&assignment).unwrap());
        assert!(manager.mounted().unwrap().is_empty());
        assert_eq!(manager.store.list().unwrap().len(), 1);
        let failures = manager.check_health().unwrap();
        assert!(matches!(
            failures.as_slice(),
            [SnapshotRecoveryOutcome::Failed {
                assignment: Some(failed),
                code: AgentErrorCode::MountUnavailable,
                message,
            }] if failed == &assignment
                && message == "Snapshot FUSE mount is no longer active"
                && !message.contains(mount_root.to_string_lossy().as_ref())
        ));
        assert!(manager.check_health().unwrap().is_empty());

        live.store(true, Ordering::SeqCst);
        manager
            .mount_local(assignment.clone(), snapshot, false)
            .unwrap();
        assert!(manager.is_mounted(&assignment).unwrap());
        assert_eq!(mounts.load(Ordering::SeqCst), 2);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn fuse_view_reads_across_chunk_boundaries_from_local_cas() {
        let temporary = tempfile::tempdir().unwrap();
        let mountpoint = temporary.path().join("mount");
        let objects_root = temporary.path().join("objects");
        fs::create_dir(&mountpoint).unwrap();
        let (snapshot, objects) = snapshot_with_objects(&objects_root);
        let object_faulted = Arc::new(AtomicBool::new(false));
        let filesystem = platform::ReadOnlySnapshotFs::new(
            &mountpoint,
            ContentDigest::from_bytes([0x44; 32]),
            snapshot,
            objects,
            Arc::clone(&object_faulted),
        )
        .unwrap();
        let inode = filesystem.inode_for_path("nested/file.bin").unwrap();
        assert_eq!(filesystem.read_range(inode, 2, 5).unwrap(), b"cdefg");
        assert!(!object_faulted.load(Ordering::SeqCst));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn fuse_read_failure_latches_unavailable_volume_objects() {
        let temporary = tempfile::tempdir().unwrap();
        let mountpoint = temporary.path().join("mount");
        let objects_root = temporary.path().join("objects");
        fs::create_dir(&mountpoint).unwrap();
        let (snapshot, objects) = snapshot_with_objects(&objects_root);
        let object_faulted = Arc::new(AtomicBool::new(false));
        let filesystem = platform::ReadOnlySnapshotFs::new(
            &mountpoint,
            ContentDigest::from_bytes([0x44; 32]),
            snapshot,
            objects,
            Arc::clone(&object_faulted),
        )
        .unwrap();
        let inode = filesystem.inode_for_path("nested/file.bin").unwrap();

        fs::remove_dir_all(&objects_root).unwrap();

        let error = filesystem.read_range(inode, 0, 8).unwrap_err();
        assert_eq!(error.code(), AgentErrorCode::ObjectTransferFailed);
        assert_eq!(error.message(), "Snapshot object data is unavailable");
        assert!(object_faulted.load(Ordering::SeqCst));
    }

    #[test]
    fn bootstrap_protocol_version_remains_v1_fixture_sanity() {
        assert_eq!(ProtocolVersion::V1, neoengram_protocol::PROTOCOL_VERSION_V1);
    }
}
