use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crate::{AgentError, AgentErrorCode, AgentResult};
use neoengram_domain::core::{ContentDigest, IndexVersion, ObjectId};
use neoengram_domain::protocol::{
    AgentId, AgentMountId, AgentResourceLifecycleAssignment, AgentResourceLifecycleScope,
    AssignmentOperation, DeletionId, JobAssignment, LifecycleGeneration, MountGeneration,
    OwnerGeneration, SessionGeneration, SnapshotDeliveryAssignment, SnapshotDeliveryId, SnapshotId,
    StorageVolumeId, TenantId,
};
use neoengram_runtime::engine::{ObjectSpec, ObjectStore};
use neoengram_runtime::fs::{sync_directory, LooseObjectStore, VerifiedRoot};
use serde::{Deserialize, Serialize};

use crate::{execution::artifact_object_store, ExecutionBridge, WorkspaceMaterializationSnapshot};

const SNAPSHOT_DELIVERY_STATE_FORMAT: u32 = 1;
const SNAPSHOT_DELIVERY_STATE_DIRECTORY: &str = "snapshot-deliveries";
const MAX_SNAPSHOT_STATE_BYTES: u64 = 256 * 1024 * 1024;
const LIFECYCLE_FENCE_FORMAT: u32 = 1;
const LIFECYCLE_FENCE_DIRECTORY: &str = "resource-lifecycle-fences";
const MAX_LIFECYCLE_FENCE_BYTES: u64 = 64 * 1024;

/// Counts verified immutable inputs exposed by one read-only Snapshot mount.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SnapshotMountStats {
    pub files: u64,
    pub bytes: u64,
    pub objects: u64,
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

/// Recovery outcome for a persisted FUSE SnapshotDelivery descriptor.
#[derive(Debug)]
pub enum SnapshotDeliveryRecoveryOutcome {
    Mounted {
        assignment: SnapshotDeliveryAssignment,
        stats: SnapshotMountStats,
    },
    Failed {
        assignment: Option<SnapshotDeliveryAssignment>,
        code: AgentErrorCode,
        message: String,
    },
}

#[derive(Debug)]
struct MountedSnapshotDelivery {
    assignment: SnapshotDeliveryAssignment,
    stats: SnapshotMountStats,
    mountpoint: PathBuf,
    guard: Box<dyn SnapshotMountGuard>,
}

/// FUSE manager for SnapshotDelivery projections.
///
/// SnapshotDelivery has its own durable descriptor namespace and registry, so several delivery
/// modes can coexist for one Snapshot without colliding with another Delivery.
pub struct SnapshotDeliveryMountManager {
    mount_root: PathBuf,
    store: SnapshotDeliveryMountStore,
    lifecycle_fences: SnapshotLifecycleFenceStore,
    binding: SnapshotMountBinding,
    bridge: Arc<dyn ExecutionBridge>,
    backend: Arc<dyn SnapshotMountBackend>,
    mounted: Mutex<BTreeMap<SnapshotDeliveryId, MountedSnapshotDelivery>>,
}

impl std::fmt::Debug for SnapshotDeliveryMountManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SnapshotDeliveryMountManager")
            .field("mount_root", &self.mount_root)
            .field("store", &self.store)
            .field("binding", &self.binding)
            .finish_non_exhaustive()
    }
}

impl SnapshotDeliveryMountManager {
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
            store: SnapshotDeliveryMountStore::open(
                state_root.as_ref().join(SNAPSHOT_DELIVERY_STATE_DIRECTORY),
            )?,
            lifecycle_fences: SnapshotLifecycleFenceStore::open(
                state_root.as_ref().join(LIFECYCLE_FENCE_DIRECTORY),
            )?,
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
            store: SnapshotDeliveryMountStore::open(
                state_root.as_ref().join(SNAPSHOT_DELIVERY_STATE_DIRECTORY),
            )?,
            lifecycle_fences: SnapshotLifecycleFenceStore::open(
                state_root.as_ref().join(LIFECYCLE_FENCE_DIRECTORY),
            )?,
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

    pub fn validate_assignment(&self, assignment: &SnapshotDeliveryAssignment) -> AgentResult<()> {
        assignment.validate().map_err(|error| {
            AgentError::new(
                AgentErrorCode::InvalidAssignment,
                format!("invalid SnapshotDelivery assignment: {error}"),
            )
        })?;
        if assignment.mode != neoengram_domain::protocol::SnapshotDeliveryMode::Fuse {
            return Err(AgentError::new(
                AgentErrorCode::InvalidAssignment,
                "SnapshotDelivery FUSE manager received a non-FUSE assignment",
            ));
        }
        if assignment.agent_id != self.binding.agent_id
            || assignment.tenant_id != self.binding.tenant_id
            || assignment.storage_volume_id != self.binding.storage_volume_id
            || assignment.agent_mount_id != self.binding.agent_mount_id
        {
            return Err(AgentError::new(
                AgentErrorCode::ScopeMismatch,
                "SnapshotDelivery assignment does not match the approved Agent mount scope",
            ));
        }
        if assignment.mount_generation != self.binding.mount_generation
            || assignment.owner_generation != self.binding.owner_generation
        {
            return Err(AgentError::new(
                AgentErrorCode::GenerationMismatch,
                "SnapshotDelivery assignment carries a stale mount or owner generation",
            ));
        }
        Ok(())
    }

    pub fn is_mounted(&self, assignment: &SnapshotDeliveryAssignment) -> AgentResult<bool> {
        self.validate_assignment(assignment)?;
        if self.lifecycle_fences.blocks_delivery(assignment)? {
            return Ok(false);
        }
        let mounted = self.lock_mounted()?;
        let Some(existing) = mounted.get(&assignment.delivery_id) else {
            return Ok(false);
        };
        if existing.assignment != *assignment {
            return Err(AgentError::new(
                AgentErrorCode::AssignmentMismatch,
                "SnapshotDelivery ID is already mounted from another immutable assignment",
            ));
        }
        existing.guard.is_live(&existing.mountpoint)
    }

    pub fn mount(&self, assignment: SnapshotDeliveryAssignment) -> AgentResult<SnapshotMountStats> {
        self.validate_assignment(&assignment)?;
        if self.lifecycle_fences.blocks_delivery(&assignment)? {
            return Err(AgentError::new(
                AgentErrorCode::InvalidState,
                "SnapshotDelivery mount is fenced by a resource lifecycle operation",
            ));
        }
        {
            let mounted = self.lock_mounted()?;
            if let Some(existing) = mounted.get(&assignment.delivery_id) {
                if existing.assignment != assignment {
                    return Err(AgentError::new(
                        AgentErrorCode::AssignmentMismatch,
                        "SnapshotDelivery ID is already mounted from another immutable assignment",
                    ));
                }
                return if existing.guard.is_live(&existing.mountpoint)? {
                    Ok(existing.stats)
                } else {
                    Err(snapshot_error(
                        "SnapshotDelivery FUSE mount is no longer active",
                    ))
                };
            }
        }
        let snapshot = self.bridge.snapshot_delivery_snapshot(&assignment)?;
        if snapshot.version().digest != assignment.source_index_digest {
            return Err(AgentError::new(
                AgentErrorCode::ProtocolInvalid,
                "SnapshotDelivery metadata differs from the assignment Index fence",
            ));
        }
        let objects = artifact_object_store(
            &self.mount_root,
            &assignment.tenant_id,
            &assignment.artifact_id,
        )?;
        let stats = verify_snapshot_objects(snapshot.files(), &objects)?;
        let mountpoint = self.prepare_mountpoint(&assignment)?;
        self.store.put(&assignment, &snapshot)?;
        let guard = match self
            .backend
            .mount(&mountpoint, assignment.commit_id, snapshot, objects)
        {
            Ok(guard) => guard,
            Err(error) => {
                let _ = self.store.remove(&assignment.delivery_id);
                return Err(error);
            }
        };
        self.lock_mounted()?.insert(
            assignment.delivery_id.clone(),
            MountedSnapshotDelivery {
                assignment,
                stats,
                mountpoint,
                guard,
            },
        );
        Ok(stats)
    }

    /// Unmounts one delivery while retaining its descriptor for crash recovery.
    pub fn unmount(&self, assignment: &SnapshotDeliveryAssignment) -> AgentResult<()> {
        self.validate_assignment(assignment)?;
        let mounted = {
            let mut mounted = self.lock_mounted()?;
            if let Some(existing) = mounted.get(&assignment.delivery_id) {
                if existing.assignment != *assignment {
                    return Err(AgentError::new(
                        AgentErrorCode::AssignmentMismatch,
                        "SnapshotDelivery unmount assignment differs from the mounted identity",
                    ));
                }
            }
            mounted.remove(&assignment.delivery_id)
        };
        if let Some(existing) = mounted {
            drop(existing.guard);
            remove_mountpoint(&existing.mountpoint)?;
        }
        Ok(())
    }

    /// Unmounts and permanently removes one delivery descriptor and its empty mount directory.
    pub fn delete(&self, assignment: &SnapshotDeliveryAssignment) -> AgentResult<()> {
        self.unmount(assignment)?;
        self.store.remove(&assignment.delivery_id)
    }

    pub(crate) fn lifecycle_fence(
        &self,
        command: &AgentResourceLifecycleAssignment,
    ) -> AgentResult<()> {
        self.validate_lifecycle_binding(command)?;
        self.lifecycle_fences.put(command)?;
        let mut mounted = self.lock_mounted()?;
        let ids = mounted
            .iter()
            .filter(|(_, entry)| {
                lifecycle_scope_matches_delivery(&command.resource_scope, &entry.assignment)
            })
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for id in ids {
            if let Some(entry) = mounted.remove(&id) {
                drop(entry.guard);
                remove_mountpoint(&entry.mountpoint)?;
            }
        }
        Ok(())
    }

    /// Restores this manager while retaining the shared no-remount fence. The resource lifecycle
    /// executor calls this before restoring Snapshot mounts; the Snapshot manager removes the
    /// fence only after the whole batch has succeeded.
    pub(crate) fn lifecycle_restore_keep_fence(
        &self,
        command: &AgentResourceLifecycleAssignment,
    ) -> AgentResult<()> {
        self.validate_lifecycle_binding(command)?;
        let exact_fence = self.lifecycle_fences.get(command)?;
        if let Some(fence) = &exact_fence {
            self.lifecycle_fences.validate_binding(fence, command)?;
        }
        for entry in self.store.entries()? {
            let (assignment, snapshot) = entry?;
            if !lifecycle_scope_matches_delivery(&command.resource_scope, &assignment) {
                continue;
            }
            if exact_fence.is_none() && self.lifecycle_fences.blocks_delivery(&assignment)? {
                return Err(AgentError::new(
                    AgentErrorCode::InvalidState,
                    "SnapshotDelivery restore is blocked by another resource lifecycle fence",
                ));
            }
            self.validate_assignment(&assignment)?;
            let already_mounted = self
                .lock_mounted()?
                .get(&assignment.delivery_id)
                .is_some_and(|mounted| mounted.assignment == assignment);
            if !already_mounted {
                self.mount_local(assignment, snapshot, false)?;
            }
        }
        Ok(())
    }

    pub(crate) fn lifecycle_restore(
        &self,
        command: &AgentResourceLifecycleAssignment,
    ) -> AgentResult<()> {
        self.lifecycle_fences.require(command)?;
        self.lifecycle_restore_keep_fence(command)?;
        self.lifecycle_fences.remove(command)
    }

    pub(crate) fn lifecycle_purge(
        &self,
        command: &AgentResourceLifecycleAssignment,
    ) -> AgentResult<()> {
        self.lifecycle_fence(command)?;
        for entry in self.store.entries()? {
            let (assignment, _) = entry?;
            if lifecycle_scope_matches_delivery(&command.resource_scope, &assignment) {
                self.store.remove(&assignment.delivery_id)?;
            }
        }
        Ok(())
    }

    pub(crate) fn validate_lifecycle_job_admission(
        &self,
        assignment: &JobAssignment,
    ) -> AgentResult<()> {
        if self.lifecycle_fences.blocks_job(assignment)? {
            return Err(AgentError::new(
                AgentErrorCode::InvalidState,
                "Agent Job is fenced by a resource lifecycle operation",
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn mounted(
        &self,
    ) -> AgentResult<Vec<(SnapshotDeliveryAssignment, SnapshotMountStats)>> {
        let mounted = self.lock_mounted()?;
        let mut live = Vec::new();
        for entry in mounted.values() {
            if !self.lifecycle_fences.blocks_delivery(&entry.assignment)?
                && entry.guard.is_live(&entry.mountpoint)?
            {
                live.push((entry.assignment.clone(), entry.stats));
            }
        }
        Ok(live)
    }

    pub(crate) fn recover(&self) -> AgentResult<Vec<SnapshotDeliveryRecoveryOutcome>> {
        let mut recovered = Vec::new();
        for entry in self.store.entries()? {
            let (assignment, snapshot) = match entry {
                Ok(value) => value,
                Err(error) => {
                    recovered.push(SnapshotDeliveryRecoveryOutcome::Failed {
                        assignment: None,
                        code: error.code(),
                        message: error.message().to_owned(),
                    });
                    continue;
                }
            };
            if let Err(error) = self.validate_assignment(&assignment) {
                recovered.push(SnapshotDeliveryRecoveryOutcome::Failed {
                    assignment: Some(assignment),
                    code: error.code(),
                    message: error.message().to_owned(),
                });
                continue;
            }
            if self.lifecycle_fences.blocks_delivery(&assignment)? {
                continue;
            }
            match self.mount_local(assignment.clone(), snapshot, false) {
                Ok(stats) => {
                    recovered.push(SnapshotDeliveryRecoveryOutcome::Mounted { assignment, stats })
                }
                Err(error) => recovered.push(SnapshotDeliveryRecoveryOutcome::Failed {
                    assignment: Some(assignment),
                    code: error.code(),
                    message: error.message().to_owned(),
                }),
            }
        }
        Ok(recovered)
    }

    fn mount_local(
        &self,
        assignment: SnapshotDeliveryAssignment,
        snapshot: WorkspaceMaterializationSnapshot,
        persist: bool,
    ) -> AgentResult<SnapshotMountStats> {
        self.validate_assignment(&assignment)?;
        if snapshot.version().digest != assignment.source_index_digest {
            return Err(AgentError::new(
                AgentErrorCode::ProtocolInvalid,
                "persisted SnapshotDelivery metadata differs from its Index fence",
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
                    let _ = self.store.remove(&assignment.delivery_id);
                }
                return Err(error);
            }
        };
        self.lock_mounted()?.insert(
            assignment.delivery_id.clone(),
            MountedSnapshotDelivery {
                assignment,
                stats,
                mountpoint,
                guard,
            },
        );
        Ok(stats)
    }

    fn prepare_mountpoint(&self, assignment: &SnapshotDeliveryAssignment) -> AgentResult<PathBuf> {
        let root = VerifiedRoot::open(&self.mount_root).map_err(AgentError::from)?;
        let mountpoint = root
            .create_dir_all(&assignment.target_relative_root)
            .map_err(AgentError::from)?;
        let metadata = fs::symlink_metadata(&mountpoint).map_err(snapshot_io)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(snapshot_error(
                "SnapshotDelivery mountpoint is not an ordinary directory",
            ));
        }
        if fs::read_dir(&mountpoint)
            .map_err(snapshot_io)?
            .next()
            .transpose()
            .map_err(snapshot_io)?
            .is_some()
        {
            return Err(AgentError::new(
                AgentErrorCode::AssignmentMismatch,
                "DELIVERY_TARGET_CONFLICT: SnapshotDelivery mountpoint must be empty",
            ));
        }
        Ok(mountpoint)
    }

    fn lock_mounted(
        &self,
    ) -> AgentResult<std::sync::MutexGuard<'_, BTreeMap<SnapshotDeliveryId, MountedSnapshotDelivery>>>
    {
        self.mounted
            .lock()
            .map_err(|_| snapshot_error("SnapshotDelivery mount registry lock is poisoned"))
    }

    fn validate_lifecycle_binding(
        &self,
        command: &AgentResourceLifecycleAssignment,
    ) -> AgentResult<()> {
        command.validate().map_err(AgentError::from)?;
        if command.agent_id != self.binding.agent_id
            || command.assignment.tenant_id != self.binding.tenant_id
            || command.resource_scope.storage_volume_id() != &self.binding.storage_volume_id
            || command.agent_mount_id != self.binding.agent_mount_id
            || command.session_generation != self.binding.session_generation
            || command.mount_generation != self.binding.mount_generation
            || command.owner_generation != self.binding.owner_generation
        {
            return Err(AgentError::new(
                AgentErrorCode::GenerationMismatch,
                "lifecycle command does not match the live SnapshotDelivery binding",
            ));
        }
        Ok(())
    }
}

fn remove_mountpoint(mountpoint: &Path) -> AgentResult<()> {
    match fs::remove_dir(mountpoint) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(snapshot_io(error)),
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredLifecycleFence {
    format: u32,
    deletion_id: DeletionId,
    tenant_id: TenantId,
    resource_scope: AgentResourceLifecycleScope,
    lifecycle_generation: LifecycleGeneration,
    request_digest: ContentDigest,
}

#[derive(Debug)]
struct SnapshotLifecycleFenceStore {
    root: PathBuf,
}

impl SnapshotLifecycleFenceStore {
    fn open(root: PathBuf) -> AgentResult<Self> {
        fs::create_dir_all(&root).map_err(snapshot_io)?;
        let metadata = fs::symlink_metadata(&root).map_err(snapshot_io)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(snapshot_error(
                "resource lifecycle fence root is not an ordinary directory",
            ));
        }
        Ok(Self { root })
    }

    fn path(&self, scope: &AgentResourceLifecycleScope) -> AgentResult<PathBuf> {
        let digest = neoengram_domain::protocol::jcs_blake3(scope).map_err(AgentError::from)?;
        Ok(self.root.join(format!("{digest}.json")))
    }

    fn put(&self, command: &AgentResourceLifecycleAssignment) -> AgentResult<()> {
        let path = self.path(&command.resource_scope)?;
        if path.exists() {
            self.validate_binding(&self.load(&path)?, command)?;
            return Ok(());
        }
        let stored = StoredLifecycleFence {
            format: LIFECYCLE_FENCE_FORMAT,
            deletion_id: command.assignment.deletion_id.clone(),
            tenant_id: command.assignment.tenant_id.clone(),
            resource_scope: command.resource_scope.clone(),
            lifecycle_generation: command.assignment.lifecycle_generation,
            request_digest: command.assignment.request_digest,
        };
        let payload = serde_json::to_vec(&stored).map_err(|error| {
            snapshot_error(format!("failed to encode lifecycle fence: {error}"))
        })?;
        if payload.len() as u64 > MAX_LIFECYCLE_FENCE_BYTES {
            return Err(snapshot_error(
                "resource lifecycle fence exceeds its size limit",
            ));
        }
        let mut temporary = tempfile::NamedTempFile::new_in(&self.root).map_err(snapshot_io)?;
        temporary.write_all(&payload).map_err(snapshot_io)?;
        temporary.as_file().sync_all().map_err(snapshot_io)?;
        match temporary.persist_noclobber(&path) {
            Ok(_) => {}
            Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
                self.validate_binding(&self.load(&path)?, command)?;
            }
            Err(error) => return Err(snapshot_io(error.error)),
        }
        sync_directory(&self.root).map_err(AgentError::from)
    }

    fn require(
        &self,
        command: &AgentResourceLifecycleAssignment,
    ) -> AgentResult<StoredLifecycleFence> {
        let stored = self.get(command)?.ok_or_else(|| {
            AgentError::new(
                AgentErrorCode::InvalidState,
                "resource lifecycle restore has no durable no-remount fence",
            )
        })?;
        self.validate_binding(&stored, command)?;
        Ok(stored)
    }

    fn get(
        &self,
        command: &AgentResourceLifecycleAssignment,
    ) -> AgentResult<Option<StoredLifecycleFence>> {
        let path = self.path(&command.resource_scope)?;
        match fs::symlink_metadata(&path) {
            Ok(_) => self.load(&path).map(Some),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(snapshot_io(error)),
        }
    }

    fn remove(&self, command: &AgentResourceLifecycleAssignment) -> AgentResult<()> {
        self.require(command)?;
        match fs::remove_file(self.path(&command.resource_scope)?) {
            Ok(()) => sync_directory(&self.root).map_err(AgentError::from),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Err(AgentError::new(
                AgentErrorCode::InvalidState,
                "resource lifecycle fence disappeared during restore",
            )),
            Err(error) => Err(snapshot_io(error)),
        }
    }

    fn blocks_delivery(&self, assignment: &SnapshotDeliveryAssignment) -> AgentResult<bool> {
        Ok(self.entries()?.iter().any(|fence| {
            fence.tenant_id == assignment.tenant_id
                && lifecycle_scope_matches_delivery(&fence.resource_scope, assignment)
        }))
    }

    fn blocks_job(&self, assignment: &JobAssignment) -> AgentResult<bool> {
        let tenant_id = match &assignment.assignment {
            AssignmentOperation::Add { input, .. } => &input.tenant_id,
            AssignmentOperation::WorkspaceMaterialize { input, .. } => &input.tenant_id,
            AssignmentOperation::SnapshotDelivery { input, .. } => &input.tenant_id,
        };
        Ok(self.entries()?.iter().any(|fence| {
            &fence.tenant_id == tenant_id
                && lifecycle_scope_matches_job(&fence.resource_scope, assignment)
        }))
    }

    fn entries(&self) -> AgentResult<Vec<StoredLifecycleFence>> {
        let mut paths = fs::read_dir(&self.root)
            .map_err(snapshot_io)?
            .map(|entry| entry.map(|entry| entry.path()).map_err(snapshot_io))
            .collect::<AgentResult<Vec<_>>>()?;
        paths.sort();
        paths.into_iter().map(|path| self.load(&path)).collect()
    }

    fn load(&self, path: &Path) -> AgentResult<StoredLifecycleFence> {
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            return Err(snapshot_error(
                "resource lifecycle fence root contains an unknown file",
            ));
        }
        let metadata = match fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(AgentError::new(
                    AgentErrorCode::MountUnavailable,
                    "resource lifecycle fence is unavailable",
                ));
            }
            Err(error) => return Err(snapshot_io(error)),
        };
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.len() > MAX_LIFECYCLE_FENCE_BYTES
        {
            return Err(snapshot_error("resource lifecycle fence file is invalid"));
        }
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        File::open(path)
            .map_err(snapshot_io)?
            .take(MAX_LIFECYCLE_FENCE_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(snapshot_io)?;
        let stored: StoredLifecycleFence = serde_json::from_slice(&bytes).map_err(|error| {
            snapshot_error(format!("failed to decode lifecycle fence: {error}"))
        })?;
        if stored.format != LIFECYCLE_FENCE_FORMAT || self.path(&stored.resource_scope)? != path {
            return Err(snapshot_error(
                "resource lifecycle fence identity or format is invalid",
            ));
        }
        Ok(stored)
    }

    fn validate_binding(
        &self,
        stored: &StoredLifecycleFence,
        command: &AgentResourceLifecycleAssignment,
    ) -> AgentResult<()> {
        if stored.deletion_id != command.assignment.deletion_id
            || stored.tenant_id != command.assignment.tenant_id
            || stored.resource_scope != command.resource_scope
            || stored.lifecycle_generation != command.assignment.lifecycle_generation
            || stored.request_digest != command.assignment.request_digest
        {
            return Err(AgentError::new(
                AgentErrorCode::AssignmentMismatch,
                "resource lifecycle fence belongs to another deletion operation",
            ));
        }
        Ok(())
    }
}

fn lifecycle_scope_matches_delivery(
    scope: &AgentResourceLifecycleScope,
    assignment: &SnapshotDeliveryAssignment,
) -> bool {
    match scope {
        AgentResourceLifecycleScope::StorageVolume { storage_volume_id } => {
            storage_volume_id == &assignment.storage_volume_id
        }
        AgentResourceLifecycleScope::Artifact {
            project_id,
            artifact_id,
            storage_volume_id,
            ..
        } => {
            project_id == &assignment.project_id
                && artifact_id == &assignment.artifact_id
                && storage_volume_id == &assignment.storage_volume_id
        }
        AgentResourceLifecycleScope::Playground { .. } => false,
        AgentResourceLifecycleScope::Snapshot {
            project_id,
            artifact_id,
            snapshot_id,
            storage_volume_id,
            ..
        } => {
            project_id == &assignment.project_id
                && artifact_id == &assignment.artifact_id
                && snapshot_id == &assignment.snapshot_id
                && storage_volume_id == &assignment.storage_volume_id
        }
    }
}

fn lifecycle_scope_matches_job(
    scope: &AgentResourceLifecycleScope,
    assignment: &JobAssignment,
) -> bool {
    match &assignment.assignment {
        AssignmentOperation::Add { input, .. } => lifecycle_scope_matches_parts(
            scope,
            &input.project_id,
            &input.artifact_id,
            Some(&input.playground_id),
            None,
            &input.storage_volume_id,
        ),
        AssignmentOperation::WorkspaceMaterialize { input, .. } => lifecycle_scope_matches_parts(
            scope,
            &input.project_id,
            &input.artifact_id,
            Some(&input.playground_id),
            None,
            &input.storage_volume_id,
        ),
        AssignmentOperation::SnapshotDelivery { input, .. } => lifecycle_scope_matches_parts(
            scope,
            &input.project_id,
            &input.artifact_id,
            None,
            Some(&input.snapshot_id),
            &input.storage_volume_id,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn lifecycle_scope_matches_parts(
    scope: &AgentResourceLifecycleScope,
    project_id: &neoengram_domain::protocol::ProjectId,
    artifact_id: &neoengram_domain::protocol::ArtifactId,
    playground_id: Option<&neoengram_domain::protocol::PlaygroundId>,
    snapshot_id: Option<&SnapshotId>,
    storage_volume_id: &StorageVolumeId,
) -> bool {
    match scope {
        AgentResourceLifecycleScope::StorageVolume {
            storage_volume_id: expected,
        } => expected == storage_volume_id,
        AgentResourceLifecycleScope::Artifact {
            project_id: expected_project,
            artifact_id: expected_artifact,
            storage_volume_id: expected_volume,
            ..
        } => {
            expected_project == project_id
                && expected_artifact == artifact_id
                && expected_volume == storage_volume_id
        }
        AgentResourceLifecycleScope::Playground {
            project_id: expected_project,
            artifact_id: expected_artifact,
            playground_id: expected_playground,
            storage_volume_id: expected_volume,
            ..
        } => {
            expected_project == project_id
                && expected_artifact == artifact_id
                && playground_id == Some(expected_playground)
                && expected_volume == storage_volume_id
        }
        AgentResourceLifecycleScope::Snapshot {
            project_id: expected_project,
            artifact_id: expected_artifact,
            snapshot_id: expected_snapshot,
            storage_volume_id: expected_volume,
            ..
        } => {
            expected_project == project_id
                && expected_artifact == artifact_id
                && snapshot_id == Some(expected_snapshot)
                && expected_volume == storage_volume_id
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredSnapshotDeliveryMount {
    format: u32,
    assignment: SnapshotDeliveryAssignment,
    index_version: IndexVersion,
    files: Vec<crate::WorkspaceMaterializationFile>,
}

#[derive(Debug)]
struct SnapshotDeliveryMountStore {
    root: PathBuf,
}

impl SnapshotDeliveryMountStore {
    fn open(root: PathBuf) -> AgentResult<Self> {
        fs::create_dir_all(&root).map_err(snapshot_io)?;
        let metadata = fs::symlink_metadata(&root).map_err(snapshot_io)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(snapshot_error(
                "SnapshotDelivery state root is not an ordinary directory",
            ));
        }
        Ok(Self { root })
    }

    fn path(&self, delivery_id: &SnapshotDeliveryId) -> PathBuf {
        self.root.join(format!("{}.json", delivery_id.as_str()))
    }

    fn put(
        &self,
        assignment: &SnapshotDeliveryAssignment,
        snapshot: &WorkspaceMaterializationSnapshot,
    ) -> AgentResult<()> {
        let payload = serde_json::to_vec(&StoredSnapshotDeliveryMount {
            format: SNAPSHOT_DELIVERY_STATE_FORMAT,
            assignment: assignment.clone(),
            index_version: *snapshot.version(),
            files: snapshot.files().to_vec(),
        })
        .map_err(|error| {
            snapshot_error(format!("failed to encode SnapshotDelivery state: {error}"))
        })?;
        if payload.len() as u64 > MAX_SNAPSHOT_STATE_BYTES {
            return Err(snapshot_error(
                "SnapshotDelivery state exceeds its size limit",
            ));
        }
        let mut temporary = tempfile::NamedTempFile::new_in(&self.root).map_err(snapshot_io)?;
        temporary.write_all(&payload).map_err(snapshot_io)?;
        temporary.as_file().sync_all().map_err(snapshot_io)?;
        temporary
            .persist(self.path(&assignment.delivery_id))
            .map_err(|error| snapshot_io(error.error))?;
        sync_directory(&self.root).map_err(AgentError::from)
    }

    fn entries(
        &self,
    ) -> AgentResult<Vec<AgentResult<(SnapshotDeliveryAssignment, WorkspaceMaterializationSnapshot)>>>
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
    ) -> AgentResult<(SnapshotDeliveryAssignment, WorkspaceMaterializationSnapshot)> {
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            return Err(snapshot_error(
                "SnapshotDelivery state root contains an unknown file",
            ));
        }
        let metadata = fs::symlink_metadata(path).map_err(snapshot_io)?;
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.len() > MAX_SNAPSHOT_STATE_BYTES
        {
            return Err(snapshot_error("SnapshotDelivery state file is invalid"));
        }
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        File::open(path)
            .map_err(snapshot_io)?
            .take(MAX_SNAPSHOT_STATE_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(snapshot_io)?;
        let stored: StoredSnapshotDeliveryMount =
            serde_json::from_slice(&bytes).map_err(|error| {
                snapshot_error(format!("failed to decode SnapshotDelivery state: {error}"))
            })?;
        if stored.format != SNAPSHOT_DELIVERY_STATE_FORMAT
            || self.path(&stored.assignment.delivery_id) != path
        {
            return Err(snapshot_error(
                "SnapshotDelivery state identity or format is invalid",
            ));
        }
        stored.assignment.validate().map_err(|error| {
            snapshot_error(format!(
                "persisted SnapshotDelivery assignment is invalid: {error}"
            ))
        })?;
        let snapshot = WorkspaceMaterializationSnapshot::new(stored.index_version, stored.files)?;
        if snapshot.version().digest != stored.assignment.source_index_digest {
            return Err(AgentError::new(
                AgentErrorCode::ProtocolInvalid,
                "persisted SnapshotDelivery metadata differs from its assignment Index fence",
            ));
        }
        Ok((stored.assignment, snapshot))
    }

    fn remove(&self, delivery_id: &SnapshotDeliveryId) -> AgentResult<()> {
        match fs::remove_file(self.path(delivery_id)) {
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
    use neoengram_domain::core::{ContentDigest, Manifest};
    use neoengram_runtime::engine::ObjectStore;
    use neoengram_runtime::fs::LooseObjectStore;

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
            // FSKit issues bootstrap requests such as STATFS as uid 0 before the mount is live.
            // fuser still enforces RootAndOwner in userspace when this maps to allow_other.
            options.push(MountOption::AllowRoot);
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
            path: &neoengram_domain::core::LogicalPath,
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
        use crate::AgentErrorCode;

        #[cfg(any(target_os = "linux", target_os = "macos"))]
        use super::snapshot_mount_options;

        #[cfg(target_os = "macos")]
        use super::{
            classify_macos_mount_error, macos_mount_line_matches, MACFUSE_FSKIT_UNAVAILABLE,
        };

        #[test]
        fn create_and_setattr_use_read_only_filesystem_errno() {
            assert_eq!(mutation_errno(), libc::EROFS);
        }

        #[cfg(target_os = "macos")]
        #[test]
        fn macos_snapshot_mount_uses_fskit_backend() {
            let options = snapshot_mount_options();
            assert!(options.contains(&fuser::MountOption::AllowRoot));
            assert!(options.iter().any(
                |option| matches!(option, fuser::MountOption::CUSTOM(value) if value == "backend=fskit")
            ));
            assert!(options.iter().any(
                |option| matches!(option, fuser::MountOption::CUSTOM(value) if value == "quiet")
            ));
        }

        #[cfg(target_os = "linux")]
        #[test]
        fn linux_snapshot_mount_does_not_require_allow_other() {
            let options = snapshot_mount_options();
            assert!(!options.contains(&fuser::MountOption::AllowRoot));
            assert!(!options.contains(&fuser::MountOption::AllowOther));
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

    use neoengram_domain::core::{
        ChunkRef, ChunkingStrategy, FileRecord, IndexVersion, LogicalPath, Manifest, ObjectId,
    };
    use neoengram_domain::protocol::{
        ArtifactPlacementId, AssignmentGeneration, AssignmentId, DecimalU64, DeletionId,
        DeliveryGeneration, EdgeClusterId, Extensions, HardlinkPolicy, LifecycleAssignmentId,
        LifecycleGeneration, PlacementGeneration, PrincipalId, PrincipalKind, PrincipalRef,
        ProjectId, ResourceLifecycleAction, ResourceLifecycleAssignment, ResourceRef,
        SnapshotDeliveryAction, SnapshotDeliveryMode, SnapshotId, VolumeMarkerId,
        CURRENT_WIRE_VERSION,
    };
    use neoengram_runtime::engine::ObjectStore;

    use super::*;

    #[derive(Debug)]
    struct NeverBridge;

    impl ExecutionBridge for NeverBridge {
        fn authoritative_index(
            &self,
            _assignment: &neoengram_domain::protocol::AddAssignment,
        ) -> AgentResult<crate::AuthoritativeIndexSnapshot> {
            panic!("recovery must not query the Server")
        }

        fn workspace_materialization_snapshot(
            &self,
            _assignment: &neoengram_domain::protocol::WorkspaceMaterializeAssignment,
        ) -> AgentResult<WorkspaceMaterializationSnapshot> {
            panic!("recovery must not query the Server")
        }

        fn stage_metadata_descriptor(
            &self,
            _assignment: &neoengram_domain::protocol::AddAssignment,
            _descriptor: &neoengram_domain::protocol::MetadataBatchDescriptor,
        ) -> AgentResult<()> {
            panic!("recovery must not stage metadata")
        }

        fn stage_metadata_page(
            &self,
            _assignment: &neoengram_domain::protocol::AddAssignment,
            _page: &neoengram_domain::protocol::MetadataBatchPage,
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

    fn delivery_assignment(
        snapshot: &WorkspaceMaterializationSnapshot,
    ) -> SnapshotDeliveryAssignment {
        let snapshot_id = SnapshotId::new("snapshot-a").unwrap();
        let delivery_id = SnapshotDeliveryId::new("delivery-a").unwrap();
        let project_id = ProjectId::new("project-a").unwrap();
        let artifact_id = neoengram_domain::protocol::ArtifactId::new("artifact-a").unwrap();
        let target_relative_root =
            neoengram_domain::protocol::SnapshotDeliveryOperation::canonical_target_relative_root(
                &project_id,
                &artifact_id,
                &snapshot_id,
                &delivery_id,
            )
            .unwrap();
        let mut assignment = SnapshotDeliveryAssignment {
            job_id: neoengram_domain::protocol::JobId::new("job-delivery-a").unwrap(),
            assignment_id: AssignmentId::new("assignment-delivery-a").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            agent_id: AgentId::new("agent-a").unwrap(),
            principal: PrincipalRef {
                kind: PrincipalKind::Service,
                id: PrincipalId::new("principal-a").unwrap(),
                extensions: Extensions::new(),
            },
            action: SnapshotDeliveryAction::Materialize,
            tenant_id: TenantId::new("tenant-a").unwrap(),
            project_id,
            artifact_id,
            snapshot_id,
            delivery_id,
            commit_id: ContentDigest::from_bytes([0x11; 32]),
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            snapshot_size_bytes: DecimalU64::new(8),
            copy_reserve_bytes: DecimalU64::new(0),
            hardlink_policy: HardlinkPolicy::Disabled,
            agent_mount_id: AgentMountId::new("mount-a").unwrap(),
            mount_generation: MountGeneration::new(1),
            owner_generation: OwnerGeneration::new(1),
            placement_generation: PlacementGeneration::new(2),
            mode: SnapshotDeliveryMode::Fuse,
            target_relative_root,
            source_index_digest: snapshot.version().digest,
            request_digest: ContentDigest::from_bytes([0; 32]),
            delivery_generation: DeliveryGeneration::new(1),
            deadline_unix_ms: neoengram_domain::protocol::UnixMillis::new(10_000),
            extensions: Extensions::new(),
        };
        assignment.request_digest = assignment.operation().request_digest().unwrap();
        assignment.validate().unwrap();
        assignment
    }

    fn delivery_lifecycle_assignment(
        delivery: &SnapshotDeliveryAssignment,
        assignment_id: &str,
        action: ResourceLifecycleAction,
    ) -> AgentResourceLifecycleAssignment {
        AgentResourceLifecycleAssignment {
            assignment: ResourceLifecycleAssignment {
                assignment_id: LifecycleAssignmentId::new(assignment_id).unwrap(),
                tenant_id: delivery.tenant_id.clone(),
                deletion_id: DeletionId::new("deletion-snapshot-a").unwrap(),
                resource: ResourceRef::Snapshot {
                    snapshot_id: delivery.snapshot_id.clone(),
                },
                action,
                lifecycle_generation: LifecycleGeneration::new(2),
                request_digest: ContentDigest::from_bytes([0x33; 32]),
                deadline_unix_ms: neoengram_domain::protocol::UnixMillis::new(10_000),
            },
            resource_scope: AgentResourceLifecycleScope::Snapshot {
                project_id: delivery.project_id.clone(),
                artifact_id: delivery.artifact_id.clone(),
                snapshot_id: delivery.snapshot_id.clone(),
                storage_volume_id: delivery.storage_volume_id.clone(),
                artifact_placement_id: ArtifactPlacementId::new("placement-a").unwrap(),
                placement_generation: delivery.placement_generation,
            },
            agent_id: delivery.agent_id.clone(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            agent_mount_id: delivery.agent_mount_id.clone(),
            volume_marker_id: VolumeMarkerId::new(delivery.storage_volume_id.as_str()).unwrap(),
            session_generation: SessionGeneration::new(7),
            mount_generation: delivery.mount_generation,
            owner_generation: delivery.owner_generation,
            extensions: Extensions::new(),
        }
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
    fn delivery_lifecycle_fence_blocks_mount_and_startup_recovery() {
        let temporary = tempfile::tempdir().unwrap();
        let mount_root = temporary.path().join("mount");
        let state_root = temporary.path().join("agent-state");
        fs::create_dir(&mount_root).unwrap();
        let (snapshot, _) = snapshot_with_objects(&temporary.path().join("fixture-objects"));
        let assignment = delivery_assignment(&snapshot);

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
        let binding = SnapshotMountBinding {
            agent_id: assignment.agent_id.clone(),
            tenant_id: assignment.tenant_id.clone(),
            storage_volume_id: assignment.storage_volume_id.clone(),
            agent_mount_id: assignment.agent_mount_id.clone(),
            mount_generation: assignment.mount_generation,
            owner_generation: assignment.owner_generation,
            session_generation: SessionGeneration::new(7),
        };
        let manager = SnapshotDeliveryMountManager::with_backend(
            &mount_root,
            &state_root,
            binding.clone(),
            Arc::new(NeverBridge),
            Arc::new(CountingBackend(Arc::clone(&mounts))),
        )
        .unwrap();
        manager
            .mount_local(assignment.clone(), snapshot, true)
            .unwrap();
        assert_eq!(mounts.load(Ordering::SeqCst), 1);
        assert!(state_root
            .join("snapshot-deliveries/delivery-a.json")
            .is_file());
        assert!(!state_root.join("snapshot-mounts/snapshot-a.json").exists());

        let quarantine = delivery_lifecycle_assignment(
            &assignment,
            "lifecycle-delivery-quarantine-a",
            ResourceLifecycleAction::Quarantine,
        );
        manager.lifecycle_fence(&quarantine).unwrap();
        assert!(!manager.is_mounted(&assignment).unwrap());
        assert!(manager.recover().unwrap().is_empty());
        assert_eq!(mounts.load(Ordering::SeqCst), 1);
        assert_eq!(
            manager.mount(assignment.clone()).unwrap_err().code(),
            AgentErrorCode::InvalidState
        );

        let restore = delivery_lifecycle_assignment(
            &assignment,
            "lifecycle-delivery-restore-a",
            ResourceLifecycleAction::Restore,
        );
        manager.lifecycle_restore_keep_fence(&restore).unwrap();
        assert!(!manager.is_mounted(&assignment).unwrap());
        let fence_owner = SnapshotDeliveryMountManager::with_backend(
            &mount_root,
            &state_root,
            binding,
            Arc::new(NeverBridge),
            Arc::new(CountingBackend(Arc::clone(&mounts))),
        )
        .unwrap();
        fence_owner.lifecycle_restore(&restore).unwrap();
        assert!(manager.is_mounted(&assignment).unwrap());
        assert_eq!(mounts.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn unhealthy_guard_is_not_reported_as_mounted_and_can_be_remounted() {
        let temporary = tempfile::tempdir().unwrap();
        let mount_root = temporary.path().join("mount");
        let state_root = temporary.path().join("agent-state");
        fs::create_dir(&mount_root).unwrap();
        let (snapshot, _) = snapshot_with_objects(&temporary.path().join("fixture-objects"));
        let assignment = delivery_assignment(&snapshot);

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
        let manager = SnapshotDeliveryMountManager::with_backend(
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
        assert_eq!(manager.store.entries().unwrap().len(), 1);

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
    fn bootstrap_wire_version_remains_v1_fixture_sanity() {
        assert_eq!(
            CURRENT_WIRE_VERSION,
            neoengram_domain::protocol::CURRENT_WIRE_VERSION
        );
    }
}
