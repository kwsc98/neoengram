//! Agent-side materialization for the new SnapshotDelivery protocol.
//!
//! The executor deliberately consumes only a signed `SnapshotDeliveryAssignment` and an
//! immutable metadata snapshot supplied by the Agent session bridge. It never accepts a host
//! path from Central or from the caller.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{AgentError, AgentErrorCode, AgentResult};
use fs2::available_space;
use neoengram_domain::core::{ChunkingStrategy, ObjectId};
use neoengram_domain::protocol::{
    AgentId, AgentMountId, HardlinkPolicy, MountGeneration, OwnerGeneration,
    SnapshotDeliveryAssignment, SnapshotDeliveryErrorCode, SnapshotDeliveryMode, StorageVolumeId,
    TenantId,
};
use neoengram_runtime::engine::{EngineError, ErrorCode as EngineErrorCode, ObjectStore};
use neoengram_runtime::fs::{rename_no_replace, sync_directory, LooseObjectStore, VerifiedRoot};

use crate::execution::{artifact_object_store, ExecutionBridge, WorkspaceMaterializationFile};

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SnapshotDeliveryStats {
    pub files: u64,
    pub bytes: u64,
    pub objects: u64,
}

#[derive(Debug, Clone)]
struct DeliveryBinding {
    agent_id: AgentId,
    tenant_id: TenantId,
    storage_volume_id: StorageVolumeId,
    agent_mount_id: AgentMountId,
    mount_generation: MountGeneration,
    owner_generation: OwnerGeneration,
}

/// Securely materializes one Delivery below an approved Volume root.
#[derive(Debug, Clone)]
pub struct SnapshotDeliveryMaterializer {
    mount_root: PathBuf,
    binding: Option<DeliveryBinding>,
    bridge: Arc<dyn ExecutionBridge>,
}

impl SnapshotDeliveryMaterializer {
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
            binding: Some(DeliveryBinding {
                agent_id,
                tenant_id,
                storage_volume_id,
                agent_mount_id,
                mount_generation,
                owner_generation,
            }),
            bridge,
        }
    }

    pub fn materialize(
        &self,
        assignment: &SnapshotDeliveryAssignment,
    ) -> AgentResult<SnapshotDeliveryStats> {
        self.validate_assignment(assignment)?;
        let mount = VerifiedRoot::open(&self.mount_root).map_err(|error| {
            AgentError::new(
                AgentErrorCode::MountUnavailable,
                format!("approved Agent mount is unavailable: {error}"),
            )
        })?;
        if assignment.mode == SnapshotDeliveryMode::Hardlink
            && matches!(assignment.hardlink_policy, HardlinkPolicy::Disabled)
        {
            return Err(AgentError::new(
                AgentErrorCode::InvalidAssignment,
                "HARDLINK_UNSAFE_VOLUME: hardlink policy is disabled for this Volume",
            ));
        }
        let snapshot = self.bridge.snapshot_delivery_snapshot(assignment)?;
        if snapshot.version().digest != assignment.source_index_digest {
            return Err(AgentError::new(
                AgentErrorCode::ProtocolInvalid,
                "SnapshotDelivery metadata differs from the assignment source Index digest",
            ));
        }
        let parent = assignment.target_relative_root.parent().ok_or_else(|| {
            AgentError::new(
                AgentErrorCode::InvalidAssignment,
                "Delivery target has no parent",
            )
        })?;
        let parent_path = mount.create_dir_all(&parent).map_err(AgentError::from)?;
        let destination = mount
            .resolve_for_create(&assignment.target_relative_root)
            .map_err(|error| delivery_target_conflict(error.to_string()))?;
        let objects = artifact_object_store(
            &self.mount_root,
            &assignment.tenant_id,
            &assignment.artifact_id,
        )?;
        let staging = deterministic_staging_path(&parent_path, assignment);
        prepare_staging_path(&staging)?;

        if fs::symlink_metadata(&destination).is_ok() {
            let result =
                verify_delivery_tree(&destination, assignment.mode, snapshot.files(), &objects);
            let cleanup = remove_delivery_tree(&staging);
            cleanup?;
            let existing = result?;
            sync_directory(&parent_path).map_err(AgentError::from)?;
            return Ok(existing);
        }

        if assignment.mode == SnapshotDeliveryMode::Copy {
            validate_copy_space(&self.mount_root, assignment)?;
        }

        let result = self.materialize_tree(&staging, assignment, snapshot.files(), &objects);
        let stats = match result {
            Ok(stats) => stats,
            Err(error) => {
                let _ = remove_delivery_tree(&staging);
                return Err(error);
            }
        };
        let stats =
            match verify_delivery_tree(&staging, assignment.mode, snapshot.files(), &objects) {
                Ok(verified) if verified == stats => verified,
                Ok(_) => {
                    let _ = remove_delivery_tree(&staging);
                    return Err(delivery_target_conflict(
                        "materialized Delivery statistics changed before publication",
                    ));
                }
                Err(error) => {
                    let _ = remove_delivery_tree(&staging);
                    return Err(error);
                }
            };
        sync_directory(&staging).map_err(AgentError::from)?;
        if let Err(error) = rename_no_replace(&staging, &destination) {
            let _ = remove_delivery_tree(&staging);
            if error.code() == neoengram_runtime::engine::ErrorCode::AlreadyExists {
                let existing = verify_delivery_tree(
                    &destination,
                    assignment.mode,
                    snapshot.files(),
                    &objects,
                )?;
                sync_directory(&parent_path).map_err(AgentError::from)?;
                return Ok(existing);
            }
            return Err(delivery_target_conflict(format!(
                "failed to publish Delivery: {error}"
            )));
        }
        // `rename_no_replace` also performs a durability barrier, but keep this explicit at the
        // Delivery boundary so a future platform-specific rename implementation cannot publish
        // a directory whose parent entry is still only in the page cache.
        sync_directory(&parent_path).map_err(AgentError::from)?;
        Ok(stats)
    }

    /// Removes a Copy or Hardlink projection without touching the shared CAS namespace.
    pub fn delete(&self, assignment: &SnapshotDeliveryAssignment) -> AgentResult<()> {
        self.validate_assignment(assignment)?;
        let root = VerifiedRoot::open(&self.mount_root).map_err(AgentError::from)?;
        let destination = match root.resolve_existing(&assignment.target_relative_root) {
            Ok(path) => path,
            Err(error) if error.code() == EngineErrorCode::ResourceNotFound => return Ok(()),
            Err(error) => return Err(AgentError::from(error)),
        };
        remove_delivery_tree(&destination)?;
        if let Some(parent) = destination.parent() {
            sync_directory(parent).map_err(AgentError::from)?;
        }
        Ok(())
    }

    fn validate_assignment(&self, assignment: &SnapshotDeliveryAssignment) -> AgentResult<()> {
        assignment.validate().map_err(|error| {
            AgentError::new(
                AgentErrorCode::InvalidAssignment,
                format!("invalid SnapshotDelivery assignment: {error}"),
            )
        })?;
        if let Some(binding) = &self.binding {
            if assignment.agent_id != binding.agent_id
                || assignment.tenant_id != binding.tenant_id
                || assignment.storage_volume_id != binding.storage_volume_id
                || assignment.agent_mount_id != binding.agent_mount_id
            {
                return Err(AgentError::new(
                    AgentErrorCode::ScopeMismatch,
                    "SnapshotDelivery assignment is outside the approved Volume scope",
                ));
            }
            if assignment.mount_generation != binding.mount_generation
                || assignment.owner_generation != binding.owner_generation
            {
                return Err(AgentError::new(
                    AgentErrorCode::GenerationMismatch,
                    "SnapshotDelivery assignment carries stale mount or owner generation",
                ));
            }
        }
        Ok(())
    }

    fn materialize_tree(
        &self,
        staging: &Path,
        assignment: &SnapshotDeliveryAssignment,
        files: &[WorkspaceMaterializationFile],
        objects: &LooseObjectStore,
    ) -> AgentResult<SnapshotDeliveryStats> {
        let mode = assignment.mode;
        let root = VerifiedRoot::open(staging).map_err(AgentError::from)?;
        let mut directories = BTreeSet::from([staging.to_path_buf()]);
        let mut stats = SnapshotDeliveryStats::default();
        for file in files {
            if mode == SnapshotDeliveryMode::Hardlink
                && (file.manifest.chunking != ChunkingStrategy::WholeFile
                    || (file.record.total_size > 0
                        && (file.manifest.chunks.len() != 1
                            || file.manifest.chunks[0].offset != 0
                            || file.manifest.chunks[0].size != file.record.total_size)))
            {
                return Err(AgentError::new(
                    AgentErrorCode::InvalidAssignment,
                    "HARDLINK_REQUIRES_WHOLE_FILE: Manifest is not a single WholeFile object",
                ));
            }
            if let Some(parent) = file.record.path.parent() {
                let path = root.create_dir_all(&parent).map_err(AgentError::from)?;
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
            let destination = root
                .resolve_for_create(&file.record.path)
                .map_err(AgentError::from)?;
            if mode == SnapshotDeliveryMode::Hardlink && file.record.total_size > 0 {
                let object = file.manifest.chunks[0].object_spec();
                let result = match assignment.hardlink_policy {
                    HardlinkPolicy::Disabled => Err(neoengram_runtime::engine::EngineError::new(
                        EngineErrorCode::Conflict,
                        "HARDLINK_UNSAFE_VOLUME: hardlink policy is disabled for this Volume",
                    )),
                    HardlinkPolicy::SealedAcl => objects.hard_link_to(&object, &destination),
                    HardlinkPolicy::TrustedLocal => {
                        objects.hard_link_to_trusted(&object, &destination)
                    }
                };
                result.map_err(map_hardlink_engine_error)?;
            } else {
                let mut output = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&destination)
                    .map_err(|error| {
                        AgentError::new(AgentErrorCode::ExecutionFailed, error.to_string())
                    })?;
                for chunk in &file.manifest.chunks {
                    objects
                        .copy_to(&chunk.object_spec(), &mut output)
                        .map_err(|error| {
                            AgentError::new(AgentErrorCode::ObjectTransferFailed, error.to_string())
                        })?;
                    stats.objects = stats.objects.saturating_add(1);
                }
                output
                    .flush()
                    .and_then(|()| output.sync_all())
                    .map_err(|error| {
                        AgentError::new(AgentErrorCode::ExecutionFailed, error.to_string())
                    })?;
                // Seal the inode through the handle used for writing, then prove the published
                // pathname still resolves to that inode. A concurrent pathname replacement must
                // never redirect chmod or turn a symlink into a Ready Delivery.
                seal_open_delivery_file(&output, &destination, file.record.path.as_str())?;
            }
            stats.files = stats.files.saturating_add(1);
            stats.bytes = stats.bytes.saturating_add(file.record.total_size);
        }
        for directory in directories.iter() {
            set_read_only_directory(directory)?;
        }
        for directory in directories {
            sync_directory(&directory).map_err(AgentError::from)?;
        }
        Ok(stats)
    }
}

fn delivery_target_conflict(detail: impl Into<String>) -> AgentError {
    AgentError::new(
        AgentErrorCode::AssignmentMismatch,
        format!("DELIVERY_TARGET_CONFLICT: {}", detail.into()),
    )
}

fn map_hardlink_engine_error(error: EngineError) -> AgentError {
    let detail = error.message().to_owned();
    let explicit = [
        (
            SnapshotDeliveryErrorCode::HardlinkCrossFilesystem,
            AgentErrorCode::InvalidAssignment,
        ),
        (
            SnapshotDeliveryErrorCode::HardlinkUnsafeVolume,
            AgentErrorCode::InvalidAssignment,
        ),
        (
            SnapshotDeliveryErrorCode::HardlinkObjectNotSealed,
            AgentErrorCode::InvalidAssignment,
        ),
        (
            SnapshotDeliveryErrorCode::DeliveryTargetConflict,
            AgentErrorCode::AssignmentMismatch,
        ),
    ];
    if let Some((_, agent_code)) = explicit
        .iter()
        .find(|(code, _)| detail.starts_with(code.as_str()))
    {
        return AgentError::new(*agent_code, detail);
    }

    match error.code() {
        EngineErrorCode::AlreadyExists | EngineErrorCode::InvalidPath => {
            delivery_target_conflict(detail)
        }
        EngineErrorCode::ObjectMissing
        | EngineErrorCode::ObjectCorrupt
        | EngineErrorCode::StorageUnavailable
        | EngineErrorCode::Io => AgentError::new(
            AgentErrorCode::ObjectTransferFailed,
            format!("DELIVERY_OBJECT_UNAVAILABLE: {detail}"),
        ),
        EngineErrorCode::IntegrityViolation => AgentError::new(
            AgentErrorCode::InvalidAssignment,
            format!(
                "{}: {detail}",
                SnapshotDeliveryErrorCode::HardlinkObjectNotSealed.as_str()
            ),
        ),
        EngineErrorCode::Conflict => delivery_target_conflict(detail),
        EngineErrorCode::InvalidArgument => {
            AgentError::new(AgentErrorCode::InvalidAssignment, detail)
        }
        _ => AgentError::new(AgentErrorCode::ExecutionFailed, detail),
    }
}

fn validate_copy_space(
    mount_root: &Path,
    assignment: &SnapshotDeliveryAssignment,
) -> AgentResult<()> {
    let required = assignment
        .snapshot_size_bytes
        .get()
        .checked_add(assignment.copy_reserve_bytes.get())
        .ok_or_else(|| {
            AgentError::new(
                AgentErrorCode::InvalidAssignment,
                "COPY_INSUFFICIENT_SPACE: required materialization size overflowed",
            )
        })?;
    let available = available_space(mount_root).map_err(|error| {
        AgentError::new(
            AgentErrorCode::MountUnavailable,
            format!("COPY_INSUFFICIENT_SPACE: failed to inspect Volume free space: {error}"),
        )
    })?;
    if available < required {
        return Err(AgentError::new(
            AgentErrorCode::ExecutionFailed,
            format!(
                "COPY_INSUFFICIENT_SPACE: Volume has {available} bytes available, but {required} bytes are required"
            ),
        ));
    }
    Ok(())
}

fn delivery_stats(
    mode: SnapshotDeliveryMode,
    files: &[WorkspaceMaterializationFile],
) -> AgentResult<SnapshotDeliveryStats> {
    let files_count = u64::try_from(files.len()).map_err(|_| {
        AgentError::new(
            AgentErrorCode::InvalidAssignment,
            "SnapshotDelivery file count exceeds u64",
        )
    })?;
    let bytes = files.iter().try_fold(0_u64, |total, file| {
        total.checked_add(file.record.total_size).ok_or_else(|| {
            AgentError::new(
                AgentErrorCode::InvalidAssignment,
                "SnapshotDelivery byte count exceeds u64",
            )
        })
    })?;
    let objects = if mode == SnapshotDeliveryMode::Copy {
        files.iter().try_fold(0_u64, |total, file| {
            let count = u64::try_from(file.manifest.chunks.len()).map_err(|_| {
                AgentError::new(
                    AgentErrorCode::InvalidAssignment,
                    "SnapshotDelivery object count exceeds u64",
                )
            })?;
            total.checked_add(count).ok_or_else(|| {
                AgentError::new(
                    AgentErrorCode::InvalidAssignment,
                    "SnapshotDelivery object count exceeds u64",
                )
            })
        })?
    } else {
        0
    };
    Ok(SnapshotDeliveryStats {
        files: files_count,
        bytes,
        objects,
    })
}

fn deterministic_staging_path(parent: &Path, assignment: &SnapshotDeliveryAssignment) -> PathBuf {
    parent.join(format!(
        ".neoengram-delivery-{}-{}",
        assignment.delivery_id.as_str(),
        assignment.delivery_generation.get()
    ))
}

fn prepare_staging_path(path: &Path) -> AgentResult<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(delivery_target_conflict(
            "deterministic staging path is a symlink",
        )),
        Ok(metadata) if metadata.is_dir() => {
            remove_delivery_tree(path)?;
            fs::create_dir(path).map_err(|error| {
                AgentError::new(
                    AgentErrorCode::ExecutionFailed,
                    format!("failed to recreate Delivery staging directory: {error}"),
                )
            })
        }
        Ok(_) => Err(delivery_target_conflict(
            "deterministic staging path is not an ordinary directory",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir(path).map_err(|error| {
                AgentError::new(
                    AgentErrorCode::ExecutionFailed,
                    format!("failed to create Delivery staging directory: {error}"),
                )
            })
        }
        Err(error) => Err(AgentError::new(
            AgentErrorCode::ExecutionFailed,
            format!("failed to inspect Delivery staging path: {error}"),
        )),
    }
}

fn verify_delivery_tree(
    root: &Path,
    mode: SnapshotDeliveryMode,
    files: &[WorkspaceMaterializationFile],
    objects: &LooseObjectStore,
) -> AgentResult<SnapshotDeliveryStats> {
    let metadata = fs::symlink_metadata(root).map_err(|error| {
        delivery_target_conflict(format!("failed to inspect existing Delivery: {error}"))
    })?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(delivery_target_conflict(
            "existing Delivery root is not an ordinary directory",
        ));
    }
    ensure_read_only(&metadata, "Delivery root")?;

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
            delivery_target_conflict(format!("failed to enumerate existing Delivery: {error}"))
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| {
                delivery_target_conflict(format!("failed to read existing Delivery entry: {error}"))
            })?;
            let path = entry.path();
            let relative = physical_relative_path(root, &path)?;
            let metadata = fs::symlink_metadata(&path).map_err(|error| {
                delivery_target_conflict(format!(
                    "failed to inspect existing Delivery entry: {error}"
                ))
            })?;
            if metadata.file_type().is_symlink() {
                return Err(delivery_target_conflict(format!(
                    "existing Delivery contains symlink {relative}"
                )));
            }
            if metadata.is_dir() {
                ensure_read_only(&metadata, &relative)?;
                if !expected_directories.contains(&relative) {
                    return Err(delivery_target_conflict(format!(
                        "existing Delivery contains unexpected directory {relative}"
                    )));
                }
                stack.push(path);
            } else if metadata.is_file() {
                let expected = expected_files.get(&relative).ok_or_else(|| {
                    delivery_target_conflict(format!(
                        "existing Delivery contains unexpected file {relative}"
                    ))
                })?;
                ensure_read_only(&metadata, expected.record.path.as_str())?;
                verify_delivery_file(&path, mode, expected, objects)?;
                observed_files.insert(relative);
            } else {
                return Err(delivery_target_conflict(format!(
                    "existing Delivery contains unsupported entry {relative}"
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
        return Err(delivery_target_conflict(format!(
            "existing Delivery is missing expected file {missing}"
        )));
    }
    delivery_stats(mode, files)
}

fn verify_delivery_file(
    path: &Path,
    mode: SnapshotDeliveryMode,
    expected: &WorkspaceMaterializationFile,
    objects: &LooseObjectStore,
) -> AgentResult<()> {
    let mut input = open_delivery_file_no_follow(path).map_err(|error| {
        delivery_target_conflict(format!(
            "failed to open materialized file {} without following links: {error}",
            expected.record.path
        ))
    })?;
    let metadata = verify_open_file_path(path, &input, expected.record.path.as_str())?;
    ensure_read_only(&metadata, expected.record.path.as_str())?;
    if metadata.len() != expected.record.total_size {
        return Err(delivery_target_conflict(format!(
            "materialized file {} has size {}, expected {}",
            expected.record.path,
            metadata.len(),
            expected.record.total_size
        )));
    }
    if mode == SnapshotDeliveryMode::Hardlink && expected.record.total_size > 0 {
        objects
            .verify_hard_link_to(&expected.manifest.chunks[0].object_spec(), path)
            .map_err(map_hardlink_engine_error)?;
        let checked = verify_open_file_path(path, &input, expected.record.path.as_str())?;
        ensure_read_only(&checked, expected.record.path.as_str())?;
        return Ok(());
    }
    for chunk in &expected.manifest.chunks {
        let observed = hash_exact_chunk(&mut input, chunk.size, &expected.record.path)?;
        if observed != chunk.object_id {
            return Err(delivery_target_conflict(format!(
                "materialized file {} differs at offset {}",
                expected.record.path, chunk.offset
            )));
        }
    }
    let mut trailing = [0_u8; 1];
    if input.read(&mut trailing).map_err(|error| {
        delivery_target_conflict(format!(
            "failed to finish verifying materialized file {}: {error}",
            expected.record.path
        ))
    })? != 0
    {
        return Err(delivery_target_conflict(format!(
            "materialized file {} contains trailing bytes",
            expected.record.path
        )));
    }
    let checked = verify_open_file_path(path, &input, expected.record.path.as_str())?;
    ensure_read_only(&checked, expected.record.path.as_str())?;
    Ok(())
}

fn hash_exact_chunk(
    input: &mut File,
    size: u64,
    logical_path: &neoengram_domain::core::LogicalPath,
) -> AgentResult<ObjectId> {
    let mut remaining = size;
    let mut buffer = [0_u8; 64 * 1024];
    let mut hasher = blake3::Hasher::new();
    while remaining > 0 {
        let limit = usize::try_from(remaining.min(buffer.len() as u64)).map_err(|_| {
            delivery_target_conflict(format!(
                "chunk size exceeds addressable memory for {logical_path}"
            ))
        })?;
        let read = input.read(&mut buffer[..limit]).map_err(|error| {
            delivery_target_conflict(format!(
                "failed to verify materialized file {logical_path}: {error}"
            ))
        })?;
        if read == 0 {
            return Err(delivery_target_conflict(format!(
                "materialized file {logical_path} ended within a declared chunk"
            )));
        }
        hasher.update(&buffer[..read]);
        remaining -= read as u64;
    }
    Ok(ObjectId::from_bytes(*hasher.finalize().as_bytes()))
}

fn physical_relative_path(root: &Path, path: &Path) -> AgentResult<String> {
    let relative = path.strip_prefix(root).map_err(|_| {
        delivery_target_conflict("existing Delivery entry escaped the destination root")
    })?;
    let mut components = Vec::new();
    for component in relative.components() {
        let value = component.as_os_str().to_str().ok_or_else(|| {
            delivery_target_conflict("existing Delivery contains a non-UTF-8 path")
        })?;
        components.push(value);
    }
    Ok(components.join("/"))
}

fn ensure_read_only(metadata: &fs::Metadata, path: &str) -> AgentResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o222 != 0 {
            return Err(delivery_target_conflict(format!(
                "Delivery entry {path} is writable"
            )));
        }
    }
    #[cfg(not(unix))]
    if !metadata.permissions().readonly() {
        return Err(delivery_target_conflict(format!(
            "Delivery entry {path} is writable"
        )));
    }
    Ok(())
}

fn open_delivery_file_no_follow(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    options.open(path)
}

fn verify_open_file_path(
    path: &Path,
    file: &File,
    logical_path: &str,
) -> AgentResult<fs::Metadata> {
    let opened = file.metadata().map_err(|error| {
        delivery_target_conflict(format!(
            "failed to inspect opened Delivery file {logical_path}: {error}"
        ))
    })?;
    let current = fs::symlink_metadata(path).map_err(|error| {
        delivery_target_conflict(format!(
            "failed to recheck Delivery file {logical_path}: {error}"
        ))
    })?;
    if !opened.is_file() || !current.is_file() || current.file_type().is_symlink() {
        return Err(delivery_target_conflict(format!(
            "Delivery entry {logical_path} is not an ordinary file"
        )));
    }
    #[cfg(unix)]
    if opened.dev() != current.dev() || opened.ino() != current.ino() {
        return Err(delivery_target_conflict(format!(
            "Delivery entry {logical_path} changed while it was being verified"
        )));
    }
    #[cfg(not(unix))]
    if opened.len() != current.len() {
        return Err(delivery_target_conflict(format!(
            "Delivery entry {logical_path} changed while it was being verified"
        )));
    }
    Ok(opened)
}

fn remove_delivery_tree(path: &Path) -> AgentResult<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(AgentError::new(
                AgentErrorCode::ExecutionFailed,
                format!("failed to inspect Delivery target: {error}"),
            ))
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(AgentError::new(
            AgentErrorCode::ScopeMismatch,
            "Delivery target contains a symbolic link",
        ));
    }
    if metadata.is_dir() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|error| {
                AgentError::new(AgentErrorCode::ExecutionFailed, error.to_string())
            })?;
        }
        for entry in fs::read_dir(path)
            .map_err(|error| AgentError::new(AgentErrorCode::ExecutionFailed, error.to_string()))?
        {
            let entry = entry.map_err(|error| {
                AgentError::new(AgentErrorCode::ExecutionFailed, error.to_string())
            })?;
            remove_delivery_tree(&entry.path())?;
        }
        fs::remove_dir(path)
            .map_err(|error| AgentError::new(AgentErrorCode::ExecutionFailed, error.to_string()))
    } else if metadata.is_file() {
        fs::remove_file(path)
            .map_err(|error| AgentError::new(AgentErrorCode::ExecutionFailed, error.to_string()))
    } else {
        Err(AgentError::new(
            AgentErrorCode::ScopeMismatch,
            "Delivery target contains an unsupported filesystem entry",
        ))
    }
}

fn seal_open_delivery_file(file: &File, path: &Path, logical_path: &str) -> AgentResult<()> {
    #[cfg(unix)]
    let permissions = fs::Permissions::from_mode(0o444);
    #[cfg(not(unix))]
    let permissions = {
        let mut permissions = file
            .metadata()
            .map_err(|error| AgentError::new(AgentErrorCode::ExecutionFailed, error.to_string()))?
            .permissions();
        permissions.set_readonly(true);
        permissions
    };
    file.set_permissions(permissions)
        .and_then(|()| file.sync_all())
        .map_err(|error| AgentError::new(AgentErrorCode::ExecutionFailed, error.to_string()))?;
    let metadata = verify_open_file_path(path, file, logical_path)?;
    ensure_read_only(&metadata, logical_path)
}

fn set_read_only_directory(path: &Path) -> AgentResult<()> {
    #[cfg(unix)]
    {
        let mut options = OpenOptions::new();
        options
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
        let directory = options
            .open(path)
            .map_err(|error| AgentError::new(AgentErrorCode::ExecutionFailed, error.to_string()))?;
        let opened = directory
            .metadata()
            .map_err(|error| AgentError::new(AgentErrorCode::ExecutionFailed, error.to_string()))?;
        directory
            .set_permissions(fs::Permissions::from_mode(0o555))
            .map_err(|error| AgentError::new(AgentErrorCode::ExecutionFailed, error.to_string()))?;
        let current = fs::symlink_metadata(path)
            .map_err(|error| AgentError::new(AgentErrorCode::ExecutionFailed, error.to_string()))?;
        if !current.is_dir()
            || current.file_type().is_symlink()
            || opened.dev() != current.dev()
            || opened.ino() != current.ino()
        {
            return Err(delivery_target_conflict(
                "Delivery directory changed while it was being sealed",
            ));
        }
    }
    #[cfg(not(unix))]
    {
        let mut permissions = fs::metadata(path)
            .map_err(|error| AgentError::new(AgentErrorCode::ExecutionFailed, error.to_string()))?
            .permissions();
        permissions.set_readonly(true);
        fs::set_permissions(path, permissions)
            .map_err(|error| AgentError::new(AgentErrorCode::ExecutionFailed, error.to_string()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use neoengram_domain::core::{ChunkRef, FileRecord, IndexVersion, Manifest, ObjectSpec};
    use neoengram_domain::protocol::{
        AddAssignment, AgentMountId, ArtifactId, AssignmentGeneration, AssignmentId, ContentDigest,
        DecimalU64, DeliveryGeneration, Extensions, JobId, MetadataBatchDescriptor,
        MetadataBatchPage, PlacementGeneration, PrincipalId, PrincipalKind, PrincipalRef,
        ProjectId, SnapshotDeliveryAction, SnapshotDeliveryId, SnapshotDeliveryOperation,
        SnapshotId, UnixMillis, WorkspaceMaterializeAssignment,
    };
    use tempfile::TempDir;

    use crate::execution::{AuthoritativeIndexSnapshot, WorkspaceMaterializationSnapshot};

    use super::*;

    #[derive(Debug)]
    struct TestBridge {
        snapshot: WorkspaceMaterializationSnapshot,
    }

    impl ExecutionBridge for TestBridge {
        fn authoritative_index(
            &self,
            _assignment: &AddAssignment,
        ) -> AgentResult<AuthoritativeIndexSnapshot> {
            Err(AgentError::new(
                AgentErrorCode::InvalidState,
                "authoritative Index is not used by this fixture",
            ))
        }

        fn workspace_materialization_snapshot(
            &self,
            _assignment: &WorkspaceMaterializeAssignment,
        ) -> AgentResult<WorkspaceMaterializationSnapshot> {
            Err(AgentError::new(
                AgentErrorCode::InvalidState,
                "Workspace materialization is not used by this fixture",
            ))
        }

        fn snapshot_delivery_snapshot(
            &self,
            _assignment: &SnapshotDeliveryAssignment,
        ) -> AgentResult<WorkspaceMaterializationSnapshot> {
            Ok(self.snapshot.clone())
        }

        fn stage_metadata_descriptor(
            &self,
            _assignment: &AddAssignment,
            _descriptor: &MetadataBatchDescriptor,
        ) -> AgentResult<()> {
            Ok(())
        }

        fn stage_metadata_page(
            &self,
            _assignment: &AddAssignment,
            _page: &MetadataBatchPage,
        ) -> AgentResult<()> {
            Ok(())
        }

        fn now_unix_ms(&self) -> AgentResult<u64> {
            Ok(1)
        }
    }

    fn copy_snapshot() -> WorkspaceMaterializationSnapshot {
        let first = b"first Copy chunk";
        let second = b"second Copy chunk";
        let manifest = Manifest::new(
            (first.len() + second.len()) as u64,
            ChunkingStrategy::FastCdc,
            vec![
                ChunkRef::new(ObjectId::for_bytes(first), 0, first.len() as u64).unwrap(),
                ChunkRef::new(
                    ObjectId::for_bytes(second),
                    first.len() as u64,
                    second.len() as u64,
                )
                .unwrap(),
            ],
        )
        .unwrap();
        snapshot_from_files(vec![("nested/payload.bin", manifest)])
    }

    fn hardlink_snapshot() -> WorkspaceMaterializationSnapshot {
        let payload = b"one immutable WholeFile object";
        let empty = Manifest::new(0, ChunkingStrategy::WholeFile, Vec::new()).unwrap();
        let whole = Manifest::new(
            payload.len() as u64,
            ChunkingStrategy::WholeFile,
            vec![ChunkRef::new(ObjectId::for_bytes(payload), 0, payload.len() as u64).unwrap()],
        )
        .unwrap();
        snapshot_from_files(vec![("empty.txt", empty), ("payload.bin", whole)])
    }

    fn snapshot_from_files(manifests: Vec<(&str, Manifest)>) -> WorkspaceMaterializationSnapshot {
        let files = manifests
            .into_iter()
            .map(|(path, manifest)| WorkspaceMaterializationFile {
                record: FileRecord::from_manifest(
                    neoengram_domain::core::LogicalPath::parse(path).unwrap(),
                    &manifest,
                )
                .unwrap(),
                manifest,
            })
            .collect::<Vec<_>>();
        let records = files
            .iter()
            .map(|file| file.record.clone())
            .collect::<Vec<_>>();
        let version = IndexVersion::from_snapshot(7, &records).unwrap();
        WorkspaceMaterializationSnapshot::new(version, files).unwrap()
    }

    fn assignment(
        snapshot: &WorkspaceMaterializationSnapshot,
        mode: SnapshotDeliveryMode,
        hardlink_policy: HardlinkPolicy,
    ) -> SnapshotDeliveryAssignment {
        let project_id = ProjectId::new("project-a").unwrap();
        let artifact_id = ArtifactId::new("artifact-a").unwrap();
        let snapshot_id = SnapshotId::new("snapshot-a").unwrap();
        let delivery_id = SnapshotDeliveryId::new(match mode {
            SnapshotDeliveryMode::Copy => "delivery-copy-a",
            SnapshotDeliveryMode::Hardlink => "delivery-hardlink-a",
            SnapshotDeliveryMode::Fuse => "delivery-fuse-a",
        })
        .unwrap();
        let target_relative_root = SnapshotDeliveryOperation::canonical_target_relative_root(
            &project_id,
            &artifact_id,
            &snapshot_id,
            &delivery_id,
        )
        .unwrap();
        let size = snapshot
            .files()
            .iter()
            .map(|file| file.record.total_size)
            .sum();
        let mut assignment = SnapshotDeliveryAssignment {
            job_id: JobId::new("job-delivery-a").unwrap(),
            assignment_id: AssignmentId::new("assignment-delivery-a").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            agent_id: AgentId::new("agent-a").unwrap(),
            principal: PrincipalRef {
                kind: PrincipalKind::System,
                id: PrincipalId::new("snapshot-delivery-test").unwrap(),
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
            snapshot_size_bytes: DecimalU64::new(size),
            copy_reserve_bytes: DecimalU64::new(0),
            hardlink_policy,
            agent_mount_id: AgentMountId::new("mount-a").unwrap(),
            mount_generation: MountGeneration::new(1),
            owner_generation: OwnerGeneration::new(1),
            placement_generation: PlacementGeneration::new(1),
            mode,
            target_relative_root,
            source_index_digest: snapshot.version().digest,
            request_digest: ContentDigest::from_bytes([0; 32]),
            delivery_generation: DeliveryGeneration::new(1),
            deadline_unix_ms: UnixMillis::new(u64::MAX),
            extensions: Extensions::new(),
        };
        assignment.request_digest = assignment.operation().request_digest().unwrap();
        assignment
    }

    fn fixture(
        snapshot: WorkspaceMaterializationSnapshot,
        mode: SnapshotDeliveryMode,
        hardlink_policy: HardlinkPolicy,
    ) -> (
        TempDir,
        SnapshotDeliveryAssignment,
        SnapshotDeliveryMaterializer,
    ) {
        let temporary = TempDir::new().unwrap();
        let mount = temporary.path().join("mount");
        fs::create_dir(&mount).unwrap();
        let assignment = assignment(&snapshot, mode, hardlink_policy);
        let objects =
            artifact_object_store(&mount, &assignment.tenant_id, &assignment.artifact_id).unwrap();
        for file in snapshot.files() {
            for chunk in &file.manifest.chunks {
                let bytes = match chunk.object_id {
                    id if id == ObjectId::for_bytes(b"first Copy chunk") => {
                        b"first Copy chunk".as_slice()
                    }
                    id if id == ObjectId::for_bytes(b"second Copy chunk") => {
                        b"second Copy chunk".as_slice()
                    }
                    id if id == ObjectId::for_bytes(b"one immutable WholeFile object") => {
                        b"one immutable WholeFile object".as_slice()
                    }
                    _ => panic!("fixture object payload is unknown"),
                };
                objects
                    .put_from(&ObjectSpec::for_bytes(bytes), &mut Cursor::new(bytes))
                    .unwrap();
            }
        }
        let materializer = SnapshotDeliveryMaterializer::for_binding(
            &mount,
            assignment.agent_id.clone(),
            assignment.tenant_id.clone(),
            assignment.storage_volume_id.clone(),
            assignment.agent_mount_id.clone(),
            assignment.mount_generation,
            assignment.owner_generation,
            Arc::new(TestBridge { snapshot }),
        );
        (temporary, assignment, materializer)
    }

    #[test]
    fn hardlink_engine_errors_keep_stable_delivery_codes() {
        let cases = [
            (
                EngineError::new(
                    EngineErrorCode::Conflict,
                    "HARDLINK_CROSS_FILESYSTEM: different devices",
                ),
                AgentErrorCode::InvalidAssignment,
                "HARDLINK_CROSS_FILESYSTEM",
            ),
            (
                EngineError::new(
                    EngineErrorCode::Conflict,
                    "HARDLINK_UNSAFE_VOLUME: inode identity is unavailable",
                ),
                AgentErrorCode::InvalidAssignment,
                "HARDLINK_UNSAFE_VOLUME",
            ),
            (
                EngineError::new(
                    EngineErrorCode::IntegrityViolation,
                    "HARDLINK_OBJECT_NOT_SEALED: object is writable",
                ),
                AgentErrorCode::InvalidAssignment,
                "HARDLINK_OBJECT_NOT_SEALED",
            ),
            (
                EngineError::new(EngineErrorCode::AlreadyExists, "destination exists"),
                AgentErrorCode::AssignmentMismatch,
                "DELIVERY_TARGET_CONFLICT",
            ),
            (
                EngineError::new(EngineErrorCode::ObjectMissing, "CAS object is missing"),
                AgentErrorCode::ObjectTransferFailed,
                "DELIVERY_OBJECT_UNAVAILABLE",
            ),
            (
                EngineError::new(EngineErrorCode::ObjectCorrupt, "CAS object digest differs"),
                AgentErrorCode::ObjectTransferFailed,
                "DELIVERY_OBJECT_UNAVAILABLE",
            ),
        ];

        for (error, expected_agent_code, expected_delivery_code) in cases {
            let mapped = map_hardlink_engine_error(error);
            assert_eq!(mapped.code(), expected_agent_code);
            assert!(
                mapped.message().starts_with(expected_delivery_code),
                "unexpected mapped error: {mapped}"
            );
        }
    }

    #[test]
    fn copy_replays_published_tree_and_cleans_assignment_staging() {
        let snapshot = copy_snapshot();
        let (temporary, assignment, materializer) = fixture(
            snapshot,
            SnapshotDeliveryMode::Copy,
            HardlinkPolicy::Disabled,
        );
        let first = materializer.materialize(&assignment).unwrap();
        assert_eq!(first.files, 1);
        assert_eq!(first.objects, 2);

        let target = temporary
            .path()
            .join("mount")
            .join(assignment.target_relative_root.as_str());
        let staging = deterministic_staging_path(target.parent().unwrap(), &assignment);
        fs::create_dir(&staging).unwrap();
        fs::write(staging.join("partial"), b"interrupted").unwrap();

        let mut replay_assignment = assignment.clone();
        // A replay of an already published, fully verified tree must not require a second full
        // allocation reserve. This value would overflow the new-materialization space check.
        replay_assignment.copy_reserve_bytes = DecimalU64::new(u64::MAX);
        replay_assignment.request_digest = replay_assignment.operation().request_digest().unwrap();
        let replay = materializer.materialize(&replay_assignment).unwrap();
        assert_eq!(replay, first);
        assert!(!staging.exists());
        assert_eq!(
            fs::read(target.join("nested/payload.bin")).unwrap(),
            [
                b"first Copy chunk".as_slice(),
                b"second Copy chunk".as_slice()
            ]
            .concat()
        );
    }

    #[cfg(unix)]
    #[test]
    fn copy_replay_rejects_writable_corrupt_missing_and_symlink_targets() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let snapshot = copy_snapshot();
        let (temporary, assignment, materializer) = fixture(
            snapshot,
            SnapshotDeliveryMode::Copy,
            HardlinkPolicy::Disabled,
        );
        materializer.materialize(&assignment).unwrap();
        let target = temporary
            .path()
            .join("mount")
            .join(assignment.target_relative_root.as_str());
        let file = target.join("nested/payload.bin");

        fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();
        let writable = materializer.materialize(&assignment).unwrap_err();
        assert_eq!(writable.code(), AgentErrorCode::AssignmentMismatch);
        assert!(writable.message().contains("writable"));

        fs::write(
            &file,
            vec![b'x'; fs::metadata(&file).unwrap().len() as usize],
        )
        .unwrap();
        fs::set_permissions(&file, fs::Permissions::from_mode(0o444)).unwrap();
        let corrupt = materializer.materialize(&assignment).unwrap_err();
        assert!(corrupt.message().contains("differs at offset"));

        let nested = target.join("nested");
        fs::set_permissions(&nested, fs::Permissions::from_mode(0o755)).unwrap();
        fs::remove_file(&file).unwrap();
        fs::set_permissions(&nested, fs::Permissions::from_mode(0o555)).unwrap();
        let missing = materializer.materialize(&assignment).unwrap_err();
        assert!(missing.message().contains("missing expected file"));

        let outside = temporary.path().join("outside");
        fs::write(&outside, b"outside").unwrap();
        fs::set_permissions(&nested, fs::Permissions::from_mode(0o755)).unwrap();
        symlink(&outside, &file).unwrap();
        fs::set_permissions(&nested, fs::Permissions::from_mode(0o555)).unwrap();
        let linked = materializer.materialize(&assignment).unwrap_err();
        assert!(linked.message().contains("contains symlink"));
    }

    #[cfg(unix)]
    #[test]
    fn hardlink_requires_sealing_and_publishes_read_only_inode_and_empty_file() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let snapshot = hardlink_snapshot();
        let (temporary, mut assignment, materializer) = fixture(
            snapshot,
            SnapshotDeliveryMode::Hardlink,
            HardlinkPolicy::SealedAcl,
        );
        let unsealed = materializer.materialize(&assignment).unwrap_err();
        assert!(unsealed.message().contains("HARDLINK_OBJECT_NOT_SEALED"));

        assignment.hardlink_policy = HardlinkPolicy::TrustedLocal;
        assignment.request_digest = assignment.operation().request_digest().unwrap();
        let stats = materializer.materialize(&assignment).unwrap();
        assert_eq!(stats.files, 2);
        assert_eq!(stats.objects, 0);

        let mount = temporary.path().join("mount");
        let target = mount.join(assignment.target_relative_root.as_str());
        let destination = fs::symlink_metadata(target.join("payload.bin")).unwrap();
        let object_id = ObjectId::for_bytes(b"one immutable WholeFile object");
        let source = fs::symlink_metadata(
            mount
                .join(".neoengram/objects/tenants/tenant-a/artifacts/artifact-a/objects")
                .join(object_id.to_hex()),
        )
        .unwrap();
        assert_eq!(
            (source.dev(), source.ino()),
            (destination.dev(), destination.ino())
        );
        assert_eq!(destination.permissions().mode() & 0o222, 0);
        assert_eq!(
            fs::symlink_metadata(target.join("empty.txt"))
                .unwrap()
                .permissions()
                .mode()
                & 0o222,
            0
        );
        assert_eq!(materializer.materialize(&assignment).unwrap(), stats);
    }
}
