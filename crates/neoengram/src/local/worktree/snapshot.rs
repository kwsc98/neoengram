//! Immutable, read-only materialization of a Commit into an independent directory.
//!
//! A read-only snapshot deliberately does not touch the current worktree, index, or HEAD.  The
//! repository write lock is held while the immutable Tree and Chunk objects are read so a
//! concurrent writer/GC cannot change the object set midway through publication.

use std::{
    fs,
    io::{self, Write},
    path::{Component, Path, PathBuf},
};

use anyhow::{bail, ensure, Context, Result};
use neoengram_core::{FileNode, Tree};
use tempfile::{Builder, NamedTempFile};
use walkdir::WalkDir;

use crate::local::{
    fs::durable::{create_dir_all_durable, rename_noreplace_durable, sync_directory},
    objects::ObjectSpec,
    repository::{is_neoengram_dir_name, Repository},
};

use super::workspace::logical_join;

/// Result returned after a snapshot has been atomically published.
#[derive(Debug, Clone)]
pub(crate) struct ReadOnlySnapshotOutcome {
    commit_id: String,
    destination: PathBuf,
    file_count: usize,
}

impl ReadOnlySnapshotOutcome {
    pub(crate) fn commit_id(&self) -> &str {
        &self.commit_id
    }

    pub(crate) fn destination(&self) -> &Path {
        &self.destination
    }

    pub(crate) fn file_count(&self) -> usize {
        self.file_count
    }
}

/// Materialize `target` into a new read-only directory without changing repository state.
pub(crate) fn materialize_read_only_snapshot(
    repository: &Repository,
    target: &str,
    destination: &Path,
) -> Result<ReadOnlySnapshotOutcome> {
    ensure_supported_platform()?;
    ensure!(!target.trim().is_empty(), "Checkout TARGET 不能为空");
    repository.ensure_no_unfinished_transactions()?;

    // A snapshot reads immutable metadata and objects for potentially a long time.  Serialize it
    // with add/commit/checkout/gc so GC cannot remove a Chunk after the Tree has been resolved.
    let mut lock = repository.acquire_write_lock()?;
    lock.set_operation("read-only-snapshot", None)?;
    let destination = validate_destination(repository, destination)?;
    let (commit_id, tree) = resolve_target(repository, target.trim())?;

    let parent = destination
        .parent()
        .context("只读快照目标没有父目录")?
        .to_path_buf();
    let temporary = Builder::new()
        .prefix(".neoengram-read-only-")
        .tempdir_in(&parent)
        .with_context(|| format!("无法创建只读快照临时目录: {}", parent.display()))?;

    if let Err(error) = materialize_tree(repository, &tree, temporary.path()) {
        cleanup_staging(temporary.path());
        return Err(error);
    }
    if let Err(error) = set_read_only_tree(temporary.path()) {
        cleanup_staging(temporary.path());
        return Err(error);
    }

    // Keep the staging directory alive until the no-replace rename succeeds.  If another
    // process wins the destination race, explicitly make the tree writable before removing it;
    // read-only directory modes otherwise prevent cleanup on Unix.
    let staging = temporary.keep();
    if let Err(error) = rename_noreplace_durable(&staging, &destination) {
        cleanup_staging(&staging);
        return Err(error).with_context(|| {
            format!(
                "无法以 no-replace 方式发布只读快照 {}",
                destination.display()
            )
        });
    }

    Ok(ReadOnlySnapshotOutcome {
        commit_id,
        destination,
        file_count: tree.files.len(),
    })
}

fn resolve_target(repository: &Repository, target: &str) -> Result<(String, Tree)> {
    let target_id = repository.resolve_target_id(target)?;
    let commit = repository.read_commit(&target_id)?;
    let tree = repository.read_tree(&commit.tree_hash)?;
    Ok((target_id, tree))
}

fn materialize_tree(repository: &Repository, tree: &Tree, staging_root: &Path) -> Result<()> {
    for file in &tree.files {
        repository.validate_logical_path(&file.path)?;
        materialize_file(repository, file, staging_root)?;
    }
    Ok(())
}

fn materialize_file(repository: &Repository, file: &FileNode, staging_root: &Path) -> Result<()> {
    let destination = logical_join(staging_root, &file.path);
    let parent = destination
        .parent()
        .with_context(|| format!("只读快照文件没有父目录: {}", file.path))?;
    create_dir_all_durable(parent)?;
    ensure_snapshot_parent(staging_root, parent)?;

    let mut temporary = NamedTempFile::new_in(parent)
        .with_context(|| format!("无法创建只读快照临时文件: {}", parent.display()))?;
    let mut written = 0_u64;
    let object_store = repository.object_store();
    for chunk in &file.chunks {
        let spec = ObjectSpec::new(chunk.hash.clone(), chunk.size)?;
        object_store
            .copy_to(&spec, temporary.as_file_mut())
            .with_context(|| format!("无法读取文件 {} 的 Chunk {}", file.path, chunk.hash))?;
        written = written
            .checked_add(chunk.size)
            .context("只读快照文件大小溢出")?;
    }
    ensure!(
        written == file.total_size,
        "只读快照文件大小与 Tree 不一致: {}",
        file.path
    );
    temporary
        .as_file_mut()
        .flush()
        .with_context(|| format!("无法刷新只读快照文件: {}", file.path))?;
    ensure!(
        temporary.as_file().metadata()?.len() == file.total_size,
        "只读快照文件大小与实际写入不一致: {}",
        file.path
    );
    temporary
        .as_file()
        .sync_all()
        .with_context(|| format!("无法同步只读快照文件: {}", file.path))?;

    // Keep the final path no-replace even inside the private staging tree.  A malformed Tree or
    // an unexpected race must never silently replace another path.
    temporary
        .persist_noclobber(&destination)
        .map_err(|error| error.error)
        .with_context(|| format!("无法发布只读快照文件: {}", destination.display()))?;
    sync_directory(parent)
}

/// Verify that every parent component in the private staging root is a real directory.  This is
/// cheap compared with reading the Chunk payload and keeps a future caller from accidentally
/// following a symlink if the staging implementation changes.
fn ensure_snapshot_parent(staging_root: &Path, parent: &Path) -> Result<()> {
    let relative = parent
        .strip_prefix(staging_root)
        .with_context(|| format!("只读快照父目录越过临时根: {}", parent.display()))?;
    let mut current = staging_root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            bail!("只读快照路径包含危险组件: {}", parent.display());
        };
        current.push(name);
        let metadata = fs::symlink_metadata(&current)
            .with_context(|| format!("无法检查只读快照目录: {}", current.display()))?;
        ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "只读快照父路径不是普通目录: {}",
            current.display()
        );
    }
    Ok(())
}

fn validate_destination(repository: &Repository, requested: &Path) -> Result<PathBuf> {
    let current_dir = std::env::current_dir().context("无法确定当前工作目录")?;
    let absolute = if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        current_dir.join(requested)
    };
    let normalized = lexical_normalize(&absolute)?;
    let file_name = normalized
        .file_name()
        .context("只读快照目标必须是一个新目录名")?;
    ensure!(
        file_name != "." && file_name != "..",
        "只读快照目标目录名无效: {}",
        requested.display()
    );

    let parent = normalized.parent().context("只读快照目标没有父目录")?;
    let parent_metadata = fs::symlink_metadata(parent)
        .with_context(|| format!("只读快照目标的父目录不存在或不可访问: {}", parent.display()))?;
    ensure!(
        parent_metadata.is_dir() && !parent_metadata.file_type().is_symlink(),
        "只读快照目标的父级不是普通目录: {}",
        parent.display()
    );
    ensure_no_symlink_ancestors(parent)?;

    let canonical_parent = fs::canonicalize(parent)
        .with_context(|| format!("无法解析只读快照目标父目录: {}", parent.display()))?;
    let storage_root = fs::canonicalize(repository.repository_dir())?;
    ensure!(
        !canonical_parent.starts_with(&storage_root),
        "只读快照目标不能位于当前 NeoEngram 工作区或内部目录: {}",
        normalized.display()
    );
    let managed_exports = fs::canonicalize(repository.container_root().join("exports"))?;
    if !canonical_parent.starts_with(&managed_exports) {
        for workspace in repository.list_workspaces()? {
            ensure!(
                !canonical_parent.starts_with(&workspace.root),
                "只读快照目标不能位于当前 NeoEngram 工作区或内部目录: {}",
                normalized.display()
            );
        }
    }

    match fs::symlink_metadata(&normalized) {
        Ok(_) => bail!(
            "只读快照目标已存在；为避免覆盖现有内容，请选择一个新目录: {}",
            normalized.display()
        ),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("无法检查只读快照目标: {}", normalized.display()));
        }
    }
    Ok(canonical_parent.join(file_name))
}

fn ensure_no_symlink_ancestors(path: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        let metadata = fs::symlink_metadata(&current)
            .with_context(|| format!("无法检查只读快照目标的路径祖先: {}", current.display()))?;
        ensure!(
            !metadata.file_type().is_symlink(),
            "只读快照目标的路径祖先不能是符号链接: {}",
            current.display()
        );
        if current != path {
            ensure!(
                metadata.is_dir(),
                "只读快照目标的路径祖先不是目录: {}",
                current.display()
            );
        }
    }
    Ok(())
}

fn lexical_normalize(path: &Path) -> Result<PathBuf> {
    ensure!(path.is_absolute(), "只读快照目标路径必须可解析为绝对路径");
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                ensure!(
                    normalized.pop(),
                    "只读快照目标路径越过文件系统根目录: {}",
                    path.display()
                );
            }
            Component::Normal(name) => {
                ensure!(
                    !is_neoengram_dir_name(name),
                    "只读快照目标不能包含 NeoEngram 内部目录: {}",
                    path.display()
                );
                normalized.push(name);
            }
        }
    }
    ensure!(
        normalized.parent().is_some() && normalized.file_name().is_some(),
        "只读快照目标必须是一个新目录，而不能是文件系统根目录"
    );
    Ok(normalized)
}

#[cfg(unix)]
fn ensure_supported_platform() -> Result<()> {
    Ok(())
}

#[cfg(not(unix))]
fn ensure_supported_platform() -> Result<()> {
    bail!("`export` 当前仅支持 Unix/macOS；当前平台不提供安全的只读权限发布")
}

#[cfg(unix)]
fn set_read_only_tree(root: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut directories = Vec::new();
    for entry in WalkDir::new(root).follow_links(false) {
        let entry = entry.with_context(|| format!("无法扫描只读快照: {}", root.display()))?;
        ensure!(
            !entry.file_type().is_symlink(),
            "只读快照中不允许出现符号链接: {}",
            entry.path().display()
        );
        if entry.file_type().is_dir() {
            directories.push(entry.into_path());
        } else if entry.file_type().is_file() {
            fs::set_permissions(entry.path(), fs::Permissions::from_mode(0o444))
                .with_context(|| format!("无法设置只读快照文件权限: {}", entry.path().display()))?;
        } else {
            bail!("只读快照包含不支持的文件类型: {}", entry.path().display());
        }
    }
    // Set children before parents so the root remains writable until every file has been checked.
    for directory in directories.into_iter().rev() {
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o555))
            .with_context(|| format!("无法设置只读快照目录权限: {}", directory.display()))?;
        sync_directory(&directory)?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn set_read_only_tree(_root: &Path) -> Result<()> {
    bail!("`export` 当前仅支持 Unix/macOS；无法设置 0444/0555 权限")
}

#[cfg(unix)]
fn cleanup_staging(root: &Path) {
    use std::os::unix::fs::PermissionsExt;

    if !root.exists() {
        return;
    }
    // Best effort: cleanup is only needed after an error, and the original error is more useful
    // than a secondary permission failure.
    let mut directories = Vec::new();
    if let Ok(entries) = WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
    {
        for entry in entries {
            if entry.file_type().is_dir() {
                directories.push(entry.into_path());
            } else if !entry.file_type().is_symlink() {
                let _ = fs::set_permissions(entry.path(), fs::Permissions::from_mode(0o600));
            }
        }
        for directory in directories {
            let _ = fs::set_permissions(directory, fs::Permissions::from_mode(0o700));
        }
    }
    let _ = fs::remove_dir_all(root);
}

#[cfg(not(unix))]
fn cleanup_staging(root: &Path) {
    let _ = fs::remove_dir_all(root);
}
