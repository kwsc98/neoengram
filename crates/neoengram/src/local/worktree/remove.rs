use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
};

use anyhow::{bail, ensure, Context, Result};
use neoengram_core::{FileNode, Index};
use serde::{Deserialize, Serialize};
use tempfile::{Builder, NamedTempFile};

use crate::local::{
    fs::durable::{rename_durable, sync_directory, sync_parent},
    repository::{is_neoengram_dir_name, Repository},
};

const IO_BUFFER_SIZE: usize = 64 * 1024;
const TRANSACTION_FORMAT_VERSION: u32 = 1;
const PREPARED_STATE: &str = "PREPARED";
const INDEX_COMMITTED_STATE: &str = "INDEX_COMMITTED";

/// 从暂存区移除一个文件或目录范围。
///
/// 默认行为与 `git rm` 一致：同时移除工作区中的已跟踪文件和 index 项。`cached`
/// 只修改 index；除非显式传入 `force`，两种模式都会拒绝丢弃与 index 不一致的文件。
pub(crate) fn remove_tracked_path(
    repository: &Repository,
    current_dir: &Path,
    requested_path: &Path,
    cached: bool,
    force: bool,
) -> Result<Vec<String>> {
    let scope = repository_scope(requested_path, current_dir, repository.root())?;
    let _lock = repository.acquire_write_lock()?;

    let index = repository.read_index()?;
    let mut removed_nodes = Vec::new();
    let mut retained_nodes = Vec::with_capacity(index.files.len());
    for file in &index.files {
        if path_is_in_scope(&file.path, &scope) {
            removed_nodes.push(file.clone());
        } else {
            retained_nodes.push(file.clone());
        }
    }
    ensure!(
        !removed_nodes.is_empty(),
        "路径没有已跟踪文件: {}",
        requested_path.display()
    );

    // 即使是 --cached，也默认检查工作区是否与 index 一致。这样一次拼写错误不会静默
    // 丢弃已经 add 但尚未 commit 的版本；用户可以用 --force 明确接受该结果。
    if !force {
        for file in &removed_nodes {
            ensure_workspace_is_clean(repository.root(), file)?;
        }
    }

    let next_index = Index {
        format_version: index.format_version,
        files: retained_nodes,
    };

    if cached {
        repository.write_index(&next_index)?;
    } else {
        remove_workspace_and_update_index(repository, &removed_nodes, &index, &next_index, force)?;
    }

    Ok(removed_nodes.into_iter().map(|file| file.path).collect())
}

fn remove_workspace_and_update_index(
    repository: &Repository,
    removed_nodes: &[FileNode],
    previous_index: &Index,
    next_index: &Index,
    force: bool,
) -> Result<()> {
    let mut transaction =
        RemovalTransaction::create(repository, removed_nodes, previous_index, next_index)?;
    for file in removed_nodes {
        if let Err(error) = transaction.move_workspace_file(repository.root(), file, force) {
            return rollback_operation(&mut transaction, error);
        }
    }

    if let Err(error) = repository.write_index(next_index) {
        return rollback_operation(&mut transaction, error);
    }
    super::test_crash_at("rm-after-index");

    // index 已原子发布后绝不能再回滚工作区，否则会形成“index 已删除、文件却恢复”的
    // 半完成状态。若状态标记或清理失败，保留事务让 recover 根据 index.before/after 判定。
    transaction.mark_index_committed().with_context(|| {
        format!(
            "index 已更新，但无法标记 rm 事务完成；请运行 recover: {}",
            transaction.root.display()
        )
    })?;
    transaction.finish()
}

fn rollback_operation(transaction: &mut RemovalTransaction, error: anyhow::Error) -> Result<()> {
    match transaction.rollback() {
        Ok(()) => Err(error),
        Err(rollback_error) => Err(error.context(format!(
            "rm 回滚失败: {rollback_error:#}；备份保留在 {}",
            transaction.root.display()
        ))),
    }
}

fn ensure_workspace_is_clean(repository_root: &Path, file: &FileNode) -> Result<()> {
    let path = workspace_path(repository_root, &file.path);
    let Some(metadata) = safe_leaf_metadata(repository_root, &file.path)? else {
        // 工作区文件已经被用户删除，此时 rm 只是把该删除记录到 index。
        return Ok(());
    };
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "工作区文件类型已改变，使用 `--force` 明确移除: {}",
        file.path
    );
    ensure!(
        file_matches_node(&path, file)?,
        "工作区文件包含未暂存修改，使用 `--force` 明确移除: {}",
        file.path
    );
    Ok(())
}

struct MovedFile {
    logical_path: String,
    original: PathBuf,
    backup: PathBuf,
}

/// rm 先把工作区文件原子移动到仓库内部，再更新 index。任何进程内错误都会按逆序恢复
/// 已移动文件；进程崩溃时事务目录也会保留原始内容，避免数据被直接 unlink。
struct RemovalTransaction {
    root: PathBuf,
    repository_root: PathBuf,
    moved_files: Vec<MovedFile>,
}

impl RemovalTransaction {
    fn create(
        repository: &Repository,
        removed_nodes: &[FileNode],
        previous_index: &Index,
        next_index: &Index,
    ) -> Result<Self> {
        let temporary = Builder::new()
            .prefix("rm-")
            .tempdir_in(repository.transactions_dir())
            .with_context(|| {
                format!(
                    "无法创建 rm 事务: {}",
                    repository.transactions_dir().display()
                )
            })?;
        let root = temporary.path().to_path_buf();
        fs::create_dir(root.join("backup"))
            .with_context(|| format!("无法创建 rm 备份目录: {}", root.display()))?;
        write_synced(&root.join("OPERATION"), b"rm\n")?;
        write_json_synced(&root.join("index.before.json"), previous_index)?;
        write_json_synced(&root.join("index.after.json"), next_index)?;
        let manifest = RemovalManifest {
            format_version: TRANSACTION_FORMAT_VERSION,
            entries: removed_nodes
                .iter()
                .map(|file| RemovalManifestEntry {
                    logical_path: file.path.clone(),
                    state: "planned".to_owned(),
                })
                .collect(),
        };
        // manifest、前后 index 和 PREPARED 状态全部持久化之后，才允许第一次 rename。
        // recover 因而总能判定每个 backup 应恢复到哪个逻辑路径。
        write_json_synced(&root.join("manifest.json"), &manifest)?;
        write_synced(
            &root.join("STATE"),
            format!("{PREPARED_STATE}\n").as_bytes(),
        )?;
        sync_directory(&root.join("backup"))?;
        sync_directory(&root)?;
        sync_directory(&repository.transactions_dir())?;
        let root = temporary.keep();
        Ok(Self {
            root,
            repository_root: repository.root().to_path_buf(),
            moved_files: Vec::new(),
        })
    }

    fn move_workspace_file(
        &mut self,
        repository_root: &Path,
        file: &FileNode,
        force: bool,
    ) -> Result<()> {
        let original = workspace_path(repository_root, &file.path);
        let Some(metadata) = safe_leaf_metadata(repository_root, &file.path)? else {
            return Ok(());
        };
        ensure!(
            metadata.is_file() || metadata.file_type().is_symlink(),
            "拒绝用 rm 递归删除替代已跟踪文件的目录或特殊文件: {}",
            file.path
        );
        if !force {
            // 在真正 rename 前再次核对内容，缩小预检与修改工作区之间的竞态窗口。
            ensure!(
                metadata.is_file()
                    && !metadata.file_type().is_symlink()
                    && file_matches_node(&original, file)?,
                "工作区文件在 rm 执行期间发生变化: {}",
                file.path
            );
        }

        let backup = logical_join(&self.root.join("backup"), &file.path);
        let parent = backup
            .parent()
            .with_context(|| format!("备份路径没有父目录: {}", backup.display()))?;
        create_dir_all_durable(parent)?;
        rename_durable(&original, &backup)
            .with_context(|| format!("无法把工作区文件移动到 rm 事务: {}", original.display()))?;
        super::test_crash_at("rm-after-backup");
        self.moved_files.push(MovedFile {
            logical_path: file.path.clone(),
            original,
            backup,
        });
        Ok(())
    }

    fn mark_index_committed(&self) -> Result<()> {
        write_atomic_synced(
            &self.root.join("STATE"),
            format!("{INDEX_COMMITTED_STATE}\n").as_bytes(),
        )
    }

    fn rollback(&mut self) -> Result<()> {
        for moved in self.moved_files.iter().rev() {
            ensure_path_absent(&moved.original)?;
            ensure_safe_parent_directories(&self.repository_root, &moved.logical_path)?;
            rename_durable(&moved.backup, &moved.original).with_context(|| {
                format!("回滚时无法恢复工作区文件: {}", moved.original.display())
            })?;
        }
        remove_dir_all_durable(&self.root)
            .with_context(|| format!("无法清理已回滚的 rm 事务: {}", self.root.display()))?;
        self.moved_files.clear();
        Ok(())
    }

    fn finish(self) -> Result<()> {
        remove_dir_all_durable(&self.root)
            .with_context(|| format!("无法清理已完成的 rm 事务: {}", self.root.display()))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RemovalManifest {
    format_version: u32,
    entries: Vec<RemovalManifestEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RemovalManifestEntry {
    logical_path: String,
    state: String,
}

/// 恢复单个遗留的 rm 事务。
///
/// 调用方必须已经持有全仓库 recovery 锁；这里不能再获取普通写锁，因为普通写锁会把
/// transactions 中的遗留项视为需要人工处理的异常。`abort` 只允许回滚尚未发布 index
/// 的事务，绝不会把已经提交的新 index 反向改写。
pub(super) fn recover_transaction(
    repository: &Repository,
    transaction_root: &Path,
    abort: bool,
) -> Result<()> {
    validate_transaction_root(repository, transaction_root)?;
    let operation =
        fs::read_to_string(transaction_root.join("OPERATION")).context("无法读取 rm 事务类型")?;
    ensure!(operation.trim() == "rm", "事务类型不是 rm");

    let state =
        fs::read_to_string(transaction_root.join("STATE")).context("无法读取 rm 事务状态")?;
    let state = state.trim();
    ensure!(
        state == PREPARED_STATE || state == INDEX_COMMITTED_STATE,
        "未知的 rm 事务状态: {state}"
    );

    let before: Index = read_json(&transaction_root.join("index.before.json"))?;
    let after: Index = read_json(&transaction_root.join("index.after.json"))?;
    repository.validate_index_snapshot(&before)?;
    repository.validate_index_snapshot(&after)?;
    let manifest: RemovalManifest = read_json(&transaction_root.join("manifest.json"))?;
    validate_manifest(repository, &manifest, &before, &after)?;
    let current = repository.read_index()?;

    if current == after {
        ensure!(
            !abort,
            "rm 的 index 已经提交，不能 abort；请执行普通 recover 完成清理"
        );
        return remove_dir_all_durable(transaction_root)
            .with_context(|| format!("无法清理已提交的 rm 事务: {}", transaction_root.display()));
    }

    ensure!(
        current == before,
        "当前 index 既不匹配 rm 事务的 before，也不匹配 after；拒绝自动恢复"
    );
    ensure!(
        state == PREPARED_STATE,
        "rm 标记为 INDEX_COMMITTED，但当前 index 仍是 before；拒绝删除备份"
    );

    restore_manifest_backups(repository, transaction_root, &manifest)?;
    remove_dir_all_durable(transaction_root)
        .with_context(|| format!("无法清理已回滚的 rm 事务: {}", transaction_root.display()))
}

fn validate_transaction_root(repository: &Repository, transaction_root: &Path) -> Result<()> {
    ensure!(
        transaction_root.parent() == Some(repository.transactions_dir().as_path()),
        "rm 事务不在当前仓库的 transactions 目录中: {}",
        transaction_root.display()
    );
    let metadata = fs::symlink_metadata(transaction_root)
        .with_context(|| format!("无法检查 rm 事务目录: {}", transaction_root.display()))?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "rm 事务路径不是普通目录: {}",
        transaction_root.display()
    );
    Ok(())
}

fn validate_manifest(
    repository: &Repository,
    manifest: &RemovalManifest,
    before: &Index,
    after: &Index,
) -> Result<()> {
    ensure!(
        manifest.format_version == TRANSACTION_FORMAT_VERSION,
        "不支持的 rm 事务格式版本 {}",
        manifest.format_version
    );
    let mut paths = BTreeSet::new();
    for entry in &manifest.entries {
        ensure!(entry.state == "planned", "未知的 rm manifest entry 状态");
        repository.validate_logical_path(&entry.logical_path)?;
        ensure!(
            paths.insert(entry.logical_path.as_str()),
            "rm manifest 包含重复路径: {}",
            entry.logical_path
        );
    }

    let before_files: std::collections::BTreeMap<&str, &FileNode> = before
        .files
        .iter()
        .map(|file| (file.path.as_str(), file))
        .collect();
    let after_files: std::collections::BTreeMap<&str, &FileNode> = after
        .files
        .iter()
        .map(|file| (file.path.as_str(), file))
        .collect();
    for (path, file) in &after_files {
        ensure!(
            before_files
                .get(path)
                .is_some_and(|before| *before == *file),
            "rm index.after 新增或修改了路径: {path}"
        );
    }
    let expected_paths: BTreeSet<&str> = before_files
        .keys()
        .filter(|path| !after_files.contains_key(**path))
        .copied()
        .collect();
    ensure!(
        paths == expected_paths,
        "rm manifest 路径集合与 index.before - index.after 不一致"
    );
    Ok(())
}

fn restore_manifest_backups(
    repository: &Repository,
    transaction_root: &Path,
    manifest: &RemovalManifest,
) -> Result<()> {
    for entry in manifest.entries.iter().rev() {
        let backup = logical_join(&transaction_root.join("backup"), &entry.logical_path);
        match fs::symlink_metadata(&backup) {
            Ok(metadata) => ensure!(
                metadata.is_file() || metadata.file_type().is_symlink(),
                "rm 备份不是普通文件或符号链接: {}",
                backup.display()
            ),
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("无法检查 rm 备份: {}", backup.display()));
            }
        }

        let destination = workspace_path(repository.root(), &entry.logical_path);
        ensure_path_absent(&destination)?;
        ensure_safe_parent_directories(repository.root(), &entry.logical_path)?;
        rename_durable(&backup, &destination)
            .with_context(|| format!("无法恢复 rm 备份到工作区: {}", destination.display()))?;
    }
    Ok(())
}

fn repository_scope(path: &Path, current_dir: &Path, repository_root: &Path) -> Result<String> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        current_dir.join(path)
    };
    let normalized = normalize_absolute_path(&absolute)?;
    ensure!(
        normalized.starts_with(repository_root),
        "不能移除仓库之外的路径: {}",
        path.display()
    );

    let relative = normalized
        .strip_prefix(repository_root)
        .context("无法计算 rm 路径的仓库相对位置")?;
    let mut names = Vec::new();
    let components: Vec<_> = relative.components().collect();
    let mut current = repository_root.to_path_buf();
    for (index, component) in components.iter().enumerate() {
        let Component::Normal(name) = component else {
            bail!("rm 路径未规范化: {}", path.display());
        };
        let name = name
            .to_str()
            .with_context(|| format!("rm 路径不是有效 UTF-8: {}", path.display()))?;
        ensure!(
            !is_neoengram_dir_name(std::ffi::OsStr::new(name)),
            "不能移除 NeoEngram 内部路径: {}",
            path.display()
        );
        names.push(name);

        // leaf 可以是 symlink，force 模式只会移动该链接本身；任何祖先 symlink 都会
        // 改变路径解析边界，因此无条件拒绝。
        current.push(name);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if index + 1 < components.len() => ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "rm 路径祖先不是普通目录: {}",
                current.display()
            ),
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => break,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("无法检查 rm 路径: {}", current.display()));
            }
        }
    }
    Ok(names.join("/"))
}

fn normalize_absolute_path(path: &Path) -> Result<PathBuf> {
    ensure!(path.is_absolute(), "内部错误：rm 路径不是绝对路径");
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                ensure!(
                    normalized.pop(),
                    "rm 路径越过文件系统根目录: {}",
                    path.display()
                );
            }
            Component::Normal(name) => normalized.push(name),
        }
    }
    Ok(normalized)
}

fn path_is_in_scope(path: &str, scope: &str) -> bool {
    scope.is_empty()
        || path == scope
        || path
            .strip_prefix(scope)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn safe_leaf_metadata(repository_root: &Path, logical_path: &str) -> Result<Option<fs::Metadata>> {
    let components: Vec<&str> = logical_path.split('/').collect();
    let mut current = repository_root.to_path_buf();
    for (index, component) in components.iter().enumerate() {
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if index + 1 < components.len() => ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "工作区路径祖先不是普通目录: {}",
                current.display()
            ),
            Ok(metadata) => return Ok(Some(metadata)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("无法检查工作区路径: {}", current.display()));
            }
        }
    }
    Ok(None)
}

fn file_matches_node(path: &Path, node: &FileNode) -> Result<bool> {
    let metadata = fs::metadata(path)
        .with_context(|| format!("无法读取工作区文件元数据: {}", path.display()))?;
    if !metadata.is_file() || metadata.len() != node.total_size {
        return Ok(false);
    }

    let mut reader =
        File::open(path).with_context(|| format!("无法打开工作区文件: {}", path.display()))?;
    for chunk in &node.chunks {
        let Some(actual_hash) = read_exact_hash(&mut reader, chunk.size)? else {
            return Ok(false);
        };
        if actual_hash != chunk.hash {
            return Ok(false);
        }
    }

    let mut trailing = [0_u8; 1];
    Ok(reader
        .read(&mut trailing)
        .with_context(|| format!("无法检查工作区文件结尾: {}", path.display()))?
        == 0)
}

fn read_exact_hash(reader: &mut File, size: u64) -> Result<Option<String>> {
    let mut remaining = size;
    let mut buffer = [0_u8; IO_BUFFER_SIZE];
    let mut hasher = blake3::Hasher::new();
    while remaining > 0 {
        let limit = usize::try_from(remaining.min(IO_BUFFER_SIZE as u64))
            .context("当前平台无法表示读取缓冲区大小")?;
        let read = reader
            .read(&mut buffer[..limit])
            .context("无法读取工作区文件")?;
        if read == 0 {
            return Ok(None);
        }
        hasher.update(&buffer[..read]);
        remaining = remaining
            .checked_sub(u64::try_from(read).context("读取长度超出 u64 范围")?)
            .context("工作区文件读取长度溢出")?;
    }
    Ok(Some(hasher.finalize().to_hex().to_string()))
}

fn ensure_path_absent(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Ok(_) => bail!("回滚目标已被重新创建，拒绝覆盖: {}", path.display()),
        Err(error) => Err(error).with_context(|| format!("无法检查回滚目标: {}", path.display())),
    }
}

fn ensure_safe_parent_directories(repository_root: &Path, logical_path: &str) -> Result<()> {
    let components: Vec<&str> = logical_path.split('/').collect();
    let mut current = repository_root.to_path_buf();
    for component in components.iter().take(components.len().saturating_sub(1)) {
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) => ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "rm 恢复路径祖先不是普通目录: {}",
                current.display()
            ),
            Err(error) if error.kind() == io::ErrorKind::NotFound => fs::create_dir(&current)
                .with_context(|| format!("无法创建 rm 恢复目录: {}", current.display()))
                .and_then(|()| sync_parent(&current))?,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("无法检查 rm 恢复目录: {}", current.display()));
            }
        }
    }
    Ok(())
}

fn create_dir_all_durable(path: &Path) -> Result<()> {
    let mut missing = Vec::new();
    let mut current = path;
    loop {
        match fs::symlink_metadata(current) {
            Ok(metadata) => {
                ensure!(
                    metadata.is_dir() && !metadata.file_type().is_symlink(),
                    "rm 事务路径祖先不是普通目录: {}",
                    current.display()
                );
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                missing.push(current.to_path_buf());
                current = current
                    .parent()
                    .with_context(|| format!("目录没有父级: {}", current.display()))?;
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("无法检查 rm 事务目录: {}", current.display()));
            }
        }
    }

    for directory in missing.iter().rev() {
        fs::create_dir(directory)
            .with_context(|| format!("无法创建 rm 事务目录: {}", directory.display()))?;
        sync_parent(directory)?;
    }
    Ok(())
}

fn remove_dir_all_durable(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .with_context(|| format!("待删除事务目录没有父级: {}", path.display()))?;
    fs::remove_dir_all(path).with_context(|| format!("无法删除事务目录: {}", path.display()))?;
    sync_directory(parent)
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let bytes = fs::read(path).with_context(|| format!("无法读取 JSON: {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("JSON 格式无效: {}", path.display()))
}

fn write_json_synced<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let mut bytes = serde_json::to_vec_pretty(value).context("无法序列化 rm 事务 JSON")?;
    bytes.push(b'\n');
    write_synced(path, &bytes)
}

fn write_synced(path: &Path, contents: &[u8]) -> Result<()> {
    let mut file =
        File::create(path).with_context(|| format!("无法创建事务状态文件: {}", path.display()))?;
    file.write_all(contents)
        .with_context(|| format!("无法写入事务状态文件: {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("无法同步事务状态文件: {}", path.display()))?;
    sync_parent(path)
}

fn write_atomic_synced(path: &Path, contents: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .with_context(|| format!("事务状态路径没有父目录: {}", path.display()))?;
    let mut temporary = NamedTempFile::new_in(parent)
        .with_context(|| format!("无法创建临时事务状态文件: {}", parent.display()))?;
    temporary
        .write_all(contents)
        .with_context(|| format!("无法写入临时事务状态文件: {}", path.display()))?;
    temporary
        .as_file()
        .sync_all()
        .with_context(|| format!("无法同步临时事务状态文件: {}", path.display()))?;
    temporary
        .persist(path)
        .map(|_| ())
        .map_err(|error| error.error)
        .with_context(|| format!("无法原子更新事务状态: {}", path.display()))?;
    sync_directory(parent)
}

fn workspace_path(repository_root: &Path, logical_path: &str) -> PathBuf {
    logical_join(repository_root, logical_path)
}

fn logical_join(base: &Path, logical_path: &str) -> PathBuf {
    let mut path = base.to_path_buf();
    for component in logical_path.split('/') {
        path.push(component);
    }
    path
}
