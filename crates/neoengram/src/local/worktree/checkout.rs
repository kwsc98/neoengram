use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{bail, ensure, Context, Result};
use neoengram_core::{FileNode, Index, Tree, INDEX_FORMAT_VERSION};
use serde::{Deserialize, Serialize};
use tempfile::{Builder, NamedTempFile};

use crate::local::{
    fs::durable::{rename_durable, sync_directory, sync_parent, write_json_atomic},
    metadata::HeadState,
    objects::ObjectSpec,
    repository::{Repository, RepositoryWriteLock},
};

const FILE_HASH_DOMAIN: &[u8] = b"neoengram-file-v2";
const IO_BUFFER_SIZE: usize = 64 * 1024;
const JOURNAL_FORMAT_VERSION: u32 = 2;
const JOURNAL_FILE_NAME: &str = "journal.json";
const ORIGINAL_INDEX_FILE_NAME: &str = "index.before.json";
const OPERATION_FILE_NAME: &str = "OPERATION";

pub(crate) struct CheckoutOutcome {
    commit_id: String,
    head_state: HeadState,
    file_count: usize,
    removed_count: usize,
}

impl CheckoutOutcome {
    pub(crate) fn commit_id(&self) -> &str {
        &self.commit_id
    }

    pub(crate) fn is_detached(&self) -> bool {
        matches!(self.head_state, HeadState::Detached { .. })
    }

    pub(crate) fn branch_name(&self) -> Option<&str> {
        match &self.head_state {
            HeadState::Attached { reference } => {
                reference.strip_prefix("refs/heads/").or(Some(reference))
            }
            HeadState::Detached { .. } => None,
        }
    }

    pub(crate) fn file_count(&self) -> usize {
        self.file_count
    }

    pub(crate) fn removed_count(&self) -> usize {
        self.removed_count
    }
}

pub(crate) fn checkout_snapshot(
    repository: &Repository,
    target: &str,
    force: bool,
) -> Result<CheckoutOutcome> {
    let mut lock = repository.acquire_write_lock()?;
    checkout_snapshot_locked(repository, target, force, &mut lock)
}

fn checkout_snapshot_locked(
    repository: &Repository,
    target: &str,
    force: bool,
    lock: &mut RepositoryWriteLock,
) -> Result<CheckoutOutcome> {
    let original_head = repository.current_head()?;
    let head_id = original_head
        .commit_id()
        .context("仓库还没有 Commit；请先运行 `neoengram commit`")?;
    let current_index = repository.read_index()?;
    if !force {
        let head_commit = repository.read_commit(head_id)?;
        let index_tree = Tree {
            files: current_index.files.clone(),
        };
        ensure!(
            repository.tree_id(&index_tree)? == head_commit.tree_hash,
            "index 包含尚未提交的变更；使用 `--force` 丢弃这些变更"
        );
    }

    let (target_id, target_head) = if target.eq_ignore_ascii_case("HEAD") {
        (head_id.to_owned(), original_head.state().clone())
    } else if target == "main" || target == "refs/heads/main" {
        let branch_head = repository.default_branch_head()?;
        let branch_id = branch_head
            .commit_id()
            .context("分支 main 还没有 Commit；请先运行 `neoengram commit`")?;
        (branch_id.to_owned(), branch_head.state().clone())
    } else {
        ensure!(!target.is_empty(), "Checkout TARGET 不能为空");
        (
            target.to_owned(),
            HeadState::Detached {
                commit_id: target.to_owned(),
            },
        )
    };
    let target_commit = repository.read_commit(&target_id)?;
    let target_tree = repository.read_tree(&target_commit.tree_hash)?;

    let initial_plan = preflight_workspace(repository.root(), &current_index, &target_tree, force)?;

    // 只校验和组装实际需要写入的文件。未变化文件不会触碰工作区；需要对全部历史和
    // 不可达对象做完整检查时使用 `neoengram fsck`。
    let file_caches = prepare_file_caches(repository, &target_tree, &initial_plan.writes)?;

    let mut transaction = CheckoutTransaction::create(
        repository,
        &target_id,
        original_head.state(),
        head_id,
        &target_head,
        &current_index,
    )?;
    lock.set_operation("checkout", Some(transaction.id()))?;
    let checkout_result = apply_checkout_transaction(
        repository,
        &current_index,
        &target_tree,
        force,
        &initial_plan.writes,
        &file_caches,
        &mut transaction,
    );

    match checkout_result {
        Ok(removed_count) => {
            transaction.finish()?;
            lock.set_operation("checkout", None)?;
            Ok(CheckoutOutcome {
                commit_id: target_id,
                head_state: target_head,
                file_count: target_tree.files.len(),
                removed_count,
            })
        }
        Err(error) => handle_checkout_failure(repository, transaction, error),
    }
}

fn apply_checkout_transaction(
    repository: &Repository,
    current_index: &Index,
    target_tree: &Tree,
    force: bool,
    stage_paths: &BTreeSet<String>,
    file_caches: &BTreeMap<String, PathBuf>,
    transaction: &mut CheckoutTransaction,
) -> Result<usize> {
    stage_files(transaction, target_tree, file_caches, stage_paths)?;

    // 对象校验和缓存准备可能很久，因此在真正修改工作区前再次检查，避免静默覆盖
    // 用户在预检之后产生的修改。所有 ancestor symlink 都会被拒绝，leaf 也会在
    // rename 前再次检查。
    let final_plan = preflight_workspace(repository.root(), current_index, target_tree, force)?;
    ensure!(
        final_plan
            .writes
            .iter()
            .all(|path| stage_paths.contains(path)),
        "工作区在 Checkout 准备期间发生变化，请重试"
    );

    transaction.set_phase(CheckoutPhase::Applying)?;
    apply_workspace_changes(repository, target_tree, &final_plan, transaction)?;
    transaction.set_phase(CheckoutPhase::WorkspaceApplied)?;
    repository.write_index(&Index {
        format_version: INDEX_FORMAT_VERSION,
        files: target_tree.files.clone(),
    })?;
    super::test_crash_at("checkout-after-index");
    transaction.set_phase(CheckoutPhase::IndexUpdated)?;
    transaction.publish_head(repository)?;
    super::test_crash_at("checkout-after-head");
    transaction.set_phase(CheckoutPhase::HeadUpdated)?;
    Ok(final_plan.removals.len())
}

struct CheckoutPlan {
    writes: BTreeSet<String>,
    removals: Vec<FileNode>,
    displacements: BTreeSet<String>,
}

fn preflight_workspace(
    repository_root: &Path,
    current_index: &Index,
    target_tree: &Tree,
    force: bool,
) -> Result<CheckoutPlan> {
    let current_files: BTreeMap<&str, &FileNode> = current_index
        .files
        .iter()
        .map(|file| (file.path.as_str(), file))
        .collect();
    let target_paths: BTreeSet<&str> = target_tree
        .files
        .iter()
        .map(|file| file.path.as_str())
        .collect();

    let mut writes = BTreeSet::new();
    let mut displacements = BTreeSet::new();
    for target in &target_tree.files {
        let current = current_files.get(target.path.as_str()).copied();
        if force {
            if let Some(blocker) = forced_displacement(repository_root, &target.path)? {
                insert_displacement(&mut displacements, blocker);
                writes.insert(target.path.clone());
                continue;
            }
        }
        if check_target_conflict(repository_root, current, target, force)? {
            writes.insert(target.path.clone());
        }
    }

    let mut removals = Vec::new();
    for current in &current_index.files {
        if target_paths.contains(current.path.as_str()) {
            continue;
        }
        if displacements
            .iter()
            .any(|root| path_is_same_or_descendant(&current.path, root))
        {
            continue;
        }
        check_removal_conflict(repository_root, current, force)?;
        removals.push(current.clone());
    }

    Ok(CheckoutPlan {
        writes,
        removals,
        displacements,
    })
}

/// 找出 `--force` 下必须整体移开的文件/目录根。
///
/// 父级符号链接仍然拒绝：即使只 rename 链接本身，后续逐组件路径 API 也存在跟随链接
/// 的风险。普通文件父级和目录叶子则可以安全地整体移动到事务备份后完成类型转换。
fn forced_displacement(repository_root: &Path, logical_path: &str) -> Result<Option<String>> {
    let components: Vec<&str> = logical_path.split('/').collect();
    let mut current = repository_root.to_path_buf();
    let mut logical = Vec::new();

    for component in components.iter().take(components.len().saturating_sub(1)) {
        current.push(component);
        logical.push(*component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                bail!(
                    "Checkout 路径的父级不是普通目录（符号链接）: {}",
                    current.display()
                )
            }
            Ok(metadata) if metadata.is_dir() => {}
            Ok(metadata) if metadata.is_file() => return Ok(Some(logical.join("/"))),
            Ok(_) => bail!(
                "Checkout 路径的父级不是普通文件或目录: {}",
                current.display()
            ),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("无法检查工作区路径: {}", current.display()));
            }
        }
    }

    let destination = workspace_path(repository_root, logical_path);
    match fs::symlink_metadata(&destination) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
            Ok(Some(logical_path.to_owned()))
        }
        Ok(_) => Ok(None),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => {
            Err(error).with_context(|| format!("无法检查工作区路径: {}", destination.display()))
        }
    }
}

fn insert_displacement(displacements: &mut BTreeSet<String>, candidate: String) {
    if displacements
        .iter()
        .any(|root| path_is_same_or_descendant(&candidate, root))
    {
        return;
    }
    displacements.retain(|root| !path_is_same_or_descendant(root, &candidate));
    displacements.insert(candidate);
}

fn path_is_same_or_descendant(path: &str, root: &str) -> bool {
    path == root
        || path
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn check_removal_conflict(repository_root: &Path, current: &FileNode, force: bool) -> Result<()> {
    let destination = workspace_path(repository_root, &current.path);
    let metadata = safe_leaf_metadata(repository_root, &current.path)?;
    let Some(metadata) = metadata else {
        ensure!(
            force,
            "已跟踪文件缺失，拒绝覆盖本地删除: {}；使用 `--force`",
            current.path
        );
        return Ok(());
    };

    if metadata.file_type().is_symlink() {
        ensure!(
            force,
            "已跟踪路径变成了符号链接: {}；使用 `--force`",
            current.path
        );
        return Ok(());
    }
    if !metadata.is_file() {
        ensure!(
            force && metadata.is_dir(),
            "拒绝删除非普通文件路径，即使使用 --force: {}",
            current.path
        );
        return Ok(());
    }
    if !force {
        ensure!(
            file_matches_node(&destination, current)?,
            "已跟踪文件存在本地修改: {}；使用 `--force`",
            current.path
        );
    }
    Ok(())
}

fn check_target_conflict(
    repository_root: &Path,
    current: Option<&FileNode>,
    target: &FileNode,
    force: bool,
) -> Result<bool> {
    let destination = workspace_path(repository_root, &target.path);
    let metadata = safe_leaf_metadata(repository_root, &target.path)?;
    let Some(metadata) = metadata else {
        if current.is_some() {
            ensure!(
                force,
                "已跟踪文件缺失，拒绝覆盖本地删除: {}；使用 `--force`",
                target.path
            );
        }
        return Ok(true);
    };

    if metadata.file_type().is_symlink() {
        ensure!(
            force,
            "目标路径是符号链接: {}；使用 `--force` 只替换链接本身",
            target.path
        );
        return Ok(true);
    }
    ensure!(
        metadata.is_file(),
        "目标路径不是普通文件，拒绝递归覆盖: {}",
        target.path
    );

    if file_matches_node(&destination, target)? {
        return Ok(false);
    }
    if force {
        return Ok(true);
    }

    if let Some(current) = current {
        ensure!(
            file_matches_node(&destination, current)?,
            "已跟踪文件存在本地修改: {}；使用 `--force`",
            target.path
        );
    } else {
        bail!(
            "未跟踪文件会被 Checkout 覆盖: {}；使用 `--force`",
            target.path
        );
    }
    Ok(true)
}

fn safe_leaf_metadata(repository_root: &Path, logical_path: &str) -> Result<Option<fs::Metadata>> {
    let components: Vec<&str> = logical_path.split('/').collect();
    let mut current = repository_root.to_path_buf();
    for component in components.iter().take(components.len().saturating_sub(1)) {
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) => ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "Checkout 路径的父级不是普通目录: {}",
                current.display()
            ),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("无法检查工作区路径: {}", current.display()));
            }
        }
    }

    let destination = workspace_path(repository_root, logical_path);
    match fs::symlink_metadata(&destination) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => {
            Err(error).with_context(|| format!("无法检查工作区路径: {}", destination.display()))
        }
    }
}

fn prepare_file_caches(
    repository: &Repository,
    tree: &Tree,
    paths: &BTreeSet<String>,
) -> Result<BTreeMap<String, PathBuf>> {
    let mut cache_by_id: BTreeMap<String, PathBuf> = BTreeMap::new();
    let mut cache_by_path = BTreeMap::new();

    for file in &tree.files {
        if !paths.contains(&file.path) {
            continue;
        }
        let file_id = file_cache_id(file);
        let cache_path = if let Some(existing) = cache_by_id.get(&file_id) {
            existing.clone()
        } else {
            let cache = ensure_file_cache(repository, file, &file_id)?;
            cache_by_id.insert(file_id, cache.clone());
            cache
        };
        cache_by_path.insert(file.path.clone(), cache_path);
    }
    Ok(cache_by_path)
}

fn file_cache_id(file: &FileNode) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(FILE_HASH_DOMAIN);
    hasher.update(&[0]);
    hasher.update(&file.total_size.to_le_bytes());
    for chunk in &file.chunks {
        hasher.update(chunk.hash.as_bytes());
        hasher.update(&chunk.size.to_le_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

fn ensure_file_cache(repository: &Repository, file: &FileNode, file_id: &str) -> Result<PathBuf> {
    let cache_path = repository.files_dir().join(file_id);
    match fs::symlink_metadata(&cache_path) {
        Ok(metadata) => {
            ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "完整文件缓存不是普通文件: {}",
                cache_path.display()
            );
            if file_matches_node(&cache_path, file)? {
                return Ok(cache_path);
            }
            fs::remove_file(&cache_path)
                .with_context(|| format!("无法移除损坏的完整文件缓存: {}", cache_path.display()))?;
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("无法检查完整文件缓存: {}", cache_path.display()));
        }
    }

    let mut temporary =
        NamedTempFile::new_in(repository.files_dir()).context("无法创建完整文件缓存临时文件")?;
    for chunk in &file.chunks {
        repository.object_store().copy_to(
            &ObjectSpec::new(chunk.hash.clone(), chunk.size)?,
            temporary.as_file_mut(),
        )?;
    }
    temporary
        .as_file()
        .sync_all()
        .with_context(|| format!("无法同步完整文件缓存: {file_id}"))?;
    ensure!(
        temporary.as_file().metadata()?.len() == file.total_size,
        "完整文件缓存大小与 FileNode 不一致: {}",
        file.path
    );

    match temporary.persist_noclobber(&cache_path) {
        Ok(_) => Ok(cache_path),
        Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
            ensure!(
                file_matches_node(&cache_path, file)?,
                "并发创建的完整文件缓存已损坏: {}",
                cache_path.display()
            );
            Ok(cache_path)
        }
        Err(error) => Err(error.error)
            .with_context(|| format!("无法发布完整文件缓存: {}", cache_path.display())),
    }
}

fn file_matches_node(path: &Path, file_node: &FileNode) -> Result<bool> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error).with_context(|| format!("无法检查文件: {}", path.display()));
        }
    };
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() != file_node.total_size
    {
        return Ok(false);
    }

    let mut file = File::open(path).with_context(|| format!("无法打开文件: {}", path.display()))?;
    for chunk in &file_node.chunks {
        let Some(actual_hash) = read_exact_hash(&mut file, chunk.size)? else {
            return Ok(false);
        };
        if actual_hash != chunk.hash {
            return Ok(false);
        }
    }
    let mut trailing = [0_u8; 1];
    Ok(file.read(&mut trailing)? == 0)
}

fn read_exact_hash(reader: &mut File, size: u64) -> Result<Option<String>> {
    let mut remaining = size;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; IO_BUFFER_SIZE];

    while remaining > 0 {
        let limit =
            usize::try_from(remaining.min(IO_BUFFER_SIZE as u64)).context("读取大小超出 usize")?;
        let read = reader.read(&mut buffer[..limit])?;
        if read == 0 {
            return Ok(None);
        }
        hasher.update(&buffer[..read]);
        remaining -= u64::try_from(read).context("读取大小超出 u64")?;
    }
    Ok(Some(hasher.finalize().to_hex().to_string()))
}

fn stage_files(
    transaction: &CheckoutTransaction,
    tree: &Tree,
    cache_by_path: &BTreeMap<String, PathBuf>,
    paths: &BTreeSet<String>,
) -> Result<()> {
    for file in &tree.files {
        if !paths.contains(&file.path) {
            continue;
        }
        let cache_path = cache_by_path
            .get(&file.path)
            .with_context(|| format!("缺少完整文件缓存: {}", file.path))?;
        let staged_path = transaction.staged_path(&file.path);
        let parent = staged_path
            .parent()
            .with_context(|| format!("暂存路径没有父目录: {}", staged_path.display()))?;
        fs::create_dir_all(parent)
            .with_context(|| format!("无法创建 Checkout 暂存目录: {}", parent.display()))?;

        reflink_copy::reflink_or_copy(cache_path, &staged_path).with_context(|| {
            format!("无法通过 Reflink 或复制暂存文件 {}", staged_path.display())
        })?;
        let staged = File::options()
            .read(true)
            .write(true)
            .open(&staged_path)
            .with_context(|| format!("无法打开暂存文件: {}", staged_path.display()))?;
        ensure!(
            staged.metadata()?.len() == file.total_size,
            "暂存文件大小不一致: {}",
            file.path
        );
        staged
            .sync_all()
            .with_context(|| format!("无法同步暂存文件: {}", file.path))?;
    }
    Ok(())
}

fn apply_workspace_changes(
    repository: &Repository,
    target_tree: &Tree,
    plan: &CheckoutPlan,
    transaction: &mut CheckoutTransaction,
) -> Result<()> {
    // 类型转换的冲突根必须整体备份。例如当前是文件 `a`、目标是 `a/b`，或当前是目录
    // `a/`、目标是文件 `a`。先移动最短冲突根可同时保留其中未跟踪内容，并避免 backup
    // 目录内出现“父路径是文件”的冲突。
    for logical_path in &plan.displacements {
        let destination = workspace_path(repository.root(), logical_path);
        transaction.back_up(logical_path, &destination, true, None)?;
    }

    for file in &plan.removals {
        let destination = workspace_path(repository.root(), &file.path);
        match fs::symlink_metadata(&destination) {
            Ok(_) => transaction.back_up(&file.path, &destination, false, None)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("无法再次检查待删除路径: {}", destination.display()));
            }
        }
    }

    for file in &target_tree.files {
        if !plan.writes.contains(&file.path) {
            continue;
        }
        let destination = workspace_path(repository.root(), &file.path);
        transaction.ensure_worktree_parents(repository.root(), &file.path)?;
        transaction.back_up(&file.path, &destination, true, Some(file))?;
        let staged_path = transaction.staged_path(&file.path);
        rename_durable(&staged_path, &destination).with_context(|| {
            format!(
                "无法原子发布工作区文件 {} -> {}",
                staged_path.display(),
                destination.display()
            )
        })?;
        super::test_crash_at("checkout-after-publish");
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CheckoutPhase {
    Prepared,
    Applying,
    WorkspaceApplied,
    IndexUpdated,
    HeadUpdated,
    RolledBack,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct JournalChange {
    path: String,
    original_present: bool,
    /// 写操作记录目标文件 recipe；纯移除或冲突祖先备份为 `None`。
    expected_file: Option<FileNode>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CheckoutJournal {
    format_version: u32,
    target_id: String,
    head_before: HeadState,
    head_before_commit_id: String,
    head_after: HeadState,
    phase: CheckoutPhase,
    changes: Vec<JournalChange>,
    created_directories: Vec<String>,
}

struct CheckoutTransaction {
    root: PathBuf,
    journal: CheckoutJournal,
    original_index: Index,
}

impl CheckoutTransaction {
    fn create(
        repository: &Repository,
        target_id: &str,
        head_before: &HeadState,
        head_before_commit_id: &str,
        head_after: &HeadState,
        original_index: &Index,
    ) -> Result<Self> {
        let temporary = Builder::new()
            .prefix("checkout-")
            .tempdir_in(repository.transactions_dir())
            .context("无法创建 Checkout 事务目录")?;
        let root = temporary.path().to_path_buf();
        fs::create_dir(root.join("staged")).context("无法创建 Checkout staged 目录")?;
        fs::create_dir(root.join("backup")).context("无法创建 Checkout backup 目录")?;
        let journal = CheckoutJournal {
            format_version: JOURNAL_FORMAT_VERSION,
            target_id: target_id.to_owned(),
            head_before: head_before.clone(),
            head_before_commit_id: head_before_commit_id.to_owned(),
            head_after: head_after.clone(),
            phase: CheckoutPhase::Prepared,
            changes: Vec::new(),
            created_directories: Vec::new(),
        };
        write_transaction_marker(&root.join(OPERATION_FILE_NAME), b"checkout\n")?;
        write_json_atomic(&root.join(ORIGINAL_INDEX_FILE_NAME), original_index)?;
        write_json_atomic(&root.join(JOURNAL_FILE_NAME), &journal)?;
        sync_directory(&root)?;
        sync_directory(&repository.transactions_dir())?;
        let root = temporary.keep();
        Ok(Self {
            root,
            journal,
            original_index: original_index.clone(),
        })
    }

    fn load(repository: &Repository, root: PathBuf) -> Result<Self> {
        ensure!(
            root.parent() == Some(repository.transactions_dir().as_path()),
            "Checkout 事务不在当前仓库的 transactions 目录中: {}",
            root.display()
        );
        let metadata = fs::symlink_metadata(&root)
            .with_context(|| format!("无法检查 Checkout 事务目录: {}", root.display()))?;
        ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "Checkout 事务路径不是普通目录: {}",
            root.display()
        );
        let operation = fs::read_to_string(root.join(OPERATION_FILE_NAME))
            .context("无法读取 Checkout 事务类型")?;
        ensure!(operation.trim() == "checkout", "事务类型不是 checkout");
        let journal: CheckoutJournal = read_transaction_json(&root.join(JOURNAL_FILE_NAME))?;
        ensure!(
            journal.format_version == JOURNAL_FORMAT_VERSION,
            "不支持的 Checkout journal 版本 {}",
            journal.format_version
        );
        journal.head_before.validate()?;
        journal.head_after.validate()?;
        repository.validate_commit_id(&journal.target_id)?;
        repository.validate_commit_id(&journal.head_before_commit_id)?;
        if let HeadState::Detached { commit_id } = &journal.head_before {
            ensure!(
                commit_id == &journal.head_before_commit_id,
                "Checkout journal 的原 Detached HEAD 与原 Commit 不一致"
            );
        }
        if let HeadState::Detached { commit_id } = &journal.head_after {
            ensure!(
                commit_id == &journal.target_id,
                "Checkout journal 的目标 Detached HEAD 与目标 Commit 不一致"
            );
        }
        for change in &journal.changes {
            repository.validate_logical_path(&change.path)?;
            if let Some(file) = &change.expected_file {
                ensure!(
                    file.path == change.path,
                    "Checkout journal 的目标文件路径不匹配: {}",
                    change.path
                );
                repository.validate_file_snapshot(file)?;
            }
        }
        for path in &journal.created_directories {
            repository.validate_logical_path(path)?;
        }
        let original_index: Index = read_transaction_json(&root.join(ORIGINAL_INDEX_FILE_NAME))?;
        repository.validate_index_snapshot(&original_index)?;
        Ok(Self {
            root,
            journal,
            original_index,
        })
    }

    fn id(&self) -> &str {
        self.root
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("checkout-transaction")
    }

    fn persist_journal(&self) -> Result<()> {
        write_json_atomic(&self.root.join(JOURNAL_FILE_NAME), &self.journal)
    }

    fn set_phase(&mut self, phase: CheckoutPhase) -> Result<()> {
        self.journal.phase = phase;
        self.persist_journal()
    }

    fn reset_for_replay(&mut self) -> Result<()> {
        reset_transaction_subdirectory(&self.root.join("staged"))?;
        reset_transaction_subdirectory(&self.root.join("backup"))?;
        self.journal.phase = CheckoutPhase::Prepared;
        self.journal.changes.clear();
        self.journal.created_directories.clear();
        self.persist_journal()
    }

    fn publish_head(&self, repository: &Repository) -> Result<()> {
        let before = repository.resolve_head_state(self.journal.head_before.clone())?;
        ensure!(
            before.commit_id() == Some(self.journal.head_before_commit_id.as_str()),
            "HEAD 的原分支在 Checkout 期间发生变化（期望 {}，实际 {:?}）",
            self.journal.head_before_commit_id,
            before.commit_id()
        );
        let after = repository.resolve_head_state(self.journal.head_after.clone())?;
        ensure!(
            after.commit_id() == Some(self.journal.target_id.as_str()),
            "Checkout 目标 HEAD 不再指向目标 Commit（期望 {}，实际 {:?}）",
            self.journal.target_id,
            after.commit_id()
        );
        repository.transition_head(&self.journal.head_before, &self.journal.head_after)
    }

    fn staged_path(&self, logical_path: &str) -> PathBuf {
        logical_join(&self.root.join("staged"), logical_path)
    }

    fn backup_path(&self, logical_path: &str) -> PathBuf {
        logical_join(&self.root.join("backup"), logical_path)
    }

    fn back_up(
        &mut self,
        logical_path: &str,
        destination: &Path,
        record_absence: bool,
        expected_file: Option<&FileNode>,
    ) -> Result<()> {
        if let Some(existing) = self
            .journal
            .changes
            .iter_mut()
            .find(|change| change.path == logical_path)
        {
            if let Some(file) = expected_file {
                existing.expected_file = Some(file.clone());
                self.persist_journal()?;
            }
            return Ok(());
        }
        let metadata = match fs::symlink_metadata(destination) {
            Ok(metadata) => Some(metadata),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("无法检查待备份路径: {}", destination.display()));
            }
        };

        let original_present = metadata.is_some();
        if original_present || record_absence {
            // 持久化意图必须先于 workspace rename。恢复时 backup 是否存在即可区分 rename
            // 是否已经发生，而无需依赖最后一次内存状态。
            self.journal.changes.push(JournalChange {
                path: logical_path.to_owned(),
                original_present,
                expected_file: expected_file.cloned(),
            });
            self.persist_journal()?;
            super::test_crash_at("checkout-after-intent");
        }

        if let Some(metadata) = metadata {
            ensure!(
                metadata.is_file() || metadata.is_dir() || metadata.file_type().is_symlink(),
                "拒绝备份并覆盖特殊文件路径: {}",
                destination.display()
            );
            let backup = self.backup_path(logical_path);
            let parent = backup
                .parent()
                .with_context(|| format!("备份路径没有父目录: {}", backup.display()))?;
            fs::create_dir_all(parent)
                .with_context(|| format!("无法创建 Checkout 备份目录: {}", parent.display()))?;
            sync_directory(parent)?;
            rename_durable(destination, &backup)
                .with_context(|| format!("无法备份工作区路径: {}", destination.display()))?;
        }
        Ok(())
    }

    fn ensure_worktree_parents(
        &mut self,
        repository_root: &Path,
        logical_path: &str,
    ) -> Result<()> {
        let components: Vec<&str> = logical_path.split('/').collect();
        let mut current = repository_root.to_path_buf();
        let mut logical_components = Vec::new();
        for component in components.iter().take(components.len().saturating_sub(1)) {
            current.push(component);
            logical_components.push(*component);
            match fs::symlink_metadata(&current) {
                Ok(metadata) => ensure!(
                    metadata.is_dir() && !metadata.file_type().is_symlink(),
                    "Checkout 路径的父级不是普通目录: {}",
                    current.display()
                ),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    self.journal
                        .created_directories
                        .push(logical_components.join("/"));
                    self.persist_journal()?;
                    fs::create_dir(&current)
                        .with_context(|| format!("无法创建工作区目录: {}", current.display()))?;
                    sync_parent(&current)?;
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("无法检查工作区目录: {}", current.display()));
                }
            }
        }
        Ok(())
    }

    fn rollback(&mut self, repository: &Repository) -> Result<()> {
        repository.transition_head(&self.journal.head_after, &self.journal.head_before)?;
        let restored_head = repository.resolve_head_state(self.journal.head_before.clone())?;
        ensure!(
            restored_head.commit_id() == Some(self.journal.head_before_commit_id.as_str()),
            "回滚后的 HEAD 不再指向事务开始时的 Commit（期望 {}，实际 {:?}）",
            self.journal.head_before_commit_id,
            restored_head.commit_id()
        );
        for change in self.journal.changes.iter().rev() {
            let destination = workspace_path(repository.root(), &change.path);
            let backup = self.backup_path(&change.path);
            let staged = self.staged_path(&change.path);
            let backup_exists = match fs::symlink_metadata(&backup) {
                Ok(_) => true,
                Err(error) if error.kind() == io::ErrorKind::NotFound => false,
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("无法检查事务备份: {}", backup.display()));
                }
            };

            if backup_exists {
                remove_destination_before_restore(
                    &destination,
                    &staged,
                    change.expected_file.as_ref(),
                )?;
                let parent = destination
                    .parent()
                    .with_context(|| format!("恢复目标没有父目录: {}", destination.display()))?;
                fs::create_dir_all(parent)
                    .with_context(|| format!("无法创建恢复目标父目录: {}", parent.display()))?;
                rename_durable(&backup, &destination)
                    .with_context(|| format!("回滚时无法恢复工作区路径: {}", change.path))?;
            } else if !change.original_present {
                remove_published_absent_destination(
                    &destination,
                    &staged,
                    change.expected_file.as_ref(),
                )?;
            }
        }

        for logical_path in self.journal.created_directories.iter().rev() {
            let directory = workspace_path(repository.root(), logical_path);
            match fs::symlink_metadata(&directory) {
                Ok(metadata) if !metadata.is_dir() || metadata.file_type().is_symlink() => {}
                Ok(_) => match fs::remove_dir(&directory) {
                    Ok(()) => sync_parent(&directory)?,
                    Err(error) if error.kind() == io::ErrorKind::DirectoryNotEmpty => {}
                    Err(error) => {
                        return Err(error).with_context(|| {
                            format!("回滚时无法移除目录: {}", directory.display())
                        });
                    }
                },
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("回滚时无法检查目录: {}", directory.display()));
                }
            }
        }
        repository.write_index(&self.original_index)?;
        self.journal.phase = CheckoutPhase::RolledBack;
        self.persist_journal()
    }

    fn finish(self) -> Result<()> {
        let parent = self
            .root
            .parent()
            .map(Path::to_path_buf)
            .with_context(|| format!("Checkout 事务目录没有父目录: {}", self.root.display()))?;
        fs::remove_dir_all(&self.root).with_context(|| {
            format!(
                "Checkout 已完成，但无法清理事务目录: {}",
                self.root.display()
            )
        })?;
        sync_directory(&parent)
    }
}

fn remove_destination_before_restore(
    destination: &Path,
    staged: &Path,
    expected_file: Option<&FileNode>,
) -> Result<()> {
    let metadata = match fs::symlink_metadata(destination) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("回滚时无法检查: {}", destination.display()));
        }
    };

    if let Some(file) = expected_file {
        ensure!(
            !staged.exists(),
            "Checkout 发布前目标路径被外部创建，拒绝覆盖: {}",
            destination.display()
        );
        ensure!(
            metadata.is_file()
                && !metadata.file_type().is_symlink()
                && file_matches_node(destination, file)?,
            "Checkout 后目标文件被外部修改，拒绝自动回滚: {}",
            destination.display()
        );
        fs::remove_file(destination)
            .with_context(|| format!("回滚时无法移除文件: {}", destination.display()))?;
        return sync_parent(destination);
    }

    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "Checkout 事务路径出现未知内容，拒绝自动回滚: {}",
        destination.display()
    );
    fs::remove_dir(destination).with_context(|| {
        format!(
            "Checkout 冲突目录包含事务未记录的内容，拒绝递归删除: {}",
            destination.display()
        )
    })?;
    sync_parent(destination)
}

fn remove_published_absent_destination(
    destination: &Path,
    staged: &Path,
    expected_file: Option<&FileNode>,
) -> Result<()> {
    // staged 仍存在说明 publish rename 尚未执行；此时同名工作区路径不属于事务，必须保留。
    if fs::symlink_metadata(staged).is_ok() {
        return Ok(());
    }
    let Some(expected_file) = expected_file else {
        return Ok(());
    };
    match fs::symlink_metadata(destination) {
        Ok(metadata) => {
            ensure!(
                metadata.is_file()
                    && !metadata.file_type().is_symlink()
                    && file_matches_node(destination, expected_file)?,
                "Checkout 后目标文件被外部修改，拒绝自动回滚: {}",
                destination.display()
            );
            fs::remove_file(destination)
                .with_context(|| format!("回滚时无法移除已发布文件: {}", destination.display()))?;
            sync_parent(destination)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error)
            .with_context(|| format!("回滚时无法检查已发布文件: {}", destination.display())),
    }
}

fn handle_checkout_failure(
    repository: &Repository,
    mut transaction: CheckoutTransaction,
    error: anyhow::Error,
) -> Result<CheckoutOutcome> {
    if let Err(rollback_error) = transaction.rollback(repository) {
        return Err(error.context(format!(
            "Checkout 回滚失败: {rollback_error:#}；备份保留在 {}",
            transaction.root.display()
        )));
    }
    if let Err(cleanup_error) = transaction.finish() {
        return Err(error.context(format!("Checkout 回滚后清理失败: {cleanup_error:#}")));
    }
    Err(error)
}

pub(crate) struct RecoveryOutcome {
    target_id: String,
    aborted: bool,
}

impl RecoveryOutcome {
    pub(super) fn target_id(&self) -> &str {
        &self.target_id
    }

    pub(super) fn aborted(&self) -> bool {
        self.aborted
    }
}

/// 恢复指定的未完成 Checkout 事务。
///
/// 完成模式采用“回滚后重放”：先按 journal 幂等恢复原工作区和 index，再复用同一个
/// 持久事务强制执行目标 Commit。旧 journal 在成功前始终存在，因此恢复过程再次断电
/// 也不会出现没有 journal 的空窗。
pub(super) fn recover_transaction(
    repository: &Repository,
    transaction_root: &Path,
    abort: bool,
    lock: &mut RepositoryWriteLock,
) -> Result<RecoveryOutcome> {
    let mut transaction = CheckoutTransaction::load(repository, transaction_root.to_path_buf())?;
    let target_id = transaction.journal.target_id.clone();
    lock.set_operation("checkout-recover", Some(transaction.id()))?;

    // 完成恢复前先准备目标 payload；失败时不触碰当前半完成事务，用户仍可 --abort。
    let prepared_target = if abort {
        None
    } else {
        let commit = repository.read_commit(&target_id)?;
        let tree = repository.read_tree(&commit.tree_hash)?;
        let current_index = transaction.original_index.clone();
        let all_paths: BTreeSet<String> = tree.files.iter().map(|file| file.path.clone()).collect();
        let caches = prepare_file_caches(repository, &tree, &all_paths)?;
        Some((tree, current_index, caches))
    };
    transaction.rollback(repository)?;

    if abort {
        transaction.finish()?;
        lock.set_operation("checkout-recover", None)?;
        return Ok(RecoveryOutcome {
            target_id,
            aborted: true,
        });
    }

    let (target_tree, current_index, caches) =
        prepared_target.context("内部错误：缺少 Checkout 恢复目标")?;
    let replay_plan = preflight_workspace(repository.root(), &current_index, &target_tree, true)?;
    transaction.reset_for_replay()?;
    let replay = apply_checkout_transaction(
        repository,
        &current_index,
        &target_tree,
        true,
        &replay_plan.writes,
        &caches,
        &mut transaction,
    );
    match replay {
        Ok(_) => {
            transaction.finish()?;
            lock.set_operation("checkout-recover", None)?;
            Ok(RecoveryOutcome {
                target_id,
                aborted: false,
            })
        }
        Err(error) => {
            if let Err(rollback_error) = transaction.rollback(repository) {
                return Err(error.context(format!(
                    "Checkout 恢复重放失败，且回滚失败: {rollback_error:#}；事务保留在 {}",
                    transaction.root.display()
                )));
            }
            transaction.finish()?;
            Err(error.context("Checkout 恢复重放失败，已恢复到原工作区"))
        }
    }
}

fn reset_transaction_subdirectory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
            fs::remove_dir_all(path)
                .with_context(|| format!("无法清理 Checkout 事务子目录: {}", path.display()))?;
        }
        Ok(_) => bail!("Checkout 事务子路径不是普通目录: {}", path.display()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("无法检查 Checkout 事务子目录: {}", path.display()));
        }
    }
    fs::create_dir(path)
        .with_context(|| format!("无法重建 Checkout 事务子目录: {}", path.display()))?;
    sync_parent(path)
}

fn read_transaction_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let bytes = fs::read(path)
        .with_context(|| format!("无法读取 Checkout 事务 JSON: {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("Checkout 事务 JSON 格式无效: {}", path.display()))
}

fn write_transaction_marker(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = File::create(path)
        .with_context(|| format!("无法创建 Checkout 事务标记: {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("无法写入 Checkout 事务标记: {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("无法同步 Checkout 事务标记: {}", path.display()))?;
    sync_parent(path)
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
