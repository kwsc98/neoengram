use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{ensure, Context, Result};
use neoengram_core::{LogicalPath, ManifestId};
use neoengram_engine::{MutationAction, ProgressSink};
use serde::{Deserialize, Serialize};
use tempfile::{Builder, NamedTempFile};

use crate::local::{
    engine_mutation,
    fs::durable::{
        create_dir_all_durable, publish_transaction_draft, remove_dir_all_durable,
        rename_noreplace_durable, sync_directory, sync_parent, write_bytes_atomic,
        write_json_atomic, RESTORE_TRANSACTION_DRAFT_PREFIX,
    },
    metadata::describe_manifest,
    model::FileNode,
    objects::ObjectSpec,
    repository::Repository,
};

use super::workspace::{file_matches_node, logical_join, safe_leaf_metadata, workspace_path};

const JOURNAL_FORMAT_VERSION: u32 = 1;
const OPERATION_FILE_NAME: &str = "OPERATION";
const JOURNAL_FILE_NAME: &str = "journal.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RestoreRecovery {
    Completed,
    RolledBack,
}

pub(crate) fn restore_worktree_paths(
    repository: &Repository,
    scopes: &[String],
    force: bool,
    progress: &dyn ProgressSink,
) -> Result<Vec<String>> {
    let versioned_index = repository.read_index_versioned()?;
    let (index, expected_index_version) = versioned_index.into_parts();
    let files: Vec<FileNode> = index
        .files
        .into_iter()
        .filter(|file| {
            scopes
                .iter()
                .any(|scope| path_is_in_scope(&file.path, scope))
        })
        .collect();
    ensure!(!files.is_empty(), "指定路径没有已暂存文件");

    let mut writes = Vec::new();
    for file in &files {
        let destination = workspace_path(repository.root(), &file.path);
        let metadata = safe_leaf_metadata(repository.root(), &file.path)?;
        if let Some(metadata) = metadata {
            ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "restore 只覆盖普通文件，路径类型异常: {}",
                file.path
            );
            if file_matches_node(&destination, file)? {
                continue;
            }
            ensure!(
                force,
                "工作区文件包含未暂存修改: {}；使用 `--force`",
                file.path
            );
        }
        writes.push(file.clone());
    }

    let restored_paths = files.into_iter().map(|file| file.path).collect();
    if writes.is_empty() {
        return Ok(restored_paths);
    }

    let mut transaction = RestoreTransaction::create(repository, &writes)?;
    transaction.stage(repository)?;
    transaction.publish()?;
    let plan_id = transaction.id().to_owned();
    let actions = writes
        .iter()
        .map(|file| {
            Ok(MutationAction::WriteFile {
                path: LogicalPath::parse(&file.path)?,
                manifest_id: describe_manifest(file.total_size, file.chunking, &file.chunks)?
                    .id
                    .parse::<ManifestId>()?,
                total_size: file.total_size,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let plan = engine_mutation::build_plan(
        repository,
        format!("restore-{plan_id}"),
        expected_index_version,
        actions,
    )?;
    let journal_directory = transaction.engine_journal_directory();
    let mut executed = engine_mutation::execute(
        repository,
        plan,
        &journal_directory,
        format!("restore:{plan_id}").into_bytes(),
        progress,
        move |_| {
            let mut transaction = transaction;
            match transaction.apply(repository, force) {
                Ok(()) => Ok(transaction),
                Err(error) => Err(rollback_restore_error(repository, transaction, error)),
            }
        },
    )?;
    let transaction = executed.take_value();
    executed.finalize().with_context(|| {
        format!(
            "restore 已发布，但 Engine journal finalize 失败；请运行 recover: {}",
            transaction.root.display()
        )
    })?;
    transaction.finish()?;
    Ok(restored_paths)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RestorePhase {
    Prepared,
    Applying,
    Applied,
    RolledBack,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RestoreEntry {
    file: FileNode,
    original_present: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RestoreJournal {
    format_version: u32,
    phase: RestorePhase,
    entries: Vec<RestoreEntry>,
    created_directories: Vec<String>,
}

struct RestoreTransaction {
    root: PathBuf,
    published: bool,
    journal: RestoreJournal,
}

impl RestoreTransaction {
    fn create(repository: &Repository, files: &[FileNode]) -> Result<Self> {
        let temporary = Builder::new()
            .prefix(RESTORE_TRANSACTION_DRAFT_PREFIX)
            .tempdir_in(repository.transactions_dir())
            .context("无法创建 restore 事务目录")?;
        let root = temporary.path().to_path_buf();
        fs::create_dir(root.join("staged")).context("无法创建 restore staged 目录")?;
        fs::create_dir(root.join("backup")).context("无法创建 restore backup 目录")?;
        let journal = RestoreJournal {
            format_version: JOURNAL_FORMAT_VERSION,
            phase: RestorePhase::Prepared,
            entries: files
                .iter()
                .cloned()
                .map(|file| RestoreEntry {
                    file,
                    original_present: None,
                })
                .collect(),
            created_directories: Vec::new(),
        };
        write_bytes_atomic(&root.join(OPERATION_FILE_NAME), b"restore\n")?;
        write_json_atomic(&root.join(JOURNAL_FILE_NAME), &journal)?;
        sync_directory(&root.join("staged"))?;
        sync_directory(&root.join("backup"))?;
        sync_directory(&root)?;
        let root = temporary.keep();
        Ok(Self {
            root,
            published: false,
            journal,
        })
    }

    fn load(repository: &Repository, root: PathBuf) -> Result<Self> {
        ensure!(
            root.parent() == Some(repository.transactions_dir().as_path()),
            "restore 事务不在当前仓库 transactions 目录中: {}",
            root.display()
        );
        let metadata = fs::symlink_metadata(&root)
            .with_context(|| format!("无法检查 restore 事务目录: {}", root.display()))?;
        ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "restore 事务路径不是普通目录: {}",
            root.display()
        );
        let operation = fs::read_to_string(root.join(OPERATION_FILE_NAME))
            .context("无法读取 restore 事务类型")?;
        ensure!(operation.trim() == "restore", "事务类型不是 restore");
        let bytes = fs::read(root.join(JOURNAL_FILE_NAME)).context("无法读取 restore journal")?;
        let journal: RestoreJournal =
            serde_json::from_slice(&bytes).context("restore journal JSON 无效")?;
        ensure!(
            journal.format_version == JOURNAL_FORMAT_VERSION,
            "不支持的 restore journal 版本 {}",
            journal.format_version
        );
        for entry in &journal.entries {
            repository.validate_file_snapshot(&entry.file)?;
        }
        for path in &journal.created_directories {
            repository.validate_logical_path(path)?;
        }
        Ok(Self {
            root,
            published: true,
            journal,
        })
    }

    fn id(&self) -> &str {
        self.root
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("restore-transaction")
    }

    fn engine_journal_directory(&self) -> PathBuf {
        self.root.join("engine-journal")
    }

    fn staged_path(&self, path: &str) -> PathBuf {
        logical_join(&self.root.join("staged"), path)
    }

    fn backup_path(&self, path: &str) -> PathBuf {
        logical_join(&self.root.join("backup"), path)
    }

    fn persist(&self) -> Result<()> {
        write_json_atomic(&self.root.join(JOURNAL_FILE_NAME), &self.journal)
    }

    fn set_phase(&mut self, phase: RestorePhase) -> Result<()> {
        self.journal.phase = phase;
        self.persist()
    }

    fn stage(&self, repository: &Repository) -> Result<()> {
        for entry in &self.journal.entries {
            let staged = self.staged_path(&entry.file.path);
            let parent = staged.parent().context("restore staged 路径没有父目录")?;
            create_dir_all_durable(parent)?;
            let mut temporary = NamedTempFile::new_in(parent)
                .with_context(|| format!("无法创建 restore staged 文件: {}", staged.display()))?;
            let mut written = 0_u64;
            for chunk in &entry.file.chunks {
                repository.object_store().copy_to(
                    &ObjectSpec::new(chunk.hash.clone(), chunk.size)?,
                    temporary.as_file_mut(),
                )?;
                written = written
                    .checked_add(chunk.size)
                    .context("restore 文件大小溢出")?;
            }
            ensure!(
                written == entry.file.total_size,
                "restore 文件大小与 index 不一致: {}",
                entry.file.path
            );
            temporary.as_file_mut().flush()?;
            temporary.as_file().sync_all()?;
            temporary
                .persist_noclobber(&staged)
                .map_err(|error| error.error)?;
            sync_directory(parent)?;
        }
        Ok(())
    }

    fn publish(&mut self) -> Result<()> {
        if self.published {
            return Ok(());
        }
        sync_directory(&self.root.join("staged"))?;
        sync_directory(&self.root.join("backup"))?;
        sync_directory(&self.root)?;
        let draft = self.root.clone();
        self.root = publish_transaction_draft(&draft, "restore")?;
        self.published = true;
        Ok(())
    }

    fn apply(&mut self, repository: &Repository, force: bool) -> Result<()> {
        self.set_phase(RestorePhase::Applying)?;
        for index in 0..self.journal.entries.len() {
            let file = self.journal.entries[index].file.clone();
            let destination = workspace_path(repository.root(), &file.path);
            super::test_pause_at("restore-before-publish");
            let metadata = safe_leaf_metadata(repository.root(), &file.path)?;
            if let Some(metadata) = &metadata {
                ensure!(
                    metadata.is_file() && !metadata.file_type().is_symlink(),
                    "restore 目标不是普通文件: {}",
                    file.path
                );
                if file_matches_node(&destination, &file)? {
                    continue;
                }
                ensure!(
                    force,
                    "工作区文件包含未暂存修改: {}；使用 `--force`",
                    file.path
                );
            }

            self.journal.entries[index].original_present = Some(metadata.is_some());
            self.persist()?;
            if metadata.is_some() {
                let backup = self.backup_path(&file.path);
                let parent = backup.parent().context("restore backup 路径没有父目录")?;
                create_dir_all_durable(parent)?;
                rename_noreplace_durable(&destination, &backup)?;
            }
            self.ensure_parents(repository.root(), &file.path)?;
            let staged = self.staged_path(&file.path);
            super::test_pause_at("restore-before-noreplace");
            rename_noreplace_durable(&staged, &destination).with_context(|| {
                format!(
                    "restore 目标在发布前被创建，拒绝覆盖: {}",
                    destination.display()
                )
            })?;
        }
        self.set_phase(RestorePhase::Applied)
    }

    fn ensure_parents(&mut self, root: &Path, logical_path: &str) -> Result<()> {
        let components: Vec<&str> = logical_path.split('/').collect();
        let mut current = root.to_path_buf();
        let mut logical = Vec::new();
        for component in components.iter().take(components.len().saturating_sub(1)) {
            current.push(component);
            logical.push(*component);
            match fs::symlink_metadata(&current) {
                Ok(metadata) => ensure!(
                    metadata.is_dir() && !metadata.file_type().is_symlink(),
                    "restore 路径父级不是普通目录: {}",
                    current.display()
                ),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    self.journal.created_directories.push(logical.join("/"));
                    self.persist()?;
                    create_dir_all_durable(&current)?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    fn rollback(&mut self, repository: &Repository) -> Result<()> {
        for entry in self.journal.entries.iter().rev() {
            let staged = self.staged_path(&entry.file.path);
            let backup = self.backup_path(&entry.file.path);
            let destination = workspace_path(repository.root(), &entry.file.path);
            let staged_exists = entry_exists(&staged)?;
            let backup_exists = entry_exists(&backup)?;
            match entry.original_present {
                None => {}
                Some(true) => {
                    if backup_exists {
                        remove_published_target(&destination, &entry.file, staged_exists)?;
                        ensure_restore_parents(repository.root(), &entry.file.path)?;
                        rename_noreplace_durable(&backup, &destination)?;
                    } else {
                        ensure!(
                            staged_exists,
                            "restore 原文件和备份同时缺失: {}",
                            entry.file.path
                        );
                    }
                }
                Some(false) => {
                    ensure!(
                        !backup_exists,
                        "restore 不应存在原文件备份: {}",
                        entry.file.path
                    );
                    if !staged_exists {
                        remove_published_target(&destination, &entry.file, false)?;
                    }
                }
            }
        }
        for logical in self.journal.created_directories.iter().rev() {
            let directory = workspace_path(repository.root(), logical);
            match fs::remove_dir(&directory) {
                Ok(()) => sync_parent(&directory)?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => {}
                Err(error) => return Err(error.into()),
            }
        }
        self.set_phase(RestorePhase::RolledBack)
    }

    fn finish(self) -> Result<()> {
        remove_dir_all_durable(&self.root)
            .with_context(|| format!("无法清理 restore 事务: {}", self.root.display()))
    }
}

pub(super) fn recover_transaction(
    repository: &Repository,
    transaction_root: &Path,
    abort: bool,
) -> Result<RestoreRecovery> {
    let mut transaction = RestoreTransaction::load(repository, transaction_root.to_path_buf())?;
    if !abort && transaction.journal.phase == RestorePhase::Applied {
        for entry in &transaction.journal.entries {
            let destination = workspace_path(repository.root(), &entry.file.path);
            ensure!(
                file_matches_node(&destination, &entry.file)?,
                "restore 已发布文件后来被修改，拒绝完成恢复: {}",
                entry.file.path
            );
        }
        transaction.finish()?;
        return Ok(RestoreRecovery::Completed);
    }
    transaction.rollback(repository)?;
    transaction.finish()?;
    Ok(RestoreRecovery::RolledBack)
}

fn rollback_restore_error(
    repository: &Repository,
    mut transaction: RestoreTransaction,
    error: anyhow::Error,
) -> anyhow::Error {
    if let Err(rollback_error) = transaction.rollback(repository) {
        return error.context(format!(
            "restore 回滚失败: {rollback_error:#}；事务保留在 {}",
            transaction.root.display()
        ));
    }
    if let Err(cleanup_error) = transaction.finish() {
        return error.context(format!("restore 回滚后清理失败: {cleanup_error:#}"));
    }
    error
}

fn remove_published_target(destination: &Path, file: &FileNode, staged_exists: bool) -> Result<()> {
    if staged_exists {
        return Ok(());
    }
    match fs::symlink_metadata(destination) {
        Ok(metadata) => {
            ensure!(
                metadata.is_file()
                    && !metadata.file_type().is_symlink()
                    && file_matches_node(destination, file)?,
                "restore 后目标文件被外部修改，拒绝自动回滚: {}",
                destination.display()
            );
            fs::remove_file(destination)?;
            sync_parent(destination)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn entry_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotADirectory => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn ensure_restore_parents(root: &Path, logical_path: &str) -> Result<()> {
    let components: Vec<&str> = logical_path.split('/').collect();
    let mut current = root.to_path_buf();
    for component in components.iter().take(components.len().saturating_sub(1)) {
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) => ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "restore 恢复路径父级不是普通目录: {}",
                current.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                create_dir_all_durable(&current)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn path_is_in_scope(path: &str, scope: &str) -> bool {
    scope.is_empty()
        || path == scope
        || path
            .strip_prefix(scope)
            .is_some_and(|suffix| suffix.starts_with('/'))
}
